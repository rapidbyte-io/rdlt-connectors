//! Credential attachment + lifecycle. Schemes: none, bearer, arbitrary
//! header, basic, api_key (header|query), OAuth2 client-credentials (lazy
//! single-flight token cache, expiry margin, ONE 401 re-fetch then fatal —
//! a post-refresh 401 means wrong credentials, never a retry loop).
//! Secrets attach via [`Secret::reveal`] at the request only.

use std::time::{Duration, Instant};

use rdlt_connector_sdk::spi::{Secret, SourceError};

use crate::source::config::{Auth, KeyLocation};

/// Runtime credential state built from the config [`Auth`] scheme.
#[derive(Debug)]
pub struct Credentials {
    scheme: Auth,
    /// The SAME client the data path uses, so the token fetch inherits its
    /// read deadline. A separate client would leave the credential fetch as
    /// the one request in the source that can stall forever.
    http: reqwest::Client,
    /// OAuth2 token cache: single-flight via the async mutex.
    token: tokio::sync::Mutex<TokenState>,
}

/// The cached credential and the generation it belongs to.
///
/// The generation is what makes a 401 discriminating. Under fan-out several
/// requests hold the same credential; the first 401 refreshes it, and the
/// others then arrive reporting a 401 for a credential that has ALREADY
/// been replaced. Without a generation each of those drops the fresh token
/// and forces another round trip — bounded, but pure waste, and it briefly
/// leaves the source with no credential at all.
#[derive(Debug, Default)]
struct TokenState {
    generation: u64,
    cached: Option<CachedToken>,
}

#[derive(Debug, Clone)]
struct CachedToken {
    access_token: Secret,
    expires_at: Option<Instant>,
}

impl Credentials {
    pub fn new(scheme: Auth, http: reqwest::Client) -> Self {
        Self {
            scheme,
            http,
            token: tokio::sync::Mutex::new(TokenState::default()),
        }
    }

    /// Attach credentials to a request (fetching/refreshing OAuth2 tokens
    /// as needed).
    ///
    /// Returns the request and, for a refreshable scheme, the GENERATION of
    /// the credential it carries — which [`Self::on_unauthorized`] needs to
    /// tell "the token I used was rejected" from "someone already replaced
    /// it".
    pub async fn attach(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<(reqwest::RequestBuilder, Option<u64>), SourceError> {
        match &self.scheme {
            Auth::None => Ok((request, None)),
            Auth::Bearer { token } => Ok((request.bearer_auth(token.reveal()), None)),
            Auth::Header { name, value } => Ok((request.header(name, value.reveal()), None)),
            Auth::Basic { username, password } => {
                Ok((request.basic_auth(username, Some(password.reveal())), None))
            }
            Auth::ApiKey {
                name,
                key,
                location,
            } => Ok((
                match location {
                    KeyLocation::Header => request.header(name, key.reveal()),
                    KeyLocation::Query => request.query(&[(name, key.reveal())]),
                },
                None,
            )),
            Auth::OAuth2ClientCredentials { .. } => {
                let (token, generation) = self.current_token().await?;
                Ok((request.bearer_auth(token.reveal()), Some(generation)))
            }
        }
    }

    /// 401 hook: refreshable schemes drop their cache and report "retry
    /// once"; everything else reports "no, the 401 is final".
    ///
    /// `failed` names the generation the rejected request carried. If the
    /// cache has already moved past it, another request refreshed in the
    /// meantime and the cached credential is not the one that was rejected —
    /// so it stays, and this request simply retries with it.
    pub async fn on_unauthorized(&self, failed: Option<u64>) -> Result<bool, SourceError> {
        if !matches!(&self.scheme, Auth::OAuth2ClientCredentials { .. }) {
            return Ok(false);
        }
        let mut guard = self.token.lock().await;
        match failed {
            Some(generation) if generation != guard.generation => Ok(true),
            _ if guard.cached.is_none() => Ok(false),
            _ => {
                guard.cached = None;
                guard.generation = guard.generation.wrapping_add(1);
                Ok(true)
            }
        }
    }

    async fn current_token(&self) -> Result<(Secret, u64), SourceError> {
        let Auth::OAuth2ClientCredentials {
            token_url,
            client_id,
            client_secret,
            scopes,
            audience,
            expiry_margin_secs,
        } = &self.scheme
        else {
            unreachable!("current_token is only called for oauth2");
        };
        let mut guard = self.token.lock().await;
        if let Some(cached) = guard.cached.as_ref()
            && cached.expires_at.is_none_or(|at| Instant::now() < at)
        {
            return Ok((cached.access_token.clone(), guard.generation));
        }
        // Single-flight by construction: the mutex is held across the fetch.
        let mut form: Vec<(&str, String)> = vec![
            ("grant_type", "client_credentials".into()),
            ("client_id", client_id.clone()),
            ("client_secret", client_secret.reveal().to_owned()),
        ];
        if !scopes.is_empty() {
            form.push(("scope", scopes.join(" ")));
        }
        if let Some(audience) = audience {
            form.push(("audience", audience.clone()));
        }
        // The token endpoint does NOT go through this source's `Client`, so
        // it gets no pacing and no default headers — but it shares the same
        // underlying reqwest client, so it shares the read deadline, and it
        // reuses the shared error classification: 5xx/network = transient
        // (engine budget), 4xx = fatal.
        let response = self
            .http
            .post(token_url)
            .form(&form)
            .send()
            .await
            .map_err(super::classify::transport)?;
        let status = response.status();
        if !status.is_success() {
            // The shared classification's three-way split, with the token
            // endpoint named: a 429 here is the issuer pacing its clients —
            // rate-limited (engine backoff), NEVER fatal; a fatal would
            // abort the whole run over an ordinary shared-issuer limit.
            if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                return Err(SourceError::RateLimited {
                    retry_after: super::classify::server_retry_after(&response),
                    source: format!("oauth2 token endpoint: HTTP {status}").into(),
                });
            }
            if status.is_server_error() {
                return Err(SourceError::transient(format!(
                    "oauth2 token endpoint: HTTP {status}"
                )));
            }
            return Err(SourceError::fatal(format!(
                "oauth2 token endpoint: HTTP {status} — check client credentials"
            )));
        }
        let body: serde_json::Value = response
            .json()
            .await
            .map_err(|e| SourceError::fatal(format!("oauth2 token response: {e}")))?;
        let access_token = body
            .get("access_token")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                SourceError::fatal("oauth2 token response: missing `access_token` field")
            })?;
        let expires_at = body.get("expires_in").and_then(|v| v.as_u64()).map(|secs| {
            let margin = Duration::from_secs(*expiry_margin_secs);
            let life = Duration::from_secs(secs);
            Instant::now() + life.saturating_sub(margin)
        });
        let token = CachedToken {
            access_token: Secret::new(access_token),
            expires_at,
        };
        let issued = token.access_token.clone();
        guard.cached = Some(token);
        guard.generation = guard.generation.wrapping_add(1);
        Ok((issued, guard.generation))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn oauth_credentials() -> Credentials {
        Credentials::new(
            Auth::OAuth2ClientCredentials {
                token_url: "http://127.0.0.1:1/token".into(),
                client_id: "id".into(),
                client_secret: Secret::new("secret"),
                scopes: Vec::new(),
                audience: None,
                expiry_margin_secs: 0,
            },
            reqwest::Client::new(),
        )
    }

    /// Under fan-out several requests carry the same credential. The first
    /// 401 refreshes it; the others then report a 401 for a credential that
    /// has ALREADY been replaced, and must NOT drop the fresh one — doing
    /// so costs another token round trip and briefly leaves the source with
    /// none.
    #[tokio::test]
    async fn a_stale_401_does_not_evict_a_freshly_refreshed_token() {
        let credentials = oauth_credentials();
        {
            let mut guard = credentials.token.lock().await;
            guard.cached = Some(CachedToken {
                access_token: Secret::new("t1"),
                expires_at: None,
            });
            guard.generation = 7;
        }

        // A request that carried generation 7 is rejected: that IS the
        // cached credential, so it is dropped and the generation moves on.
        assert!(credentials.on_unauthorized(Some(7)).await.expect("hook"));
        {
            let guard = credentials.token.lock().await;
            assert!(guard.cached.is_none(), "the rejected credential is dropped");
            assert_eq!(guard.generation, 8);
        }

        // A fresh credential exists again...
        {
            let mut guard = credentials.token.lock().await;
            guard.cached = Some(CachedToken {
                access_token: Secret::new("t2"),
                expires_at: None,
            });
        }
        // ...and a LATE 401 from the old generation arrives. It must retry
        // against the new credential, not discard it.
        assert!(credentials.on_unauthorized(Some(7)).await.expect("hook"));
        let guard = credentials.token.lock().await;
        assert!(
            guard.cached.is_some(),
            "a 401 for a superseded credential must not evict the current one"
        );
        assert_eq!(guard.generation, 8, "and must not bump the generation");
    }

    /// A scheme with no refreshable credential reports "the 401 is final".
    #[tokio::test]
    async fn a_non_refreshable_scheme_does_not_retry() {
        let credentials = Credentials::new(
            Auth::Bearer {
                token: Secret::new("static"),
            },
            reqwest::Client::new(),
        );
        assert!(!credentials.on_unauthorized(None).await.expect("hook"));
    }
}
