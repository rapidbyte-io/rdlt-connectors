//! The send loop: ONE configured HTTP client per source document (reqwest,
//! credentials, source-level defaults, pacing state) with every in-source
//! wait bounded by construction.

use std::time::{Duration, Instant};

use rdlt_connector_sdk::spi::error::SourceError;
use reqwest::header::{HeaderName, HeaderValue};

use super::Credentials;
use super::classify;

/// One configured HTTP client for a source document.
#[derive(Debug)]
pub struct Client {
    http: reqwest::Client,
    credentials: Credentials,
    /// Source-level headers, merged UNDER per-stream headers. Parsed once
    /// at construction: config validation guarantees parseability, so
    /// per-page requests reuse the typed name/value pairs without
    /// re-parsing strings.
    default_headers: Vec<(HeaderName, HeaderValue)>,
    /// Source-level params, merged UNDER per-stream params.
    default_params: Vec<(String, String)>,
    pacing_floor: Duration,
    retry_after_cap: Duration,
    last_request_at: tokio::sync::Mutex<Option<Instant>>,
}

impl Client {
    pub fn new(
        credentials: Credentials,
        http: reqwest::Client,
        default_headers: Vec<(String, String)>,
        default_params: Vec<(String, String)>,
        min_request_interval_ms: u64,
        retry_after_cap_secs: u64,
    ) -> Self {
        // Parse once. `Config::validate` rejects an unparseable
        // source-level header before this constructor is reached, so a
        // parse failure here is a config-validation bug, not runtime input.
        let default_headers = default_headers
            .into_iter()
            .map(|(name, value)| {
                let name: HeaderName = name
                    .parse()
                    .expect("source header name validated by Config::validate");
                let value: HeaderValue = value
                    .parse()
                    .expect("source header value validated by Config::validate");
                (name, value)
            })
            .collect();
        Self {
            http,
            credentials,
            default_headers,
            default_params,
            pacing_floor: Duration::from_millis(min_request_interval_ms),
            retry_after_cap: Duration::from_secs(retry_after_cap_secs),
            last_request_at: tokio::sync::Mutex::new(None),
        }
    }

    /// Pacing floor: at least `pacing_floor` between request sends.
    async fn pace(&self) {
        if self.pacing_floor.is_zero() {
            return;
        }
        let mut last = self.last_request_at.lock().await;
        if let Some(at) = *last {
            let elapsed = at.elapsed();
            if elapsed < self.pacing_floor {
                tokio::time::sleep(self.pacing_floor - elapsed).await;
            }
        }
        *last = Some(Instant::now());
    }

    /// Send with credentials + defaults + pacing. `Err` carries anything
    /// WITHOUT a data-response status to match — transport failures
    /// (transient), token-endpoint failures (already classified), request
    /// build failures (fatal); an HTTP error status comes back as
    /// `Ok(response)` after the bounded in-source handling below, so the
    /// caller can match declared response actions on the TYPED status
    /// before classifying. Retry-After on 429/503 is
    /// honored IN-SOURCE up to the cap (one wait, one retry per send).
    pub async fn send(
        &self,
        build: impl Fn(&reqwest::Client) -> reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, SourceError> {
        // Bounded by construction: at most ONE Retry-After wait and at most
        // one credential re-fetch per send — never a free loop; persistent
        // rate limiting surfaces to the engine's budget with the header
        // value.
        let mut rate_limit_waited = false;
        let mut auth_retried = false;
        loop {
            self.pace().await;
            let (request, generation) = self.credentials.attach(build(&self.http)).await?;
            let mut request = request
                .build()
                .map_err(|e| SourceError::fatal(format!("request build: {e}")))?;
            self.apply_defaults(&mut request);
            let response = self
                .http
                .execute(request)
                .await
                .map_err(classify::transport)?;
            let status = response.status();
            if status.is_success() {
                return Ok(response);
            }
            // 401 once-through PER SEND: a refreshable credential re-fetches
            // and the loop retries exactly once; a second 401 is fatal.
            if status == reqwest::StatusCode::UNAUTHORIZED
                && !auth_retried
                && self.credentials.on_unauthorized(generation).await?
            {
                auth_retried = true;
                continue;
            }
            // In-source Retry-After wait for 429/503 within the cap — once.
            if !rate_limit_waited
                && matches!(
                    status,
                    reqwest::StatusCode::TOO_MANY_REQUESTS
                        | reqwest::StatusCode::SERVICE_UNAVAILABLE
                )
                && let Some(wait) = classify::server_retry_after(&response)
                && wait <= self.retry_after_cap
            {
                rate_limit_waited = true;
                tokio::time::sleep(wait).await;
                continue;
            }
            return Ok(response);
        }
    }

    /// Source-level defaults merge UNDER what the request already carries:
    /// a header or query param the stream (or auth) set wins over the
    /// source-level default of the same name.
    fn apply_defaults(&self, request: &mut reqwest::Request) {
        for (name, value) in &self.default_headers {
            if !request.headers().contains_key(name) {
                request.headers_mut().insert(name.clone(), value.clone());
            }
        }
        if !self.default_params.is_empty() {
            let url = request.url_mut();
            let existing: std::collections::BTreeSet<String> =
                url.query_pairs().map(|(k, _)| k.into_owned()).collect();
            let missing: Vec<&(String, String)> = self
                .default_params
                .iter()
                .filter(|(name, _)| !existing.contains(name))
                .collect();
            if !missing.is_empty() {
                let mut pairs = url.query_pairs_mut();
                for (name, value) in missing {
                    pairs.append_pair(name, value);
                }
            }
        }
    }
}
