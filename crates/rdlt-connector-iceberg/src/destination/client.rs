//! The one boundary with the iceberg library.
//!
//! Every library type stops at this file's seams: what crosses out is
//! the SPI error taxonomy, catalogs and tables as opaque handles, and
//! strings. Classification lives here — by `ErrorKind` and, for
//! unexpected transport errors, the HTTP status — never by matching
//! rendered prose, with ONE deliberate exception documented at
//! [`status_from_context`].
//!
//! Every `Secret::reveal` in the crate is concentrated in
//! [`catalog_props`] — the single function a credential audit reads.

use std::collections::HashMap;
use std::sync::Arc;

use iceberg::{Catalog, CatalogBuilder as _};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_storage_opendal::OpenDalResolvingStorageFactory;
use rdlt_connector_sdk::spi::DestinationError;

use super::config::Config;

/// A fatal SPI error from anything renderable — the crate-wide
/// shorthand.
pub(super) fn fatal(message: impl ToString) -> DestinationError {
    DestinationError::fatal(message.to_string())
}

/// Does this error mean a competing writer won the optimistic commit?
/// The bounded retry loop keys on exactly this.
pub(super) fn is_commit_conflict(error: &iceberg::Error) -> bool {
    matches!(error.kind(), iceberg::ErrorKind::CatalogCommitConflicts)
}

/// Translate a library error into the SPI taxonomy.
///
/// Deterministic failures are Fatal, service pressure is RateLimited,
/// and everything a fresh attempt could plausibly change — network
/// faults, 5xx, an unreadable status — is Transient. Unknown kinds
/// from the non-exhaustive enum classify Fatal: loud beats a silent
/// retry loop on an error nobody has categorised.
pub(super) fn classify(context: &str, error: iceberg::Error) -> DestinationError {
    use iceberg::ErrorKind;
    match error.kind() {
        ErrorKind::Unexpected => match status_from_context(&error) {
            Some(401 | 403) => DestinationError::fatal(format!(
                "{context}: authentication/authorization rejected — fix the credential or its \
                 grants: {error}"
            )),
            Some(429) => DestinationError::RateLimited {
                retry_after: None,
                source: format!("{context}: {error}").into(),
            },
            Some(status) if (400..500).contains(&status) => {
                DestinationError::fatal(format!("{context}: {error}"))
            }
            _ => DestinationError::transient(format!("{context}: {error}")),
        },
        ErrorKind::CatalogCommitConflicts => DestinationError::fatal(format!(
            "{context}: commit conflicts exhausted the bounded retry — a competing writer keeps \
             winning: {error}"
        )),
        _ => DestinationError::fatal(format!("{context}: {error}")),
    }
}

/// The transport status inside an unexpected error, if any.
///
/// The library exposes no getter for its context entries, so this
/// reads the RENDERED error — the one deliberate, pinned exception to
/// the never-match-prose rule. It anchors on the context BLOCK
/// (`", context: { "`), TRUNCATED at the block's closing ` } => ` so
/// the message/source tail is never scanned, and then on an entry KEY.
/// TWO keys are accepted: the data path attaches `status`, while the
/// library's token-endpoint path attaches the HTTP status under
/// `code` — generation 1 read only `status`, so a wrong client secret
/// (a deterministic 400/401) classified TRANSIENT and would retry
/// forever; found live while authoring the suite, verified against
/// both attachment sites in the library source (the third `code`
/// site, ErrorModel's numeric body code, rides DataInvalid and never
/// reaches this parser). First match wins — genuine entries are
/// inserted before anything an error body could carry.
fn status_from_context(error: &iceberg::Error) -> Option<u16> {
    let rendered = format!("{error}");
    let (_, block) = rendered.split_once(", context: { ")?;
    let block = block.split(" } => ").next().unwrap_or(block);
    block
        .split(", ")
        .find_map(|entry| {
            entry
                .strip_prefix("status: ")
                .or_else(|| entry.strip_prefix("code: "))
        })?
        .split_whitespace()
        .next()?
        .parse()
        .ok()
}

/// The catalog property keys that carry a credential.
///
/// Shared with `config::Config::validate`, which refuses any of these
/// arriving through `catalog.props` — the single source of truth for
/// both the gate and this function's guarantee below.
pub(super) const CREDENTIAL_PROP_KEYS: &[&str] = &[
    "credential",
    "token",
    "s3.access-key-id",
    "s3.secret-access-key",
];

/// The catalog load-property vocabulary, from the validated config.
///
/// EVERY `Secret::reveal` in the crate happens here and nowhere else.
/// User `catalog.props` are inserted LAST, so they win over generated
/// ones — the documented escape hatch for non-secret keys (e.g.
/// `uri`). The validate gate guarantees none of `CREDENTIAL_PROP_KEYS`
/// reaches this function, so the override can never touch a
/// credential.
pub(super) fn catalog_props(config: &Config) -> HashMap<String, String> {
    let mut props = HashMap::new();
    props.insert("uri".to_owned(), config.catalog.uri.clone());
    props.insert("warehouse".to_owned(), config.catalog.warehouse.clone());
    if let Some(oauth) = &config.catalog.auth.oauth2_client_credentials {
        props.insert(
            "credential".to_owned(),
            format!("{}:{}", oauth.client_id, oauth.client_secret.reveal()),
        );
        if !oauth.scopes.is_empty() {
            props.insert("scope".to_owned(), oauth.scopes.join(" "));
        }
        if let Some(url) = &oauth.token_url {
            props.insert("oauth2-server-uri".to_owned(), url.clone());
        }
    }
    if let Some(bearer) = &config.catalog.auth.bearer {
        props.insert("token".to_owned(), bearer.token.reveal().to_owned());
    }
    if let Some(s3) = config.storage.as_ref().and_then(|s| s.s3.as_ref()) {
        if let Some(endpoint) = &s3.endpoint {
            props.insert("s3.endpoint".to_owned(), endpoint.clone());
        }
        if let Some(region) = &s3.region {
            props.insert("s3.region".to_owned(), region.clone());
        }
        props.insert(
            "s3.access-key-id".to_owned(),
            s3.access_key.reveal().to_owned(),
        );
        props.insert(
            "s3.secret-access-key".to_owned(),
            s3.secret_key.reveal().to_owned(),
        );
        props.insert("s3.path-style-access".to_owned(), s3.path_style.to_string());
    }
    for (key, value) in &config.catalog.props {
        props.insert(key.clone(), value.clone());
    }
    props
}

/// Open the catalog handle. The storage factory resolves vended or
/// overridden object-store credentials per table.
pub(super) async fn connect(config: &Config) -> Result<Arc<dyn Catalog>, DestinationError> {
    let context = format!(
        "catalog `{}` warehouse `{}`",
        config.catalog.uri, config.catalog.warehouse
    );
    RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalResolvingStorageFactory::new()))
        .load(config.catalog.warehouse.clone(), catalog_props(config))
        .await
        .map(|catalog| Arc::new(catalog) as Arc<dyn Catalog>)
        .map_err(|e| classify(&context, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::ErrorKind;

    fn unexpected_with_status(status: &str) -> iceberg::Error {
        iceberg::Error::new(ErrorKind::Unexpected, "request failed").with_context("status", status)
    }

    /// The classification table over transport statuses: auth is
    /// fatal with advice, 429 is rate-limited, other 4xx are fatal,
    /// 5xx and no-status are transient.
    #[test]
    fn transport_statuses_classify_by_the_frozen_table() {
        for status in ["401 Unauthorized", "403 Forbidden"] {
            let err = classify("table `t`", unexpected_with_status(status));
            assert!(
                matches!(err, DestinationError::Fatal(_)),
                "{status}: {err:?}"
            );
            assert!(
                format!("{err}").contains("fix the credential or its grants"),
                "{err}"
            );
        }
        let err = classify("table `t`", unexpected_with_status("429 Too Many Requests"));
        assert!(
            matches!(err, DestinationError::RateLimited { .. }),
            "{err:?}"
        );

        let err = classify("table `t`", unexpected_with_status("404 Not Found"));
        assert!(matches!(err, DestinationError::Fatal(_)), "{err:?}");

        for status in ["500 Internal Server Error", "503 Service Unavailable"] {
            let err = classify("table `t`", unexpected_with_status(status));
            assert!(
                matches!(err, DestinationError::Transient(_)),
                "{status}: {err:?}"
            );
        }
        let err = classify(
            "table `t`",
            iceberg::Error::new(ErrorKind::Unexpected, "connection reset"),
        );
        assert!(matches!(err, DestinationError::Transient(_)), "{err:?}");
    }

    /// The anchor rule: a status quoted in a response BODY renders
    /// outside the context block and must never classify the error.
    #[test]
    fn a_status_quoted_in_a_body_is_not_the_transport_status() {
        let err = iceberg::Error::new(ErrorKind::Unexpected, "request failed")
            .with_context("response", "body mentions status: 401 somewhere");
        assert_eq!(status_from_context(&err), None);
        assert!(matches!(
            classify("table `t`", err),
            DestinationError::Transient(_)
        ));

        let err = unexpected_with_status("401 Unauthorized");
        assert_eq!(status_from_context(&err), Some(401));

        // The tail past the context block's ` } => ` is never scanned.
        // The error must CARRY a context block for this pin to bite —
        // without one the parser bails before the truncation, and the
        // pin passes with the truncation deleted (a mutation proved
        // it): a keyless context entry puts the block in play, and the
        // comma-separated `status:` fragment rides the MESSAGE tail.
        let err = iceberg::Error::new(
            ErrorKind::Unexpected,
            "proxy said: retry later, status: 429 somewhere",
        )
        .with_context("operation", "auth");
        assert_eq!(status_from_context(&err), None);
    }

    /// The token-endpoint path attaches the HTTP status under `code` —
    /// generation 1 read only `status`, so a wrong client secret (a
    /// deterministic 400) classified TRANSIENT and retried forever.
    /// Found live; both attachment sites verified in the library.
    #[test]
    fn the_token_endpoint_code_key_classifies_like_a_status() {
        let err = iceberg::Error::new(ErrorKind::Unexpected, "auth failed")
            .with_context("code", "400 Bad Request");
        assert_eq!(status_from_context(&err), Some(400));
        assert!(matches!(
            classify("catalog `c` warehouse `w`", err),
            DestinationError::Fatal(_)
        ));

        let err = iceberg::Error::new(ErrorKind::Unexpected, "auth failed")
            .with_context("code", "401 Unauthorized");
        assert!(
            format!("{}", classify("catalog `c` warehouse `w`", err))
                .contains("fix the credential or its grants")
        );
    }

    /// Conflict exhaustion renders "exhausted" EXACTLY once — the
    /// retry loop's context prefix must not repeat it.
    #[test]
    fn conflict_exhaustion_says_exhausted_exactly_once() {
        let err = classify(
            "table `t` (commit attempt 4/4)",
            iceberg::Error::new(ErrorKind::CatalogCommitConflicts, "competing snapshot"),
        );
        let rendered = format!("{err}");
        assert!(matches!(err, DestinationError::Fatal(_)));
        assert_eq!(rendered.matches("exhausted").count(), 1, "{rendered}");
        assert!(
            rendered.contains("a competing writer keeps winning"),
            "{rendered}"
        );
    }

    /// Every other kind classifies FATAL with the bare context frame —
    /// loud, never a silent retry.
    #[test]
    fn every_other_kind_is_fatal_with_the_bare_frame() {
        for kind in [
            ErrorKind::DataInvalid,
            ErrorKind::FeatureUnsupported,
            ErrorKind::TableNotFound,
        ] {
            let err = classify("table `t`", iceberg::Error::new(kind, "boom"));
            assert!(matches!(err, DestinationError::Fatal(_)), "{kind:?}");
            // The SPI's Display adds its own taxonomy frame; the
            // frozen part is the context-then-error body.
            assert!(format!("{err}").contains("table `t`: "), "{err}");
        }
    }

    /// The whole credential surface, in one place: generated keys,
    /// the scope join, and the user-props-win-last rule.
    #[test]
    fn catalog_props_carry_the_frozen_vocabulary_and_user_props_win() {
        use rdlt_connector_sdk::config::Document;
        let config = Config::from_value(serde_json::json!({
            "catalog": {
                "uri": "http://c/api/catalog",
                "warehouse": "wh",
                "auth": {"oauth2_client_credentials": {
                    "client_id": "id", "client_secret": "sec",
                    "scopes": ["a", "b"], "token_url": "http://t",
                }},
                "props": {"uri": "http://override", "extra": "kept"},
            },
            "namespace": "raw",
            "storage": {"s3": {
                "endpoint": "http://s3", "region": "r",
                "access_key": "ak", "secret_key": "sk",
            }},
        }))
        .expect("valid");
        let props = catalog_props(&config);
        assert_eq!(props["credential"], "id:sec");
        assert_eq!(props["scope"], "a b");
        assert_eq!(props["oauth2-server-uri"], "http://t");
        assert_eq!(props["s3.endpoint"], "http://s3");
        assert_eq!(props["s3.access-key-id"], "ak");
        assert_eq!(props["s3.secret-access-key"], "sk");
        assert_eq!(props["s3.path-style-access"], "true");
        assert_eq!(props["warehouse"], "wh");
        assert_eq!(
            props["uri"], "http://override",
            "user props win over generated"
        );
        assert_eq!(props["extra"], "kept");
    }
}
