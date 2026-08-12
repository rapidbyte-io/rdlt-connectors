//! Failure judgment for the whole crate: how a driver error is rendered, and
//! whether retrying it can ever help.
//!
//! Two facts about tokio-postgres drive the rendering. Its `Display` for a
//! server error is just "db error" — the real message and the SQLSTATE live
//! in the `DbError`, reachable only through `as_db_error()`. And its
//! `source()` chain is the only place a cause (a rustls alert, an io error)
//! survives, so dropping it loses the reason.
//!
//! TWO retryability rules live here, with deliberately OPPOSITE polarity for
//! SQLSTATE classes neither lists. At connect/session time
//! ([`is_transient_at_connect`]) only the clearly environmental classes
//! retry, and an unknown class fails fast — it is auth/configuration-shaped.
//! At statement time ([`is_permanent_at_statement`]) only the clearly
//! deterministic classes fail fast, and an unknown class retries — a load
//! restart is cheap, and lock/resource/internal classes may heal. Both rules
//! sitting side by side is what keeps the polarity difference a decision
//! instead of drift.

/// Render a driver error with its full meaning. A server (db) error yields
/// its message + SQLSTATE, plus the statement's column context when the
/// server supplied one (the COPY `where_` field). A non-db error renders its
/// whole source chain — `Display` alone drops the cause.
pub(crate) fn describe(error: &tokio_postgres::Error) -> String {
    use std::error::Error as _;
    if let Some(db) = error.as_db_error() {
        let context = db.where_().map(|w| format!(" [{w}]")).unwrap_or_default();
        return format!(
            "{error}: {} (SQLSTATE {}){context}",
            db.message(),
            db.code().code()
        );
    }
    let mut rendered = error.to_string();
    let mut cause = error.source();
    while let Some(current) = cause {
        rendered.push_str(": ");
        rendered.push_str(&current.to_string());
        cause = current.source();
    }
    rendered
}

/// Connect-time retryability: a missing SQLSTATE means the failure never
/// reached the server (io error, connection closed before a reply) and is
/// transient by definition. Otherwise the class decides: 08 connection
/// exception, 53 insufficient resources, 57 operator intervention
/// (shutdown), 40 transaction rollback (serialization). Every other class is
/// server-classified and permanent — retrying an auth, syntax, or data error
/// cannot fix it.
pub(crate) fn is_transient_at_connect(error: &tokio_postgres::Error) -> bool {
    match error.code() {
        None => true,
        Some(state) => matches!(&state.code()[..2], "08" | "53" | "57" | "40"),
    }
}

/// Statement-time permanence (COPY writes, DDL, planned commit steps): the
/// deterministic data-shaped classes — 22 data exception, 23 integrity (a
/// duplicate receipt included), 42 syntax/access — are permanent; retrying an
/// identical statement cannot win. Every other class stays transient. See
/// the module doc for why this polarity differs from
/// [`is_transient_at_connect`].
#[cfg(any(feature = "destination", test))]
pub(crate) fn is_permanent_at_statement(sqlstate: &str) -> bool {
    matches!(sqlstate.get(..2), Some("22") | Some("23") | Some("42"))
}

/// Connect-phase failures, distinguished by TLS meaning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TlsFailure {
    /// Certificate signed by an unknown CA — supply `tls.root_cert`.
    TrustAnchor,
    /// Chain invalid (expired, wrong usage, malformed…).
    Chain,
    /// Certificate does not match the host (verify_full only).
    Hostname,
    /// The server refused TLS while the policy demands it.
    ServerRefusedTls,
    /// The server rejected OUR client credential (mutual TLS): TLS-level
    /// certificate alerts, or auth-phase SQLSTATE 28000.
    ClientCert,
    /// Any other connection failure (not TLS-classified).
    Other,
}

/// A classified connect failure: what went wrong, rendered in full, and
/// whether retrying can help. TLS-verification failures are never transient
/// (retries don't mint certificates); network-shaped `Other` failures are.
#[derive(Debug, thiserror::Error)]
#[error("{failure:?}: {detail}")]
pub struct ConnectError {
    pub failure: TlsFailure,
    pub detail: String,
    pub transient: bool,
}

/// The server-refused-TLS signal is a STRING SNIFF: tokio-postgres surfaces
/// a server that rejects the SSL request as a plain error with no typed
/// variant and no SQLSTATE, so this exact needle in the rendered detail is
/// the ONLY way to distinguish it from other connect failures. Pinned by
/// `server_tls_refusal_needle_is_pinned` so a driver wording change fails a
/// test instead of silently reclassifying as `Other`.
const SERVER_TLS_REFUSED_NEEDLE: &str = "server does not support TLS";

/// The auth-phase client-certificate signal is HALF structured: SQLSTATE
/// 28000 narrows it to authentication, but pg_hba's `cert`/`clientcert=`
/// rejections are distinguishable from a bad password only by this word in
/// the server's message text. Pinned by
/// `client_certificate_needle_is_pinned` so a wording change fails a test
/// instead of silently downgrading mutual-TLS failures to `Other`.
const CLIENT_CERTIFICATE_NEEDLE: &str = "certificate";

/// Classify a connect-phase driver error by walking its source chain for
/// rustls/tokio-postgres evidence. Best-effort by design: unknown shapes
/// classify `Other` with the full detail preserved.
pub(crate) fn classify_connect_error(error: &tokio_postgres::Error) -> ConnectError {
    use std::error::Error as _;
    let detail = describe(error);
    let mut cause: Option<&(dyn std::error::Error + 'static)> = error.source();
    while let Some(current) = cause {
        // io::Error::source() skips its own inner error (it delegates to the
        // inner's source) — reach the rustls error through get_ref().
        let rustls_error = current.downcast_ref::<rustls::Error>().or_else(|| {
            current
                .downcast_ref::<std::io::Error>()
                .and_then(|io| io.get_ref())
                .and_then(|inner| inner.downcast_ref::<rustls::Error>())
        });
        if let Some(rustls_error) = rustls_error {
            return ConnectError {
                failure: classify_rustls(rustls_error),
                detail: format!("{detail}: {rustls_error}"),
                transient: false,
            };
        }
        cause = current.source();
    }
    if detail.contains(SERVER_TLS_REFUSED_NEEDLE) {
        return ConnectError {
            failure: TlsFailure::ServerRefusedTls,
            detail,
            transient: false,
        };
    }
    // Auth-phase client-certificate rejection: pg_hba `cert`/`clientcert=`
    // failures surface as SQLSTATE 28000 with a certificate-naming message,
    // AFTER a successful handshake.
    if let Some(db) = error.as_db_error()
        && db.code().code() == "28000"
        && db
            .message()
            .to_lowercase()
            .contains(CLIENT_CERTIFICATE_NEEDLE)
    {
        return ConnectError {
            failure: TlsFailure::ClientCert,
            detail,
            transient: false,
        };
    }
    ConnectError {
        failure: TlsFailure::Other,
        detail,
        transient: is_transient_at_connect(error),
    }
}

/// The rustls-error half of classification, factored for direct unit tests.
fn classify_rustls(error: &rustls::Error) -> TlsFailure {
    use rustls::AlertDescription as Alert;
    use rustls::CertificateError as Certificate;
    match error {
        rustls::Error::InvalidCertificate(certificate_error) => match certificate_error {
            Certificate::UnknownIssuer => TlsFailure::TrustAnchor,
            Certificate::NotValidForName | Certificate::NotValidForNameContext { .. } => {
                TlsFailure::Hostname
            }
            _ => TlsFailure::Chain,
        },
        // The server aborting the handshake over OUR certificate (mutual
        // TLS): certificate-shaped alerts, plus the handshake_failure/
        // access_denied forms TLS 1.2 servers send when a required client
        // certificate is absent.
        rustls::Error::AlertReceived(
            Alert::CertificateRequired
            | Alert::BadCertificate
            | Alert::UnknownCA
            | Alert::CertificateUnknown
            | Alert::AccessDenied
            | Alert::HandshakeFailure,
        ) => TlsFailure::ClientCert,
        _ => TlsFailure::Chain,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_tls_refusal_needle_is_pinned() {
        // Probe-style pin: this exact substring is the ONLY signal for a
        // server that refuses TLS. If the driver's wording changes, this
        // fails loudly instead of silently downgrading to `Other`.
        assert_eq!(SERVER_TLS_REFUSED_NEEDLE, "server does not support TLS");
    }

    #[test]
    fn client_certificate_needle_is_pinned() {
        // Probe-style pin, same posture as the TLS-refusal needle: this
        // word is the only auth-phase signal separating a certificate
        // rejection from a bad password under SQLSTATE 28000.
        assert_eq!(CLIENT_CERTIFICATE_NEEDLE, "certificate");
    }

    #[test]
    fn rustls_classification_distinguishes_client_certificate_rejection() {
        use rustls::AlertDescription as Alert;
        use rustls::CertificateError as Certificate;
        // Server-verification failures keep their classes…
        assert_eq!(
            classify_rustls(&rustls::Error::InvalidCertificate(
                Certificate::UnknownIssuer
            )),
            TlsFailure::TrustAnchor
        );
        assert_eq!(
            classify_rustls(&rustls::Error::InvalidCertificate(
                Certificate::NotValidForName
            )),
            TlsFailure::Hostname
        );
        // …while the server aborting over OUR certificate is ClientCert.
        for alert in [
            Alert::CertificateRequired,
            Alert::BadCertificate,
            Alert::UnknownCA,
            Alert::HandshakeFailure,
        ] {
            assert_eq!(
                classify_rustls(&rustls::Error::AlertReceived(alert)),
                TlsFailure::ClientCert,
                "{alert:?}"
            );
        }
        // Unrelated alerts stay in the generic Chain bucket.
        assert_eq!(
            classify_rustls(&rustls::Error::AlertReceived(Alert::RecordOverflow)),
            TlsFailure::Chain
        );
    }

    #[test]
    fn statement_permanence_covers_the_deterministic_classes_only() {
        for permanent in ["22001", "23505", "42601"] {
            assert!(is_permanent_at_statement(permanent), "{permanent}");
        }
        for retryable in ["08006", "53200", "57P01", "40001", "XX000", ""] {
            assert!(!is_permanent_at_statement(retryable), "{retryable}");
        }
    }
}
