//! The classification rulebook and the server's own pacing header — one
//! home, shared by the data path and the token endpoint, so a failure is
//! never classified two ways.

use std::time::Duration;

use rdlt_connector_sdk::spi::error::SourceError;

/// Connection-level problems are the textbook transient failure.
pub(crate) fn transport(error: reqwest::Error) -> SourceError {
    SourceError::transient(error)
}

/// Classify an undeclared HTTP error status from its parts: 429 is
/// rate-limited (carrying any Retry-After), a 5xx status is transient,
/// everything else is fatal. Taken from parts because the response body may
/// already be consumed by the time an undeclared error status is
/// classified.
pub(crate) fn status(status: reqwest::StatusCode, retry_after: Option<Duration>) -> SourceError {
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return SourceError::RateLimited {
            retry_after,
            source: "HTTP 429 from API".into(),
        };
    }
    if status.is_server_error() {
        return SourceError::transient(format!("HTTP {status}"));
    }
    SourceError::fatal(format!("HTTP {status}"))
}

/// The server's own pacing, in either form RFC 9110 allows: delta-seconds,
/// or an HTTP-date. Several rate limiters send the date form, and reading
/// only the delta form silently discards the instruction — the caller then
/// falls back to generic backoff and paces itself worse than the server
/// asked for.
///
/// A date at or before now yields `Duration::ZERO`, not `None`. The date
/// form means "do not retry before this instant"; once it has passed, the
/// reading is "retry now" — not "there was no instruction". `None` would
/// discard a real header, and a client clock even slightly ahead of the
/// server's makes that the COMMON case, not an edge one.
pub(crate) fn server_retry_after(response: &reqwest::Response) -> Option<Duration> {
    let raw = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?;
    if let Ok(seconds) = raw.trim().parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }
    let at = httpdate::parse_http_date(raw.trim()).ok()?;
    Some(
        at.duration_since(std::time::SystemTime::now())
            .unwrap_or(Duration::ZERO),
    )
}
