//! The source's typed, phase-tagged error surface + SPI classification.
//!
//! The source NEVER retries. It classifies: connection-shaped failures are
//! `Transient` (the engine retries with backoff, and
//! resume-from-committed-cursor makes that double-apply-safe);
//! config/auth/data-shaped failures are `Fatal`. Every message names the
//! phase and, when one is in play, the table.

use rdlt_connector_sdk::spi::SourceError;

/// Where in the source lifecycle a failure happened — part of every message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Phase {
    Connect,
    Reflect,
    Copy,
    Decode,
    /// CDC slot/publication lifecycle + feed access.
    Slot,
}

impl std::fmt::Display for Phase {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Phase::Connect => "connect",
            Phase::Reflect => "reflect",
            Phase::Copy => "copy",
            Phase::Decode => "decode",
            Phase::Slot => "cdc-slot",
        })
    }
}

/// The rendered failure: phase + optional table + detail, in one line.
#[derive(Debug, thiserror::Error)]
#[error("postgres source, {phase} phase{}: {detail}", table.as_deref().map(|t| format!(" (table `{t}`)")).unwrap_or_default())]
pub(crate) struct Error {
    pub(crate) phase: Phase,
    pub(crate) table: Option<String>,
    pub(crate) detail: String,
}

fn tagged(phase: Phase, table: Option<&str>, detail: impl std::fmt::Display) -> Error {
    Error {
        phase,
        table: table.map(str::to_owned),
        detail: detail.to_string(),
    }
}

pub(crate) fn fatal(
    phase: Phase,
    table: Option<&str>,
    detail: impl std::fmt::Display,
) -> SourceError {
    SourceError::fatal(tagged(phase, table, detail))
}

pub(crate) fn transient(
    phase: Phase,
    table: Option<&str>,
    detail: impl std::fmt::Display,
) -> SourceError {
    SourceError::transient(tagged(phase, table, detail))
}

/// Classify a driver error by the shared connect-polarity SQLSTATE rule,
/// carrying the server's full rendered detail either way.
pub(crate) fn classify(
    phase: Phase,
    table: Option<&str>,
    error: &tokio_postgres::Error,
) -> SourceError {
    let detail = crate::session::describe(error);
    if crate::session::is_transient_at_connect(error) {
        transient(phase, table, detail)
    } else {
        // 28 auth, 3D invalid catalog, 42 syntax/undefined object, 22 data
        // exception, 0A unsupported — and anything else server-classified:
        // retrying cannot fix it.
        fatal(phase, table, detail)
    }
}

/// The session-establish adapter: retry-worthiness comes from the session's
/// one rule; the phase tag is always Connect; the DETAIL is the inner
/// failure alone — the enum's own framing would render "connect: " twice.
pub(crate) fn establish_failure(error: crate::session::EstablishError) -> SourceError {
    let detail = error.detail();
    if error.is_transient() {
        transient(Phase::Connect, None, detail)
    } else {
        fatal(Phase::Connect, None, detail)
    }
}

#[cfg(test)]
mod establish_adapter_tests {
    use super::*;

    #[test]
    fn adapter_renders_the_inner_failure_without_wrapper_framing() {
        let config_error = crate::tls::ConfigError::Syntax("does not parse: x".into());
        let error = establish_failure(crate::session::EstablishError::Config(config_error));
        let message = format!("{error:?}");
        assert!(message.contains("conn string does not parse"), "{message}");
        assert!(
            !message.contains("tls config:"),
            "wrapper framing must not reach the operator: {message}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_and_table_appear_in_message() {
        let message = tagged(Phase::Copy, Some("orders"), "boom").to_string();
        assert!(message.contains("copy phase"), "{message}");
        assert!(message.contains("`orders`"), "{message}");
    }

    #[test]
    fn tableless_message_has_no_table_clause() {
        let message = tagged(Phase::Connect, None, "refused").to_string();
        assert!(message.contains("connect phase:"), "{message}");
        assert!(!message.contains("table"), "{message}");
    }
}
