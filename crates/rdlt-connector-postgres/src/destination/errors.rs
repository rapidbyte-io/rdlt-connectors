//! The destination's typed, phase-tagged error surface — parity with the
//! source half: every failure names its phase, and retry-worthiness is
//! decided by the session's shared SQLSTATE rules, never ad hoc.

use rdlt_connector_sdk::spi::DestinationError;

/// Where in the load-session lifecycle a failure happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Phase {
    Connect,
    Ensure,
    Write,
    Commit,
}

impl std::fmt::Display for Phase {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Phase::Connect => "connect",
            Phase::Ensure => "ensure",
            Phase::Write => "write",
            Phase::Commit => "commit",
        })
    }
}

/// The rendered failure: phase + detail, one line.
#[derive(Debug, thiserror::Error)]
#[error("postgres destination, {phase} phase: {detail}")]
pub(super) struct Error {
    pub(super) phase: Phase,
    pub(super) detail: String,
}

fn tagged(phase: Phase, detail: impl std::fmt::Display) -> Error {
    Error {
        phase,
        detail: detail.to_string(),
    }
}

pub(super) fn fatal(phase: Phase, detail: impl std::fmt::Display) -> DestinationError {
    DestinationError::fatal(tagged(phase, detail))
}

pub(super) fn transient(phase: Phase, detail: impl std::fmt::Display) -> DestinationError {
    DestinationError::transient(tagged(phase, detail))
}

/// A driver failure on a session-shaped statement (connection, transaction
/// bookkeeping): rendered fully, always transient — these statements carry
/// no data whose retry could differ.
pub(super) fn driver_transient(phase: Phase, error: tokio_postgres::Error) -> DestinationError {
    transient(phase, crate::session::describe(&error))
}

/// Statement-error classification shared by the COPY write path AND table
/// DDL: the deterministic data-shaped SQLSTATE classes (22 data exception,
/// 23 integrity, 42 syntax/access) are PERMANENT — a poisoned batch or an
/// unwinnable DDL statement must not burn the engine's retry budget on
/// retries that cannot win. Everything else stays transient
/// (connection-shaped).
pub(super) fn classify_statement(phase: Phase, error: tokio_postgres::Error) -> DestinationError {
    match error.as_db_error() {
        Some(db) if crate::session::is_permanent_at_statement(db.code().code()) => {
            fatal(phase, crate::session::describe(&error))
        }
        _ => transient(phase, crate::session::describe(&error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn phase_appears_in_message() {
        let message = tagged(Phase::Commit, "boom").to_string();
        assert!(message.contains("commit phase"), "{message}");
        assert!(message.contains("destination"), "{message}");
    }
}
