//! The crash-point registry the destination sweep pins and iterates —
//! verbatim from the module root; the testkit registry scanner verifies it
//! against this crate's own sources.

/// Fail-point registry: every `crash_point!` site in the destination — the
/// ENGINE-OWNED protocol boundaries of a commit unit, in the order a unit
/// reaches them. Postgres' internal transaction atomicity is the database's
/// own guarantee and is deliberately NOT instrumented.
///
/// - `pg.unit.begin` — before the unit transaction opens
/// - `pg.target.clear` — before a Replace target is cleared, inside that
///   transaction; a crash here must leave the target's OLD rows intact
/// - `pg.unit.write` — before a batch is written (into the target directly,
///   or into a stage for merge)
/// - `pg.publish.begin` — at `commit`, before the first publish step
/// - `pg.tx.commit` — the redelivery window: the client dies without
///   learning whether the transaction committed
/// - `pg.tx.acked` — the same window from the other side: the transaction
///   HAS committed durably and the client dies before acting on it, so
///   recovery must return the existing receipt rather than publish a second
///   time
#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub const FAIL_POINTS: &[&str] = &[
    "pg.unit.begin",
    "pg.target.clear",
    "pg.unit.write",
    "pg.publish.begin",
    "pg.tx.commit",
    "pg.tx.acked",
];
