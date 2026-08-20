//! The commit-unit transaction: its three literal statements, its lazily
//! opened state, and the once-per-load clear bookkeeping that must live and
//! die with it.
//!
//! There is deliberately no `tokio_postgres::Transaction` here: it borrows
//! `&mut Client`, so a session that owns its client cannot also hold one
//! across `write` calls. The transaction is therefore driven by literal
//! `BEGIN`/`COMMIT`/`ROLLBACK` through the same connection — a borrow fact,
//! not a preference, and not worth buying a self-referential-struct
//! dependency to defeat.

use std::collections::BTreeSet;

use rdlt_connector_sdk::spi::core::{crash_point, id::TableName};
use rdlt_connector_sdk::spi::error::DestinationError;

use super::errors::{Phase, driver_transient};
use crate::session::Connection;

/// The unit transaction's literal statements. Named (and pinned in the
/// golden-unit-SQL suite) because the isolation level is load-bearing: the
/// "a reload is never observed empty" guarantee is a property of the
/// transaction, and a silent edit here would weaken it without failing
/// anything else.
pub const UNIT_BEGIN: &str = "BEGIN ISOLATION LEVEL READ COMMITTED";
pub const UNIT_COMMIT: &str = "COMMIT";
pub const UNIT_ROLLBACK: &str = "ROLLBACK";
/// Transaction-scoped sort memory for the merge dedup — see `begin`.
pub const UNIT_WORK_MEM: &str = "SET LOCAL work_mem = '64MB'";

/// The facts an OPEN unit transaction carries.
#[derive(Debug)]
pub(super) struct Open {
    /// Whether any unit of this load already committed — read once, as the
    /// first statement after `BEGIN`, because it is the durable half of the
    /// Replace once-per-load guard and every write of this unit consults
    /// it.
    pub(super) load_committed_before: bool,
    /// Targets THIS unit cleared. Promoted into the session's set on COMMIT
    /// and discarded on ROLLBACK, because a rolled-back clear did not
    /// happen.
    pub(super) cleared: BTreeSet<TableName>,
}

/// The unit transaction: `Closed` between units, `Open` from the first
/// write (or a write-less commit) until COMMIT/ROLLBACK.
#[derive(Debug, Default)]
pub(super) struct Unit {
    open: Option<Open>,
}

impl Unit {
    pub(super) fn open(&self) -> Option<&Open> {
        self.open.as_ref()
    }

    pub(super) fn open_mut(&mut self) -> Option<&mut Open> {
        self.open.as_mut()
    }

    /// Open the transaction if it is not already open. Idempotent, so both
    /// `write` and a write-less `commit` can call it unconditionally.
    ///
    /// `load_committed_before` is read as the FIRST statement inside the
    /// transaction: it is the durable half of the Replace once-per-load
    /// guard, so reading it inside the same transaction that will do the
    /// clearing is what makes "clear at most once per load" hold across a
    /// crash.
    pub(super) async fn begin_if_closed(
        &mut self,
        connection: &Connection,
        load_id: &str,
    ) -> Result<(), DestinationError> {
        if self.open.is_some() {
            return Ok(());
        }
        crash_point!(
            "pg.unit.begin",
            Err(DestinationError::fatal("injected crash at pg.unit.begin"))
        );
        // READ COMMITTED is stated rather than inherited: the atomicity this
        // unit relies on (a reader never sees a cleared-but-unfilled target)
        // is a property of the isolation level, so it is not left to a
        // server-side default a deployment could change.
        connection
            .batch_execute(UNIT_BEGIN)
            .await
            .map_err(|e| driver_transient(Phase::Commit, e))?;
        // Merge dedup sorts the staged rows, and Postgres's default work_mem
        // of 4 MB makes that spill to disk on any load worth benchmarking.
        //
        // 64 MB does NOT stop the spill at a million rows — measured, not
        // assumed: the dedup sort reports `external merge Disk: 169832kB`
        // there. Raising it was A/B'd on that exact shape and is
        // deliberately not done: 128 MB keeps the sort in memory and buys
        // about 45 ms against a 4.8 s load — under 1% — and past 128 MB the
        // number stops moving at all.
        //
        // `SET LOCAL` is the whole reason this is safe to do unasked: it is
        // scoped to THIS transaction and reverts at COMMIT or ROLLBACK, so
        // it cannot affect another session, another pipeline sharing the
        // database, or even this connection's next unit. A bare `SET` would
        // leak into all three.
        connection
            .batch_execute(UNIT_WORK_MEM)
            .await
            .map_err(|e| driver_transient(Phase::Commit, e))?;
        let load_committed_before = connection
            .query_one(
                &format!(
                    "SELECT count(*) FROM {} WHERE load_id = $1",
                    rdlt_connector_sqlcore::names::COMMITS_TABLE
                ),
                &[&load_id],
            )
            .await
            .map_err(|e| driver_transient(Phase::Commit, e))?
            .get::<_, i64>(0)
            > 0;
        self.open = Some(Open {
            load_committed_before,
            cleared: BTreeSet::new(),
        });
        Ok(())
    }

    /// Commit the open transaction and hand back its facts so the caller
    /// can promote the clears and marks — promotions happen only AFTER the
    /// server made them durable.
    pub(super) async fn commit(
        &mut self,
        connection: &Connection,
    ) -> Result<Open, DestinationError> {
        connection
            .batch_execute(UNIT_COMMIT)
            .await
            .map_err(|e| driver_transient(Phase::Commit, e))?;
        Ok(self.open.take().expect("unit open at commit"))
    }

    /// Abandon the open transaction. Called on every error path out of the
    /// session's mutating methods: a failed statement leaves the connection
    /// in an aborted transaction where every later statement fails with
    /// 25P02, so a retry that did not roll back could never succeed. The
    /// in-unit clear record is discarded with it — a rolled-back TRUNCATE
    /// did not happen, and a retry must clear again.
    pub(super) async fn rollback(&mut self, connection: &Connection) {
        if self.open.take().is_none() {
            return;
        }
        if let Err(e) = connection.batch_execute(UNIT_ROLLBACK).await {
            // The unit is already failing; this is the diagnostic, not the
            // outcome. A connection too broken to ROLLBACK is one the driver
            // will drop, which aborts the transaction anyway.
            tracing::warn!(error = %crate::session::describe(&e), "unit rollback failed");
        }
    }
}
