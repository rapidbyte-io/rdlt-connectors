//! The load backend: the sdk's `Backend` implemented as a coordinator
//! over three owned states — the [`Connection`], the ensured [`Catalog`],
//! and the [`Unit`] transaction — plus the once-per-load guards that span
//! units. The sdk session choreography drives it: `existing_receipt`
//! (the replay probe) always precedes `replay` or `publish`.
//!
//! Every mutating method wraps its body so that ANY error abandons the open
//! unit: a failed statement poisons the connection with 25P02 until
//! ROLLBACK, and the engine may retry a transient failure on this same
//! session.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use rdlt_connector_sdk::destination::Backend;
use rdlt_connector_sdk::spi::{
    arrow::RecordBatch,
    core::commit::CommitMeta,
    core::commit::CommitReceipt,
    core::commit::WriteMode,
    core::{
        crash_point, id::LoadId, id::PipelineId, id::TableName, schema::TableSchema,
        state::StateDoc,
    },
    error::DestinationError,
};
use rdlt_connector_sqlcore::protocol::unit as unit_rules;
use rdlt_connector_sqlcore::{
    CommitContext, FullLoadPublish, MergeDialect, Step, plan_commit, prepare_target,
    quote_identifier, staged_probe_targets,
};

use super::catalog::{Catalog, stage_name};
use super::config::DestinationOptions;
use super::dialect::Dialect;
use super::errors::{Phase, driver_transient, fatal};
use super::unit::Unit;
use super::{executor, write};
use crate::session::Connection;

/// The per-session system IO —
/// [`DestinationConnector::connect`](rdlt_connector_sdk::destination::DestinationConnector::connect)
/// (`super::connector`) opens one; the sdk session drives it. Public only
/// as the connector's associated `Backend` type; every field and method
/// stays crate-internal.
// Debug is required by the workspace lint; the fields' debug value is nil
// (a connection handle and guard sets), so the derive-free minimal form.
impl std::fmt::Debug for Load {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Load")
            .field("pipeline", &self.pipeline)
            .field("load_id", &self.load_id)
            .finish_non_exhaustive()
    }
}

pub struct Load {
    pub(super) connection: Connection,
    pub(super) pipeline: PipelineId,
    /// The load this session belongs to. It scopes the Replace
    /// once-per-load clear guard, so replaying a crashed load's batches
    /// must happen through a session opened under THAT load's id — which is
    /// why the engine gives WAL recovery its own session.
    pub(super) load_id: LoadId,
    pub(super) catalog: Catalog,
    pub(super) options: DestinationOptions,
    /// The unit transaction, opened lazily at the first `write` of a unit
    /// and closed by `commit`.
    pub(super) unit: Unit,
    /// Targets a COMMITTED unit of this load has already cleared — the
    /// in-process half of the once-per-load guard, which stops units 2..N
    /// of a live load re-clearing what unit 1 published.
    pub(super) cleared_targets: BTreeSet<TableName>,
    /// Single-unit discipline, PER TABLE: tables whose stage has already
    /// published non-empty in an earlier commit unit of THIS load.
    /// Session-scoped is load-scoped: a session spans one engine run = one
    /// load; a crash starts both afresh. Marked only AFTER the unit's
    /// transaction commits (a rolled-back unit never counts), and re-marked
    /// on the replay branch (a committed unit whose outcome the client
    /// never learned still counts).
    pub(super) single_unit_done: BTreeSet<TableName>,
}

impl Load {
    /// The planning facts known WITHOUT touching the server. The two
    /// probe-derived arguments are supplied by the caller — `write` knows
    /// neither and needs neither.
    fn commit_context<'a>(
        &'a self,
        replayed: bool,
        staged_nonempty: &'a BTreeSet<TableName>,
        cleared: &'a BTreeSet<TableName>,
    ) -> CommitContext<'a> {
        CommitContext {
            replayed,
            load_committed_before: self
                .unit
                .open()
                .is_some_and(|open| open.load_committed_before),
            single_unit_done: &self.single_unit_done,
            staged_nonempty,
            full_load_publish: FullLoadPublish::DirectToTarget,
            cleared_targets: cleared,
        }
    }

    /// The union of durably-promoted and in-unit clears — what the planner
    /// must treat as already cleared.
    fn cleared_union(&self) -> BTreeSet<TableName> {
        let mut cleared = self.cleared_targets.clone();
        if let Some(open) = self.unit.open() {
            cleared.extend(open.cleared.iter().cloned());
        }
        cleared
    }

    /// Everything `write` must do to a target before its first row lands:
    /// open the unit, then run whatever the planner says has to precede the
    /// data (for Replace, exactly one `TRUNCATE` per load).
    async fn prepare_for_write(
        &mut self,
        table: &TableName,
        load_id: &str,
    ) -> Result<(), DestinationError> {
        self.unit.begin_if_closed(&self.connection, load_id).await?;
        // Only Replace has anything to prepare. Checked before building the
        // union below, so the common path (Append, and every merge table)
        // allocates nothing.
        if !matches!(
            self.catalog.tables.get(table),
            Some((_, WriteMode::Replace))
        ) {
            return Ok(());
        }
        // Seed the guard from the DURABLE record before asking the planner.
        // A crash-recovery session has fresh memory and would otherwise
        // re-clear a target an earlier unit of this same load already
        // cleared and filled.
        let already_known = self.cleared_targets.contains(table)
            || self.unit.open().expect("unit open").cleared.contains(table);
        if !already_known && self.target_cleared_durably(load_id, table).await? {
            self.cleared_targets.insert(table.clone());
        }
        let cleared = self.cleared_union();
        let steps = prepare_target(
            &self.catalog.tables,
            &self.commit_context(false, &BTreeSet::new(), &cleared),
            table,
        );
        for step in steps {
            let Step::ClearTarget { table } = step else {
                // `prepare_target` emits nothing else; anything else would
                // be a planner change this executor has not been taught.
                return Err(fatal(
                    Phase::Write,
                    "internal: unexpected step planned before a direct write",
                ));
            };
            crash_point!(
                "pg.target.clear",
                Err(DestinationError::fatal("injected crash at pg.target.clear"))
            );
            self.connection
                .batch_execute(&Dialect.clear_table(&quote_identifier(table.as_str())))
                .await
                .map_err(|e| super::errors::classify_statement(Phase::Write, e))?;
            // Recorded in the SAME transaction as the TRUNCATE it
            // describes, so the two cannot disagree: a rolled-back clear
            // leaves no record claiming it happened, and a durable record
            // always has the empty target behind it.
            self.connection
                .execute(
                    &format!(
                        "INSERT INTO {} VALUES ($1, $2) ON CONFLICT DO NOTHING",
                        rdlt_connector_sqlcore::names::CLEARED_TABLE
                    ),
                    &[&load_id, &table.as_str()],
                )
                .await
                .map_err(|e| super::errors::classify_statement(Phase::Write, e))?;
            self.unit
                .open_mut()
                .expect("unit open")
                .cleared
                .insert(table);
        }
        Ok(())
    }

    /// Whether a COMMITTED unit of this load already cleared this target —
    /// the durable half of the once-per-(load, target) guard.
    async fn target_cleared_durably(
        &self,
        load_id: &str,
        table: &TableName,
    ) -> Result<bool, DestinationError> {
        let cleared: bool = self
            .connection
            .query_one(
                &format!(
                    "SELECT EXISTS (SELECT 1 FROM {} WHERE load_id = $1 AND table_name = $2)",
                    rdlt_connector_sqlcore::names::CLEARED_TABLE
                ),
                &[&load_id, &table.as_str()],
            )
            .await
            .map_err(|e| driver_transient(Phase::Write, e))?
            .get(0);
        Ok(cleared)
    }

    /// Where this table's rows land: its stage if it merges, the target
    /// itself otherwise.
    fn write_relation(&self, table: &TableName) -> String {
        match self.catalog.tables.get(table) {
            Some((_, WriteMode::Merge { .. })) => stage_name(&self.pipeline, table),
            // A table nothing ensured cannot merge, so the target is right.
            _ => table.as_str().to_owned(),
        }
    }

    async fn write_inner(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        let load_id = self.load_id.as_str().to_owned();
        self.prepare_for_write(table, &load_id).await?;
        crash_point!(
            "pg.unit.write",
            Err(DestinationError::fatal("injected crash at pg.unit.write"))
        );
        let relation = self.write_relation(table);
        // Wire decisions come from the ENSURED schema's logical types.
        let table_schema = self.catalog.tables.get(table).map(|(schema, _)| schema);
        write::copy_batch(&self.connection, &relation, table_schema, &batch).await
    }

    /// The choreography's first commit step: the idempotence probe by
    /// `(load_id, commit_seq)`. Opens the unit if `write` never did — a
    /// unit with no writes still has to publish state and a receipt, and
    /// the probe must read inside that transaction.
    async fn existing_receipt_inner(
        &mut self,
        load_id: &LoadId,
        commit_seq: u64,
    ) -> Result<Option<CommitReceipt>, DestinationError> {
        // The clear guard is scoped by the SESSION's load, so a unit
        // committed under a different one would consult the wrong guard.
        // The engine gives WAL recovery its own session precisely so this
        // holds; check it rather than assume it.
        if load_id != &self.load_id {
            return Err(fatal(
                Phase::Commit,
                format!(
                    "internal: session opened for load `{}` asked to commit load `{}`",
                    self.load_id, load_id
                ),
            ));
        }
        let session_load = self.load_id.as_str().to_owned();
        self.unit
            .begin_if_closed(&self.connection, &session_load)
            .await?;
        let replayed = self
            .connection
            .query_one(
                &unit_rules::receipt_exists_sql(|n| format!("${n}")),
                &[&load_id.as_str(), &(commit_seq as i64)],
            )
            .await
            .map_err(|e| driver_transient(Phase::Commit, e))?
            .get::<_, i64>(0)
            > 0;
        Ok(replayed.then(|| CommitReceipt {
            load_id: load_id.clone(),
            commit_seq,
        }))
    }

    /// Probe the full-feed stages the planner needs. An up-front probe
    /// matches a lazy per-table check because no stage is written before
    /// the steps that READ it — the script's stage truncations come only
    /// after every table publishes.
    async fn probe_staged_nonempty(&self) -> Result<BTreeSet<TableName>, DestinationError> {
        let mut staged_nonempty = BTreeSet::new();
        for table in staged_probe_targets(&self.catalog.tables, &self.options) {
            let stage = quote_identifier(&stage_name(&self.pipeline, table));
            let staged: bool = self
                .connection
                .query_one(&unit_rules::stage_nonempty_sql(&stage), &[])
                .await
                .map_err(|e| driver_transient(Phase::Commit, e))?
                .get(0);
            if staged {
                staged_nonempty.insert(table.clone());
            }
        }
        Ok(staged_nonempty)
    }

    /// A REDELIVERED unit must be thrown away, not published.
    ///
    /// What a redelivered unit owes depends on WHERE its rows are, and
    /// the answer is inverted between publish paths — so it is the
    /// shared planner's to state, not this executor's to remember:
    /// `unit_rules::replay_disposition(FullLoadPublish::DirectToTarget)`
    /// is `DiscardUnit` on this path.
    ///
    /// Here `write` COPYed the redelivered rows straight into the target
    /// (or, for merge, into the stage) inside the transaction still open,
    /// so committing would land them a SECOND time. Rolling back discards
    /// exactly what this unit wrote and leaves the earlier commit
    /// standing; the framework then returns the stored receipt, because
    /// from the caller's side the unit did commit — it just committed the
    /// first time.
    ///
    /// The single-unit marks are still applied: a full-feed unit whose
    /// outcome the client never learned still counts against the
    /// discipline.
    async fn replay_inner(&mut self, meta: &CommitMeta) -> Result<(), DestinationError> {
        debug_assert_eq!(
            unit_rules::replay_disposition(FullLoadPublish::DirectToTarget),
            unit_rules::ReplayDisposition::DiscardUnit,
        );
        let staged_nonempty = self.probe_staged_nonempty().await?;
        let cleared = self.cleared_union();
        let script = plan_commit(
            &self.catalog.tables,
            &self.options,
            &self.commit_context(true, &staged_nonempty, &cleared),
        )
        .map_err(|e| fatal(Phase::Commit, e))?;
        let _ = meta;
        self.unit.rollback(&self.connection).await;
        self.single_unit_done.extend(script.marks);
        Ok(())
    }

    /// The fresh-unit publish: plan, execute, commit the unit transaction,
    /// then promote the once-per-load guards.
    async fn publish_inner(
        &mut self,
        meta: &CommitMeta,
    ) -> Result<CommitReceipt, DestinationError> {
        let receipt = CommitReceipt {
            load_id: meta.load_id.clone(),
            commit_seq: meta.commit_seq,
        };
        let roots: BTreeMap<TableName, TableName> = self
            .catalog
            .tables
            .keys()
            .map(|table| (table.clone(), self.catalog.root_of(table)))
            .collect();
        let staged_nonempty = self.probe_staged_nonempty().await?;

        // The planner owns every decision + the ordering; this session
        // executes.
        let cleared = self.cleared_union();
        let script = plan_commit(
            &self.catalog.tables,
            &self.options,
            &self.commit_context(false, &staged_nonempty, &cleared),
        )
        .map_err(|e| fatal(Phase::Commit, e))?;

        // Narrowed from "before BEGIN" to "before the first publish step":
        // the unit transaction is already open and already holds this
        // unit's rows, so this is the edge where a crash must leave them
        // invisible.
        crash_point!(
            "pg.publish.begin",
            Err(DestinationError::fatal(
                "injected crash at pg.publish.begin"
            ))
        );
        for step in &script.steps {
            executor::run_step(
                &self.connection,
                &self.pipeline,
                &self.catalog,
                &self.options,
                &roots,
                meta,
                step,
            )
            .await?;
        }

        // The canonical redelivery window: on a fresh unit everything is
        // published in ONE server-side transaction, so a crash at either
        // edge of the commit must replay idempotently — the injected error
        // models the client dying without learning the outcome. A replay
        // unit only rolls back and carries no receipt/state edge, so the
        // crash point stays confined to this fresh path.
        crash_point!(
            "pg.tx.commit",
            Err(DestinationError::fatal("injected crash at pg.tx.commit"))
        );
        let open = self.unit.commit(&self.connection).await?;
        // The other half of the redelivery window, and the sharper half:
        // the transaction is COMMITTED and durable, and the client dies
        // before it can act on that fact. `pg.tx.commit` above covers "the
        // commit may or may not have landed"; here it definitely landed, so
        // recovery MUST find the published unit and return its receipt
        // instead of re-publishing. The clear/mark promotions below have
        // not applied yet, so replay has to reconstruct them from the
        // durable state alone — exactly the property exactly-once rests on.
        crash_point!(
            "pg.tx.acked",
            Err(DestinationError::fatal("injected crash at pg.tx.acked"))
        );
        // Applied only after the unit's transaction committed (a
        // rolled-back unit never counts).
        self.cleared_targets.extend(open.cleared);
        self.single_unit_done.extend(script.marks);
        Ok(receipt)
    }
}

#[async_trait]
impl Backend for Load {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        // Same discipline as `write`: this runs DDL, and once a unit is
        // open that DDL joins the unit transaction. An error must abandon
        // the unit rather than leave every later statement failing with
        // 25P02.
        let result = self
            .catalog
            .ensure(
                &self.connection,
                &self.pipeline,
                &self.options,
                schema,
                mode,
            )
            .await;
        if result.is_err() {
            self.unit.rollback(&self.connection).await;
        }
        result
    }

    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        let result = self.write_inner(table, batch).await;
        if result.is_err() {
            self.unit.rollback(&self.connection).await;
        }
        result
    }

    async fn existing_receipt(
        &mut self,
        load_id: &LoadId,
        commit_seq: u64,
    ) -> Result<Option<CommitReceipt>, DestinationError> {
        match self.existing_receipt_inner(load_id, commit_seq).await {
            Ok(receipt) => Ok(receipt),
            Err(e) => {
                self.unit.rollback(&self.connection).await;
                Err(e)
            }
        }
    }

    async fn replay(
        &mut self,
        meta: &CommitMeta,
        _receipt: &CommitReceipt,
    ) -> Result<(), DestinationError> {
        match self.replay_inner(meta).await {
            Ok(()) => Ok(()),
            Err(e) => {
                self.unit.rollback(&self.connection).await;
                Err(e)
            }
        }
    }

    async fn publish(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        match self.publish_inner(&meta).await {
            Ok(receipt) => Ok(receipt),
            Err(e) => {
                self.unit.rollback(&self.connection).await;
                Err(e)
            }
        }
    }

    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        let row = self
            .connection
            .query_opt(
                &format!(
                    "SELECT doc FROM {} WHERE pipeline = $1",
                    rdlt_connector_sqlcore::names::STATE_TABLE
                ),
                &[&pipeline.as_str()],
            )
            .await
            .map_err(|e| driver_transient(Phase::Commit, e))?;
        match row {
            Some(row) => {
                let document: String = row.get(0);
                Ok(Some(
                    serde_json::from_str(&document).map_err(|e| fatal(Phase::Commit, e))?,
                ))
            }
            None => Ok(None),
        }
    }
}
