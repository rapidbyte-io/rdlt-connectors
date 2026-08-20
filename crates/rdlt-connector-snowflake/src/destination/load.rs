//! The load: one session's conversation with the service, as the sdk's
//! `Backend`.
//!
//! The protocol is the shared one every SQL destination runs — the plan
//! comes from sqlcore, this executes it — under the service constraint
//! the unit module owns: schema work strictly before the unit opens,
//! and the unit itself pure DML on a guarded executor.
//!
//! The sdk session drives the hooks in a fixed order (`existing_receipt`
//! always precedes `replay` or `publish`), and every hook keeps the
//! exact error-path disposition generation 1's single `commit` had:
//! which failures roll the unit back and discard staged parts, and
//! which propagate bare, is recorded protocol behavior — not style.

use super::parts;
use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use rdlt_connector_sdk::destination::Backend;
use rdlt_connector_sdk::spi::core::{
    commit::WriteMode, id::LoadId, id::PipelineId, id::TableName, schema::TableSchema,
    state::StateDoc,
};
use rdlt_connector_sdk::spi::{
    arrow::RecordBatch, core::commit::CommitMeta, core::commit::CommitReceipt,
    error::DestinationError,
};
use rdlt_connector_sqlcore::plan::scope_replace_sql;
use rdlt_connector_sqlcore::protocol::unit as protocol;
use rdlt_connector_sqlcore::{
    CommitContext, FullLoadPublish, MergeArm, MergeDialect as _, Step, build_merge_plan,
    column_list_with, insert_select_sql, plan_commit, prepare_target, render_arm,
    staged_probe_targets,
};

use super::catalog::{self, Catalog};
use super::client::{self, Executor};
use super::config::Config;
use super::dialect::Dialect;
use super::encode;
use super::stage::{self, Part, Stage};
use super::unit::{DmlOnly, Unit};
use super::{ddl, ddl::quote};

/// Append and Replace publish straight into their targets inside the
/// unit transaction — no staging twin, nothing written twice. Merge
/// alone stages, because its arms join delivered rows against the
/// target.
const PUBLISH: FullLoadPublish = FullLoadPublish::DirectToTarget;

/// The column a COPY result reports each file's loaded rowcount in.
const COPY_ROWS_LOADED: &str = "rows_loaded";

/// An injected crash as a VALUE, for the two unit-edge points: a crash
/// there has cleanup to run first (abandon the transaction, drop the
/// staged parts), and the macro's early return would leave the session
/// holding an open transaction the test then blames on the protocol.
#[cfg(feature = "failpoints")]
fn crash_at(name: &str) -> Option<DestinationError> {
    rdlt_connector_sdk::spi::core::failpoint::fail::fail_point!(name, |_| {
        Some(DestinationError::fatal(format!("injected crash at {name}")))
    });
    None
}

#[cfg(not(feature = "failpoints"))]
fn crash_at(_name: &str) -> Option<DestinationError> {
    None
}

/// One session's system IO — `connect` (`super::connector`) opens one,
/// the sdk session drives it. Public only as the connector's associated
/// `Backend` type; everything inside is crate-internal.
pub struct Load {
    pub(super) config: Config,
    pub(super) executor: Box<dyn Executor>,
    pub(super) pipeline: PipelineId,
    pub(super) load_id: LoadId,
    /// The catalog image, read once per table per session.
    pub(super) catalog: Catalog,
    /// Ensured tables, each with the mode it was ensured under.
    pub(super) tables: BTreeMap<TableName, (TableSchema, WriteMode)>,
    /// Targets cleared by units that COMMITTED — the once-per-load
    /// Replace guard's durable half.
    pub(super) cleared: BTreeSet<TableName>,
    /// Targets cleared inside the CURRENT unit: promoted into `cleared`
    /// at commit, dropped at rollback — a rolled-back DELETE cleared
    /// nothing, and forgetting that would let a later unit skip a clear
    /// the server never saw.
    pub(super) cleared_in_unit: BTreeSet<TableName>,
    /// Clears a mid-unit ensure's rollback took back, still OWED to the
    /// load: re-executed the next time a unit opens. Waiting for "the
    /// next write to that table" instead would not do — the table may
    /// never be written again before publish, and a Replace whose
    /// DELETE silently vanished commits the old rows alongside the new.
    pub(super) reclear_owed: BTreeSet<TableName>,
    /// The commit-unit transaction.
    pub(super) unit: Unit,
    /// Tables whose one full-feed unit has committed — marked only
    /// AFTER the commit; a rolled-back unit never counts.
    pub(super) single_unit_done: BTreeSet<TableName>,
    /// The staging identity: where parts go, locally and remotely.
    pub(super) stage: Stage,
    /// Parts written but not yet loaded, per destination table,
    /// together with the column list they carry (recorded at build
    /// time, where the schema is known — deriving it later from a
    /// stage-table name would be guesswork). The COPY waits for the
    /// commit: one statement names every part a table accumulated,
    /// and the SaaS round trip is the cost that matters.
    pub(super) pending: BTreeMap<TableName, (Vec<String>, Vec<Part>)>,
    /// How large a staged part grows before it is uploaded.
    pub(super) parts: parts::Options,
    /// Parts still accumulating, keyed by DESTINATION table — the same
    /// key `pending` uses, so a part and the COPY that will name it
    /// cannot disagree about which table they belong to.
    pub(super) open: BTreeMap<String, encode::OpenPart>,
    /// Where closed parts are reported. Advisory.
    pub(super) part_events: Option<rdlt_connector_sdk::spi::destination::PartEventFn>,
}

// Debug is the workspace lint's requirement; the executor handle and
// guard sets have no useful rendering, so the minimal form.
impl std::fmt::Debug for Load {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Load")
            .field("pipeline", &self.pipeline)
            .field("load_id", &self.load_id)
            .finish_non_exhaustive()
    }
}

// ---- shared plumbing -------------------------------------------------------

impl Load {
    /// A table's fully-qualified, quoted name. Always three-part: a
    /// changed server-side default must not retarget a pipeline
    /// mid-load.
    fn qualified(&self, table: &str) -> String {
        format!(
            "{}.{}.{}",
            quote(&self.config.database),
            quote(&self.config.schema),
            quote(table)
        )
    }

    /// Everything the planner must treat as already cleared: durable
    /// clears plus the current unit's own.
    fn cleared_union(&self) -> BTreeSet<TableName> {
        self.cleared.union(&self.cleared_in_unit).cloned().collect()
    }

    /// Open the unit if it is closed, and re-execute any clears still
    /// owed from a mid-unit ensure's rollback. Every unit-opening path
    /// comes through here so an owed clear cannot outlive the next
    /// transaction — the re-run happens before any planner consults
    /// `cleared_union`, which then counts the table cleared again.
    async fn open_unit(&mut self) -> Result<(), DestinationError> {
        self.unit.begin_if_closed(&*self.executor).await?;
        for table in std::mem::take(&mut self.reclear_owed) {
            self.clear_target(table).await?;
        }
        Ok(())
    }

    /// Execute one Replace clear: the DELETE, the durable record beside
    /// it in the SAME transaction, and the in-unit mark — rolled back
    /// together or durable together, so the record and the empty target
    /// cannot disagree. The record is sqlcore's contract for a
    /// DirectToTarget destination: a crash-recovery session has fresh
    /// memory, and without it the next write would re-clear a target an
    /// earlier unit of this load already cleared and COMMITTED —
    /// deleting its rows silently. Generation 1 never wrote the record.
    async fn clear_target(&mut self, table: TableName) -> Result<(), DestinationError> {
        let step = Step::ClearTarget {
            table: table.clone(),
        };
        self.execute_step(&prepare_meta(&self.load_id, &self.pipeline), &step)
            .await?;
        let executor = DmlOnly(&*self.executor);
        executor
            .execute(&format!(
                "INSERT INTO {cleared} ({load_col}, {table_col}) VALUES ('{load}', '{t}')",
                cleared = self.qualified(rdlt_connector_sqlcore::names::CLEARED_TABLE),
                load_col = quote("load_id"),
                table_col = quote("table_name"),
                load = encode::sql_literal_body(self.load_id.as_str()),
                t = encode::sql_literal_body(table.as_str()),
            ))
            .await?;
        self.cleared_in_unit.insert(table);
        Ok(())
    }

    /// Read a table's columns unless this session already has.
    async fn observe(&mut self, table: &str) -> Result<(), DestinationError> {
        if self.catalog.is_known(table) {
            return Ok(());
        }
        let columns = catalog::observe_table(
            &*self.executor,
            &self.config.database,
            &self.config.schema,
            table,
        )
        .await?;
        self.catalog.observe(table, columns);
        Ok(())
    }

    /// Close one open part, upload it, and record it for the COPY.
    ///
    /// The upload is where a part becomes real — before it, the bytes
    /// exist only in this process and a crash simply loses them, which
    /// is correct because nothing has claimed they landed.
    async fn close_part(
        &mut self,
        table: &str,
        reason: rdlt_connector_sdk::spi::destination::PartCloseReason,
    ) -> Result<(), DestinationError> {
        let Some(part) = self.open.remove(table) else {
            return Ok(());
        };
        // Asked BEFORE finishing: an empty part has no file worth
        // finalising, and a zero-row part in `pending` would make the
        // COPY name a file the service has nothing to load from.
        if part.rows() == 0 {
            return Ok(());
        }
        let (bytes, rows) = part.finish()?;
        let Self {
            stage,
            executor,
            config,
            ..
        } = self;
        let qualified_stage = format!(
            "{}.{}.{}",
            quote(&config.database),
            quote(&config.schema),
            quote(stage.name())
        );
        let encoded_bytes = bytes.len() as u64;
        let staged = stage
            .put_part(&**executor, &qualified_stage, table, bytes, rows)
            .await?;
        // Reported once UPLOADED — the bytes are final and the size is
        // the file's own, never an estimate.
        if let Some(listener) = &self.part_events {
            listener(rdlt_connector_sdk::spi::destination::PartClosed::new(
                rdlt_connector_sdk::spi::core::id::TableName::new(table),
                encoded_bytes,
                reason,
            ));
        }
        // `or_default` rather than `expect`: the column list is written
        // by `write` on the same path, but a part closed by the memory
        // ceiling can reach here for a table whose entry is a beat
        // behind, and an empty list would be repaired by that write.
        self.pending
            .entry(TableName::from(table))
            .or_default()
            .1
            .push(staged);
        Ok(())
    }

    /// Close EVERY open part — no part spans a commit, because the
    /// COPY names whole staged files and a part still open has none.
    async fn close_all_parts(&mut self) -> Result<(), DestinationError> {
        for table in self.open.keys().cloned().collect::<Vec<_>>() {
            self.close_part(
                &table,
                rdlt_connector_sdk::spi::destination::PartCloseReason::Commit,
            )
            .await?;
        }
        Ok(())
    }

    /// Keep the open parts inside their memory ceiling, closing the
    /// LARGEST first — it is nearest its target, so it is the least
    /// undersized part available.
    async fn enforce_open_budget(&mut self) -> Result<(), DestinationError> {
        loop {
            let total: u64 = self.open.values().map(encode::OpenPart::encoded_len).sum();
            if !self.parts.over_budget(total) {
                return Ok(());
            }
            let Some(largest) = self
                .open
                .iter()
                .max_by_key(|(_, part)| part.encoded_len())
                .map(|(table, _)| table.clone())
            else {
                return Ok(());
            };
            self.close_part(
                &largest,
                rdlt_connector_sdk::spi::destination::PartCloseReason::Budget,
            )
            .await?;
        }
    }

    /// Load every pending part into its table, inside the open unit —
    /// one COPY per table, its loaded rowcount checked against what was
    /// written. Nothing should be able to make those differ, which is
    /// exactly why a difference means an assumption broke and the unit
    /// must not commit on it.
    async fn load_staged_parts(&mut self) -> Result<(), DestinationError> {
        self.close_all_parts().await?;
        let pending = std::mem::take(&mut self.pending);
        for (table, (columns, parts)) in pending {
            if parts.is_empty() {
                continue;
            }
            let sql = stage::copy_sql(
                &self.qualified(table.as_str()),
                &self.qualified(self.stage.name()),
                &columns,
                &parts,
            );
            let written: u64 = parts.iter().map(|part| part.rows).sum();
            let loaded = DmlOnly(&*self.executor)
                .sum_column(&sql, COPY_ROWS_LOADED)
                .await?;
            if loaded != written {
                return Err(DestinationError::fatal(format!(
                    "snowflake: loading `{table}` staged {written} rows in {} part(s) but the \
                     service reported {loaded} loaded; the unit is abandoned rather than \
                     committed short",
                    parts.len()
                )));
            }
        }
        Ok(())
    }

    /// Drop every part this load staged, loaded or not. Best effort —
    /// the parts are dead either way (each is named by exactly one
    /// COPY), and a cleanup failure must not fail a committed load; the
    /// aged reclaim at the next open sweeps the rest.
    async fn discard_staged(&mut self) {
        // The OPEN parts are dropped outright — they exist only in
        // memory and were never claimed to land.
        self.open.clear();
        self.pending.clear();
        let qualified_stage = self.qualified(self.stage.name());
        self.stage.remove(&*self.executor, &qualified_stage).await;
    }

    /// Qualify a rendered DDL statement's object name. The ddl module
    /// renders names quoted but unqualified — qualification belongs to
    /// the session that knows the database and schema — and the
    /// substitution anchors on the statement verb, so it cannot touch a
    /// column or a literal.
    fn qualify_ddl(&self, sql: &str) -> String {
        let prefix = format!(
            "{}.{}.",
            quote(&self.config.database),
            quote(&self.config.schema)
        );
        for verb in [
            "CREATE TABLE IF NOT EXISTS ",
            "CREATE TRANSIENT TABLE IF NOT EXISTS ",
            "ALTER TABLE ",
        ] {
            if let Some(rest) = sql.strip_prefix(verb) {
                return format!("{verb}{prefix}{rest}");
            }
        }
        sql.to_owned()
    }
}

// ---- the commit program ----------------------------------------------------

impl Load {
    /// Execute one planned step. Every statement here is DML by
    /// construction — the planner emits no schema work — and runs
    /// through the guarded executor, which enforces that instead of
    /// assuming it.
    /// Takes `&mut self` although it mutates nothing, and builds its
    /// own guarded executor rather than being handed one. Both are
    /// forced by the same fact: this session holds an open parquet
    /// writer, which is `Send` but never `Sync`, so a `&self` borrow
    /// held across an await would not compile. `&mut self` reborrows
    /// shared inside the body, so the reads below are unaffected.
    async fn execute_step(
        &mut self,
        meta: &CommitMeta,
        step: &Step,
    ) -> Result<(), DestinationError> {
        let executor = &DmlOnly(&*self.executor);
        match step {
            Step::ClearTarget { table } => {
                // The dialect spells this DELETE — TRUNCATE would commit
                // the unit (see the unit module).
                executor
                    .execute(&Dialect.clear_table(&self.qualified(table.as_str())))
                    .await
            }
            Step::UpsertState => {
                let doc = serde_json::to_string(&meta.state).map_err(DestinationError::fatal)?;
                executor
                    .execute(&format!(
                        "MERGE INTO {state} t USING (SELECT '{pipeline}' AS PIPELINE) s \
                         ON t.{pipeline_col} = s.PIPELINE \
                         WHEN MATCHED THEN UPDATE SET {doc_col} = '{doc}' \
                         WHEN NOT MATCHED THEN INSERT ({pipeline_col}, {doc_col}) \
                         VALUES (s.PIPELINE, '{doc}')",
                        state = self.qualified(rdlt_connector_sqlcore::names::STATE_TABLE),
                        pipeline = encode::sql_literal_body(meta.state.pipeline.as_str()),
                        pipeline_col = quote("pipeline"),
                        doc_col = quote("doc"),
                        doc = encode::sql_literal_body(&doc),
                    ))
                    .await
            }
            Step::InsertReceipt => {
                executor
                    .execute(&format!(
                        "INSERT INTO {commits} ({load_col}, {seq_col}) VALUES ('{load}', {seq})",
                        commits = self.qualified(rdlt_connector_sqlcore::names::COMMITS_TABLE),
                        load_col = quote("load_id"),
                        seq_col = quote("commit_seq"),
                        load = encode::sql_literal_body(meta.load_id.as_str()),
                        seq = meta.commit_seq,
                    ))
                    .await
            }
            Step::InsertSelect { table } => {
                let (schema, _) = &self.tables[table];
                executor
                    .execute(&insert_select_sql(
                        &self.qualified(table.as_str()),
                        &column_list_with(schema, quote),
                        &self.qualified(&ddl::stage_name(self.pipeline.as_str(), table)),
                    ))
                    .await
            }
            Step::ScopeReplace { table, scope } => {
                executor
                    .execute(&scope_replace_sql(
                        &Dialect,
                        &self.qualified(table.as_str()),
                        &self.qualified(&ddl::stage_name(self.pipeline.as_str(), table)),
                        scope,
                    ))
                    .await
            }
            Step::MergeArm { table, arm } => {
                for sql in self.merge_statements(table, arm)? {
                    if let Err(e) = executor.execute(&sql).await {
                        return Err(self.explain_merge_failure(table, e));
                    }
                }
                Ok(())
            }
            Step::TruncateStage { table } => {
                executor
                    .execute(&Dialect.clear_table(
                        &self.qualified(&ddl::stage_name(self.pipeline.as_str(), table)),
                    ))
                    .await
            }
        }
    }

    /// Render one merge arm. Every decision is the planner's; only the
    /// spelling is the dialect's. Built here to keep the borrow of
    /// `self.tables` contained.
    fn merge_statements(
        &self,
        table: &TableName,
        arm: &MergeArm,
    ) -> Result<Vec<String>, DestinationError> {
        let (schema, mode) = self.tables.get(table).ok_or_else(|| {
            DestinationError::fatal(format!(
                "snowflake: merge arm planned for unknown `{table}`"
            ))
        })?;
        let WriteMode::Merge { key } = mode else {
            return Err(DestinationError::fatal(format!(
                "snowflake: merge arm planned for non-merge table `{table}`"
            )));
        };
        let roots = protocol::roots_of(&self.tables);
        let root = roots.get(table).unwrap_or(table).clone();
        let pipeline = self.pipeline.as_str();
        // Locals, because the plan borrows every one of them.
        let target = self.qualified(table.as_str());
        let stage_table = self.qualified(&ddl::stage_name(pipeline, table));
        let columns = column_list_with(schema, quote);
        let plan = build_merge_plan(
            &Dialect,
            &self.config.options,
            table,
            schema,
            key,
            &target,
            &stage_table,
            &columns,
            &root,
            self.qualified(&ddl::stage_name(pipeline, &root)),
            self.tables.get(&root).map(|(s, _)| s),
        );
        Ok(render_arm(&plan, arm))
    }

    /// Exchange a duplicate-merge-key failure for the SHARED diagnosis,
    /// recognised by structured CODE, never by message text — the
    /// wording is the service's to change, and the diagnosis names the
    /// code rather than carrying the discarded error. Everything else
    /// passes through: replacing an unrelated error with merge advice
    /// would send an operator hunting a duplicate that is not there.
    fn explain_merge_failure(
        &self,
        table: &TableName,
        error: DestinationError,
    ) -> DestinationError {
        let key = match self.tables.get(table) {
            Some((_, WriteMode::Merge { key })) => key.as_slice(),
            _ => &[],
        };
        match merge_diagnosis(table.as_str(), key, client::code_in(&error).as_deref()) {
            Some(diagnosis) => DestinationError::fatal(diagnosis),
            None => error,
        }
    }
}

// ---- the Backend hooks -----------------------------------------------------

#[async_trait]
impl Backend for Load {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        let table = schema.table.as_str().to_owned();
        self.observe(&table).await?;
        if matches!(mode, WriteMode::Merge { .. }) {
            let stage_table = ddl::stage_name(self.pipeline.as_str(), &schema.table);
            self.observe(&stage_table).await?;
        }

        let previous = self.tables.get(&schema.table).map(|(s, _)| s.clone());
        let statements = ddl::table_ddl_stmts(
            self.pipeline.as_str(),
            schema,
            mode,
            self.config.table_type,
            previous.as_ref(),
            &self.catalog,
        );
        // Phase 2 is rendered HERE, before anything executes: its scd2
        // validity ALTERs are DDL like any other, and the unit gate
        // below must see ALL owed schema work — gating on phase 1 alone
        // would let a validity-only ensure auto-commit an open unit.
        // (Rendering against the pre-`record_created` image is
        // equivalent: validity columns never appear in
        // `schema.columns`, so recording them cannot change what phase
        // 2 finds missing.)
        let merge_statements =
            ddl::merge_ensure_stmts(&self.config.options, schema, mode, &self.catalog)
                .map_err(DestinationError::fatal)?;
        // The engine legitimately ensures MID-UNIT when a source's
        // schema evolves between batches. DDL here auto-commits the
        // open transaction — publishing a partial unit with no receipt —
        // so when real schema work is owed while a unit is open, the
        // unit is deliberately ENDED first, by ROLLBACK: staged parts
        // are FILES and survive (an upload rides outside transaction
        // semantics), `pending` stays valid, and the only transactional
        // work a unit holds before publish is a Replace clear, whose
        // rolled-back DELETEs move to `reclear_owed` and re-run when
        // the unit next opens (`open_unit`). Generation 1 had no
        // handling at all — a debug build panicked on an assertion here
        // and a release build silently committed the partial unit.
        // Found by this rewrite's review; pinned live by
        // `a_column_added_mid_unit_keeps_its_data`.
        if (!statements.is_empty() || !merge_statements.is_empty()) && self.unit.is_open() {
            self.unit.rollback(&*self.executor).await;
            self.reclear_owed
                .extend(std::mem::take(&mut self.cleared_in_unit));
        }
        for (sql, kind) in statements {
            let result = self.executor.execute(&self.qualify_ddl(&sql)).await;
            // A widen is the one phase-1 statement the service can
            // refuse for a TYPE reason (cross-type; in-place
            // VARCHAR-length/NUMBER-precision widens succeed) — and
            // unlike `explain_merge_failure`, which REPLACES its error
            // with a structured-code diagnosis because the discarded
            // wording is the service's to change, this ENRICHES: the
            // widen failure's own wording is load-bearing (it names the
            // column and the attempted type), so it stays, with the
            // manual-migration path appended.
            if let (Err(e), ddl::StmtKind::Widen) = (&result, kind) {
                return Err(DestinationError::fatal(format!(
                    "{e}: the service widens in place only VARCHAR length and NUMBER \
                     precision; a cross-type change needs a manual migration — \
                     add a new column, cast-copy, then swap"
                )));
            }
            result?;
        }
        // Fold the applied work into the image so a re-ensure at the
        // same schema version emits nothing.
        self.catalog.record_created(
            &table,
            &schema
                .columns
                .iter()
                .map(|c| c.name.clone())
                .collect::<Vec<_>>(),
        );
        if matches!(mode, WriteMode::Merge { .. }) {
            // The stage leg folds in too. Left "read, absent", a later
            // same-session evolution SKIPPED the stage's ADD COLUMN
            // (its re-rendered CREATE IF NOT EXISTS is a no-op on the
            // service), so the COPY into the stage named a column the
            // table never gained. Inherited from generation 1; pinned
            // live by the merge-mode mid-unit widening cell.
            let mut stage_columns: Vec<String> =
                schema.columns.iter().map(|c| c.name.clone()).collect();
            stage_columns.push(rdlt_connector_sqlcore::names::ARRIVAL_COL.to_owned());
            self.catalog.record_created(
                &ddl::stage_name(self.pipeline.as_str(), &schema.table),
                &stage_columns,
            );
        }

        for sql in merge_statements {
            self.executor.execute(&self.qualify_ddl(&sql)).await?;
            if let Some(column) = column_of_add(&sql) {
                self.catalog.record_column(&table, &column);
            }
        }

        self.tables
            .insert(schema.table.clone(), (schema.clone(), mode.clone()));
        Ok(())
    }

    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        // The sdk session refuses un-ensured writes before this point;
        // the lookup is needed for the schema and mode regardless, so
        // the refusal stays as its defensive twin.
        let Some((schema, mode)) = self.tables.get(table).cloned() else {
            return Err(DestinationError::fatal(format!(
                "snowflake: `{table}` was written before it was ensured"
            )));
        };
        self.open_unit().await?;

        // Merge rows land in the STAGE table — the arms join delivered
        // rows against the target, and rows written straight there
        // would be both sides of the join.
        let destination_table = if matches!(mode, WriteMode::Merge { .. }) {
            ddl::stage_name(self.pipeline.as_str(), table)
        } else {
            table.as_str().to_owned()
        };

        // Replace clears its target once per load, inside the unit,
        // ahead of the first row. The planner owns the decision; this
        // runs what it returns.
        let no_stages = BTreeSet::new();
        let empty_done = BTreeSet::new();
        let cleared = self.cleared_union();
        let steps = prepare_target(
            &self.tables,
            &CommitContext {
                replayed: false,
                load_committed_before: false,
                single_unit_done: &empty_done,
                staged_nonempty: &no_stages,
                full_load_publish: PUBLISH,
                cleared_targets: &cleared,
            },
            table,
        );
        for step in steps {
            match step {
                Step::ClearTarget { table } => self.clear_target(table).await?,
                other => {
                    self.execute_step(&prepare_meta(&self.load_id, &self.pipeline), &other)
                        .await?;
                }
            }
        }

        // Empty batches stage nothing — a zero-row part buys only a
        // file the COPY reads nothing from. Safe ONLY after the steps
        // above: a Replace still clears, the unit still commits the
        // position, and the next run does not re-read.
        let rows = batch.num_rows() as u64;
        if rows == 0 {
            return Ok(());
        }

        // The rows join the table's OPEN part, which spans as many
        // writes as `parts` allows before it is uploaded. A parquet
        // file holds one schema, so a widened projection closes the
        // part first rather than trying to grow into it.
        let key = destination_table.to_string();
        if self
            .open
            .get(&key)
            .is_some_and(|part| part.shape_differs(&schema, &batch))
        {
            self.close_part(
                &key,
                rdlt_connector_sdk::spi::destination::PartCloseReason::Schema,
            )
            .await?;
        }
        match self.open.get_mut(&key) {
            Some(part) => part.append(&schema, &batch)?,
            None => {
                self.open
                    .insert(key.clone(), encode::OpenPart::begin(&schema, &batch)?);
            }
        }
        let part = self.open.get(&key).expect("just opened");
        let (encoded, open_for) = (part.encoded_len(), part.open_for_secs());
        if self.parts.should_roll(encoded, open_for) {
            let reason = if self.parts.target_bytes.is_some_and(|t| encoded >= t.max(1)) {
                rdlt_connector_sdk::spi::destination::PartCloseReason::Target
            } else {
                rdlt_connector_sdk::spi::destination::PartCloseReason::Time
            };
            self.close_part(&key, reason).await?;
        }

        // The column list follows the LATEST write's schema, not the
        // first's: evolution is additive, so the newest set is a
        // superset covering every earlier part (whose files simply lack
        // the added column and load NULL for it). Capturing only the
        // first write's list — generation 1's shape — silently dropped
        // a mid-unit added column's values for the whole unit: the COPY
        // projected the narrower list, row counts still matched, and
        // nothing errored. Found by review of this rewrite; pinned live
        // by `a_column_added_mid_unit_keeps_its_data`.
        let entry = self
            .pending
            .entry(TableName::from(destination_table.as_str()))
            .or_default();
        entry.0 = schema.columns.iter().map(|c| c.name.clone()).collect();
        self.enforce_open_budget().await
    }

    async fn existing_receipt(
        &mut self,
        load_id: &LoadId,
        commit_seq: u64,
    ) -> Result<Option<CommitReceipt>, DestinationError> {
        // A session opened for one load must not commit another — the
        // receipt it would write is one no recovery could match. The
        // choreography's first commit contact, so the guard lives here,
        // before anything begins.
        if let Some(message) = protocol::load_mismatch(&self.load_id, load_id) {
            return Err(DestinationError::fatal(format!("snowflake: {message}")));
        }
        self.open_unit().await?;

        // The probe's errors propagate BARE — generation 1 rolled
        // nothing back here, and dispositions are protocol behavior.
        let replayed = DmlOnly(&*self.executor)
            .scalar_u64(
                &protocol::receipt_exists_sql(|_| "?".to_owned()),
                &[load_id.as_str(), &commit_seq.to_string()],
            )
            .await?
            > 0;
        Ok(replayed.then(|| CommitReceipt {
            load_id: load_id.clone(),
            commit_seq,
        }))
    }

    async fn replay(
        &mut self,
        _meta: &CommitMeta,
        _receipt: &CommitReceipt,
    ) -> Result<(), DestinationError> {
        // A redelivered unit's rows are already durable; this attempt's
        // copies sit in the still-open transaction. The shared planner
        // owns the answer — on a direct-publish path the disposition is
        // DiscardUnit: abandon the transaction, which drops exactly
        // what this attempt wrote, and remove its staged parts. No
        // single-unit re-marking happens on replay (generation 1
        // computed no plan here and marked nothing — a recorded
        // divergence from the postgres split).
        debug_assert_eq!(
            protocol::replay_disposition(PUBLISH),
            protocol::ReplayDisposition::DiscardUnit,
        );
        self.unit.rollback(&*self.executor).await;
        // This attempt's in-unit clears roll back with it — but the
        // receipt proves a PRIOR incarnation committed this very unit,
        // and by then it had durably cleared every Replace table the
        // unit writes. So the marks PROMOTE instead of dropping:
        // forgetting them would let a later unit of this load re-clear
        // rows the load already committed. (Generation 1's single
        // never-rolled-back set had exactly this observable state.)
        self.cleared
            .extend(std::mem::take(&mut self.cleared_in_unit));
        self.discard_staged().await;
        Ok(())
    }

    async fn publish(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        let receipt = CommitReceipt {
            load_id: meta.load_id.clone(),
            commit_seq: meta.commit_seq,
        };

        // The unit is normally already open — `existing_receipt`
        // precedes publish in the sdk choreography and opens it — but a
        // COPY on a closed session would autocommit per statement, so
        // the open (and any owed re-clear) is asserted rather than
        // assumed.
        self.open_unit().await?;

        // Parts land BEFORE anything asks what the stages hold, and
        // before the publish steps, in the same transaction — the
        // receipt this unit writes claims those rows durable.
        if let Err(e) = self.load_staged_parts().await {
            self.unit.rollback(&*self.executor).await;
            self.cleared_in_unit.clear();
            self.discard_staged().await;
            return Err(e);
        }

        // Which full-feed stages actually hold rows — probed, not
        // remembered: rows may arrive through either ingestion path and
        // the stage table is where both agree. Probe errors propagate
        // bare.
        let mut staged_nonempty = BTreeSet::new();
        for table in staged_probe_targets(&self.tables, &self.config.options) {
            let stage_table = self.qualified(&ddl::stage_name(self.pipeline.as_str(), table));
            let rows = DmlOnly(&*self.executor)
                .scalar_u64(&protocol::stage_nonempty_sql(&stage_table), &[])
                .await?;
            if rows > 0 {
                staged_nonempty.insert(table.clone());
            }
        }

        let cleared = self.cleared_union();
        let script = plan_commit(
            &self.tables,
            &self.config.options,
            &CommitContext {
                replayed: false,
                load_committed_before: false,
                single_unit_done: &self.single_unit_done,
                staged_nonempty: &staged_nonempty,
                full_load_publish: PUBLISH,
                cleared_targets: &cleared,
            },
        )
        .map_err(DestinationError::fatal)?;

        for step in &script.steps {
            if let Err(e) = self.execute_step(&meta, step).await {
                self.unit.rollback(&*self.executor).await;
                self.cleared_in_unit.clear();
                self.discard_staged().await;
                return Err(e);
            }
        }

        // Everything is written, nothing durable — recovery must find
        // the target exactly as before this attempt.
        if let Some(injected) = crash_at("sf.unit.publish") {
            self.unit.rollback(&*self.executor).await;
            self.cleared_in_unit.clear();
            self.discard_staged().await;
            return Err(injected);
        }

        self.unit.commit(&*self.executor).await?;
        // Only now, for both guards: a rolled-back unit neither counts
        // as a full feed nor cleared anything.
        self.single_unit_done.extend(staged_nonempty);
        self.cleared
            .extend(std::mem::take(&mut self.cleared_in_unit));

        // Durable, and the caller is about to be told otherwise — the
        // one crash nothing can undo; recovery must find the receipt
        // and publish nothing.
        if let Some(injected) = crash_at("sf.receipt.visible") {
            self.discard_staged().await;
            return Err(injected);
        }

        // After the commit only: a part removed before its rows are
        // durable is a part no recovery could re-read.
        self.discard_staged().await;
        Ok(receipt)
    }

    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        let sql = format!(
            "SELECT {doc} FROM {state} WHERE {pipeline_col} = ?",
            doc = quote("doc"),
            state = self.qualified(rdlt_connector_sqlcore::names::STATE_TABLE),
            pipeline_col = quote("pipeline"),
        );
        let rows = self
            .executor
            .rows(&sql, &[pipeline.as_str()], &["doc"])
            .await?;
        let Some(row) = rows.into_iter().next() else {
            return Ok(None);
        };
        serde_json::from_str(&row[0])
            .map(Some)
            .map_err(DestinationError::fatal)
    }
}

/// The tables a COMMITTED unit of this load already cleared, from the
/// durable record — the seed for `cleared` at connect. A recovery
/// session's memory is empty; only this read stops its next write from
/// re-clearing (deleting) rows an earlier unit committed.
pub(super) async fn read_cleared_targets(
    executor: &dyn Executor,
    qualified_cleared: &str,
    load_id: &LoadId,
) -> Result<BTreeSet<TableName>, DestinationError> {
    let rows = executor
        .rows(
            &format!(
                "SELECT {col} FROM {qualified_cleared} WHERE {load} = ?",
                col = quote("table_name"),
                load = quote("load_id"),
            ),
            &[load_id.as_str()],
            &["table_name"],
        )
        .await?;
    Ok(rows
        .into_iter()
        .map(|mut row| TableName::from(row.remove(0).as_str()))
        .collect())
}

// ---- helpers ---------------------------------------------------------------

/// The column an `ADD COLUMN` statement adds, so the catalog image can
/// fold it in without a re-read.
fn column_of_add(sql: &str) -> Option<String> {
    let rest = sql.split("ADD COLUMN IF NOT EXISTS ").nth(1)?;
    let name = rest.split_whitespace().next()?;
    Some(name.trim_matches('"').to_owned())
}

/// The shared duplicate-merge-key advice, when the structured code says
/// it applies. Split from the plumbing so the decision — which code,
/// what advice — tests without a live error to carry it.
fn merge_diagnosis(table: &str, key: &[String], code: Option<&str>) -> Option<String> {
    (code? == client::DUPLICATE_ROW_IN_DML).then(|| {
        rdlt_connector_sqlcore::names::duplicate_merge_key_diagnosis(
            table,
            key,
            &format!("Snowflake error {}", client::DUPLICATE_ROW_IN_DML),
        )
    })
}

/// A meta for the write-path prepare steps, which read none of it:
/// `ClearTarget` names its own table, and the step executor takes a
/// `CommitMeta` only because the receipt and state steps need one.
fn prepare_meta(load_id: &LoadId, pipeline: &PipelineId) -> CommitMeta {
    CommitMeta {
        load_id: load_id.clone(),
        commit_seq: 0,
        state: StateDoc::new(pipeline.clone(), ""),
        counters: Default::default(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The duplicate code (and only it) earns the SHARED advice — the
    /// sentence an operator would read on any SQL destination — with
    /// the service's own error kept as the cause.
    #[test]
    fn the_duplicate_key_code_becomes_the_shared_diagnosis() {
        let key = vec!["id".to_string(), "day".to_string()];
        let diagnosis = merge_diagnosis("orders", &key, Some(client::DUPLICATE_ROW_IN_DML))
            .expect("the duplicate code earns advice");
        assert_eq!(
            diagnosis,
            rdlt_connector_sqlcore::names::duplicate_merge_key_diagnosis(
                "orders",
                &key,
                &format!("Snowflake error {}", client::DUPLICATE_ROW_IN_DML)
            )
        );
        assert!(diagnosis.contains("id, day"), "{diagnosis}");
        assert!(diagnosis.contains("delete_insert"), "{diagnosis}");
    }

    /// Any other failure keeps its own error — a wrong diagnosis is
    /// worse than none, because it reads as one.
    #[test]
    fn an_unrelated_failure_keeps_its_own_error() {
        for code in [None, Some("000904"), Some("002003")] {
            assert!(
                merge_diagnosis("orders", &["id".to_string()], code).is_none(),
                "{code:?}"
            );
        }
    }

    // ---- the unit-vs-DDL choreography, driven offline ----------------

    use std::sync::{Arc, Mutex};

    use rdlt_connector_sdk::spi::core::{
        schema::Column, schema::ColumnType, schema::Provenance, types::LogicalType,
    };

    /// A scripted executor recording every statement it is handed.
    struct Recorder(Arc<Mutex<Vec<String>>>);

    #[async_trait]
    impl Executor for Recorder {
        async fn execute(&self, sql: &str) -> Result<(), DestinationError> {
            self.0.lock().expect("lock").push(sql.to_owned());
            Ok(())
        }
        async fn scalar_u64(&self, sql: &str, _: &[&str]) -> Result<u64, DestinationError> {
            self.0.lock().expect("lock").push(sql.to_owned());
            Ok(0)
        }
        async fn sum_column(&self, sql: &str, _: &str) -> Result<u64, DestinationError> {
            self.0.lock().expect("lock").push(sql.to_owned());
            Ok(0)
        }
        async fn rows(
            &self,
            sql: &str,
            _: &[&str],
            _: &[&str],
        ) -> Result<Vec<Vec<String>>, DestinationError> {
            self.0.lock().expect("lock").push(sql.to_owned());
            Ok(Vec::new())
        }
    }

    /// A session over the recorder — the whole Backend choreography runs
    /// against it, no account required.
    fn recorded_load(options: serde_json::Value) -> (Load, Arc<Mutex<Vec<String>>>) {
        use rdlt_connector_sdk::config::Document;
        let mut doc = serde_json::json!({
            "account": "MYORG-MYACCT",
            "user": "LOADER",
            "auth": {"key_pair": {"private_key": "/k.p8"}},
            "database": "DB",
            "schema": "S",
        });
        if let (Some(doc), Some(extra)) = (doc.as_object_mut(), options.as_object()) {
            for (key, value) in extra {
                doc.insert(key.clone(), value.clone());
            }
        }
        let config = Config::from_value(doc).expect("valid");
        let log = Arc::new(Mutex::new(Vec::new()));
        let load = Load {
            config,
            executor: Box::new(Recorder(Arc::clone(&log))),
            pipeline: PipelineId::from("p"),
            load_id: LoadId::from("load-1"),
            catalog: Catalog::default(),
            tables: BTreeMap::new(),
            cleared: BTreeSet::new(),
            cleared_in_unit: BTreeSet::new(),
            reclear_owed: BTreeSet::new(),
            unit: Unit::default(),
            single_unit_done: BTreeSet::new(),
            stage: Stage::new("p", "load-1"),
            pending: BTreeMap::new(),
            parts: parts::Options::default(),
            open: BTreeMap::new(),
            part_events: None,
        };
        (load, log)
    }

    fn table_schema(table: &str, columns: &[&str]) -> TableSchema {
        TableSchema {
            table: TableName::from(table),
            parent: None,
            columns: columns
                .iter()
                .map(|name| Column {
                    name: (*name).to_owned(),
                    column_type: ColumnType::scalar(LogicalType::Int64),
                    nullable: true,
                    provenance: Provenance::Inferred,
                })
                .collect(),
        }
    }

    // ---- the widen-advice wrap: 037 US5 ------------------------------

    /// A recorder that also fails any statement containing a chosen
    /// marker — arms `ensure_table`'s phase-1 loop with a scripted
    /// refusal instead of a live cross-type failure from the service.
    struct FailingOn {
        marker: &'static str,
        log: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl Executor for FailingOn {
        async fn execute(&self, sql: &str) -> Result<(), DestinationError> {
            self.log_sql(sql);
            if sql.contains(self.marker) {
                return Err(DestinationError::fatal(
                    "SQL compilation error: invalid type conversion",
                ));
            }
            Ok(())
        }
        async fn scalar_u64(&self, sql: &str, _: &[&str]) -> Result<u64, DestinationError> {
            self.log_sql(sql);
            Ok(0)
        }
        async fn sum_column(&self, sql: &str, _: &str) -> Result<u64, DestinationError> {
            self.log_sql(sql);
            Ok(0)
        }
        async fn rows(
            &self,
            sql: &str,
            _: &[&str],
            _: &[&str],
        ) -> Result<Vec<Vec<String>>, DestinationError> {
            self.log_sql(sql);
            Ok(Vec::new())
        }
    }

    impl FailingOn {
        fn log_sql(&self, sql: &str) {
            self.log.lock().expect("lock").push(sql.to_owned());
        }
    }

    /// A load wired to [`FailingOn`] in place of the recorder — every
    /// statement is logged, and any containing `marker` is refused.
    fn scripted_load_failing_on(marker: &'static str) -> (Load, Arc<Mutex<Vec<String>>>) {
        let (mut load, _unused) = recorded_load(serde_json::json!({}));
        let log = Arc::new(Mutex::new(Vec::new()));
        load.executor = Box::new(FailingOn {
            marker,
            log: Arc::clone(&log),
        });
        (load, log)
    }

    /// Drive an ensure whose ONLY owed DDL is a cross-type widen: the
    /// catalog already knows `events."ID"`, `previous` narrows it to
    /// Int64, and the new schema widens it to Utf8 — exactly the shape
    /// `ddl::a_widened_column_sets_its_data_type` pins at the renderer;
    /// this drives the SAME rendering through the real async path, so
    /// the phase-1 loop's wrapping is under test, not just the SQL.
    async fn drive_widen_ensure(load: &mut Load) -> Result<(), DestinationError> {
        load.catalog
            .observe("events", ["ID".to_owned()].into_iter().collect());
        load.tables.insert(
            TableName::from("events"),
            (table_schema("events", &["id"]), WriteMode::Append),
        );
        let wide = TableSchema {
            table: TableName::from("events"),
            parent: None,
            columns: vec![Column {
                name: "id".to_owned(),
                column_type: ColumnType::scalar(LogicalType::Utf8),
                nullable: true,
                provenance: Provenance::Inferred,
            }],
        };
        load.ensure_table(&wide, &WriteMode::Append).await
    }

    /// Drive an ensure whose only owed DDL is a plain CREATE (an
    /// unknown table) — the non-widen counterpart, proving the wrap is
    /// gated on `StmtKind::Widen`, not on "phase 1 failed".
    async fn drive_plain_ensure(load: &mut Load) -> Result<(), DestinationError> {
        load.ensure_table(&table_schema("plain", &["id"]), &WriteMode::Append)
            .await
    }

    /// A failing widen carries the manual-migration advice, byte-pinned
    /// and appended after the service's own (verbatim) wording.
    #[tokio::test]
    async fn a_failing_widen_carries_the_manual_path_advice() {
        let (mut load, _log) = scripted_load_failing_on("SET DATA TYPE");
        let err = drive_widen_ensure(&mut load)
            .await
            .expect_err("the scripted executor refuses the cross-type ALTER");
        let text = err.to_string();
        assert!(
            text.contains("widens in place only VARCHAR length and NUMBER precision"),
            "{text}"
        );
        assert!(
            text.contains("add a new column, cast-copy, then swap"),
            "{text}"
        );
        assert!(
            text.contains("SQL compilation error: invalid type conversion"),
            "the service's own wording stays, verbatim: {text}"
        );
    }

    /// A failing non-widen statement stays bare: no advice text, the
    /// service's error passes through untouched.
    #[tokio::test]
    async fn a_failing_plain_statement_stays_bare() {
        let (mut load, _log) = scripted_load_failing_on("CREATE TABLE");
        let err = drive_plain_ensure(&mut load)
            .await
            .expect_err("the scripted executor refuses the CREATE");
        let text = err.to_string();
        assert!(
            !text.contains("manual migration"),
            "a non-widen failure carries no advice: {text}"
        );
        assert!(
            text.contains("SQL compilation error: invalid type conversion"),
            "{text}"
        );
    }

    /// A mid-unit ensure whose ONLY owed DDL is phase 2 — the scd2
    /// validity columns — still ends the unit first. The gate is
    /// computed over every statement the ensure will run: an ALTER
    /// through the unguarded executor auto-commits the partial unit
    /// exactly like a CREATE, and gating on phase 1 alone left this
    /// window open (review round 2's second finding).
    #[tokio::test]
    async fn a_validity_only_ensure_still_ends_the_open_unit_first() {
        let (mut load, log) = recorded_load(serde_json::json!({"merge_strategy": "scd2"}));
        // The image already holds both legs at the data schema, so
        // phase 1 renders nothing; validity columns are the only work.
        load.catalog
            .observe("events", ["ID".to_owned()].into_iter().collect());
        load.catalog.observe(
            &ddl::stage_name("p", &TableName::from("events")),
            ["ID".to_owned(), "__RDLT_ARRIVAL".to_owned()]
                .into_iter()
                .collect(),
        );
        load.unit
            .begin_if_closed(&*load.executor)
            .await
            .expect("begin");

        load.ensure_table(
            &table_schema("events", &["id"]),
            &WriteMode::Merge {
                key: vec!["id".into()],
            },
        )
        .await
        .expect("ensure");

        let log = log.lock().expect("lock");
        let rollback = log
            .iter()
            .position(|s| s == "ROLLBACK")
            .expect("the unit was ended");
        let alter = log
            .iter()
            .position(|s| s.contains("ADD COLUMN"))
            .expect("the validity DDL ran");
        assert!(rollback < alter, "rollback precedes the DDL: {log:?}");
        assert!(
            !log.iter().any(|s| s.contains("CREATE TABLE")),
            "phase 1 was empty by construction: {log:?}"
        );
    }

    /// A clear rolled back by a mid-unit ensure is re-executed when the
    /// unit next OPENS — not when its table is next written, because
    /// that write may never come: here `a` is written once, `b`'s
    /// ensure rolls the unit back, and only the receipt probe touches
    /// the session again before publish. A Replace whose DELETE
    /// silently vanished would commit the previous load's rows
    /// alongside the new ones (review round 2's first finding).
    #[tokio::test]
    async fn a_rolled_back_clear_is_owed_to_the_next_unit_not_the_next_write() {
        let (mut load, log) = recorded_load(serde_json::json!({}));
        load.catalog
            .observe("a", ["ID".to_owned()].into_iter().collect());
        load.tables.insert(
            TableName::from("a"),
            (table_schema("a", &["id"]), WriteMode::Replace),
        );

        // An empty batch still runs the prepare steps: the unit opens
        // and `a`'s Replace DELETE executes inside it.
        let empty = RecordBatch::new_empty(Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, true),
        ])));
        load.write(&TableName::from("a"), empty)
            .await
            .expect("write");
        assert_eq!(load.cleared_in_unit.len(), 1, "the clear is in-unit");

        // `b` arrives mid-unit needing real DDL: the unit ends, and
        // `a`'s clear — rolled back with it — becomes owed.
        load.ensure_table(&table_schema("b", &["id"]), &WriteMode::Append)
            .await
            .expect("ensure");
        assert!(load.cleared_in_unit.is_empty(), "the mark rolled back");
        assert_eq!(load.reclear_owed.len(), 1, "…and is owed");

        // The next unit-opening contact — the receipt probe, first in
        // the publish choreography — re-runs the DELETE.
        let receipt = load
            .existing_receipt(&LoadId::from("load-1"), 1)
            .await
            .expect("probe");
        assert!(receipt.is_none());
        assert!(load.reclear_owed.is_empty(), "the debt is settled");
        assert_eq!(load.cleared_in_unit.len(), 1, "…and marked in-unit again");

        let log = log.lock().expect("lock");
        let deletes: Vec<usize> = log
            .iter()
            .enumerate()
            .filter(|(_, s)| s.starts_with("DELETE FROM"))
            .map(|(i, _)| i)
            .collect();
        assert_eq!(
            deletes.len(),
            2,
            "cleared, rolled back, re-cleared: {log:?}"
        );
        let rollback = log
            .iter()
            .position(|s| s == "ROLLBACK")
            .expect("the mid-unit ensure ended the unit");
        let reopen = log
            .iter()
            .rposition(|s| s == "BEGIN")
            .expect("the unit reopened");
        assert!(
            deletes[0] < rollback && rollback < reopen && reopen < deletes[1],
            "clear → rollback → reopen → re-clear: {log:?}"
        );
    }

    /// The clear's durable record rides in the SAME transaction as its
    /// DELETE — the sqlcore contract for a DirectToTarget destination.
    /// Without it a crash-recovery session (fresh memory) re-clears a
    /// target an earlier unit of this load already cleared and
    /// COMMITTED, silently deleting its rows; generation 1 never wrote
    /// the record.
    #[tokio::test]
    async fn a_clear_writes_its_durable_record_beside_it_in_the_unit() {
        let (mut load, log) = recorded_load(serde_json::json!({}));
        load.catalog
            .observe("a", ["ID".to_owned()].into_iter().collect());
        load.tables.insert(
            TableName::from("a"),
            (table_schema("a", &["id"]), WriteMode::Replace),
        );
        let empty = RecordBatch::new_empty(Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, true),
        ])));
        load.write(&TableName::from("a"), empty)
            .await
            .expect("write");

        let log = log.lock().expect("lock");
        let delete = log
            .iter()
            .position(|s| s.starts_with("DELETE FROM"))
            .expect("the clear ran");
        let record = log
            .iter()
            .position(|s| s.contains("\"_RDLT_CLEARED\""))
            .expect("the durable record ran");
        assert!(delete < record, "record beside its DELETE: {log:?}");
        assert!(
            log[record].contains("'load-1'") && log[record].contains("'a'"),
            "keyed by load and table: {}",
            log[record]
        );
        assert!(
            !log.iter().any(|s| s == "COMMIT"),
            "both still inside the open unit: {log:?}"
        );
    }

    /// An executor whose `rows` answers a canned result — the seed
    /// read's shape.
    struct Canned(Vec<Vec<String>>);

    #[async_trait]
    impl Executor for Canned {
        async fn execute(&self, _: &str) -> Result<(), DestinationError> {
            Ok(())
        }
        async fn scalar_u64(&self, _: &str, _: &[&str]) -> Result<u64, DestinationError> {
            Ok(0)
        }
        async fn sum_column(&self, _: &str, _: &str) -> Result<u64, DestinationError> {
            Ok(0)
        }
        async fn rows(
            &self,
            _: &str,
            _: &[&str],
            _: &[&str],
        ) -> Result<Vec<Vec<String>>, DestinationError> {
            Ok(self.0.clone())
        }
    }

    /// The recovery half: a target the durable record says an earlier
    /// unit cleared is seeded into `cleared` and never cleared again —
    /// the DELETE that would have destroyed the committed rows is not
    /// planned at all.
    #[tokio::test]
    async fn a_durably_cleared_target_seeds_the_guard_and_is_not_cleared_again() {
        let seeded = read_cleared_targets(
            &Canned(vec![vec!["a".to_owned()]]),
            "\"DB\".\"S\".\"_RDLT_CLEARED\"",
            &LoadId::from("load-1"),
        )
        .await
        .expect("seed read");
        assert_eq!(seeded.len(), 1);
        assert!(seeded.contains(&TableName::from("a")));

        let (mut load, log) = recorded_load(serde_json::json!({}));
        load.cleared = seeded;
        load.catalog
            .observe("a", ["ID".to_owned()].into_iter().collect());
        load.tables.insert(
            TableName::from("a"),
            (table_schema("a", &["id"]), WriteMode::Replace),
        );
        let empty = RecordBatch::new_empty(Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, true),
        ])));
        load.write(&TableName::from("a"), empty)
            .await
            .expect("write");
        assert!(
            !log.lock()
                .expect("lock")
                .iter()
                .any(|s| s.starts_with("DELETE FROM")),
            "an already-durable clear must not re-run"
        );
    }

    /// Across units of ONE session: a committed unit's clear promotes,
    /// and a later unit neither re-clears (which would DELETE the
    /// earlier unit's committed rows) nor forgets it cleared.
    #[tokio::test]
    async fn a_committed_units_clear_is_promoted_and_never_rerun() {
        let (mut load, log) = recorded_load(serde_json::json!({}));
        load.catalog
            .observe("a", ["ID".to_owned()].into_iter().collect());
        load.tables.insert(
            TableName::from("a"),
            (table_schema("a", &["id"]), WriteMode::Replace),
        );
        let empty = || {
            RecordBatch::new_empty(Arc::new(arrow_schema::Schema::new(vec![
                arrow_schema::Field::new("id", arrow_schema::DataType::Int64, true),
            ])))
        };
        load.write(&TableName::from("a"), empty())
            .await
            .expect("unit 1");
        load.publish(CommitMeta {
            load_id: LoadId::from("load-1"),
            commit_seq: 1,
            state: StateDoc::new(PipelineId::from("p"), ""),
            counters: Default::default(),
        })
        .await
        .expect("unit 1 commits");
        assert!(load.cleared.contains(&TableName::from("a")), "promoted");

        load.write(&TableName::from("a"), empty())
            .await
            .expect("unit 2");
        let log = log.lock().expect("lock");
        assert_eq!(
            log.iter().filter(|s| s.starts_with("DELETE FROM")).count(),
            1,
            "one load, one clear: {log:?}"
        );
    }

    /// The COPY shortfall guard: fewer rows loaded than staged abandons
    /// the unit with the frozen wording — never a short commit.
    #[tokio::test]
    async fn a_short_copy_load_abandons_the_unit_rather_than_committing() {
        let (mut load, log) = recorded_load(serde_json::json!({}));
        load.tables.insert(
            TableName::from("a"),
            (table_schema("a", &["id"]), WriteMode::Append),
        );
        load.pending.insert(
            TableName::from("a"),
            (
                vec!["id".to_owned()],
                vec![Part {
                    tail: "seg/tab/00000000.parquet".to_owned(),
                    rows: 3,
                }],
            ),
        );
        let err = load
            .publish(CommitMeta {
                load_id: LoadId::from("load-1"),
                commit_seq: 1,
                state: StateDoc::new(PipelineId::from("p"), ""),
                counters: Default::default(),
            })
            .await
            .expect_err("the recorder loads 0 of 3 staged rows");
        let rendered = format!("{err}");
        assert!(
            rendered.contains("staged 3 rows") && rendered.contains("reported 0 loaded"),
            "{rendered}"
        );
        assert!(
            rendered.contains("abandoned rather than committed short"),
            "{rendered}"
        );
        let log = log.lock().expect("lock");
        let rollback = log
            .iter()
            .position(|s| s == "ROLLBACK")
            .expect("the unit is abandoned");
        let remove = log
            .iter()
            .position(|s| s.starts_with("REMOVE @"))
            .expect("the staged parts are discarded");
        assert!(rollback < remove, "abandon, then discard: {log:?}");
    }
}
