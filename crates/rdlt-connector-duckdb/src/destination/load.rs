//! One load session's system IO — the [`Backend`] behind the sdk
//! choreography, mapping generation 1's ONE-transaction commit onto
//! the framework hooks.
//!
//! Staging: rows land in TEMP tables on THIS session's connection.
//! Temp tables die with the connection, so a fresh open reclaims a
//! crashed predecessor's stage for free — that is the whole orphan
//! story. The commit is one DuckDB transaction that publishes staged
//! rows, persists state, and inserts the receipt together; the
//! PLANNER (sqlcore `plan_commit`) owns every decision and the
//! ordering.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use duckdb::params;
use rdlt_connector_sdk::destination::Backend;
use rdlt_connector_sdk::spi::core::crash_point;
use rdlt_connector_sdk::spi::core::{
    CommitMeta, CommitReceipt, LoadId, PipelineId, StateDoc, TableName, TableSchema, WriteMode,
};
use rdlt_connector_sdk::spi::{DestinationError, RecordBatch};
use rdlt_connector_sqlcore::protocol::{
    CommitContext, FullLoadPublish, Step, build_merge_plan, plan_commit, render_arm,
    staged_probe_targets, unit,
};
use rdlt_connector_sqlcore::{DestinationOptions, MergeDialect as _, column_list, names, root_of};

use super::catalog;
use super::client::{
    Db, classify, index_dependency_diagnosis, is_constraint_violation, is_index_dependency_error,
};
use super::dialect::DuckDialect;
use super::schema::{merge_ddl, quote, stage_name, table_ddl};

/// The meta tables, ensured at open. The SCHEMAS are the persisted
/// format — column-identical with every database generation 1 wrote
/// (gen 1's literal differed only in insignificant whitespace); the
/// names here are the same tables sqlcore's STATE_TABLE/COMMITS_TABLE
/// constants spell in every query. Forward-frozen from here.
const META_DDL: &str = "CREATE TABLE IF NOT EXISTS _rdlt_state (pipeline VARCHAR PRIMARY KEY, doc VARCHAR);\nCREATE TABLE IF NOT EXISTS _rdlt_commits (\n    load_id VARCHAR, commit_seq BIGINT, PRIMARY KEY (load_id, commit_seq));";

/// The session state.
pub struct Load {
    conn: duckdb::Connection,
    /// Held for the CLAIM, not for queries: the session's cloned
    /// connection is its own database handle, so without this the
    /// registry would free the path while the session still writes —
    /// and a second open would truncate this session's WAL (031
    /// round-2 catch: the claim must outlive every session).
    _db: Db,
    options: DestinationOptions,
    /// Everything this session ensured — the planner's world.
    tables: BTreeMap<TableName, (TableSchema, WriteMode)>,
    /// The schema each table LAST ensured this session — what makes
    /// the widen a within-run rule.
    previous: BTreeMap<TableName, TableSchema>,
    /// FULL-FEED tables (merge_scope-scoped, or scd2 absent:retire)
    /// whose single commit unit has run.
    single_unit_done: BTreeSet<TableName>,
}

impl std::fmt::Debug for Load {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Load").finish_non_exhaustive()
    }
}

impl Load {
    /// Open one session: a cloned connection (its own temp-table
    /// catalog — a dead session's stages are unreachable), the meta
    /// tables ensured.
    pub(super) fn open(db: &Db, options: DestinationOptions) -> Result<Self, DestinationError> {
        let conn = db.session()?;
        conn.execute_batch(META_DDL).map_err(classify)?;
        Ok(Self {
            conn,
            _db: db.clone(),
            options,
            tables: BTreeMap::new(),
            previous: BTreeMap::new(),
            single_unit_done: BTreeSet::new(),
        })
    }

    /// Probe whether a `(load, seq)` receipt exists — runs on
    /// whatever connection view the caller holds (in or out of a
    /// transaction).
    fn receipt_recorded(
        conn: &duckdb::Connection,
        load: &LoadId,
        seq: u64,
    ) -> Result<bool, DestinationError> {
        let sql = unit::receipt_exists_sql(|_| "?".into());
        let count: u64 = conn
            .query_row(&sql, params![load.as_str(), ledger_seq(seq)?], |row| {
                row.get(0)
            })
            .map_err(classify)?;
        Ok(count > 0)
    }

    /// The full-feed stage probes the planner consumes, evaluated
    /// up front — nothing writes a stage after this point in a
    /// commit, so eager equals the old lazy per-table check.
    fn staged_nonempty(&self) -> Result<BTreeSet<TableName>, DestinationError> {
        let mut nonempty = BTreeSet::new();
        for table in staged_probe_targets(&self.tables, &self.options) {
            let sql = unit::stage_nonempty_sql(&quote(&stage_name(table.as_str())));
            let filled: bool = self
                .conn
                .query_row(&sql, [], |row| row.get(0))
                .map_err(classify)?;
            if filled {
                nonempty.insert(table.clone());
            }
        }
        Ok(nonempty)
    }

    /// Run one planned step. Every statement classifies; the merge
    /// arms re-render from the plan each time.
    fn run_step(&self, step: &Step, meta: &CommitMeta) -> Result<(), DestinationError> {
        match step {
            Step::ClearTarget { table } => {
                let sql = DuckDialect.clear_table(&quote(table.as_str()));
                self.conn.execute_batch(&sql).map_err(classify)
            }
            Step::InsertSelect { table } => {
                let (schema, _) = &self.tables[table];
                let sql = rdlt_connector_sqlcore::protocol::insert_select_sql(
                    &quote(table.as_str()),
                    &column_list(schema),
                    &quote(&stage_name(table.as_str())),
                );
                self.conn.execute_batch(&sql).map_err(classify)
            }
            Step::ScopeReplace { table, scope } => {
                let sql = rdlt_connector_sqlcore::plan::scope_replace_sql(
                    &DuckDialect,
                    &quote(table.as_str()),
                    &quote(&stage_name(table.as_str())),
                    scope,
                );
                self.conn.execute_batch(&sql).map_err(classify)
            }
            Step::MergeArm { table, arm } => {
                let (schema, mode) = &self.tables[table];
                let WriteMode::Merge { key } = mode else {
                    return Err(DestinationError::fatal(format!(
                        "internal: merge arm planned for non-merge table `{table}`"
                    )));
                };
                let root = root_of(&self.tables, table);
                let root_schema = self.tables.get(&root).map(|(s, _)| s);
                let target_sql = quote(table.as_str());
                let stage_sql = quote(&stage_name(table.as_str()));
                let cols_sql = column_list(schema);
                let plan = build_merge_plan(
                    &DuckDialect,
                    &self.options,
                    table,
                    schema,
                    key,
                    &target_sql,
                    &stage_sql,
                    &cols_sql,
                    &root,
                    quote(&stage_name(root.as_str())),
                    root_schema,
                );
                for sql in render_arm(&plan, arm) {
                    self.conn.execute_batch(&sql).map_err(classify)?;
                }
                Ok(())
            }
            Step::TruncateStage { table } => {
                let sql = DuckDialect.clear_table(&quote(&stage_name(table.as_str())));
                self.conn.execute_batch(&sql).map_err(classify)
            }
            Step::UpsertState => {
                let doc = serde_json::to_string(&meta.state)
                    .map_err(|e| DestinationError::fatal(e.to_string()))?;
                self.conn
                    .execute(
                        &format!(
                            "INSERT OR REPLACE INTO {} VALUES (?, ?)",
                            names::STATE_TABLE
                        ),
                        params![meta.state.pipeline.as_str(), doc],
                    )
                    .map(|_| ())
                    .map_err(classify)
            }
            // A duplicate receipt here is the idempotence-anomaly
            // signal — fail loudly, never absorb.
            Step::InsertReceipt => self
                .conn
                .execute(
                    &format!("INSERT INTO {} VALUES (?, ?)", names::COMMITS_TABLE),
                    params![meta.load_id.as_str(), ledger_seq(meta.commit_seq)?],
                )
                .map(|_| ())
                .map_err(classify),
        }
    }

    /// Plan and execute one commit unit inside ONE transaction — the
    /// probes included (031 review A2/D7): the replay probe, the
    /// load-committed probe, and the stage probes all read under the
    /// SAME snapshot the steps run in, so nothing can move between the
    /// decision and its execution. This is generation 1's shape.
    fn run_commit(&mut self, meta: &CommitMeta) -> Result<(), DestinationError> {
        self.conn.execute_batch("BEGIN").map_err(classify)?;
        match self.commit_in_tx(meta) {
            Ok(marks) => {
                self.conn.execute_batch("COMMIT").map_err(classify)?;
                // Marks apply ONLY after the transaction committed; the
                // planner re-emits them on replay, so a crash-recovery
                // replay re-marks the single-unit discipline.
                self.single_unit_done.extend(marks);
                Ok(())
            }
            Err(e) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(e)
            }
        }
    }

    /// Everything between BEGIN and COMMIT: probes, plan, steps, and
    /// the crash point. Returns the planner's marks for `run_commit`
    /// to apply after the COMMIT lands.
    fn commit_in_tx(&self, meta: &CommitMeta) -> Result<Vec<TableName>, DestinationError> {
        // The durable truth lives in THIS database: a receipt found
        // here means the unit already committed, and the planner
        // reduces the script to stage reclamation — the replay branch
        // arrives at the same answer whether the choreography routed
        // publish() or replay().
        let replayed = Self::receipt_recorded(&self.conn, &meta.load_id, meta.commit_seq)?;
        let load_committed_before = {
            let sql = unit::load_committed_sql(|_| "?".into());
            let count: u64 = self
                .conn
                .query_row(&sql, params![meta.load_id.as_str()], |row| row.get(0))
                .map_err(classify)?;
            count > 0
        };
        let staged_nonempty = self.staged_nonempty()?;
        let empty = BTreeSet::new();
        let script = plan_commit(
            &self.tables,
            &self.options,
            &CommitContext {
                replayed,
                load_committed_before,
                single_unit_done: &self.single_unit_done,
                staged_nonempty: &staged_nonempty,
                full_load_publish: FullLoadPublish::Staged,
                cleared_targets: &empty,
            },
        )
        .map_err(|e| DestinationError::fatal(e.to_string()))?;
        for step in &script.steps {
            self.run_step(step, meta)?;
        }
        if !replayed {
            crash_point!(
                "duck.tx.commit",
                Err(DestinationError::fatal("injected crash at duck.tx.commit"))
            );
        }
        Ok(script.marks)
    }
}

/// The receipt ledger stores `commit_seq` as BIGINT; the engine hands
/// a u64. The narrowing is checked deliberately (031 review S7) — a
/// silent wrap would alias two different sequences in the ledger.
fn ledger_seq(seq: u64) -> Result<i64, DestinationError> {
    i64::try_from(seq).map_err(|_| {
        DestinationError::fatal(format!(
            "commit_seq {seq} exceeds the receipt ledger's BIGINT range"
        ))
    })
}

#[async_trait]
impl Backend for Load {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        // Within a session, re-ensure may only APPEND columns — the
        // engine's evolution contract is append-only, and the physical
        // TEMP stage's column order is fixed at creation (a reorder
        // emits NO DDL, only no-op ADD COLUMN IF NOT EXISTS). So the
        // previously ensured columns must reappear as a PREFIX in the
        // same order: a drop is not evolution, and a REORDER would
        // desynchronize `previous` from the physical order the
        // positional append is verified against — the write guard
        // would then bless batches that land values in the wrong
        // columns (031 review S1, and the /code-review round's reorder
        // trace). Refuse both before any DDL runs.
        if let Some(previous) = self.previous.get(&schema.table) {
            let dropped: Vec<&str> = previous
                .columns
                .iter()
                .filter(|c| schema.column(&c.name).is_none())
                .map(|c| c.name.as_str())
                .collect();
            if !dropped.is_empty() {
                return Err(DestinationError::fatal(format!(
                    "table `{}`: re-ensure drops previously ensured columns ({}) — \
                     engine evolution is append-only, so this is two streams \
                     colliding on one table or a harness defect",
                    schema.table,
                    dropped.join(", ")
                )));
            }
            for (i, was) in previous.columns.iter().enumerate() {
                let now = schema.columns.get(i).map(|c| c.name.as_str());
                if now != Some(was.name.as_str()) {
                    return Err(DestinationError::fatal(format!(
                        "table `{}`: re-ensure reorders previously ensured columns \
                         (position {i} was `{}`, now `{}`) — the stage is positional \
                         and its physical order is fixed at creation; engine evolution \
                         only appends",
                        schema.table,
                        was.name,
                        now.unwrap_or("nothing"),
                    )));
                }
            }
        }
        // The widen planner's `previous` is session memory FIRST — a
        // within-run rule — falling back to the catalog image ONLY when
        // this session has never ensured the table (037 US5, 031's S3
        // record): a fresh session otherwise sees `previous = None` and
        // plans no widen at all, so a cross-run type change lands only
        // no-op DDL and the appender then rejects the mismatched batch.
        // The image feeds the WIDEN PLANNER ONLY — the drop/reorder
        // guard above already ran against `self.previous` alone, so
        // cross-run drift-by-name stays legal. `prefilter` (2026-08
        // review round 1, F1/F2) rewrites the image FIRST so the raw
        // types-differ comparison below only ever sees a genuine widen —
        // never a same-physical-type rendering (Uuid/Utf8) and never a
        // cross-run narrowing/incompatible drift.
        let image = if self.previous.contains_key(&schema.table) {
            None
        } else {
            catalog::live_schema(&self.conn, schema)?.map(|image| catalog::prefilter(image, schema))
        };
        let planning_previous = self.previous.get(&schema.table).or(image.as_ref());

        // Computed up front so a widen that lands on the merge key can
        // drop its UNIQUE index before the ALTER that would otherwise
        // be refused (Task 15 probe: DuckDB's `SET DATA TYPE` refuses a
        // column a UNIQUE ART index depends on — "Cannot change the
        // type of this column: an index depends on it!"). Any
        // validation error here surfaces at its ORIGINAL position below
        // (after `table_ddl` runs), unchanged from before this feature:
        // this early call only ever reads its `Ok` arm.
        let merge_statements = merge_ddl(&self.options, schema, mode);
        if let Ok(statements) = &merge_statements {
            for drop_sql in catalog::pre_alter_index_drops(planning_previous, schema, statements) {
                self.conn.execute_batch(&drop_sql).map_err(classify)?;
            }
        }

        for sql in table_ddl(schema, planning_previous) {
            if let Err(e) = self.conn.execute_batch(&sql) {
                // A PLAIN index (identity, delete_insert/scd2 key
                // columns, merge_scope) is not pre-dropped — only the
                // upsert arbiter is (above) — so a widen landing on one
                // still hits DuckDB's raw refusal. Name it instead of
                // surfacing the bare catalog error (2026-08 review
                // round 1, F4).
                if is_index_dependency_error(&e) {
                    return Err(DestinationError::fatal(index_dependency_diagnosis(
                        &sql,
                        &e.to_string(),
                    )));
                }
                return Err(classify(e));
            }
        }
        // The recreate is the SAME renderer merge_ddl already runs on
        // every ensure (`CREATE UNIQUE INDEX IF NOT EXISTS` in
        // schema.rs) — the drop above just clears the way for it; no
        // second copy of the index DDL is written here.
        let statements = merge_statements.map_err(|e| DestinationError::fatal(e.to_string()))?;
        for (sql, unique_index) in statements {
            if let Err(e) = self.conn.execute_batch(&sql) {
                // The duplicate-key diagnosis: only a genuine
                // constraint violation on the arbiter index gets the
                // guidance wording; anything else classifies.
                if let Some(columns) = &unique_index
                    && is_constraint_violation(&e)
                {
                    return Err(DestinationError::fatal(
                        names::duplicate_merge_key_diagnosis(
                            schema.table.as_str(),
                            columns,
                            &e.to_string(),
                        ),
                    ));
                }
                return Err(classify(e));
            }
        }
        self.previous.insert(schema.table.clone(), schema.clone());
        self.tables
            .insert(schema.table.clone(), (schema.clone(), mode.clone()));
        Ok(())
    }

    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        crash_point!(
            "duck.append",
            Err(DestinationError::fatal("injected crash at duck.append"))
        );
        // The appender is POSITIONAL: batch column i lands in stage
        // column i. The engine contract delivers batch columns in
        // ensured-schema order — verify instead of trusting (031
        // review S4), because a reordered batch would land values in
        // the wrong columns silently. An unensured table never reaches
        // here: the sdk choreography refuses write-before-ensure.
        if let Some(ensured) = self.previous.get(table) {
            let batch_schema = batch.schema();
            for (i, field) in batch_schema.fields().iter().enumerate() {
                let want = ensured.columns.get(i).map(|c| c.name.as_str());
                if want != Some(field.name().as_str()) {
                    return Err(DestinationError::fatal(format!(
                        "table `{table}`: batch column {i} is `{}` but the ensured \
                         schema has `{}` — the stage append is positional; refusing \
                         to land values in the wrong columns",
                        field.name(),
                        want.unwrap_or("no column at that position"),
                    )));
                }
            }
            // Exact arity, not a prefix: the appender itself demands
            // it (probed: a shorter batch fails "incorrect column
            // count in AppendDataChunk"), so a prefix allowance would
            // only trade this typed refusal for the raw library error.
            if batch_schema.fields().len() != ensured.columns.len() {
                return Err(DestinationError::fatal(format!(
                    "table `{table}`: the batch carries {} columns but the ensured \
                     schema has {} — the stage append is positional and exact; \
                     re-ensure before writing a changed shape",
                    batch_schema.fields().len(),
                    ensured.columns.len(),
                )));
            }
        }
        let stage = stage_name(table.as_str());
        let mut appender = self.conn.appender(&stage).map_err(classify)?;
        appender.append_record_batch(batch).map_err(classify)?;
        // Appender drop swallows errors; flush explicitly so failures
        // surface as DestinationError instead of silently losing
        // staged rows.
        appender.flush().map_err(classify)
    }

    async fn existing_receipt(
        &mut self,
        load_id: &LoadId,
        commit_seq: u64,
    ) -> Result<Option<CommitReceipt>, DestinationError> {
        Ok(
            Self::receipt_recorded(&self.conn, load_id, commit_seq)?.then(|| CommitReceipt {
                load_id: load_id.clone(),
                commit_seq,
            }),
        )
    }

    async fn replay(
        &mut self,
        meta: &CommitMeta,
        _receipt: &CommitReceipt,
    ) -> Result<(), DestinationError> {
        // The staged path's replay disposition is RunScript: for a
        // replay the script is stage truncation and nothing else —
        // the redelivered rows reached no reader, so there is nothing
        // to roll back — plus the re-marking of the single-unit
        // discipline. The in-transaction probe re-finds the receipt
        // the choreography saw, so this is the same call publish makes.
        debug_assert_eq!(
            unit::replay_disposition(FullLoadPublish::Staged),
            unit::ReplayDisposition::RunScript
        );
        self.run_commit(meta)
    }

    async fn publish(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        // No pre-probe here: the replay decision is taken INSIDE the
        // commit transaction (031 review A2/D7), where the durable
        // truth cannot move between the probe and the steps.
        self.run_commit(&meta)?;
        Ok(CommitReceipt {
            load_id: meta.load_id,
            commit_seq: meta.commit_seq,
        })
    }

    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        let doc: Result<String, _> = self.conn.query_row(
            &format!("SELECT doc FROM {} WHERE pipeline = ?", names::STATE_TABLE),
            params![pipeline.as_str()],
            |row| row.get(0),
        );
        match doc {
            // A document that reads but does not parse is corrupt —
            // that never heals on retry, so this arm stays fatal.
            Ok(doc) => serde_json::from_str(&doc)
                .map(Some)
                .map_err(|e| DestinationError::fatal(e.to_string())),
            Err(duckdb::Error::QueryReturnedNoRows) => Ok(None),
            // An idempotent read: classification decides (an IO error
            // here rides the retry budget).
            Err(e) => Err(classify(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The checked narrowing: in-range sequences pass through, an
    /// out-of-range one refuses typed instead of wrapping — `as i64`
    /// here would alias two different sequences in the ledger.
    #[test]
    fn ledger_seq_refuses_instead_of_wrapping() {
        assert_eq!(ledger_seq(7).expect("in range"), 7);
        let err = ledger_seq(u64::MAX).expect_err("beyond BIGINT").to_string();
        assert!(
            err.contains("exceeds the receipt ledger's BIGINT range"),
            "{err}"
        );
    }
}
