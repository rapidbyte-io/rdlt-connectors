//! The session: one load's conversation with the catalog, as the
//! sdk's `Backend`.
//!
//! Append maps onto fast-append snapshots; Merge and Replace are typed
//! refusals (no overwrite transaction exists in the library, and
//! emulating one would not be atomic). Exactly-once is snapshot-native
//! and PER TABLE: publish stamps each table's snapshot with the commit
//! identity and converges on replay by scanning history.
//!
//! `existing_receipt` answers from the `_rdlt_state` marker table's
//! receipt properties — `rdlt.receipt.<load_id>`, stamped by publish
//! and replay in the SAME property commit that persists the state
//! document (see `state.rs`'s format notes) — one table read, no
//! namespace enumeration. A partially published commit remains
//! exactly what the convergence pass completes — `converge_tables`,
//! shared by publish AND `replay`, since the framework returns the
//! receipt instead of publishing once `existing_receipt` answers.
//! (029 D7 deliberately returned `None` here; reversed by owner
//! ruling in 042 because the wire contract's receipt choreography
//! demands a durable load-level receipt.)

use super::parts;
use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::transaction::{ApplyTransactionAction as _, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use rdlt_connector_sdk::destination::Backend;
use rdlt_connector_sdk::spi::core::schema::ident_hash;
use rdlt_connector_sdk::spi::core::{
    commit::WriteMode, crash_point, id::LoadId, id::PipelineId, id::TableName, schema::TableSchema,
    state::StateDoc,
};
use rdlt_connector_sdk::spi::{
    arrow::RecordBatch, core::commit::CommitMeta, core::commit::CommitReceipt,
    error::DestinationError,
};

use super::client::{classify, fatal};
use super::commit::{Identity, Plan, append_commit, commit_with_retry, load_committed};
use super::config::Config;
use super::partition;
use super::schema::{self, compare_field};
use super::state::{self, STATE_TABLE, read_state_doc, write_state};
use super::write::Writer;

/// Width of the pipeline scope hash (128 bits → 32 hex chars). The scope names
/// the pipeline in snapshot summaries and the state key, so the width a session
/// opens with MUST equal the width `read_state` re-derives with. Widening from
/// 12 to 32 (037) means a 32-hex lookup finds nothing for a pipeline whose state
/// still lives under the pre-037 `rdlt.state.<12hex>` key — `read_state`
/// (below) probes that legacy key and REFUSES typed rather than silently
/// fresh-running, matching this feature's two sibling format breaks (postgres
/// cursor v2, file layout v2). Pre-037 snapshot replay identities stay a clean
/// first-run regardless — recorded in 037 D1.
pub(super) const SCOPE_HASH_LEN: usize = 32;

/// Part sizing and its telemetry, grouped: the options that decide
/// when files roll travel with the listener told when they close.
pub(super) struct PartsWiring {
    pub(super) options: parts::Options,
    pub(super) events: Option<rdlt_connector_sdk::spi::destination::PartEventFn>,
}

/// One stream table's session state.
struct TableState {
    /// The live handle, refreshed at ensure and publish boundaries.
    table: iceberg::table::Table,
    /// The field-id-annotated arrow schema batches align to.
    arrow_target: Arc<arrow_schema::Schema>,
    /// The current window's writer, opened on first write.
    writer: Option<Writer>,
    /// Files closed early — a mid-window schema change retires the
    /// writer and parks its files here until the window publishes.
    pending_files: Vec<iceberg::spec::DataFile>,
    /// Window counter for unique file-name prefixes. Survives
    /// re-ensure: resetting it would regenerate window 1's exact path
    /// and overwrite a committed file.
    window_seq: u64,
    /// When the current writer opened — the clock `roll_after_seconds`
    /// reads. `None` while no writer is open.
    writer_opened_at: Option<std::time::Instant>,
}

/// The session the connector opens — the sdk drives it.
pub struct Load {
    pub(super) config: Config,
    pub(super) catalog: Arc<dyn Catalog>,
    pub(super) namespace: NamespaceIdent,
    /// `ident_hash(pipeline, SCOPE_HASH_LEN)` — never the raw name.
    pub(super) scope: String,
    pub(super) load_id: LoadId,
    /// Unique per session; see `Writer::open`'s nonce contract.
    pub(super) nonce: String,
    /// Resolved once at connect: the translation can fail, and a load
    /// should not discover that partway through writing.
    pub(super) writer_properties: parquet::file::properties::WriterProperties,
    /// Output file sizing. `target_bytes` reaches the library's own
    /// rolling writer; `roll_after_seconds` is applied here, since the
    /// library has no time trigger.
    pub(super) parts: parts::Options,
    /// Where closed data files are reported. Advisory. One caveat this
    /// destination owns: the LIBRARY rolls files on size internally
    /// and surfaces them only when the writer closes, so every file of
    /// one writer reports the close's cause rather than its own —
    /// sizes are exact, attribution is per-window.
    pub(super) part_events: Option<rdlt_connector_sdk::spi::destination::PartEventFn>,
    tables: BTreeMap<TableName, TableState>,
}

impl std::fmt::Debug for Load {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Load")
            .field("scope", &self.scope)
            .field("load_id", &self.load_id)
            .finish_non_exhaustive()
    }
}

impl Load {
    pub(super) fn new(
        config: Config,
        catalog: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
        pipeline: &PipelineId,
        load_id: LoadId,
        writer_properties: parquet::file::properties::WriterProperties,
        parts: PartsWiring,
    ) -> Self {
        Self {
            parts: parts.options,
            part_events: parts.events,
            config,
            catalog,
            namespace,
            scope: ident_hash(pipeline.as_str(), SCOPE_HASH_LEN),
            load_id,
            nonce: session_nonce(),
            writer_properties,
            tables: BTreeMap::new(),
        }
    }

    /// Only Append maps onto a snapshot this release.
    fn check_mode(mode: &WriteMode) -> Result<(), DestinationError> {
        match mode {
            WriteMode::Append => Ok(()),
            WriteMode::Merge { .. } => Err(fatal(
                "iceberg destination does not support Merge (capabilities.merge = false)",
            )),
            WriteMode::Replace => Err(fatal(
                "iceberg destination: Replace is not supported — the underlying iceberg library \
                 exposes no overwrite transaction, which Replace requires; use Append, or a SQL \
                 destination for replace semantics",
            )),
        }
    }

    /// Report a closed writer's data files. The engine table name is
    /// the STREAM's normalized root — the same name the event feed
    /// uses everywhere else — not the physical iceberg table. An
    /// associated fn over a cloned listener handle, because the call
    /// sites hold `&mut` borrows into the table map.
    fn report_closed(
        listener: &Option<rdlt_connector_sdk::spi::destination::PartEventFn>,
        stream: &TableName,
        files: &[iceberg::spec::DataFile],
        reason: rdlt_connector_sdk::spi::destination::PartCloseReason,
    ) {
        let Some(listener) = listener else {
            return;
        };
        for file in files {
            listener(rdlt_connector_sdk::spi::destination::PartClosed::new(
                rdlt_connector_sdk::spi::core::id::TableName::new(stream.as_str()),
                file.file_size_in_bytes(),
                reason,
            ));
        }
    }

    /// Fold the freshly ensured table into the session, carrying the
    /// window counter and any in-flight writer across a re-ensure. The
    /// writer survives ONLY while the write schema is unchanged: a
    /// re-ensure carrying drift RETIRES it — its closed files (valid
    /// under the prior schema; Iceberg reads absent columns as null
    /// after additive evolution) park in `pending_files` and join the
    /// window's publish, and the next writer opens against the evolved
    /// table.
    async fn reinstall(
        &mut self,
        stream: &TableName,
        name: &str,
        table: iceberg::table::Table,
        arrow_target: Arc<arrow_schema::Schema>,
    ) -> Result<(), DestinationError> {
        let (window_seq, prev_writer, prev_target, mut pending_files, prev_opened_at) =
            match self.tables.remove(stream) {
                Some(prev) => (
                    prev.window_seq,
                    prev.writer,
                    Some(prev.arrow_target),
                    prev.pending_files,
                    prev.writer_opened_at,
                ),
                None => (0, None, None, Vec::new(), None),
            };
        let (writer, writer_opened_at) = match prev_writer {
            Some(writer) if prev_target.as_deref() != Some(arrow_target.as_ref()) => {
                let context = format!("table `{name}` (schema-change writer retirement)");
                let closed = writer.close(&context).await?;
                Self::report_closed(
                    &self.part_events,
                    stream,
                    &closed,
                    rdlt_connector_sdk::spi::destination::PartCloseReason::Schema,
                );
                pending_files.extend(closed);
                (None, None)
            }
            other => (other, prev_opened_at),
        };
        self.tables.insert(
            stream.clone(),
            TableState {
                table,
                arrow_target,
                writer,
                pending_files,
                window_seq,
                writer_opened_at,
            },
        );
        Ok(())
    }

    /// Align an engine batch to the table's arrow target: columns
    /// matched BY NAME, cast where representations differ, null-filled
    /// where the table's column is nullable and the batch lacks it,
    /// and typed — attributed to the TABLE — where it is required.
    fn align(
        context: &str,
        target: &Arc<arrow_schema::Schema>,
        batch: &RecordBatch,
    ) -> Result<RecordBatch, DestinationError> {
        let mut columns = Vec::with_capacity(target.fields().len());
        for field in target.fields() {
            let column = match batch.schema().index_of(field.name()) {
                Ok(index) => {
                    let column = batch.column(index);
                    if column.data_type() == field.data_type() {
                        column.clone()
                    } else {
                        arrow_cast::cast(column, field.data_type()).map_err(|e| {
                            fatal(format!(
                                "{context}: column `{}` cannot cast {} -> {}: {e}",
                                field.name(),
                                column.data_type(),
                                field.data_type()
                            ))
                        })?
                    }
                }
                Err(_) if field.is_nullable() => {
                    arrow_array::new_null_array(field.data_type(), batch.num_rows())
                }
                Err(_) => {
                    return Err(fatal(format!(
                        "{context}: the live table requires column `{}` but the stream no longer \
                         provides it",
                        field.name()
                    )));
                }
            };
            columns.push(column);
        }
        RecordBatch::try_new(target.clone(), columns)
            .map_err(|e| fatal(format!("{context}: aligning batch: {e}")))
    }
}

/// A recovery session replaying (load, window) must never reuse a
/// prior session's data-file names: wall clock plus a process-wide
/// counter.
fn session_nonce() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    // The pid closes the two-processes-in-one-nanosecond window the
    // per-process counter cannot see.
    format!(
        "{:x}-{}-{}",
        nanos,
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// The table's field-id-annotated arrow schema — recomputed at ensure
/// AND publish boundaries through this one helper, because a
/// concurrent writer's additive evolution changes it.
fn arrow_target(
    context: &str,
    table: &iceberg::table::Table,
) -> Result<Arc<arrow_schema::Schema>, DestinationError> {
    iceberg::arrow::schema_to_arrow_schema(table.metadata().current_schema())
        .map(Arc::new)
        .map_err(|e| fatal(format!("{context}: arrow schema conversion: {e}")))
}

/// Load-or-create the namespace per the explicit config flag.
pub(super) async fn ensure_namespace(
    catalog: &Arc<dyn Catalog>,
    namespace: &NamespaceIdent,
    create_if_missing: bool,
) -> Result<(), DestinationError> {
    let context = format!("namespace `{}`", namespace.to_url_string());
    let exists = catalog
        .namespace_exists(namespace)
        .await
        .map_err(|e| classify(&context, e))?;
    if exists {
        return Ok(());
    }
    if !create_if_missing {
        return Err(fatal(format!(
            "{context} does not exist and create_namespace is false — create it (or set \
             create_namespace: true)"
        )));
    }
    match catalog
        .create_namespace(namespace, std::collections::HashMap::new())
        .await
    {
        Ok(_) => Ok(()),
        Err(e) if matches!(e.kind(), iceberg::ErrorKind::NamespaceAlreadyExists) => Ok(()),
        Err(e) => Err(classify(&context, e)),
    }
}

/// Load-or-create the table, then reconcile drift: additive nullable
/// columns are APPLIED through the shared retry (a competitor adding
/// the same column converges to an empty addition set, which settles);
/// contradictory drift and partition disagreement are typed.
async fn ensure_stream_table(
    catalog: &Arc<dyn Catalog>,
    namespace: &NamespaceIdent,
    name: &str,
    wanted: &iceberg::spec::Schema,
    fields: &[super::config::PartitionField],
) -> Result<iceberg::table::Table, DestinationError> {
    let ident = TableIdent::new(namespace.clone(), name.to_owned());
    let context = format!("table `{ident}`");
    let exists = catalog
        .table_exists(&ident)
        .await
        .map_err(|e| classify(&context, e))?;
    if !exists {
        let spec = partition::to_partition_spec(&context, wanted, fields)?;
        let creation = iceberg::TableCreation::builder()
            .name(name.to_owned())
            .schema(wanted.clone())
            .partition_spec_opt(spec)
            .build();
        match catalog.create_table(namespace, creation).await {
            Ok(table) => return Ok(table),
            // A concurrent creator: fall through to reconcile theirs.
            Err(e) if matches!(e.kind(), iceberg::ErrorKind::TableAlreadyExists) => {}
            Err(e) => return Err(classify(&context, e)),
        }
    }
    reconcile(catalog, &ident, wanted, fields).await
}

/// One reconcile attempt per retry round: partition check first, then
/// per wanted column — present compares structurally, absent becomes
/// an optional AddColumn (additive ONLY: the new column is nullable in
/// the table even where the stream declares it required — existing
/// rows have no value for it).
async fn reconcile(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    wanted: &iceberg::spec::Schema,
    fields: &[super::config::PartitionField],
) -> Result<iceberg::table::Table, DestinationError> {
    let context = format!("table `{ident}`");
    let initial = catalog
        .load_table(ident)
        .await
        .map_err(|e| classify(&context, e))?;
    let entropy = context.clone();
    commit_with_retry(
        catalog,
        ident,
        &context,
        "schema commit",
        &entropy,
        initial,
        |current| {
            partition::check_live_spec(&context, current, fields)?;
            let live = current.metadata().current_schema();
            let mut additions: Vec<iceberg::transaction::AddColumn> = Vec::new();
            for field in wanted.as_struct().fields() {
                match live
                    .as_struct()
                    .fields()
                    .iter()
                    .find(|f| f.name == field.name)
                {
                    Some(live_field) => {
                        if let Some(drift) = compare_field(field, live_field) {
                            return Err(fatal(format!(
                                "{context} column `{}`: {} — contradictory drift is never applied",
                                field.name,
                                drift.detail()
                            )));
                        }
                    }
                    None => additions.push(iceberg::transaction::AddColumn::optional(
                        &field.name,
                        field.field_type.as_ref().clone(),
                    )),
                }
            }
            if additions.is_empty() {
                return Ok(Plan::Settled);
            }
            let tx = Transaction::new(current);
            let mut action = tx.update_schema();
            for addition in additions {
                action = action.add_column(addition);
            }
            let tx = action.apply(tx).map_err(|e| classify(&context, e))?;
            Ok(Plan::Commit(Box::new(tx)))
        },
    )
    .await
}

impl Load {
    /// ONE convergence pass over every staged table window — the
    /// commit body publish and replay share (042 fix round 1): close
    /// the window's writer, and per table either discard the files
    /// (the settled check found the commit already in history) or
    /// append them under `identity`. Publish and replay run the SAME
    /// load-keyed settled check (round-10 — they briefly differed:
    /// publish re-checked its full scope-matched identity, see the
    /// check below for the window that opened); sharing the body is
    /// what makes "the two mechanisms agree" true rather than claimed
    /// — a replay reaching a table the killed attempt never committed
    /// CONVERGES it, the exact window a wholesale discard was
    /// measured to lose.
    async fn converge_tables(&mut self, identity: &Identity) -> Result<(), DestinationError> {
        let listener = self.part_events.clone();
        for (table_name, state) in self.tables.iter_mut() {
            let context = format!("table `{}`", self.config.table_name(table_name.as_str()));
            // `take` empties the parked files BEFORE the fallible
            // commit below. Safe ONLY because a failed commit pass is
            // never retried on this session — the engine restarts a
            // whole run from committed state, with fresh TableState —
            // so the emptied list is never consulted again. An
            // in-process retry policy added later would silently drop
            // these files; move the take after the commit first.
            let mut files = std::mem::take(&mut state.pending_files);
            if let Some(writer) = state.writer.take() {
                let closed = writer.close(&context).await?;
                Self::report_closed(
                    &listener,
                    table_name,
                    &closed,
                    rdlt_connector_sdk::spi::destination::PartCloseReason::Commit,
                );
                files.extend(closed);
                state.writer_opened_at = None;
            }
            if files.is_empty() {
                // An empty window publishes no snapshot.
                continue;
            }
            // Settled detection against FRESH metadata: an
            // already-committed identity discards this window's files —
            // orphaned and invisible, no snapshot names them — and
            // publishes nothing for this table. LOAD-keyed for publish
            // AND replay (round-10 fix): a crash BETWEEN a table's
            // append_commit and the receipt stamp leaves the snapshot
            // committed under the DYING attempt's pipeline scope with
            // no receipt, so a sibling-scope re-drive (the kill
            // matrix's `-r` re-run, or any orchestrator re-scope)
            // honestly finds no receipt and re-drives publish — whose
            // scope-matched check missed the dead scope's snapshot and
            // appended the window a SECOND time. A load-id match IS
            // the same load regardless of scope: load-id uniqueness
            // across every pipeline sharing a destination is the house
            // contract (docs/connector-authoring.md, "Load identity" —
            // sqlcore's shared receipt lookup rests on the same key).
            // A scope match implies a load match, so the load-keyed
            // check SUBSUMES the old scope-matched one.
            let fresh = self
                .catalog
                .load_table(state.table.identifier())
                .await
                .map_err(|e| classify(&context, e))?;
            let done = load_committed(&fresh, &identity.load_id, identity.commit_seq);
            if done {
                state.table = fresh;
            } else {
                state.table = append_commit(&self.catalog, fresh, files, identity).await?;
            }
            // The refresh may carry a concurrent writer's additive
            // evolution: realign so the next window agrees with it.
            state.arrow_target = arrow_target(&context, &state.table)?;
        }
        Ok(())
    }

    /// The commit tail publish and replay share: state LAST, after
    /// every table's data commit — the per-table snapshot receipts
    /// make a re-attempt converge even when the crash lands before
    /// this write, WHICHEVER path (publish's re-run or replay's
    /// receipt fast path) the re-attempt takes; both therefore write
    /// it, or the fast path would strand the cursor and the next run
    /// would re-ingest (042 fix round 1's second measured window).
    /// The load-level receipt rides the SAME property commit (round-2
    /// fix wave) — both paths stamp it, replay included, which
    /// refreshes its retention recency and merges the sequence high
    /// water by MAX inside `write_state`. `&mut self` deliberately: a
    /// shared borrow across the await would demand `Load: Sync`, which
    /// the live parquet writers in `tables` forbid.
    async fn persist_state(&mut self, meta: &CommitMeta) -> Result<(), DestinationError> {
        crash_point!(
            "ice.receipt.visible",
            Err(DestinationError::fatal(
                "injected crash at ice.receipt.visible"
            ))
        );
        let state_json =
            serde_json::to_string(&meta.state).map_err(|e| fatal(format!("state doc: {e}")))?;
        write_state(
            &self.catalog,
            &self.namespace,
            &self.scope,
            state_json,
            Some(state::ReceiptStamp {
                load_id: meta.load_id.as_str(),
                commit_seq: meta.commit_seq,
            }),
        )
        .await
    }
}

#[async_trait]
impl Backend for Load {
    async fn ensure_table(
        &mut self,
        table_schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        Self::check_mode(mode)?;
        let stream = table_schema.table.as_str();
        let name = self.config.table_name(stream);
        // Cheap and infallible BEFORE the fallible mapping: a
        // misconfigured name fails on its own terms.
        if name == STATE_TABLE {
            return Err(fatal(format!(
                "table name `{name}` is reserved for the rdlt state marker table"
            )));
        }
        // The config gate refuses duplicate EXPLICIT names; a rename
        // onto another stream's DEFAULT name is only visible here,
        // where both resolutions exist. Sharing one physical table
        // would interleave colliding file paths and read one stream's
        // commit as the other's replay.
        if let Some(other) = self
            .tables
            .keys()
            .find(|s| s.as_str() != stream && self.config.table_name(s.as_str()) == name)
        {
            return Err(fatal(format!(
                "streams `{other}` and `{stream}` both resolve to table `{name}` — two streams \
                 may not share one table"
            )));
        }
        let wanted = schema::to_iceberg_schema(table_schema)?;
        let fields = self.config.partition_fields(stream);
        let table =
            ensure_stream_table(&self.catalog, &self.namespace, &name, &wanted, fields).await?;
        let target = arrow_target(&format!("table `{name}`"), &table)?;
        self.reinstall(&table_schema.table, &name, table, target)
            .await
    }

    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        let listener = self.part_events.clone();
        let state = self
            .tables
            .get_mut(table)
            .ok_or_else(|| fatal(format!("write before ensure_table for `{table}`")))?;
        let context = format!("table `{}`", self.config.table_name(table.as_str()));
        let aligned = Self::align(&context, &state.arrow_target, &batch)?;
        // The TIME threshold is applied here because the library's
        // rolling writer has no clock — only a size. Retiring the whole
        // writer is the same move a mid-window schema change makes: its
        // files park in `pending_files` and join the window's publish.
        if let Some(opened_at) = state.writer_opened_at
            && self.parts.rolls_on_time(opened_at.elapsed().as_secs())
            && let Some(writer) = state.writer.take()
        {
            let retire = format!("{context} (roll_after_seconds writer retirement)");
            let closed = writer.close(&retire).await?;
            Self::report_closed(
                &listener,
                table,
                &closed,
                rdlt_connector_sdk::spi::destination::PartCloseReason::Time,
            );
            state.pending_files.extend(closed);
            state.writer_opened_at = None;
        }
        if state.writer.is_none() {
            state.window_seq += 1;
            let prefix = format!("{}-{}", self.load_id, state.window_seq);
            state.writer = Some(
                Writer::open(
                    &state.table,
                    &prefix,
                    &self.nonce,
                    self.writer_properties.clone(),
                    self.parts.target_file_size(),
                )
                .await?,
            );
            state.writer_opened_at = Some(std::time::Instant::now());
        }
        state
            .writer
            .as_mut()
            .expect("writer just ensured")
            .write(&context, aligned)
            .await
    }

    async fn existing_receipt(
        &mut self,
        load_id: &LoadId,
        commit_seq: u64,
    ) -> Result<Option<CommitReceipt>, DestinationError> {
        // 029 D7 deliberately returned None here; reversed by owner
        // ruling in 042 because the wire contract's receipt
        // choreography demands a durable load-level receipt. THE
        // RECEIPT'S HOME (round-2 fix wave, the ruling's write-side
        // door): publish stamps `rdlt.receipt.<load_id>` onto the ONE
        // `_rdlt_state` marker table in the SAME property commit that
        // persists the state document — so this lookup reads exactly
        // one table, never enumerating the namespace (a foreign broken
        // table cannot fail it, a huge namespace cannot stall it) and
        // never depending on which tables this session has ensured
        // (the wire's receipt-before-ensure posture answers the same).
        // LOAD-keyed, never scope-keyed: a re-attempt under a
        // different pipeline scope (the kill matrix's sibling re-run)
        // still finds it. Sequences are monotone per load, so the
        // recorded high water answers membership; a crash BEFORE the
        // property commit honestly answers None, the framework
        // re-drives publish, and `converge_tables`' per-table history
        // scan (unchanged) discards whatever the dead attempt already
        // committed — K-D5's shape.
        match state::read_receipt(&self.catalog, &self.namespace, load_id.as_str()).await? {
            Some(high_water) if commit_seq <= high_water => Ok(Some(CommitReceipt {
                load_id: load_id.clone(),
                commit_seq,
            })),
            _ => Ok(None),
        }
    }

    async fn replay(
        &mut self,
        meta: &CommitMeta,
        _receipt: &CommitReceipt,
    ) -> Result<(), DestinationError> {
        // CONVERGE, never discard wholesale (042 fix round 1 — both
        // regressions were measured live before this landed): the
        // receipt that routed the framework here proves the load's
        // identity is in SOME table's history, not every table's —
        // publish commits tables sequentially, so a crash between two
        // per-table commits leaves the load partially published, and
        // the framework returns the receipt INSTEAD of publishing once
        // `existing_receipt` answers. This pass discards each
        // already-committed table's redelivered window and LANDS the
        // unfinished ones (load-keyed settled check: the prior
        // attempt's stamp may carry a different pipeline scope), then
        // persists the state doc — publish never runs on this path, so
        // the write publish normally owns happens here or nowhere,
        // and a stranded cursor re-ingests everything next run.
        let identity = Identity {
            scope: self.scope.clone(),
            load_id: meta.load_id.as_str().to_owned(),
            commit_seq: meta.commit_seq,
        };
        self.converge_tables(&identity).await?;
        self.persist_state(meta).await
    }

    async fn publish(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        let receipt = CommitReceipt {
            load_id: meta.load_id.clone(),
            commit_seq: meta.commit_seq,
        };
        let identity = Identity {
            scope: self.scope.clone(),
            load_id: meta.load_id.as_str().to_owned(),
            commit_seq: meta.commit_seq,
        };
        self.converge_tables(&identity).await?;
        self.persist_state(&meta).await?;
        Ok(receipt)
    }

    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        let scope = ident_hash(pipeline.as_str(), SCOPE_HASH_LEN);
        let Some(raw) = read_state_doc(&self.catalog, &self.namespace, &scope).await? else {
            // No 32-hex property: either a genuine first run, or state
            // stranded under the pre-037 12-hex key (037 D1) — probe
            // it before agreeing this pipeline is new.
            return refuse_legacy_state(&self.catalog, &self.namespace, pipeline).await;
        };
        let state: StateDoc =
            serde_json::from_str(&raw).map_err(|e| fatal(format!("state doc parse: {e}")))?;
        // The scope is a truncated hash; the pipeline filter turns an
        // astronomically unlikely collision into a clean first-run.
        Ok(Some(state).filter(|s| &s.pipeline == pipeline))
    }
}

/// The 037 D1 legacy-key refusal gate: the 32-hex scope lookup found
/// nothing, so before agreeing this is a fresh pipeline, probe the
/// pre-037 12-hex key it used to write under. Finding state there —
/// for THIS pipeline — means the widen orphaned it: silently treating
/// that as a first run would let Append re-publish every row the
/// pipeline already committed, so this refuses typed instead, the
/// same shape the widen's postgres/file siblings take on their own
/// format breaks.
///
/// GATE BEFORE PARSE: only the `pipeline` field is read, through
/// untyped JSON, never a full `StateDoc` decode — the refusal path
/// never constructs one, so decoding further would only manufacture
/// ways to fail before reaching the one comparison that matters. A
/// legacy property that decodes with a DIFFERENT `pipeline` is a hash
/// collision on the narrower 12-hex width, not this pipeline's state —
/// a clean `None`, the same filter the 32-hex path applies. Every
/// other shape under the legacy key — a match, a `pipeline` field
/// that is absent, or JSON this build cannot even parse — REFUSES.
/// The absent-field and undecodable arms are deliberately
/// conservative-loud rather than a clean `None`: a document living
/// under a key hashed from THIS pipeline's own name that is
/// simultaneously unrecognizable is overwhelmingly more likely to be
/// genuine pre-037 state (perhaps predating a field this build added,
/// or truncated) than an unrelated foreign collision that ALSO
/// happens to be corrupt — and silently agreeing to a fresh run is the
/// one outcome this gate exists to prevent.
async fn refuse_legacy_state(
    catalog: &Arc<dyn Catalog>,
    namespace: &NamespaceIdent,
    pipeline: &PipelineId,
) -> Result<Option<StateDoc>, DestinationError> {
    let legacy_scope = ident_hash(pipeline.as_str(), state::LEGACY_SCOPE_HASH_LEN);
    let Some(raw) = read_state_doc(catalog, namespace, &legacy_scope).await? else {
        return Ok(None);
    };
    let refuses = match serde_json::from_str::<serde_json::Value>(&raw) {
        Ok(value) => match value.get("pipeline").and_then(|v| v.as_str()) {
            Some(found) if found != pipeline.as_str() => false,
            // A match, or the field is absent: refuse (see the doc
            // comment's conservative-loud rationale for the latter).
            _ => true,
        },
        // Undecodable JSON under OUR legacy key: refuse (same
        // rationale).
        Err(_) => true,
    };
    if !refuses {
        return Ok(None);
    }
    Err(fatal(format!(
        "state for pipeline `{pipeline}` predates this build: the pipeline scope key widened \
         (12-hex to 32-hex); point the pipeline at a fresh warehouse or namespace, or — \
         accepting that the table already holds every previously-loaded row and Append would \
         re-add them — remove the stale `rdlt.state.{legacy_scope}` property from the \
         `_rdlt_state` table and re-run"
    )))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    fn target(fields: Vec<Field>) -> Arc<Schema> {
        Arc::new(Schema::new(fields))
    }

    /// A nullable table column the batch lacks is null-filled — schema
    /// narrowing and concurrent additive evolution both land here.
    #[test]
    fn align_null_fills_a_missing_nullable_column() {
        let target = target(vec![
            Field::new("seq", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![7, 8, 9]))],
        )
        .expect("batch");
        let aligned = Load::align("table `t`", &target, &batch).expect("aligns");
        assert_eq!(aligned.num_columns(), 2);
        assert_eq!(aligned.column(1).null_count(), 3);
    }

    /// A REQUIRED column the stream stopped providing is typed and
    /// attributed to the TABLE.
    #[test]
    fn align_types_a_missing_required_column_naming_the_table() {
        let target = target(vec![
            Field::new("seq", DataType::Int64, false),
            Field::new("mandatory", DataType::Utf8, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("seq", DataType::Int64, false)])),
            vec![Arc::new(Int64Array::from(vec![7]))],
        )
        .expect("batch");
        let err = Load::align("table `t`", &target, &batch).expect_err("typed");
        let text = format!("{err}");
        assert!(
            text.contains("live table requires column `mandatory`")
                && text.contains("stream no longer provides"),
            "{text}"
        );
    }

    /// Present columns cast where representations differ.
    #[test]
    fn align_casts_present_columns() {
        let target = target(vec![Field::new("label", DataType::LargeUtf8, true)]);
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("label", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec!["wide"]))],
        )
        .expect("batch");
        let aligned = Load::align("table `t`", &target, &batch).expect("casts");
        assert_eq!(aligned.column(0).data_type(), &DataType::LargeUtf8);
    }

    /// Merge and Replace refuse with the frozen spellings; Append
    /// passes.
    #[test]
    fn merge_and_replace_refuse_with_the_frozen_spellings() {
        assert!(Load::check_mode(&WriteMode::Append).is_ok());
        let err = Load::check_mode(&WriteMode::Merge { key: vec![] }).expect_err("merge");
        assert!(
            format!("{err}").contains(
                "iceberg destination does not support Merge (capabilities.merge = false)"
            ),
            "{err}"
        );
        let err = Load::check_mode(&WriteMode::Replace).expect_err("replace");
        let text = format!("{err}");
        assert!(
            text.contains("Replace is not supported")
                && text.contains("no overwrite transaction")
                && text.contains("use Append, or a SQL destination for replace semantics"),
            "{text}"
        );
    }

    /// One CAS conflict against the schema commit must not fail the
    /// load: reconcile rides the same bounded retry as every other
    /// commit kind, and the attempt count proves it retried rather
    /// than resigned.
    #[tokio::test]
    async fn reconcile_retries_schema_commit_conflicts() {
        use std::sync::atomic::Ordering;

        use iceberg::spec::{NestedField, PrimitiveType, Type};

        use super::super::commit::COMMIT_ATTEMPTS;
        use super::super::testsupport::ConflictCatalog;

        let catalog = ConflictCatalog::failing(COMMIT_ATTEMPTS - 1);
        let arc: Arc<dyn Catalog> = catalog.clone();
        let wanted = iceberg::spec::Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Long),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "note",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .expect("schema");
        let ident = TableIdent::new(iceberg::NamespaceIdent::new("ns".into()), "events".into());
        reconcile(&arc, &ident, &wanted, &[])
            .await
            .expect("lands within the bound");
        assert_eq!(catalog.commits.load(Ordering::SeqCst), COMMIT_ATTEMPTS);
    }

    /// Session nonces never repeat within a process.
    #[test]
    fn session_nonces_are_unique_within_a_process() {
        assert_ne!(session_nonce(), session_nonce());
    }

    /// The contradictory-drift REFUSAL at its enforcement site: a
    /// wanted type conflicting with the live table refuses with the
    /// full frozen wording — deleting the reconcile check would let
    /// align silently arrow-cast the mismatch, with every layer-below
    /// pin still green (the round-2 lesson, one layer up).
    #[tokio::test]
    async fn reconcile_refuses_contradictory_drift_with_the_frozen_wording() {
        use iceberg::spec::{NestedField, PrimitiveType, Type};

        use super::super::testsupport::ConflictCatalog;

        let catalog = ConflictCatalog::failing(0);
        let arc: std::sync::Arc<dyn Catalog> = catalog.clone();
        // The mock table carries `id: long` (required); wanting string
        // is contradictory drift, never applied.
        let wanted = iceberg::spec::Schema::builder()
            .with_fields(vec![std::sync::Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::String),
            ))])
            .build()
            .expect("schema");
        let ident = TableIdent::new(iceberg::NamespaceIdent::new("ns".into()), "events".into());
        let err = reconcile(&arc, &ident, &wanted, &[])
            .await
            .expect_err("contradictory drift refuses");
        let text = format!("{err}");
        assert!(
            text.contains(
                "column `id`: stream type string conflicts with the table\'s long — \
                           contradictory drift is never applied"
            ) || (text.contains("column `id`")
                && text.contains("conflicts with the table\'s")
                && text.contains("contradictory drift is never applied")),
            "{text}"
        );
        assert_eq!(
            catalog.commits.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "nothing may commit past the refusal"
        );
    }

    /// The retirement choreography OFFLINE (the live mid-window cell
    /// needs containers; a container-less gate must still catch a
    /// regression here): an identical re-ensure keeps the writer, a
    /// changed target retires it and PARKS its closed files, and the
    /// window counter survives both.
    #[tokio::test]
    async fn a_schema_change_retires_the_writer_and_parks_its_files() {
        use rdlt_connector_sdk::config::Document;

        use super::super::testsupport::{ConflictCatalog, table_with_schema};
        use super::super::write::writer_properties;

        let table = {
            use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
            table_with_schema(
                Schema::builder()
                    .with_fields(vec![std::sync::Arc::new(NestedField::required(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Long),
                    ))])
                    .build()
                    .expect("schema"),
            )
        };
        let catalog = ConflictCatalog::failing(0);
        let arc: std::sync::Arc<dyn Catalog> = catalog.clone();
        let config = Config::from_value(serde_json::json!({
            "catalog": {
                "uri": "http://localhost:1/api/catalog",
                "warehouse": "wh",
                "auth": {"bearer": {"token": "t"}},
            },
            "namespace": "ns",
        }))
        .expect("valid");
        let mut load = Load::new(
            config,
            arc,
            iceberg::NamespaceIdent::new("ns".into()),
            &PipelineId::from("p"),
            LoadId::from("l"),
            writer_properties(&Default::default()).expect("props"),
            PartsWiring {
                options: parts::Options::default(),
                events: None,
            },
        );
        let stream = TableName::from("events");
        let target = arrow_target("table `events`", &table).expect("target");
        load.reinstall(&stream, "events", table.clone(), target.clone())
            .await
            .expect("install");

        // One staged batch opens the window's writer.
        let batch = RecordBatch::try_new(
            target.clone(),
            vec![std::sync::Arc::new(arrow_array::Int64Array::from(vec![
                1, 2,
            ]))],
        )
        .expect("batch");
        load.write(&stream, batch).await.expect("write");
        let seq_before = load.tables[&stream].window_seq;
        assert!(load.tables[&stream].writer.is_some());

        // Identical target: the writer survives the re-ensure. The
        // target is RECOMPUTED, not the same Arc — production always
        // derives a fresh one, so a value-eq-to-pointer-eq regression
        // must fail here rather than silently retiring every window.
        let recomputed = arrow_target("table `events`", &table).expect("target");
        load.reinstall(&stream, "events", table.clone(), recomputed)
            .await
            .expect("re-ensure");
        assert!(
            load.tables[&stream].writer.is_some(),
            "an unchanged schema keeps the in-flight writer"
        );

        // A changed target retires it; the closed files are PARKED for
        // the window's publish, and the counter survives.
        let evolved = std::sync::Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("note", arrow_schema::DataType::Utf8, true),
        ]));
        load.reinstall(&stream, "events", table, evolved)
            .await
            .expect("evolving re-ensure");
        let state = &load.tables[&stream];
        assert!(state.writer.is_none(), "the writer was retired");
        assert!(
            !state.pending_files.is_empty(),
            "the retired writer\'s files joined the window"
        );
        assert_eq!(state.window_seq, seq_before, "the counter survived");
    }

    /// 034: `parts.target_bytes` reaches the LIBRARY's rolling writer
    /// rather than being reimplemented above it, and `None` means the
    /// library never rolls on size.
    ///
    /// The library takes a `usize` with no "unlimited" spelling, so
    /// absence has to become a number no file reaches. Pinning it
    /// here because the conversion is the only place that decision
    /// lives, and a silent truncation of a `u64` target would shrink
    /// files rather than fail.
    #[test]
    fn the_target_size_reaches_the_library_and_absence_never_rolls() {
        // The shipping default: 128 MiB, NOT the library's own 512.
        assert_eq!(
            parts::Options::default().target_file_size(),
            128 * 1024 * 1024
        );
        assert_eq!(parts::Options::unbounded().target_file_size(), usize::MAX);
        assert_eq!(
            parts::Options {
                target_bytes: Some(64 * 1024 * 1024),
                ..parts::Options::default()
            }
            .target_file_size(),
            64 * 1024 * 1024
        );
    }

    /// 034: the TIME threshold is rdlt's to apply — the library rolls
    /// on size only. Pinned as its own predicate because asking
    /// `should_roll` here would answer the size half twice, once from
    /// each side, and the two would disagree.
    #[test]
    fn the_time_threshold_is_answered_without_the_size_one() {
        let timed = parts::Options {
            roll_after_seconds: Some(900),
            ..parts::Options::default()
        };
        assert!(!timed.rolls_on_time(899));
        assert!(timed.rolls_on_time(900));
        // The default names no time bound, so no elapsed time rolls.
        assert!(!parts::Options::default().rolls_on_time(u64::MAX));
    }

    /// 034: a writer retired ON TIME parks its files exactly as a
    /// schema change does, and the next write opens a fresh one.
    #[tokio::test]
    async fn an_elapsed_writer_is_retired_and_its_files_parked() {
        use rdlt_connector_sdk::config::Document;

        use super::super::testsupport::{ConflictCatalog, table_with_schema};
        use super::super::write::writer_properties;

        let table = {
            use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
            table_with_schema(
                Schema::builder()
                    .with_fields(vec![std::sync::Arc::new(NestedField::required(
                        1,
                        "id",
                        Type::Primitive(PrimitiveType::Long),
                    ))])
                    .build()
                    .expect("schema"),
            )
        };
        let catalog = ConflictCatalog::failing(0);
        let arc: std::sync::Arc<dyn Catalog> = catalog.clone();
        let config = Config::from_value(serde_json::json!({
            "catalog": {
                "uri": "http://localhost:1/api/catalog",
                "warehouse": "wh",
                "auth": {"bearer": {"token": "t"}},
            },
            "namespace": "ns",
        }))
        .expect("valid");
        let mut load = Load::new(
            config,
            arc,
            iceberg::NamespaceIdent::new("ns".into()),
            &PipelineId::from("p"),
            LoadId::from("load-a"),
            writer_properties(&Default::default()).expect("props"),
            // Zero seconds is not configurable — the gate refuses it —
            // but constructed directly it makes "already elapsed" the
            // condition under test without sleeping.
            PartsWiring {
                options: parts::Options {
                    roll_after_seconds: Some(0),
                    ..parts::Options::default()
                },
                events: None,
            },
        );
        let stream = TableName::from("events");
        // The FIELD-ID-annotated target, as production derives it — a
        // bare arrow schema writes batches the library cannot place.
        let target = arrow_target("table `events`", &table).expect("target");
        load.reinstall(&stream, "events", table, target.clone())
            .await
            .expect("ensure");

        let batch = RecordBatch::try_new(
            target.clone(),
            vec![std::sync::Arc::new(arrow_array::Int64Array::from(vec![
                1_i64,
            ]))],
        )
        .expect("batch");
        load.write(&stream, batch.clone()).await.expect("write");
        let seq_after_first = load.tables[&stream].window_seq;
        assert!(load.tables[&stream].pending_files.is_empty());

        // The second write finds the first writer already elapsed.
        load.write(&stream, batch).await.expect("write");
        let state = &load.tables[&stream];
        assert!(
            !state.pending_files.is_empty(),
            "the elapsed writer's files were parked for the window"
        );
        assert!(state.writer.is_some(), "a fresh writer took over");
        assert_eq!(
            state.window_seq,
            seq_after_first + 1,
            "the new writer got its own window prefix — reusing one \
             would overwrite the retired writer's files"
        );
    }

    /// 037 US4: the scope is 32 hex chars: write-side and read-side
    /// widths MUST agree (the constant is shared, but nothing pinned
    /// the width itself until 037).
    #[test]
    fn the_scope_is_thirty_two_hex_chars_on_both_sides() {
        let scope = super::super::connector::testhook::scope_of("p");
        assert_eq!(scope.len(), 32, "{scope}");
        assert!(scope.chars().all(|c| c.is_ascii_hexdigit()));
    }

    /// The exact frozen refusal text (037 review r2 F2/F3), pinned as
    /// ONE contiguous string here — the safe remedy leads, the widen
    /// is named plainly — so a wording change anywhere in it must
    /// touch this literal, not just a keyword fragment. Shared by
    /// every offline test that reaches the refusal, including the
    /// enforcement-site test below that goes through the real
    /// `Backend::read_state`.
    fn frozen_legacy_refusal(pipeline: &PipelineId, legacy_scope: &str) -> String {
        format!(
            "state for pipeline `{pipeline}` predates this build: the pipeline scope key \
             widened (12-hex to 32-hex); point the pipeline at a fresh warehouse or namespace, \
             or — accepting that the table already holds every previously-loaded row and \
             Append would re-add them — remove the stale `rdlt.state.{legacy_scope}` property \
             from the `_rdlt_state` table and re-run"
        )
    }

    /// 037 D1: state stranded under the pre-037 12-hex key for THIS
    /// pipeline refuses typed, with the frozen spelling naming both
    /// the pipeline and the legacy property key — never silently
    /// treated as a fresh pipeline, which would let Append duplicate
    /// every row the pipeline already committed.
    #[tokio::test]
    async fn legacy_scoped_state_for_this_pipeline_refuses_typed() {
        use super::super::testsupport::{ConflictCatalog, test_table_with_properties};

        let pipeline = PipelineId::from("legacy-pipeline");
        let legacy_scope = ident_hash(pipeline.as_str(), state::LEGACY_SCOPE_HASH_LEN);
        let doc = StateDoc::new(pipeline.clone(), "test");
        let mut properties = std::collections::HashMap::new();
        properties.insert(
            format!("rdlt.state.{legacy_scope}"),
            serde_json::to_string(&doc).expect("state json"),
        );
        let table = test_table_with_properties(properties);
        let catalog = ConflictCatalog::over(table, 0);
        let arc: Arc<dyn Catalog> = catalog.clone();

        let err = refuse_legacy_state(&arc, &NamespaceIdent::new("ns".into()), &pipeline)
            .await
            .expect_err("pre-037 state for this pipeline must refuse");
        let text = format!("{err}");
        assert!(
            text.contains(&frozen_legacy_refusal(&pipeline, &legacy_scope)),
            "{text}"
        );
    }

    /// 037 review r2 F1: the enforcement site is `Backend::read_state`
    /// itself, not just the private `refuse_legacy_state` helper — a
    /// mutation collapsing `read_state`'s `None` arm back to `Ok(None)`
    /// (the exact pre-fix defect) left every helper-direct test above
    /// green while the real bug — silent fresh-run duplication — stayed
    /// alive, because none of them ever call through the trait method.
    /// This drives a full `Load` through `Backend::read_state`, the
    /// same construction idiom the schema-retirement tests use.
    /// Red-proved: reverting `read_state`'s legacy-probe arm to
    /// `Ok(None)` turns this green-suite entry red while the three
    /// helper-direct tests above stayed green.
    #[tokio::test]
    async fn read_state_through_the_backend_trait_refuses_legacy_state() {
        use rdlt_connector_sdk::config::Document;

        use super::super::testsupport::{ConflictCatalog, test_table_with_properties};
        use super::super::write::writer_properties;

        let pipeline = PipelineId::from("legacy-pipeline-e2e");
        let legacy_scope = ident_hash(pipeline.as_str(), state::LEGACY_SCOPE_HASH_LEN);
        let doc = StateDoc::new(pipeline.clone(), "test");
        let mut properties = std::collections::HashMap::new();
        properties.insert(
            format!("rdlt.state.{legacy_scope}"),
            serde_json::to_string(&doc).expect("state json"),
        );
        let table = test_table_with_properties(properties);
        let catalog = ConflictCatalog::over(table, 0);
        let arc: Arc<dyn Catalog> = catalog.clone();
        let config = Config::from_value(serde_json::json!({
            "catalog": {
                "uri": "http://localhost:1/api/catalog",
                "warehouse": "wh",
                "auth": {"bearer": {"token": "t"}},
            },
            "namespace": "ns",
        }))
        .expect("valid");
        let mut load = Load::new(
            config,
            arc,
            NamespaceIdent::new("ns".into()),
            &pipeline,
            LoadId::from("l"),
            writer_properties(&Default::default()).expect("props"),
            PartsWiring {
                options: parts::Options::default(),
                events: None,
            },
        );

        let err = load
            .read_state(&pipeline)
            .await
            .expect_err("Backend::read_state must refuse, not silently agree to a fresh run");
        let text = format!("{err}");
        assert!(
            text.contains(&frozen_legacy_refusal(&pipeline, &legacy_scope)),
            "{text}"
        );
    }

    /// A legacy-key property belonging to a DIFFERENT pipeline — a
    /// hash collision on the narrower 12-hex width — is NOT this
    /// pipeline's state: a clean `None`, the same filter the 32-hex
    /// path applies, not a refusal.
    #[tokio::test]
    async fn a_legacy_scoped_collision_for_another_pipeline_stays_a_clean_none() {
        use super::super::testsupport::{ConflictCatalog, test_table_with_properties};

        let pipeline = PipelineId::from("this-pipeline");
        let legacy_scope = ident_hash(pipeline.as_str(), state::LEGACY_SCOPE_HASH_LEN);
        let other = StateDoc::new(PipelineId::from("some-other-pipeline"), "test");
        let mut properties = std::collections::HashMap::new();
        properties.insert(
            format!("rdlt.state.{legacy_scope}"),
            serde_json::to_string(&other).expect("state json"),
        );
        let table = test_table_with_properties(properties);
        let catalog = ConflictCatalog::over(table, 0);
        let arc: Arc<dyn Catalog> = catalog.clone();

        let result = refuse_legacy_state(&arc, &NamespaceIdent::new("ns".into()), &pipeline)
            .await
            .expect("a collision on the legacy width is not a refusal");
        assert!(result.is_none(), "{result:?}");
    }

    /// 037 review r2 F4: valid JSON under the legacy key with no
    /// `pipeline` field at all still refuses — conservative-loud, the
    /// same as an undecodable document (see the next test) — never a
    /// clean `None` that would silently agree to a fresh run.
    #[tokio::test]
    async fn legacy_json_missing_the_pipeline_field_refuses_typed() {
        use super::super::testsupport::{ConflictCatalog, test_table_with_properties};

        let pipeline = PipelineId::from("fieldless-pipeline");
        let legacy_scope = ident_hash(pipeline.as_str(), state::LEGACY_SCOPE_HASH_LEN);
        let mut properties = std::collections::HashMap::new();
        properties.insert(
            format!("rdlt.state.{legacy_scope}"),
            serde_json::json!({"cursor": 9}).to_string(),
        );
        let table = test_table_with_properties(properties);
        let catalog = ConflictCatalog::over(table, 0);
        let arc: Arc<dyn Catalog> = catalog.clone();

        let err = refuse_legacy_state(&arc, &NamespaceIdent::new("ns".into()), &pipeline)
            .await
            .expect_err("JSON missing `pipeline` under our own legacy key must refuse");
        let text = format!("{err}");
        assert!(
            text.contains(&frozen_legacy_refusal(&pipeline, &legacy_scope)),
            "{text}"
        );
    }

    /// 037 review r2 F4: JSON under the legacy key that this build
    /// cannot even parse refuses — the gate reads the raw property
    /// before any typed decode, and an unrecognizable document sitting
    /// under a key hashed from THIS pipeline's own name is treated as
    /// genuine pre-037 state, not silently waved through as a first
    /// run.
    #[tokio::test]
    async fn undecodable_json_under_the_legacy_key_refuses_typed() {
        use super::super::testsupport::{ConflictCatalog, test_table_with_properties};

        let pipeline = PipelineId::from("undecodable-pipeline");
        let legacy_scope = ident_hash(pipeline.as_str(), state::LEGACY_SCOPE_HASH_LEN);
        let mut properties = std::collections::HashMap::new();
        properties.insert(
            format!("rdlt.state.{legacy_scope}"),
            "this is not json".to_owned(),
        );
        let table = test_table_with_properties(properties);
        let catalog = ConflictCatalog::over(table, 0);
        let arc: Arc<dyn Catalog> = catalog.clone();

        let err = refuse_legacy_state(&arc, &NamespaceIdent::new("ns".into()), &pipeline)
            .await
            .expect_err("undecodable JSON under our own legacy key must refuse");
        let text = format!("{err}");
        assert!(
            text.contains(&frozen_legacy_refusal(&pipeline, &legacy_scope)),
            "{text}"
        );
    }

    /// No property under either key: a genuine first run, not a
    /// refusal.
    #[tokio::test]
    async fn no_legacy_state_at_all_stays_a_clean_none() {
        use super::super::testsupport::ConflictCatalog;

        let pipeline = PipelineId::from("brand-new-pipeline");
        let catalog = ConflictCatalog::failing(0);
        let arc: Arc<dyn Catalog> = catalog.clone();

        let result = refuse_legacy_state(&arc, &NamespaceIdent::new("ns".into()), &pipeline)
            .await
            .expect("no state anywhere is a first run");
        assert!(result.is_none(), "{result:?}");
    }
}
