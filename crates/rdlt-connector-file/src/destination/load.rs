//! One load session's system IO — the [`Backend`] behind the sdk's
//! choreography, mapping the 4-phase commit onto files.
//!
//! The phases, in publish order:
//! 1. REPLAY DEDUP is the framework's: `existing_receipt` reads the
//!    persisted commit log and `replay` discards this session's staged
//!    parts, so a redelivered commit returns the prior receipt without
//!    republishing.
//! 2. REPLACE TRUNCATION, guarded DURABLY: "has any earlier commit of
//!    THIS load landed?" is read from the receipt log, never session
//!    memory — a crash-recovery session must not re-truncate files a
//!    prior commit of this load already published, and if no receipt
//!    landed, re-truncating is convergent under WAL re-delivery.
//! 3. PUBLISH each staged part to its deterministic final name, then
//!    (local only) fsync every touched directory.
//! 4. RECORD state, then the receipt LAST — the durable idempotency
//!    guard.

use std::collections::{BTreeMap, BTreeSet};

use async_trait::async_trait;
use parquet::file::properties::WriterProperties;
use rdlt_connector_sdk::destination::Backend;
use rdlt_connector_sdk::spi::core::crash_point;
use rdlt_connector_sdk::spi::core::{
    CommitMeta, CommitReceipt, LoadId, PipelineId, StateDoc, TableName, TableSchema, WriteMode,
};
use rdlt_connector_sdk::spi::{DestinationError, RecordBatch};

use super::config::DestFormat;
use super::layout::{
    CommitLog, LAYOUT_FORMAT_VERSION, Manifest, commits_file, final_tail, manifest_file,
    staged_name, staging_tail, state_file,
};
use super::lease::Lease;
use super::stage::{OpenPart, split_partitions};
use super::truncate::truncate_table;
use crate::location::Location;

/// Part sizing and its telemetry, grouped: the options that decide
/// when a part closes travel with the listener told when one does.
pub(super) struct PartsWiring {
    pub(super) options: rdlt_connector_sdk::spi::PartOptions,
    pub(super) events: Option<rdlt_connector_sdk::spi::PartEventFn>,
}

/// The identity `Load::open` acquires the session lease under (037 US2
/// T7): the raw pipeline name (the lease's messages name it verbatim —
/// `scope_of` is a one-way hash, unsuitable for a human-facing refusal)
/// and this connector instance's process-unique owner token.
pub(super) struct LeaseWiring {
    pub(super) pipeline: String,
    pub(super) owner: String,
}

/// How this session encodes output — grouped so `Load::open` stays
/// under clippy's argument-count ceiling.
pub(super) struct WriterWiring {
    pub(super) format: DestFormat,
    pub(super) partition_by: Option<String>,
    pub(super) props: WriterProperties,
}

/// The session state: where to write, how to encode, and what has been
/// staged so far.
pub struct Load {
    location: Location,
    format: DestFormat,
    partition_by: Option<String>,
    props: WriterProperties,
    scope: String,
    load_id: LoadId,
    /// Write dispositions recorded by `ensure_table` — Replace is what
    /// truncation iterates.
    tables: BTreeMap<TableName, WriteMode>,
    /// Staged parts awaiting the next publish, in staging order. The
    /// part INDEX is this list's per-(table, partition) count — per
    /// COMMIT, not per session, deliberately: a crash-recovery session
    /// resumes from committed state and stages FEWER parts, so a
    /// session-lifetime count would re-publish the pending commit
    /// under different indices and orphan the crashed attempt's
    /// already-copied finals (measured live in the S3 sweep: 6 rows
    /// where 4 were loaded).
    staged: Vec<StagedPart>,
    /// When a part is closed and the next begun.
    parts: rdlt_connector_sdk::spi::PartOptions,
    /// Parts still being written, keyed by table and partition — a
    /// part holds one table's rows for one partition, so those two
    /// are what identify it.
    open: BTreeMap<(String, Option<String>), (OpenPart, std::time::Instant)>,
    /// Where closed parts are reported. Advisory: absent changes
    /// nothing about what is written.
    part_events: Option<rdlt_connector_sdk::spi::PartEventFn>,
    /// This session's hold on the destination scope (037 US2 T7; fix
    /// round 1 moved the release point). `Some` from a successful
    /// `open` until `Backend::close` releases it — the sdk's success-
    /// path-only session-end hook, called exactly once after the
    /// engine's LAST commit, never between commits of a multi-commit
    /// session. Fix round 1's own history is worth keeping: the FIRST
    /// wiring released at the end of the FIRST successful `publish`,
    /// which left every later commit of a multi-commit session
    /// unprotected (a second session's `open` could race in between
    /// commit 1 and commit 2 and destroy this session's still-pending
    /// staging via `prepare_staging`'s scope-wide wipe) — exactly the
    /// hazard this whole lease exists to close, reopened by releasing
    /// too early. `None` only after `close` has run; `check_still_held`
    /// then has nothing left to check, which is correct because there
    /// is no more session activity left for it to guard.
    lease: Option<Lease>,
}

impl std::fmt::Debug for Load {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Manual because the part-event listener is a function: whether
        // one is attached is the only fact worth printing about it.
        f.debug_struct("Load")
            .field("scope", &self.scope)
            .field("load_id", &self.load_id)
            .field("format", &self.format)
            .field("staged", &self.staged.len())
            .field("open", &self.open.len())
            .field("part_events", &self.part_events.is_some())
            .field("lease_held", &self.lease.is_some())
            .finish_non_exhaustive()
    }
}

/// Read the receipt log.
///
/// A FREE function rather than a method, and the reason is load-bearing:
/// `Load` holds an open `ArrowWriter`, which is `Send` but never `Sync`,
/// so a `&self` borrow held across an await would not compile. Taking
/// the two fields it reads keeps the borrow narrow enough to be `Send`.
async fn commit_log(location: &Location, scope: &str) -> Result<CommitLog, DestinationError> {
    let file = commits_file(scope);
    let bytes = location.read_doc(&file).await?;
    CommitLog::decode(bytes.as_deref(), &file)
}

#[derive(Debug)]
struct StagedPart {
    table: String,
    partition: Option<String>,
    index: usize,
    staging_tail: String,
}

impl Load {
    /// Open one session: acquire the scope's lease FIRST (037 US2 T7),
    /// then reclaim the scope's staging (a crashed predecessor's
    /// debris) — in that order, deliberately: the lease is what makes
    /// `prepare_staging`'s scope-wide wipe safe to run at all, since it
    /// is the lease that refuses a second live session before that wipe
    /// could ever run against one. Ready this load's area last.
    pub(super) async fn open(
        location: Location,
        writer: WriterWiring,
        scope: String,
        load_id: LoadId,
        parts: PartsWiring,
        lease_wiring: LeaseWiring,
    ) -> Result<Self, DestinationError> {
        let mut lease = Lease::acquire(
            location.clone(),
            &scope,
            &lease_wiring.pipeline,
            &lease_wiring.owner,
        )
        .await?;
        lease.start_heartbeat();
        location.prepare_staging(&scope, load_id.as_str()).await?;
        Ok(Self {
            parts: parts.options,
            part_events: parts.events,
            open: BTreeMap::new(),
            location,
            format: writer.format,
            partition_by: writer.partition_by,
            props: writer.props,
            scope,
            load_id,
            tables: BTreeMap::new(),
            staged: Vec::new(),
            lease: Some(lease),
        })
    }

    /// Close one open part and stage it.
    ///
    /// Staging is what makes the part real: until then it exists only
    /// as encoded bytes in memory, and a crash simply loses it — which
    /// is correct, because nothing has claimed it landed.
    async fn close_part(
        &mut self,
        key: &(String, Option<String>),
        reason: rdlt_connector_sdk::spi::PartCloseReason,
    ) -> Result<(), DestinationError> {
        let Some((open, _)) = self.open.remove(key) else {
            return Ok(());
        };
        let (table, partition) = key;
        let bytes = open.finish()?;
        if bytes.is_empty() {
            return Ok(());
        }
        let encoded_bytes = bytes.len() as u64;
        let index = self
            .staged
            .iter()
            .filter(|s| &s.table == table && &s.partition == partition)
            .count();
        let name = staged_name(table, partition.as_deref(), index, self.format.extension());
        let tail = staging_tail(&self.scope, self.load_id.as_str(), &name);
        self.location.stage_put(&tail, bytes).await?;
        self.staged.push(StagedPart {
            table: table.clone(),
            partition: partition.clone(),
            index,
            staging_tail: tail,
        });
        // Reported once STAGED — the bytes are final and durable-ish;
        // the exact size is the file's, never an estimate.
        if let Some(listener) = &self.part_events {
            listener(rdlt_connector_sdk::spi::PartClosed::new(
                rdlt_connector_sdk::spi::core::TableName::new(table.as_str()),
                encoded_bytes,
                reason,
            ));
        }
        Ok(())
    }

    /// Keep the open parts inside their memory ceiling.
    ///
    /// The LARGEST is closed first: it is nearest its target, so the
    /// part this produces is the least undersized one available. The
    /// loop terminates because each pass removes one part, and an
    /// empty set holds nothing.
    async fn enforce_open_budget(&mut self) -> Result<(), DestinationError> {
        loop {
            let total: u64 = self.open.values().map(|(open, _)| open.encoded_len()).sum();
            if !self.parts.over_budget(total) {
                return Ok(());
            }
            let Some(largest) = self
                .open
                .iter()
                .max_by_key(|(_, (open, _))| open.encoded_len())
                .map(|(key, _)| key.clone())
            else {
                return Ok(());
            };
            self.close_part(&largest, rdlt_connector_sdk::spi::PartCloseReason::Budget)
                .await?;
        }
    }

    /// Close EVERY open part.
    ///
    /// Called before publish: a part never spans a commit, because the
    /// publish protocol moves whole staged files and a half-written
    /// one has nothing to move.
    async fn close_all_parts(&mut self) -> Result<(), DestinationError> {
        let keys: Vec<_> = self.open.keys().cloned().collect();
        for key in keys {
            self.close_part(&key, rdlt_connector_sdk::spi::PartCloseReason::Commit)
                .await?;
        }
        Ok(())
    }

    /// The frozen pre-015 truncation addendum applies only to the
    /// local + parquet + unpartitioned shape it was recorded for.
    fn frozen_plain_parquet(&self) -> bool {
        self.location.is_local()
            && self.format == DestFormat::Parquet
            && self.partition_by.is_none()
    }

    /// The write-path fence (037 US2 T7): refuse rather than write once
    /// this session's hold on the scope is provably gone. A `None`
    /// lease means `Backend::close` already ran (see the `lease`
    /// field's doc) — the engine itself never calls `write`/`publish`
    /// again after `close` (the SDK choreography's own call order makes
    /// that a host defect, not a reachable engine path), so this arm
    /// exists for an EMBEDDER driving the `Backend`/`LoadSession` API
    /// directly and calling a write after its own `close` — refused
    /// typed rather than silently passed (037 US2 fix round 2, M6: the
    /// prior wording here quietly returned `Ok`, which would have let
    /// exactly that misuse through). Frozen spelling, pinned by
    /// `a_write_after_close_is_refused_not_silently_allowed`.
    fn check_lease_still_held(&self) -> Result<(), DestinationError> {
        match &self.lease {
            Some(lease) => lease.check_still_held(),
            None => Err(DestinationError::fatal(
                "the destination session is closed; writes after close are a host defect",
            )),
        }
    }
}

#[async_trait]
impl Backend for Load {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        if matches!(mode, WriteMode::Merge { .. }) {
            return Err(DestinationError::fatal(
                "file destination does not support Merge (capabilities.merge = false)",
            ));
        }
        if let Some(root) = self.location.local_root() {
            std::fs::create_dir_all(root.join(schema.table.as_str()))
                .map_err(|e| DestinationError::fatal(format!("ensure_table: {e}")))?;
        }
        self.tables.insert(schema.table.clone(), mode.clone());
        Ok(())
    }

    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        self.check_lease_still_held()?;
        for (partition, group) in split_partitions(table, &batch, self.partition_by.as_deref())? {
            let key = (table.to_string(), partition.clone());
            // A parquet file carries ONE schema, so a schema change
            // closes the part rather than being appended into it.
            let schema_changed = self
                .open
                .get(&key)
                .is_some_and(|(open, _)| open.schema_differs(&group));
            if schema_changed {
                self.close_part(&key, rdlt_connector_sdk::spi::PartCloseReason::Schema)
                    .await?;
            }
            let (open, opened_at) = match self.open.remove(&key) {
                Some(existing) => existing,
                None => (
                    OpenPart::begin(self.format, &group, &self.props)?,
                    std::time::Instant::now(),
                ),
            };
            let mut open = open;
            open.append(&group)?;
            let encoded = open.encoded_len();
            self.open.insert(key.clone(), (open, opened_at));
            if self
                .parts
                .should_roll(encoded, opened_at.elapsed().as_secs())
            {
                // Which threshold fired decides the reported reason;
                // when both did in one write, size is the truer cause.
                let reason = if self.parts.target_bytes.is_some_and(|t| encoded >= t.max(1)) {
                    rdlt_connector_sdk::spi::PartCloseReason::Target
                } else {
                    rdlt_connector_sdk::spi::PartCloseReason::Time
                };
                self.close_part(&key, reason).await?;
            }
        }
        self.enforce_open_budget().await
    }

    async fn existing_receipt(
        &mut self,
        load_id: &LoadId,
        commit_seq: u64,
    ) -> Result<Option<CommitReceipt>, DestinationError> {
        let log = commit_log(&self.location, &self.scope).await?;
        let key = (load_id.as_str().to_owned(), commit_seq);
        Ok(log
            .receipts
            .iter()
            .any(|r| r == &key)
            .then(|| CommitReceipt {
                load_id: load_id.clone(),
                commit_seq,
            }))
    }

    async fn replay(
        &mut self,
        _meta: &CommitMeta,
        _receipt: &CommitReceipt,
    ) -> Result<(), DestinationError> {
        // A redelivered commit's parts must not linger for a later
        // genuine commit to find. The OPEN ones are dropped outright —
        // they exist only in memory and were never claimed to land —
        // and the staged ones are removed best-effort.
        self.open.clear();
        for part in self.staged.drain(..) {
            self.location.stage_remove(&part.staging_tail).await;
        }
        Ok(())
    }

    async fn publish(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        self.check_lease_still_held()?;
        // Phase 0 — no part spans a commit. Publish moves WHOLE staged
        // files, and a part still open has no file to move, so every
        // one is closed and staged before anything else happens. This
        // makes the commit cadence an upper bound on part size.
        // Numbered 0 deliberately: the contract inventory's four named
        // phases start at 1 = replay dedup (which the sdk choreography
        // performs through `existing_receipt` before publish is ever
        // called), and this step precedes them all.
        self.close_all_parts().await?;

        let mut log = commit_log(&self.location, &self.scope).await?;

        // Phase 2 — Replace truncation, once per LOAD, guarded by the
        // durable receipt log: any earlier commit of this load means
        // its truncation already ran and its parts are live.
        let first_commit_of_load = !log
            .receipts
            .iter()
            .any(|(load, _)| load == meta.load_id.as_str());
        if first_commit_of_load {
            if self.location.is_local() {
                crash_point!(
                    "pq.replace.truncate",
                    Err(DestinationError::fatal(
                        "injected crash at pq.replace.truncate"
                    ))
                );
            }
            let frozen = self.frozen_plain_parquet();
            let replaced: Vec<String> = self
                .tables
                .iter()
                .filter(|(_, mode)| matches!(mode, WriteMode::Replace))
                .map(|(table, _)| table.to_string())
                .collect();
            for table in replaced {
                truncate_table(&self.location, &table, frozen).await?;
            }
        }

        // Phase 3a — the crash-replay CONVERGENCE MANIFEST. Publish
        // overwrites by deterministic name, which converges only while
        // a replayed commit re-stages the SAME set of names — and with
        // `roll_after_seconds` it need not: part boundaries become a
        // function of wall clock, so a retry of a commit that crashed
        // after publishing can stage FEWER parts than its predecessor,
        // and the predecessor's higher-index finals would keep rows the
        // retry's files also carry (030's part-index defect, 6 rows
        // where 4 loaded, reopened through a new mechanism).
        //
        // The mechanism is intent-based, NEVER listing-based: the
        // manifest under this pipeline's scope records the final names
        // this commit is about to publish, durably, BEFORE any of them
        // lands. A retry reads its predecessor's manifest and removes
        // exactly (predecessor's names − its own) — tolerant of
        // absence, since intent covers more than a crash let land.
        // Deleting by directory listing was built first and REJECTED in
        // review: final names carry no pipeline marker and load ids are
        // unique only within a pipeline, so on a load-id collision
        // between pipelines sharing one output table a listing sweep
        // deletes the OTHER pipeline's rows. The manifest can only ever
        // name files this pipeline itself intended.
        //
        // Ordering is load-bearing, twice: the predecessor's stale
        // names are removed while its manifest is STILL the one on
        // record (sweeping after overwriting it would orphan them
        // forever if this attempt then crashed), and the new manifest
        // is durable before the first publish (so no file ever lands
        // unmanifested for the NEXT retry to miss).
        let new_names: BTreeSet<String> = self
            .staged
            .iter()
            .map(|part| {
                final_tail(
                    &part.table,
                    part.partition.as_deref(),
                    meta.load_id.as_str(),
                    meta.commit_seq,
                    part.index,
                    self.format.extension(),
                )
            })
            .collect();
        let manifest_doc = manifest_file(&self.scope);
        let prior = Manifest::decode(
            self.location.read_doc(&manifest_doc).await?.as_deref(),
            &manifest_doc,
        )?;
        let mut touched: BTreeSet<String> = BTreeSet::new();
        if let Some(prior) = prior
            && prior.load_id == meta.load_id.as_str()
            && prior.commit_seq == meta.commit_seq
        {
            for stale in prior.names.iter().filter(|n| !new_names.contains(*n)) {
                self.location.remove_final_if_present(stale).await?;
                if let Some((dir, _)) = stale.rsplit_once('/') {
                    // The deletion rides the same fsync barrier as the
                    // publishes below.
                    touched.insert(dir.to_owned());
                }
            }
        }
        if self.location.is_local() {
            crash_point!(
                "pq.manifest.write",
                Err(DestinationError::fatal(
                    "injected crash at pq.manifest.write"
                ))
            );
        }
        self.location
            .write_doc(
                &manifest_doc,
                &Manifest {
                    format_version: LAYOUT_FORMAT_VERSION,
                    load_id: meta.load_id.as_str().to_owned(),
                    commit_seq: meta.commit_seq,
                    names: new_names.iter().cloned().collect(),
                },
            )
            .await?;

        // Phase 3 — publish every staged part to its deterministic
        // final name, then make the renames durable (local only).
        for part in std::mem::take(&mut self.staged) {
            let tail = final_tail(
                &part.table,
                part.partition.as_deref(),
                meta.load_id.as_str(),
                meta.commit_seq,
                part.index,
                self.format.extension(),
            );
            self.location
                .publish_part(&part.staging_tail, &tail)
                .await?;
            touched.insert(part.table.clone());
            if let Some(partition) = &part.partition {
                touched.insert(format!("{}/{partition}", part.table));
            }
        }
        if self.location.is_local() {
            crash_point!(
                "pq.dir.fsync",
                Err(DestinationError::fatal("injected crash at pq.dir.fsync"))
            );
            for dir in &touched {
                self.location.sync_dir(dir)?;
            }
        }

        // Phase 4 — state, then the receipt LAST.
        if self.location.is_local() {
            crash_point!(
                "pq.state.write",
                Err(DestinationError::fatal("injected crash at pq.state.write"))
            );
        }
        self.location
            .write_doc(&state_file(&self.scope), &meta.state)
            .await?;
        if self.location.is_local() {
            crash_point!(
                "pq.receipt.write",
                Err(DestinationError::fatal(
                    "injected crash at pq.receipt.write"
                ))
            );
        }
        log.format_version = LAYOUT_FORMAT_VERSION;
        log.receipts
            .push((meta.load_id.as_str().to_owned(), meta.commit_seq));
        self.location
            .write_doc(&commits_file(&self.scope), &log)
            .await?;

        Ok(CommitReceipt {
            load_id: meta.load_id,
            commit_seq: meta.commit_seq,
        })
    }

    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        let file = state_file(&super::layout::scope_of(pipeline.as_str()));
        let Some(bytes) = self.location.read_doc(&file).await? else {
            return Ok(None);
        };
        let state: StateDoc = serde_json::from_slice(&bytes)
            .map_err(|e| DestinationError::fatal(format!("unreadable state `{file}`: {e}")))?;
        // The scope is a 12-hex hash: on the astronomically unlikely
        // collision, the embedded pipeline id is the truth.
        Ok(Some(state).filter(|s| &s.pipeline == pipeline))
    }

    /// The session's orderly end (037 US2 T7 fix round 1): release the
    /// lease HERE, at true session close — not at the end of the FIRST
    /// successful publish, which left every later commit of a
    /// multi-commit session unprotected (the hole this fix closes; see
    /// the `lease` field's doc for the shape of the bug). The sdk
    /// choreography calls this exactly once whenever the session ends —
    /// TWO modes, not one (037 final-review wave, item 3): on the
    /// success path, after the engine's last commit, where a close
    /// error would propagate as a genuine failure; and on every
    /// ABANDONMENT path (a failed or cancelled run), where the engine
    /// invokes it best-effort and discards whatever it returns (see
    /// `LoadSession::close`'s trait doc). `self.lease` is `Some` on
    /// both paths in practice; the `None` arm exists only because
    /// `Option::take` is the shape `check_lease_still_held` also
    /// needs, not because a second `close` is expected.
    /// `Lease::release` is internally best-effort regardless of which
    /// mode got us here (its own T6 contract: absence is success, a
    /// failed delete is swallowed, an unreleased lease still expires
    /// by `TTL_SECS` — and, since the final-review wave, a release
    /// that finds the document no longer names US as the owner skips
    /// the delete entirely rather than vandalizing whoever took it
    /// over unobserved; see `Lease::release`'s own doc) — nothing here
    /// can turn a release failure into a run failure, which is why
    /// this method itself always returns `Ok`.
    async fn close(&mut self) -> Result<(), DestinationError> {
        if let Some(lease) = self.lease.take() {
            lease.release().await;
        }
        Ok(())
    }
}
