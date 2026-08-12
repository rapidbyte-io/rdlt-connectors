//! Run-scoped CDC state: the replica-identity preflight, the shared control
//! and snapshot connections, and per-stream ack/cursor bookkeeping — one
//! instance per engine run.
//!
//! The connection accessors FUSE ensure-and-get: a caller asks for the
//! control or snapshot connection and receives an `Arc` in one call, so
//! there is no window in which "opened earlier" must be assumed — the
//! generation-one design split ensure from get and needed two audited
//! `expect()` panic sites to bridge them.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use rdlt_connector_sdk::spi::{Cursor, SourceError};
use serde::{Deserialize, Serialize};

use crate::session;
use crate::source::config::{CdcConfig, Config};
use crate::source::errors::{self, Phase};
use crate::source::reflect;

use super::slot;

/// The engine cursor for a CDC stream: one LSN. FROZEN JSON shape
/// (`{"cdc_lsn": …}`), deliberately distinct from the cursor-column state so
/// misrouted state fails typed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Resume {
    pub(super) cdc_lsn: u64,
}

impl Resume {
    pub(super) fn encode(self) -> Cursor {
        Cursor::new(serde_json::to_value(self).expect("cdc cursor serializes"))
    }

    pub(super) fn decode(cursor: &Cursor, table: &str) -> Result<Self, SourceError> {
        serde_json::from_value(cursor.as_value().clone()).map_err(|e| {
            errors::fatal(
                Phase::Slot,
                Some(table),
                format!("stored CDC cursor does not decode (state corruption?): {e}"),
            )
        })
    }
}

/// Per-table replica-identity preflight result.
#[derive(Debug, Clone)]
pub(crate) struct Identity {
    /// The merge key: the table's replica identity columns.
    pub(crate) key: Vec<String>,
    /// REPLICA IDENTITY FULL — old tuples carry every column (TOAST
    /// substitution is possible).
    pub(crate) covers_all_columns: bool,
}

/// Run-scoped CDC state on the source (one instance per engine run).
///
/// "One per run" is a CONTRACT, not an observation, and out of process it
/// becomes the provider's: this is a field of the connector instance, so a
/// spawned connector binary holds exactly one for its lifetime and the
/// provider ties that lifetime to the engine's handle. An embedder who
/// POOLS or reuses a connector process across engine runs breaks it, and
/// the first symptom is silent no-progress rather than an error:
/// `RunState::target` is pinned once at the first CDC read and nothing
/// resets it, so run 2 resumes from run 1's committed cursor, finds run
/// 1's stale target already at or behind it, skips the change pass
/// (`read.rs`, `if target > since`) and reports a clean empty run. No loss
/// and no falsely-advancing cursor — the stream just stops moving while
/// looking healthy.
///
/// WHY THIS IS A CONTRACT AND NOT A GUARD, since both obvious guards are
/// wrong. Refusing a second `Read` after a CDC stream ends would break
/// the engine's own transient retry: retries are RUN-level and each
/// attempt is a full run from committed state (`rdlt-engine`
/// `runtime/run.rs`), re-reading every stream inside the same connector
/// instance — legitimate, common, and indistinguishable at this layer
/// from a pooled second run. Resetting `target` whenever a fresh
/// `since` arrives at or before it would fire on exactly that retry
/// path and re-pin the target mid-run, destroying the cross-table
/// single-instant view the pinned target exists to provide. A guard that
/// could tell the two apart needs a RUN identity, and the v0 wire
/// declares none — it is frozen, and adding one is a protocol decision,
/// not a connector fix. So the obligation sits with whoever owns process
/// lifetime.
pub(crate) struct Runtime {
    pub(super) state: tokio::sync::Mutex<RunState>,
    identities: tokio::sync::OnceCell<BTreeMap<String, Identity>>,
}

impl std::fmt::Debug for Runtime {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // RunState holds live connections (no Debug); the identities map is
        // the useful part.
        formatter
            .debug_struct("cdc::Runtime")
            .field("identities", &self.identities.get())
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
pub(super) struct RunState {
    /// Lifecycle connection, opened on first use with
    /// [`session::Profile::CdcControl`] (the pgoutput TEXT forms depend on
    /// its GUC pins); also carries peeks and the ack. Arc so passes run
    /// WITHOUT the state lock held (a stream blocked on destination
    /// backpressure must never stall the other streams), and dropped on any
    /// error (the engine's transient in-run retries re-enter with this same
    /// state — a dead connection must not be reused).
    control: Option<Arc<session::Connection>>,
    pub(super) ensured: Option<slot::EnsureOutcome>,
    /// Open REPEATABLE READ snapshot transaction (first run) — ONE view for
    /// every CDC table on the error-free path. Arc + drop-on-error like
    /// `control`; after a mid-run transient error the replacement snapshot
    /// opens at a LATER horizon, so streams that snapshot after the error
    /// see a newer view than those before it (each stream stays
    /// individually gap-free — its cursor start precedes its own view; the
    /// cross-table single-instant property is best-effort under errors,
    /// exactly as in generation 1).
    snapshot: Option<Arc<session::Connection>>,
    /// The shared snapshot's cursor start point for a PRE-EXISTING slot:
    /// the WAL position read BEFORE the transaction began (its visibility
    /// horizon) — every commit ≤ it is IN the snapshot; commits after it
    /// replay and converge. (Starting at confirmed_flush instead would
    /// replay a window that can contain unappliable records — TRUNCATE,
    /// TOAST without an old image — and permanently wedge recovery.)
    pub(super) snapshot_horizon: Option<u64>,
    /// The run's target position, pinned once at the first CDC read.
    pub(super) target: Option<u64>,
    /// Per-stream ack floors: destination-committed resume positions, or
    /// the fresh snapshot start point.
    pub(super) ack_floor: BTreeMap<String, u64>,
    /// CDC streams that have not completed their pass this run; `None`
    /// until the first CDC read initializes it.
    pub(super) pending: Option<BTreeSet<String>>,
    /// Each stream's final cursor this run — the lag report's baseline.
    pub(super) final_cursor: BTreeMap<String, u64>,
}

impl RunState {
    /// The lifecycle connection, opened if this run has not yet — GUC pins
    /// applied by the session profile. Idempotent within a run; the
    /// returned Arc outlives the state lock. A PROFILE failure (the GUC
    /// SETs) is a CDC lifecycle fault and is tagged with the slot phase and
    /// the asking stream; a connect failure keeps the ordinary connect
    /// framing.
    pub(super) async fn control(
        &mut self,
        config: &Config,
        stream: &str,
    ) -> Result<Arc<session::Connection>, SourceError> {
        if let Some(control) = &self.control {
            return Ok(Arc::clone(control));
        }
        let connection = session::establish(
            &config.connection,
            config.tls.as_ref(),
            session::Profile::CdcControl,
        )
        .await
        .map_err(|error| match error {
            session::EstablishError::Profile { detail, transient } => {
                if transient {
                    errors::transient(Phase::Slot, Some(stream), detail)
                } else {
                    errors::fatal(Phase::Slot, Some(stream), detail)
                }
            }
            other => errors::establish_failure(other),
        })?;
        let control = Arc::new(connection);
        self.control = Some(Arc::clone(&control));
        Ok(control)
    }

    /// The shared REPEATABLE READ snapshot transaction, opened if this run
    /// has not yet, recording its visibility horizon. Idempotent — the
    /// first CDC stream opens it; every snapshot pass this run shares it.
    pub(super) async fn snapshot(
        &mut self,
        config: &Config,
        stream: &str,
        horizon: u64,
    ) -> Result<Arc<session::Connection>, SourceError> {
        if let Some(snapshot) = &self.snapshot {
            return Ok(Arc::clone(snapshot));
        }
        let connection = session::establish(
            &config.connection,
            config.tls.as_ref(),
            session::Profile::Plain,
        )
        .await
        .map_err(errors::establish_failure)?;
        connection
            .batch_execute("BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            .await
            .map_err(|e| errors::classify(Phase::Copy, Some(stream), &e))?;
        let snapshot = Arc::new(connection);
        self.snapshot = Some(Arc::clone(&snapshot));
        self.snapshot_horizon = Some(horizon);
        Ok(snapshot)
    }

    /// Close the shared snapshot transaction's connection (run drained).
    pub(super) fn close_snapshot(&mut self) {
        self.snapshot = None;
    }

    /// Drop every cached connection — called on any error path, because the
    /// engine's transient in-run retries re-enter with this same state and
    /// a dead connection must never be reused.
    pub(super) fn drop_connections(&mut self) {
        self.control = None;
        self.snapshot = None;
        self.snapshot_horizon = None;
    }
}

impl Runtime {
    pub(crate) fn new() -> Self {
        Self {
            state: tokio::sync::Mutex::new(RunState::default()),
            identities: tokio::sync::OnceCell::new(),
        }
    }

    /// The replica-identity preflight, once per run: key + coverage flag
    /// per CDC table, flag-column collisions, unusable identities — all
    /// typed BEFORE any stream declares itself.
    pub(crate) async fn identities(
        &self,
        config: &Config,
        cdc: &CdcConfig,
        reflected: &BTreeMap<String, reflect::Table>,
        cdc_tables: &[String],
    ) -> Result<&BTreeMap<String, Identity>, SourceError> {
        self.identities
            .get_or_try_init(|| async {
                let connection = crate::source::connect(config).await?;
                preflight(&connection, config, cdc, reflected, cdc_tables).await
            })
            .await
    }
}

async fn preflight(
    connection: &session::Connection,
    config: &Config,
    cdc: &CdcConfig,
    reflected: &BTreeMap<String, reflect::Table>,
    cdc_tables: &[String],
) -> Result<BTreeMap<String, Identity>, SourceError> {
    let rows = connection
        .query(
            "SELECT c.relname, c.relreplident::text,
                    coalesce((SELECT array_agg(a.attname ORDER BY x.ord)
                              FROM pg_index i
                              CROSS JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS x(attnum, ord)
                              JOIN pg_attribute a
                                ON a.attrelid = i.indrelid AND a.attnum = x.attnum
                              WHERE i.indrelid = c.oid AND i.indisreplident),
                             '{}') AS ident_cols
             FROM pg_class c
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relname = ANY($2)",
            &[&config.schema, &cdc_tables],
        )
        .await
        .map_err(|e| errors::classify(Phase::Slot, None, &e))?;
    let facts: HashMap<String, (String, Vec<String>)> = rows
        .into_iter()
        .map(|row| {
            (
                row.get::<_, String>(0),
                (row.get::<_, String>(1), row.get::<_, Vec<String>>(2)),
            )
        })
        .collect();

    let mut identities = BTreeMap::new();
    for table in cdc_tables {
        let reflected_table = reflected.get(table).ok_or_else(|| {
            errors::fatal(Phase::Slot, Some(table), "CDC table has no reflected shape")
        })?;
        if reflected_table.column(&cdc.flag_column).is_some() {
            return Err(errors::fatal(
                Phase::Slot,
                Some(table),
                format!(
                    "flag column `{}` collides with an existing column — set \
                     `cdc.flag_column` to an unused name",
                    cdc.flag_column
                ),
            ));
        }
        let (replica_identity, identity_columns) = facts.get(table).ok_or_else(|| {
            errors::fatal(
                Phase::Slot,
                Some(table),
                "CDC table not found in the catalog",
            )
        })?;
        let primary_key: Vec<String> = reflected_table
            .primary_key()
            .iter()
            .map(|name| (*name).to_owned())
            .collect();
        let declared = config
            .table_config(table)
            .and_then(|table_config| table_config.primary_key.clone());
        let identity = match replica_identity.as_str() {
            // Default: delete/update records carry the PRIMARY KEY columns.
            "d" if !primary_key.is_empty() => Identity {
                key: primary_key,
                covers_all_columns: false,
            },
            "d" => {
                return Err(errors::fatal(
                    Phase::Slot,
                    Some(table),
                    "CDC needs a usable replica identity and the table has no \
                     primary key — add one, or `ALTER TABLE … REPLICA IDENTITY \
                     FULL` / `USING INDEX …`",
                ));
            }
            // Explicit index: the identity columns come from that index. A
            // dropped identity index leaves relreplident = 'i' with NO
            // indisreplident row — an empty key would merge on nothing and
            // corrupt deletes, so it is a typed error, never accepted.
            "i" if !identity_columns.is_empty() => Identity {
                key: identity_columns.clone(),
                covers_all_columns: false,
            },
            "i" => {
                return Err(errors::fatal(
                    Phase::Slot,
                    Some(table),
                    "REPLICA IDENTITY USING INDEX but no replica-identity \
                     index exists (was it dropped?) — recreate the index or \
                     `ALTER TABLE … REPLICA IDENTITY DEFAULT`/`FULL`",
                ));
            }
            // FULL: old tuples carry everything, so ANY declared key has its
            // values — a declared primary_key override wins; else the
            // reflected primary key.
            "f" => match declared
                .clone()
                .or_else(|| (!primary_key.is_empty()).then(|| primary_key.clone()))
            {
                Some(key) => Identity {
                    key,
                    covers_all_columns: true,
                },
                None => {
                    return Err(errors::fatal(
                        Phase::Slot,
                        Some(table),
                        "REPLICA IDENTITY FULL but no key to merge by — add a \
                         primary key or declare `primary_key` on the table",
                    ));
                }
            },
            // NOTHING (or anything else): updates/deletes carry no key
            // data.
            other => {
                return Err(errors::fatal(
                    Phase::Slot,
                    Some(table),
                    format!(
                        "replica identity `{other}` cannot replicate \
                         updates/deletes — `ALTER TABLE … REPLICA IDENTITY \
                         DEFAULT` (with a primary key) or `FULL`"
                    ),
                ));
            }
        };
        // A declared primary_key that disagrees with the identity columns
        // would leave delete records without values for the merge key —
        // typed, not silently ignored. (When the identity covers all
        // columns, any declared key works.)
        if let Some(declared) = declared
            && !identity.covers_all_columns
            && declared != identity.key
        {
            return Err(errors::fatal(
                Phase::Slot,
                Some(table),
                format!(
                    "declared primary_key {declared:?} differs from the replica \
                     identity columns {:?} — delete records only carry the \
                     identity columns; align them or use REPLICA IDENTITY FULL",
                    identity.key
                ),
            ));
        }
        // Defense in depth: every arm above guarantees a non-empty key; an
        // empty one would pass every downstream guard vacuously.
        assert!(!identity.key.is_empty(), "preflight resolved an empty key");
        identities.insert(table.clone(), identity);
    }
    Ok(identities)
}
