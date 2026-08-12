//! The pipeline-scoped state document — and the load-level receipt —
//! as marker-table properties.
//!
//! State lives in the properties of a dedicated `_rdlt_state` table in
//! the destination namespace. NOT namespace properties —
//! `update_namespace` is unimplemented in iceberg-catalog-rest 0.10
//! (`FeatureUnsupported`, verified live) — and NOT stream-table
//! properties, because a resume cannot enumerate stream tables from
//! the destination config alone; a fixed name is enumerable. The write
//! and read sides compose the key through ONE [`state_key`], so a
//! resume reads exactly where the commit wrote.
//!
//! FORMAT NOTES — the marker table's property vocabulary (additive,
//! landed on the unpublished 042 branch):
//!
//! - `rdlt.state.<scope>` → the pipeline's state document (JSON), one
//!   per 32-hex pipeline scope. Since the beginning of this crate's
//!   second generation.
//! - `rdlt.receipt.<load_id>` →
//!   `{"commit_seq":N,"stamped_at_ms":M,"scope":S}`, the load-level
//!   receipt (042 round-2 fix wave): `N` is the highest commit
//!   sequence published for that load (sequences are monotone per
//!   load, so membership is `seq <= N` — a replayed early window
//!   merges by MAX and can never lower it), `M` the service-agnostic
//!   wall stamp retention orders by, `S` the stamping pipeline's
//!   32-hex scope retention PARTITIONS by. Stamped in the SAME
//!   property commit that persists the state document, so a crash
//!   leaves both or neither; a missing receipt re-drives publish,
//!   whose per-table history convergence discards already-committed
//!   windows.
//!   RETENTION, per PIPELINE SCOPE (round-11 fix — a global
//!   oldest-first prune let 8 foreign pipelines' loads evict a crashed
//!   load's receipt from the shared marker table before its resume):
//!   at most [`RECEIPT_RETENTION`] load receipts are kept per scope —
//!   pruning at each stamp removes only the stamping scope's OLDEST
//!   other keys (plus any receipt whose scope cannot be decoded:
//!   corrupt bookkeeping no reader accepts). A receipt only matters
//!   while its load can still be re-attempted, so the most recent
//!   loads per pipeline are the whole audience; unbounded growth
//!   would bloat every table-metadata read instead.

use std::sync::Arc;

use iceberg::transaction::{ApplyTransactionAction as _, Transaction};
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use rdlt_connector_sdk::spi::DestinationError;

use super::client::classify;
use super::commit::{Plan, commit_with_retry};

/// The property-key prefix for state documents.
pub(super) const PROP_STATE_PREFIX: &str = "rdlt.state.";

/// The marker table every pipeline's state rides on.
pub(super) const STATE_TABLE: &str = "_rdlt_state";

/// The pipeline scope hash width EVERY build before 037 wrote state
/// under. Exists ONLY to detect-and-refuse pre-widen state (037 D1) —
/// this is a refusal gate, not a migration: a pipeline whose state
/// still lives under this narrower key is never silently adopted or
/// rewritten to the current width, only recognized and reported so
/// the operator can decide (see `load.rs`'s `SCOPE_HASH_LEN` doc for
/// the widen this guards).
pub(super) const LEGACY_SCOPE_HASH_LEN: usize = 12;

/// The one key composition both sides call — drift here would
/// silently strand state across a resume.
fn state_key(scope: &str) -> String {
    format!("{PROP_STATE_PREFIX}{scope}")
}

/// The property-key prefix for load-level receipts (format notes above).
pub(super) const PROP_RECEIPT_PREFIX: &str = "rdlt.receipt.";

/// How many loads' receipts the marker table retains (format notes
/// above): the current load plus the most recent others.
pub(super) const RECEIPT_RETENTION: usize = 8;

/// The one receipt-key composition — the stamp writes and the lookup
/// reads through it.
fn receipt_key(load_id: &str) -> String {
    format!("{PROP_RECEIPT_PREFIX}{load_id}")
}

/// The load-level receipt stamped BESIDE the state document, in the
/// same property commit — a crash leaves both or neither.
pub(super) struct ReceiptStamp<'a> {
    pub(super) load_id: &'a str,
    pub(super) commit_seq: u64,
}

/// One receipt property value, decoded ONCE (round-12 — two
/// hand-rolled `Value`-navigating decoders each re-parsed the same
/// bytes): the recorded shape from the format notes above. `scope`
/// stays optional in the DECODER only so a value without one still
/// yields its seq/stamp — the reader refuses such a value loudly and
/// the pruner sweeps it as corrupt bookkeeping.
#[derive(serde::Deserialize)]
struct ReceiptRecord {
    commit_seq: u64,
    stamped_at_ms: u64,
    #[serde(default)]
    scope: Option<String>,
}

/// Decode one receipt property value; `None` for bytes outside the
/// recorded shape.
fn receipt_entry(value: &str) -> Option<ReceiptRecord> {
    serde_json::from_str(value).ok()
}

/// The reader's view: `(highest committed seq, stamp millis)`.
fn receipt_record(value: &str) -> Option<(u64, u64)> {
    receipt_entry(value).map(|record| (record.commit_seq, record.stamped_at_ms))
}

/// What one stamp does to the marker table's properties, PURE so the
/// merge and retention rules pin offline: the value to set under the
/// load's own key (merging by MAX with any recorded seq — replaying an
/// early window must never lower the high water), and the stale
/// receipt keys to remove so at most [`RECEIPT_RETENTION`] of THIS
/// SCOPE's loads remain (round-11 fix — the prune was oldest-first
/// across every pipeline sharing the marker table, so 8 foreign loads
/// could evict a crashed load's receipt before its resume): only the
/// stamping scope's own oldest keys prune, a FOREIGN scope's receipts
/// are never candidates, and a receipt with no decodable scope prunes
/// first as corrupt bookkeeping.
fn receipt_delta(
    properties: &std::collections::HashMap<String, String>,
    stamp: &ReceiptStamp<'_>,
    scope: &str,
    now_ms: u64,
) -> (String, Vec<String>) {
    let key = receipt_key(stamp.load_id);
    let recorded = properties.get(&key).and_then(|v| receipt_record(v));
    let seq = recorded
        .map(|(seq, _)| seq.max(stamp.commit_seq))
        .unwrap_or(stamp.commit_seq);
    let value =
        serde_json::json!({"commit_seq": seq, "stamped_at_ms": now_ms, "scope": scope}).to_string();

    let mut own: Vec<(u64, &String)> = properties
        .iter()
        .filter(|(k, _)| k.starts_with(PROP_RECEIPT_PREFIX) && **k != key)
        .filter_map(
            |(k, v)| match receipt_entry(v).map(|r| (r.stamped_at_ms, r.scope)) {
                // This scope's own receipt: a candidate, oldest stamps first.
                Some((ms, Some(s))) if s == scope => Some((ms, k)),
                // A foreign scope's receipt: NEVER a candidate.
                Some((_, Some(_))) => None,
                // No decodable record or scope: corrupt bookkeeping —
                // prunes ahead of anything real.
                _ => Some((0, k)),
            },
        )
        .collect();
    own.sort();
    let excess = (own.len() + 1).saturating_sub(RECEIPT_RETENTION);
    let remove = own
        .into_iter()
        .take(excess)
        .map(|(_, k)| k.clone())
        .collect();
    (value, remove)
}

/// Milliseconds since the Unix epoch — the retention stamp. A wall
/// clock before the epoch yields 0, which only makes this receipt
/// prune FIRST; correctness never reads the stamp.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The receipt lookup: the highest commit sequence published for
/// `load_id`, off the ONE marker table — no namespace enumeration, no
/// stream-table scan. An absent marker table (or namespace, or key) is
/// honestly `None`: either nothing committed, or the crash landed
/// before the property commit — and the re-driven publish converges
/// per table from snapshot history either way. A property that does
/// not decode is corrupt bookkeeping and fails loudly.
pub(super) async fn read_receipt(
    catalog: &Arc<dyn Catalog>,
    namespace: &NamespaceIdent,
    load_id: &str,
) -> Result<Option<u64>, DestinationError> {
    let ident = TableIdent::new(namespace.clone(), STATE_TABLE.to_owned());
    let table = match catalog.load_table(&ident).await {
        Ok(table) => table,
        Err(e)
            if matches!(
                e.kind(),
                iceberg::ErrorKind::TableNotFound | iceberg::ErrorKind::NamespaceNotFound
            ) =>
        {
            return Ok(None);
        }
        Err(e) => return Err(classify(&format!("state table `{ident}`"), e)),
    };
    let key = receipt_key(load_id);
    match table.metadata().properties().get(&key) {
        None => Ok(None),
        Some(value) => match receipt_record(value) {
            Some((seq, _)) => Ok(Some(seq)),
            None => Err(super::client::fatal(format!(
                "receipt property `{key}` on `{ident}` is not the recorded shape: `{value}`"
            ))),
        },
    }
}

/// The marker table's (forever-empty) schema: one optional column, so
/// the create payload is valid everywhere.
fn marker_schema() -> Result<iceberg::spec::Schema, iceberg::Error> {
    use iceberg::spec::{NestedField, PrimitiveType, Type};
    iceberg::spec::Schema::builder()
        .with_fields(vec![Arc::new(NestedField::optional(
            1,
            "scope",
            Type::Primitive(PrimitiveType::String),
        ))])
        .build()
}

/// Write the state doc as a marker-table property, creating the table
/// on first use (a concurrent creator's table is adopted, not an
/// error) — and, when a commit is what triggered the write, stamp the
/// load's receipt IN THE SAME property commit (plus its retention
/// prune; format notes above). Property commits conflict like any
/// other commit — they ride the shared bounded retry under the
/// `property commit` subject.
pub(super) async fn write_state(
    catalog: &Arc<dyn Catalog>,
    namespace: &NamespaceIdent,
    scope: &str,
    state_json: String,
    receipt: Option<ReceiptStamp<'_>>,
) -> Result<(), DestinationError> {
    let ident = TableIdent::new(namespace.clone(), STATE_TABLE.to_owned());
    let context = format!("state table `{ident}`");
    let table = match catalog.load_table(&ident).await {
        Ok(table) => table,
        Err(e) if matches!(e.kind(), iceberg::ErrorKind::TableNotFound) => {
            let schema = marker_schema().map_err(|e| classify(&context, e))?;
            let creation = iceberg::TableCreation::builder()
                .name(STATE_TABLE.to_owned())
                .schema(schema)
                .build();
            match catalog.create_table(namespace, creation).await {
                Ok(table) => table,
                Err(e) if matches!(e.kind(), iceberg::ErrorKind::TableAlreadyExists) => catalog
                    .load_table(&ident)
                    .await
                    .map_err(|e| classify(&context, e))?,
                Err(e) => return Err(classify(&context, e)),
            }
        }
        Err(e) => return Err(classify(&context, e)),
    };
    let key = state_key(scope);
    commit_with_retry(
        catalog,
        &ident,
        &context,
        "property commit",
        scope,
        table,
        |current| {
            let tx = Transaction::new(current);
            let mut action = tx
                .update_table_properties()
                .set(key.clone(), state_json.clone());
            // The receipt joins the SAME commit — recomputed per
            // attempt against the competitor's properties, so the
            // merge-by-max and the retention prune both see what
            // actually landed.
            if let Some(stamp) = &receipt {
                let (value, remove) =
                    receipt_delta(current.metadata().properties(), stamp, scope, now_ms());
                action = action.set(receipt_key(stamp.load_id), value);
                for stale in remove {
                    action = action.remove(stale);
                }
            }
            let tx = action.apply(tx).map_err(|e| classify(&context, e))?;
            Ok(Plan::Commit(Box::new(tx)))
        },
    )
    .await
    .map(|_| ())
}

/// Read the state doc back. An absent marker table — or the whole
/// namespace — is a first run, not an error.
pub(super) async fn read_state_doc(
    catalog: &Arc<dyn Catalog>,
    namespace: &NamespaceIdent,
    scope: &str,
) -> Result<Option<String>, DestinationError> {
    let ident = TableIdent::new(namespace.clone(), STATE_TABLE.to_owned());
    match catalog.load_table(&ident).await {
        Ok(table) => Ok(table
            .metadata()
            .properties()
            .get(&state_key(scope))
            .cloned()),
        Err(e)
            if matches!(
                e.kind(),
                iceberg::ErrorKind::TableNotFound | iceberg::ErrorKind::NamespaceNotFound
            ) =>
        {
            Ok(None)
        }
        Err(e) => Err(classify(&format!("state table `{ident}`"), e)),
    }
}

/// TEST-ONLY (behind the testhook): remove one load's receipt property
/// — the missing half of the mid-publish crash residue `remove_state`
/// stages (a crash between a table's `append_commit` and the receipt
/// stamp leaves data committed with NEITHER receipt NOR state).
pub(super) async fn remove_receipt(
    catalog: &Arc<dyn Catalog>,
    namespace: &NamespaceIdent,
    load_id: &str,
) -> Result<(), DestinationError> {
    let ident = TableIdent::new(namespace.clone(), STATE_TABLE.to_owned());
    let context = format!("state table `{ident}`");
    let table = catalog
        .load_table(&ident)
        .await
        .map_err(|e| classify(&context, e))?;
    let key = receipt_key(load_id);
    commit_with_retry(
        catalog,
        &ident,
        &context,
        "property commit",
        load_id,
        table,
        |current| {
            let tx = Transaction::new(current);
            let action = tx.update_table_properties().remove(key.clone());
            let tx = action.apply(tx).map_err(|e| classify(&context, e))?;
            Ok(Plan::Commit(Box::new(tx)))
        },
    )
    .await
    .map(|_| ())
}

/// Remove a scope's state property. TEST-ONLY: production never
/// deletes a state property (a load only ever `write_state`s), this
/// exists so a live cell can relocate a pipeline's state to the
/// legacy key (`testhook::move_state_to_legacy_key`) without leaving
/// BOTH the current and legacy properties behind — which would mask
/// the very refusal gate under test.
pub(super) async fn remove_state(
    catalog: &Arc<dyn Catalog>,
    namespace: &NamespaceIdent,
    scope: &str,
) -> Result<(), DestinationError> {
    let ident = TableIdent::new(namespace.clone(), STATE_TABLE.to_owned());
    let context = format!("state table `{ident}`");
    let table = catalog
        .load_table(&ident)
        .await
        .map_err(|e| classify(&context, e))?;
    let key = state_key(scope);
    commit_with_retry(
        catalog,
        &ident,
        &context,
        "property commit",
        scope,
        table,
        |current| {
            let tx = Transaction::new(current);
            let action = tx.update_table_properties().remove(key.clone());
            let tx = action.apply(tx).map_err(|e| classify(&context, e))?;
            Ok(Plan::Commit(Box::new(tx)))
        },
    )
    .await
    .map(|_| ())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::Ordering;

    use iceberg::NamespaceIdent;

    use super::super::commit::COMMIT_ATTEMPTS;
    use super::super::testsupport::ConflictCatalog;
    use super::*;

    /// One key composition, both sides: what the write stores under
    /// `state_key(scope)`, the read finds there — and scopes never
    /// collide onto one key.
    #[test]
    fn the_write_and_read_keys_are_one_composition() {
        let mut properties: HashMap<String, String> = HashMap::new();
        properties.insert(state_key("scope-a"), "{\"cursor\":9}".to_owned());
        assert_eq!(
            properties.get(&state_key("scope-a")).map(String::as_str),
            Some("{\"cursor\":9}")
        );
        assert_ne!(state_key("scope-a"), state_key("scope-b"));
        assert!(state_key("scope-a").starts_with(PROP_STATE_PREFIX));
    }

    /// The legacy scope width is frozen at 12 — the pre-037 width — and
    /// composes through the SAME `state_key`, landing on a DIFFERENT
    /// property than the current 32-hex scope for the same pipeline
    /// name; a regression collapsing the two widths would make the
    /// legacy-key refusal gate (037 D1) probe the very key it is
    /// meant to detect stale state under.
    #[test]
    fn the_legacy_scope_width_is_frozen_and_composes_a_distinct_key() {
        use rdlt_connector_sdk::spi::core::naming::ident_hash;

        use super::super::load::SCOPE_HASH_LEN;

        assert_eq!(LEGACY_SCOPE_HASH_LEN, 12);
        let pipeline = "some-pipeline";
        let legacy = ident_hash(pipeline, LEGACY_SCOPE_HASH_LEN);
        let current = ident_hash(pipeline, SCOPE_HASH_LEN);
        assert_eq!(legacy.len(), 12, "{legacy}");
        assert_eq!(current.len(), 32, "{current}");
        assert_ne!(state_key(&legacy), state_key(&current));
    }

    /// One receipt-key composition and value shape, both directions:
    /// what the stamp encodes, the reader and the pruner decode.
    #[test]
    fn the_receipt_key_and_value_compose_and_decode_as_one_shape() {
        assert_eq!(receipt_key("load-a"), "rdlt.receipt.load-a");
        let (value, _) = receipt_delta(
            &HashMap::new(),
            &ReceiptStamp {
                load_id: "load-a",
                commit_seq: 3,
            },
            "scope-a",
            77,
        );
        assert_eq!(receipt_record(&value), Some((3, 77)));
        assert_eq!(
            receipt_entry(&value)
                .and_then(|record| record.scope)
                .as_deref(),
            Some("scope-a"),
            "the stamp records its scope — the retention partition key"
        );
        assert!(receipt_entry("not json").is_none());
        assert!(receipt_entry("{\"commit_seq\":\"x\"}").is_none());
        assert!(
            receipt_entry("{\"commit_seq\":1,\"stamped_at_ms\":2}")
                .expect("seq and stamp decode without a scope")
                .scope
                .is_none(),
            "a scope-less value still yields its record — the pruner decides its fate"
        );
    }

    /// The merge rule: a replayed EARLY window must never lower the
    /// recorded high water — the stamp merges by MAX against whatever
    /// the marker table already records for the load.
    #[test]
    fn a_replayed_early_window_never_lowers_the_recorded_high_water() {
        let mut properties = HashMap::new();
        properties.insert(
            receipt_key("load-a"),
            "{\"commit_seq\":5,\"stamped_at_ms\":10}".to_owned(),
        );
        let (value, remove) = receipt_delta(
            &properties,
            &ReceiptStamp {
                load_id: "load-a",
                commit_seq: 2,
            },
            "scope-a",
            99,
        );
        assert_eq!(
            receipt_record(&value),
            Some((5, 99)),
            "seq keeps the high water; the stamp refreshes recency"
        );
        assert!(remove.is_empty(), "one load prunes nothing");
    }

    /// Retention within ONE scope: at most [`RECEIPT_RETENTION`] of
    /// the scope's loads remain after a stamp — the OLDEST own keys
    /// are removed first, with an unparseable record treated as
    /// oldest of all.
    #[test]
    fn retention_removes_the_oldest_own_scope_loads_first() {
        let mut properties = HashMap::new();
        for age in 0..RECEIPT_RETENTION as u64 {
            properties.insert(
                receipt_key(&format!("load-{age}")),
                format!(
                    "{{\"commit_seq\":1,\"stamped_at_ms\":{},\"scope\":\"s1\"}}",
                    100 + age
                ),
            );
        }
        properties.insert(receipt_key("load-corrupt"), "not json".to_owned());
        // Non-receipt properties never prune.
        properties.insert(state_key("scope-a"), "{}".to_owned());

        let (_, remove) = receipt_delta(
            &properties,
            &ReceiptStamp {
                load_id: "load-new",
                commit_seq: 1,
            },
            "s1",
            1_000,
        );
        // 9 other receipts + the new one = 10; retention 8 removes two:
        // the corrupt record (treated oldest) then the stalest stamp.
        assert_eq!(
            remove,
            vec![receipt_key("load-corrupt"), receipt_key("load-0")]
        );
    }

    /// THE CROSS-PIPELINE EVICTION CLOSED (round-11 red pin): the
    /// marker table is shared by every pipeline in the namespace, and
    /// the old global oldest-first prune let 8 foreign loads evict a
    /// crashed load's receipt before its resume — re-opening the
    /// double-append window the receipt exists to close. A foreign
    /// scope's receipts are never prune candidates now, however old.
    #[test]
    fn foreign_scope_receipts_never_evict_this_scopes_receipt() {
        let mut properties = HashMap::new();
        // The crashed load's receipt — OLDEST stamp on the table, and
        // the only one under its scope.
        properties.insert(
            receipt_key("load-crashed"),
            "{\"commit_seq\":1,\"stamped_at_ms\":1,\"scope\":\"mine\"}".to_owned(),
        );
        // Eight fresher loads from a busy foreign pipeline.
        for age in 0..RECEIPT_RETENTION as u64 {
            properties.insert(
                receipt_key(&format!("foreign-{age}")),
                format!(
                    "{{\"commit_seq\":1,\"stamped_at_ms\":{},\"scope\":\"other\"}}",
                    100 + age
                ),
            );
        }

        // The foreign pipeline's NINTH load stamps: it must prune its
        // OWN oldest, never the crashed load's receipt.
        let (_, remove) = receipt_delta(
            &properties,
            &ReceiptStamp {
                load_id: "foreign-new",
                commit_seq: 1,
            },
            "other",
            1_000,
        );
        assert_eq!(
            remove,
            vec![receipt_key("foreign-0")],
            "retention partitions by scope — the crashed load's receipt survives \
             any number of foreign stamps"
        );

        // And the crashed scope's own next stamp prunes nothing: one
        // receipt is far under its own budget.
        let (_, remove) = receipt_delta(
            &properties,
            &ReceiptStamp {
                load_id: "load-crashed",
                commit_seq: 2,
            },
            "mine",
            2_000,
        );
        assert!(remove.is_empty(), "{remove:?}");
    }

    /// The property-commit exhaustion is reachable, typed, and names
    /// its subject and bound — with "exhausted" said exactly once.
    #[tokio::test]
    async fn a_state_write_that_keeps_losing_exhausts_typed() {
        let catalog = ConflictCatalog::failing(u32::MAX);
        let arc: Arc<dyn Catalog> = catalog.clone();
        let err = write_state(
            &arc,
            &NamespaceIdent::new("ns".into()),
            "abc123",
            "{}".into(),
            None,
        )
        .await
        .expect_err("must exhaust");
        let text = format!("{err}");
        assert!(
            text.contains("property commit")
                && text.contains(&format!("attempt {COMMIT_ATTEMPTS}/{COMMIT_ATTEMPTS}")),
            "{text}"
        );
        assert_eq!(text.matches("exhausted").count(), 1, "{text}");
        assert_eq!(catalog.commits.load(Ordering::SeqCst), COMMIT_ATTEMPTS);
    }
}
