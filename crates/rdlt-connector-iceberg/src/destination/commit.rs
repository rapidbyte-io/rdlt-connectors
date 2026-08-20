//! The commit identity and the ONE bounded retry every commit rides.
//!
//! Exactly-once here is SNAPSHOT-NATIVE: each data commit stamps the
//! identity keys into its snapshot summary, and replay detection scans
//! the table's own history for them. Data appends, state property
//! writes, and schema evolution all share the single
//! optimistic-concurrency loop below, so the retry budget is one
//! number and the exhaustion diagnostic one shape.

use std::collections::HashMap;
use std::hash::BuildHasher as _;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use iceberg::transaction::{ApplyTransactionAction as _, Transaction};
use iceberg::{Catalog, TableIdent};
use rdlt_connector_sdk::spi::core::crash_point;
use rdlt_connector_sdk::spi::error::DestinationError;

use super::client::{classify, is_commit_conflict};

/// The snapshot-summary receipt keys — the persisted commit identity.
pub(super) const PROP_PIPELINE: &str = "rdlt.pipeline";
pub(super) const PROP_LOAD_ID: &str = "rdlt.load-id";
pub(super) const PROP_COMMIT_SEQ: &str = "rdlt.commit-seq";

/// The one retry budget every commit path shares.
pub(super) const COMMIT_ATTEMPTS: u32 = 4;

/// A commit's identity: the pipeline SCOPE (a hash, not the raw name —
/// the raw name is free text), the load, and the sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Identity {
    pub scope: String,
    pub load_id: String,
    pub commit_seq: u64,
}

impl Identity {
    /// The three summary properties a snapshot carries.
    pub(super) fn summary_props(&self) -> HashMap<String, String> {
        HashMap::from([
            (PROP_PIPELINE.to_owned(), self.scope.clone()),
            (PROP_LOAD_ID.to_owned(), self.load_id.clone()),
            (PROP_COMMIT_SEQ.to_owned(), self.commit_seq.to_string()),
        ])
    }

    /// Is this identity already in the table's snapshot history?
    ///
    /// Only as durable as that history: snapshot expiry removes the
    /// evidence, and an expired identity would re-apply as fresh —
    /// retention must outlive the redelivery window.
    pub(super) fn already_committed(&self, table: &iceberg::table::Table) -> bool {
        table.metadata().snapshots().any(|snapshot| {
            let summary = &snapshot.summary().additional_properties;
            summary.get(PROP_PIPELINE) == Some(&self.scope)
                && summary.get(PROP_LOAD_ID) == Some(&self.load_id)
                && summary.get(PROP_COMMIT_SEQ) == Some(&self.commit_seq.to_string())
        })
    }
}

/// Is `(load_id, commit_seq)` in this table's snapshot history under
/// ANY pipeline scope? The LOAD-keyed twin of
/// [`Identity::already_committed`], and deliberately looser: a load id
/// names ONE load wherever it ran, so the receipt lookup
/// (`existing_receipt`) matches it without the scope — a re-attempt
/// that reaches the store under a different pipeline scope (an
/// orchestrator-side re-scope; the certify kill matrix's convergence
/// re-run is the measured case) must still see the committed attempt.
/// Cross-pipeline correctness of the scope-less match rests on the
/// house load-identity contract (docs/connector-authoring.md, "Load
/// identity"): load ids are globally unique across every pipeline
/// sharing a destination, so an id minted outside that guarantee
/// would read here as an already-committed load.
/// Durability caveat: same as `already_committed` — snapshot
/// expiry removes the evidence.
pub(super) fn load_committed(
    table: &iceberg::table::Table,
    load_id: &str,
    commit_seq: u64,
) -> bool {
    let seq = commit_seq.to_string();
    table.metadata().snapshots().any(|snapshot| {
        let summary = &snapshot.summary().additional_properties;
        summary.get(PROP_LOAD_ID).map(String::as_str) == Some(load_id)
            && summary.get(PROP_COMMIT_SEQ) == Some(&seq)
    })
}

/// What one attempt of [`commit_with_retry`] decided.
pub(super) enum Plan {
    /// The desired state already holds — return the current table,
    /// commit nothing.
    Settled,
    /// Commit this transaction (boxed: it dwarfs the unit variant).
    Commit(Box<Transaction>),
}

/// The bounded optimistic-concurrency loop: plan → commit → on a CAS
/// conflict, back off, reload the table, and let `plan` REBUILD
/// against the competitor's snapshot — their history is never
/// dropped. `subject` names the commit kind in the exhaustion
/// diagnostic; `entropy` keeps two writers off an identical backoff
/// schedule.
pub(super) async fn commit_with_retry<F>(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
    context: &str,
    subject: &str,
    entropy: &str,
    initial: iceberg::table::Table,
    mut plan: F,
) -> Result<iceberg::table::Table, DestinationError>
where
    F: FnMut(&iceberg::table::Table) -> Result<Plan, DestinationError>,
{
    let mut current = initial;
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let tx = match plan(&current)? {
            Plan::Settled => return Ok(current),
            Plan::Commit(tx) => *tx,
        };
        match tx.commit(catalog.as_ref()).await {
            Ok(table) => return Ok(table),
            Err(e) if is_commit_conflict(&e) && attempt < COMMIT_ATTEMPTS => {
                backoff(entropy, attempt).await;
                current = catalog
                    .load_table(ident)
                    .await
                    .map_err(|e| classify(context, e))?;
            }
            Err(e) if is_commit_conflict(&e) => {
                // classify renders "exhausted"; this prefix must not
                // repeat it.
                return Err(classify(
                    &format!("{context} ({subject} attempt {attempt}/{COMMIT_ATTEMPTS})"),
                    e,
                ));
            }
            Err(e) => return Err(classify(context, e)),
        }
    }
}

/// Jittered exponential backoff. The jitter window is the full base,
/// keyed on (entropy, attempt) through a per-process seed: distinct
/// writers diverge, one writer stays reproducible within a run, and no
/// wall clock or global RNG enters the commit path.
async fn backoff(entropy: &str, attempt: u32) {
    tokio::time::sleep(Duration::from_millis(backoff_millis(entropy, attempt))).await;
}

/// The delay itself, pure — split from the sleep so the shape (a
/// doubling base plus a full-window keyed jitter) pins without timing
/// a test.
fn backoff_millis(entropy: &str, attempt: u32) -> u64 {
    static SEED: OnceLock<std::collections::hash_map::RandomState> = OnceLock::new();
    let base = 50u64 * (1u64 << attempt.min(4));
    let jitter = SEED
        .get_or_init(std::collections::hash_map::RandomState::new)
        .hash_one((entropy, attempt))
        % base;
    base + jitter
}

/// Publish staged data files as ONE fast-append snapshot carrying the
/// identity. The plan closure re-checks the history EVERY attempt: the
/// competitor this writer lost to may have been its own replay landing
/// the same identity, and a second append would double it.
pub(super) async fn append_commit(
    catalog: &Arc<dyn Catalog>,
    table: iceberg::table::Table,
    files: Vec<iceberg::spec::DataFile>,
    identity: &Identity,
) -> Result<iceberg::table::Table, DestinationError> {
    let ident = table.identifier().clone();
    let context = format!("table `{ident}`");
    let entropy = format!("{}:{}", identity.scope, identity.load_id);
    commit_with_retry(
        catalog,
        &ident,
        &context,
        "commit",
        &entropy,
        table,
        |current| {
            if identity.already_committed(current) {
                return Ok(Plan::Settled);
            }
            let tx = Transaction::new(current);
            let action = tx
                .fast_append()
                .add_data_files(files.clone())
                .set_snapshot_properties(identity.summary_props());
            let tx = action.apply(tx).map_err(|e| classify(&context, e))?;
            crash_point!(
                "ice.commit",
                Err(DestinationError::fatal("injected crash at ice.commit"))
            );
            Ok(Plan::Commit(Box::new(tx)))
        },
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use iceberg::{NamespaceIdent, TableIdent};

    use super::super::testsupport::{ConflictCatalog, data_file};
    use super::*;

    fn identity() -> Identity {
        Identity {
            scope: "abc123".into(),
            load_id: "load-a".into(),
            commit_seq: 1,
        }
    }

    async fn seed(catalog: &Arc<ConflictCatalog>) -> iceberg::table::Table {
        catalog
            .load_table(&TableIdent::new(
                NamespaceIdent::new("ns".into()),
                "events".into(),
            ))
            .await
            .expect("load")
    }

    /// Conflicts inside the bound retry (refresh → rebuild → commit)
    /// and the commit lands; the attempt count is exact.
    #[tokio::test]
    async fn conflicts_inside_the_bound_are_retried_and_the_commit_lands() {
        let catalog = ConflictCatalog::failing(COMMIT_ATTEMPTS - 1);
        let table = seed(&catalog).await;
        let arc: Arc<dyn Catalog> = catalog.clone();
        append_commit(&arc, table, vec![data_file()], &identity())
            .await
            .expect("lands within the bound");
        assert_eq!(
            catalog.commits.load(Ordering::SeqCst),
            COMMIT_ATTEMPTS,
            "three conflicts, then the landing attempt"
        );
    }

    /// Exhaustion is TYPED, naming the table and the bound — and never
    /// an unbounded loop.
    #[tokio::test]
    async fn exhaustion_is_typed_naming_the_table_and_the_bound() {
        let catalog = ConflictCatalog::failing(u32::MAX);
        let table = seed(&catalog).await;
        let arc: Arc<dyn Catalog> = catalog.clone();
        let err = append_commit(&arc, table, vec![data_file()], &identity())
            .await
            .expect_err("must exhaust");
        let text = format!("{err}");
        assert!(text.contains("events"), "names the table: {text}");
        assert!(
            text.contains(&format!("attempt {COMMIT_ATTEMPTS}/{COMMIT_ATTEMPTS}")),
            "names the bound: {text}"
        );
        assert_eq!(text.matches("exhausted").count(), 1, "{text}");
        assert_eq!(catalog.commits.load(Ordering::SeqCst), COMMIT_ATTEMPTS);
    }

    /// The backoff shape: base doubles per attempt (capped), the
    /// jitter never exceeds the base, one writer's schedule is
    /// reproducible within a process, and two writers diverge.
    #[test]
    fn the_backoff_doubles_jitters_within_base_and_diverges_by_writer() {
        for attempt in 1..=4u32 {
            let base = 50u64 * (1u64 << attempt.min(4));
            let delay = backoff_millis("w", attempt);
            assert!(
                (base..2 * base).contains(&delay),
                "attempt {attempt}: {delay} outside [{base}, {})",
                2 * base
            );
            assert_eq!(
                delay,
                backoff_millis("w", attempt),
                "reproducible per writer"
            );
        }
        assert!(
            (1..=8u32).any(|a| backoff_millis("writer-a", a) != backoff_millis("writer-b", a)),
            "two writers must not share every schedule"
        );
    }

    /// The identity keys are the frozen spellings, and history
    /// scanning matches all three at once.
    #[test]
    fn the_identity_keys_are_the_frozen_spellings() {
        let props = identity().summary_props();
        assert_eq!(props["rdlt.pipeline"], "abc123");
        assert_eq!(props["rdlt.load-id"], "load-a");
        assert_eq!(props["rdlt.commit-seq"], "1");
        assert_eq!(props.len(), 3);
    }
}
