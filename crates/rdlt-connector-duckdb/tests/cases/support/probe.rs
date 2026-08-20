//! The read-back probe for suites whose connector runs in ANOTHER
//! PROCESS. The in-process suites' read-only-open discipline
//! (`test_conformance`'s `FileCount`) does NOT transfer here, and the
//! difference is a lock mechanism, not a convention: duckdb's
//! cross-process file lock refuses a READ-ONLY open while a read-write
//! holder lives — measured (042 Task 6) as the SAME
//! `Could not set lock on file` refusal the second read-write open
//! gets. Same-process opens dodge that lock entirely (which is why the
//! crate carries its own in-process registry), so `FileCount` works
//! beside a live in-process shell and would fail beside a live spawned
//! one.
//!
//! What DOES work beside a live cross-process holder — also measured —
//! is reading the FILES: copy `{file, file.wal}` into a scratch
//! directory and open the COPY read-only; the read-only open replays
//! the copied WAL in memory, so the count sees every committed row.
//! The copy is consistent because every probe the certify kit and the
//! kill matrix make lands at a reply boundary, where the connector is
//! idle awaiting its next frame — nothing is mid-write, and duckdb
//! checkpoints only at commit-time thresholds or shutdown, never
//! spontaneously between frames.

use async_trait::async_trait;
use rdlt_connector_sdk::spi::core::id::TableName;
use rdlt_testkit::{conformance::destination::ProbeError, conformance::destination::TableProbe};

/// Counts a table's committed rows through a snapshot copy of the
/// database file — safe beside a LIVE connector process holding the
/// file read-write. A table the connector has not created yet counts
/// as 0 (D1 probes before any table exists); a store whose file cannot
/// be copied or whose copy cannot be opened is an oracle failure, not
/// an empty table.
pub(crate) struct SnapshotCount(pub(crate) std::path::PathBuf);

#[async_trait]
impl TableProbe for SnapshotCount {
    async fn count(&self, table: &TableName) -> Result<u64, ProbeError> {
        let oracle_failure = |message: String| ProbeError { message };
        let scratch = tempfile::tempdir()
            .map_err(|e| oracle_failure(format!("snapshot scratch dir failed: {e}")))?;
        let copy = scratch.path().join("snapshot.duckdb");
        std::fs::copy(&self.0, &copy).map_err(|e| {
            oracle_failure(format!(
                "copying the database file `{}` failed: {e}",
                self.0.display()
            ))
        })?;
        // The WAL carries every commit since the last checkpoint; a
        // missing WAL just means everything already checkpointed into
        // the main file. duckdb names it by APPENDING `.wal` to the
        // whole file name (never by swapping an extension).
        let wal = {
            let mut name = self.0.as_os_str().to_owned();
            name.push(".wal");
            std::path::PathBuf::from(name)
        };
        if wal.is_file() {
            std::fs::copy(&wal, scratch.path().join("snapshot.duckdb.wal"))
                .map_err(|e| oracle_failure(format!("copying the WAL failed: {e}")))?;
        }
        // The one absence-vs-failure rule, shared with the in-process
        // FileCount ([`crate::cases::common::count_at`]).
        crate::cases::common::count_at(&copy, table.as_str())
    }
}

/// The fail-open fold closed (042 fix wave): only ABSENCE reads as
/// zero. The copy scaffolding above is this probe's own subject; plant
/// and asserts are the shared pin body
/// ([`crate::cases::common::assert_probe_counts_absence_but_fails_broken_reads`]).
#[tokio::test]
async fn absence_counts_zero_but_a_broken_read_is_a_probe_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let file = crate::cases::common::plant_broken_view_store(dir.path());

    crate::cases::common::assert_probe_counts_absence_but_fails_broken_reads(&SnapshotCount(file))
        .await;
}
