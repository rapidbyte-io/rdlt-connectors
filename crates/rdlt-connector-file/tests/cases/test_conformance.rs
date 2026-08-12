//! The sdk conformance kits over BOTH Shells — "certified = passes
//! conformance" — on the local filesystem, so certification runs
//! anywhere the gate does.

use rdlt_connector_sdk::spi::core::TableName;
use rdlt_testkit::{ProbeError, TableProbe, assert_conformant, verify_destination, verify_source};

use super::common::{jsonl_source, local_dest, plant};

/// Counts through the destination's own testhook — the ownership
/// listing, independent of the session under test.
struct DirProbe {
    config: rdlt_connector_file::destination::Config,
}

#[async_trait::async_trait]
impl TableProbe for DirProbe {
    async fn count(&self, table: &TableName) -> Result<u64, ProbeError> {
        // An absent table already reads as an honestly-empty ownership
        // listing (the location layer treats a missing directory as no
        // keys), so ANY error here is the oracle failing — folding it
        // into 0 would certify invisibility clauses vacuously.
        rdlt_connector_file::destination::testhook::count_rows_async(&self.config, table.as_str())
            .await
            .map_err(|e| ProbeError {
                message: format!("the ownership count failed: {e}"),
            })
    }
}

#[tokio::test]
async fn the_destination_is_conformant_on_the_local_filesystem() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = local_dest(dir.path());
    let shell = rdlt_connector_file::destination::Shell::new(config.clone()).expect("valid");
    let probe = DirProbe { config };
    assert_conformant(
        verify_destination(&shell, &probe)
            .await
            .expecting_no_skips(),
    );
}

/// The fail-open fold closed (042 fix wave): an absent table counts
/// zero — the ownership listing of a directory that does not exist is
/// honestly empty, D1's fact — while an unreadable OWNED part is a
/// probe error naming the cause, never an empty table.
#[tokio::test]
async fn probe_absence_is_zero_but_an_unreadable_part_is_an_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let probe = DirProbe {
        config: local_dest(dir.path()),
    };
    assert_eq!(
        probe
            .count(&TableName::new("never_written"))
            .await
            .expect("absence is a fact, not a failure"),
        0
    );
    plant(dir.path(), "t/part-l-1-0.parquet", b"not parquet at all");
    let err = probe
        .count(&TableName::new("t"))
        .await
        .expect_err("an unreadable owned part must never read as an empty table");
    assert!(
        err.message.contains("unreadable parquet"),
        "the probe error names the cause: {}",
        err.message
    );
}

#[tokio::test]
async fn the_source_is_conformant_over_planted_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    plant(
        dir.path(),
        "data/events.jsonl",
        b"{\"id\": 1}\n{\"id\": 2}\n{\"id\": 3}\n",
    );
    let shell = rdlt_connector_file::source::Shell::new(jsonl_source(dir.path(), "data/*.jsonl"))
        .expect("valid");
    assert_conformant(verify_source(&shell).await.expecting_no_skips());
}
