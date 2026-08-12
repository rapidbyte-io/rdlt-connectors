//! THE CERTIFICATION CELL (042 Task 6): duckdb certified over the wire
//! — the first SINGLE-WRITER destination to face the full clause suite
//! out of process, and the first destination port certifying against
//! the write-side clauses P11/P12 since they landed. The REAL
//! `rdlt-connector-duckdb` bin is spawned by path, and the certify
//! library judges it: the role-generic protocol clauses (P1–P4), the
//! wire clauses on a raw handshake below the adapters (P3/P7), the
//! testkit's D-clauses reused against the managed adapter (D1–D6 plus
//! D8 LIVE — this destination declares merge, so D8 must be a real
//! Pass, never the no-merge Skip), and the session clauses
//! P8/P9/P10/P11/P12 on raw dials of the live socket.
//!
//! Hermetic on a tempdir — no container runtime, so this cell never
//! skips. The read-back probe is `SnapshotCount`, NOT a direct
//! read-only open: the connector's process holds duckdb's
//! cross-process file lock for its whole life, and a read-only open
//! from this process would be refused outright (`support::probe`'s
//! module doc carries the measurement). The certifier itself runs the
//! P3/P7 wire probe BEFORE spawning the managed adapter and reaps it
//! in between — two live processes on one single-writer file cannot
//! coexist, which this cell is what proved.

use rdlt_certify::{Target, assert_certified_all_pass, certify_destination};
use serde_json::json;

use super::support::probe::SnapshotCount;
use super::support::spawn::built_bin;

/// THE DESTINATION CELL: the built duckdb bin certifies clean over the
/// wire — every clause a destination can face, asserted present and
/// passing: D1–D6, D8 live, and ALL TEN protocol clauses including the
/// write-side P11 (one Arrow batch per write frame) and P12 (error
/// frames carry bare cause text), asserted here for the first time on
/// a shipped destination port.
#[tokio::test(flavor = "multi_thread")]
async fn the_duckdb_destination_certifies_all_pass_with_d8_live() {
    let dir = tempfile::tempdir().expect("dir");
    let file = dir.path().join("certify.duckdb");
    let config = json!({ "path": file });
    let probe = SnapshotCount(file.clone());

    let report =
        certify_destination(&Target::resolve_path(built_bin(), config), Some(&probe)).await;

    assert_certified_all_pass(
        &report,
        &[
            "D1", "D2", "D3", "D4", "D5", "D6", "D8", "P1", "P2", "P3", "P4", "P7", "P8", "P9",
            "P10", "P11", "P12",
        ],
    );
}
