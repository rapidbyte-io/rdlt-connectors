//! THE KILL MATRIX (042 Task 7, D-042-3): the spawned iceberg bin
//! SIGKILLed at every K-D boundary against the live Polaris/RUSTFS
//! fixture — the kill matrix's first CATALOG destination. All six arms
//! of the destination K-vocabulary RUN LIVE FIRST (the defining rule):
//! typed error on the dead wire, then exactly-once convergence — a
//! FRESH spawn re-runs the load and the read-back must count the
//! fixture rows EXACTLY, the count read off the catalog's own snapshot
//! summaries.
//!
//! Skip-not-fail: without a container runtime the fixture announces
//! the skip and the cell returns — the 015 convention every live
//! iceberg cell rides.

use rdlt_certify::{
    clause::k::destination as kill_matrix_destination,
    report::assert_in_order as assert_all_pass_in_order, target::Target,
};

use super::common::{CatalogFixture, LiveProbe};
use super::support::spawn::built_bin;

/// THE DESTINATION HALF: every boundary in K order, all six arms run
/// live (D-042-3 — the matrix is never narrowed on a counting
/// argument), every arm a real Pass.
#[tokio::test(flavor = "multi_thread")]
async fn the_destination_kill_matrix_passes_at_every_boundary() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "kill_wire";
    let config = fixture.doc(namespace);
    let probe = LiveProbe {
        fixture,
        namespace: namespace.into(),
    };

    let entries =
        kill_matrix_destination(&Target::resolve_path(built_bin(), config), Some(&probe)).await;

    assert_all_pass_in_order(
        &entries,
        &["K-D1", "K-D2", "K-D3", "K-D4", "K-D5", "K-D6"],
        None,
    );
}

/// ROUND-13 ACCEPTANCE (certify load-id entropy): the kill matrix runs
/// TWICE back-to-back against ONE warehouse and both invocations pass.
/// Iceberg's receipts and settled checks are DURABLE and load-keyed,
/// so before the entropy suffix the second invocation's deterministic
/// load ids met the first's receipts: its publishes were replay-masked
/// into no-ops and its convergence counts doubled — a vacuous or
/// failing re-certification either way. Fresh per-invocation ids make
/// the second run do REAL work in its own tables.
#[tokio::test(flavor = "multi_thread")]
async fn re_certifying_the_same_warehouse_does_real_work_both_times() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "kill_wire_twice";
    let config = fixture.doc(namespace);
    let probe = LiveProbe {
        fixture,
        namespace: namespace.into(),
    };

    let mut tables_after = Vec::new();
    for _invocation in 1..=2 {
        let entries = kill_matrix_destination(
            &Target::resolve_path(built_bin(), config.clone()),
            Some(&probe),
        )
        .await;
        assert_all_pass_in_order(
            &entries,
            &["K-D1", "K-D2", "K-D3", "K-D4", "K-D5", "K-D6"],
            None,
        );
        tables_after.push(probe.fixture.tables_in(namespace).await.len());
    }
    // The real-work oracle: fresh per-invocation identities land in
    // fresh tables. A replay-masked second invocation (the
    // deterministic-id defect) adds NO tables — its publishes settle
    // against the first invocation's durable receipts.
    assert!(
        tables_after[1] > tables_after[0],
        "the second invocation must do real work in its own tables: \
         {tables_after:?} tables after each invocation"
    );
}
