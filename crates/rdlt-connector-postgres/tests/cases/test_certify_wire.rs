//! THE CERTIFICATION CELLS (041 Task 3): postgres certified over the
//! wire, both roles, against a live server — the first database-backed
//! connector to face the full clause suite remotely. The REAL
//! `rdlt-connector-postgres` bin is spawned by path, and the certify
//! library judges it: the role-generic protocol clauses (P1–P4), the
//! wire clauses on raw frames below the adapters (P3/P5/P6/P7), the
//! testkit's S- and D-clauses reused against the managed adapters, and
//! the destination-only session clauses (P8-P12).
//!
//! THE CDC EXCLUSION: the certification config carries NO `cdc:` block,
//! deliberately. CDC's `create_if_missing` mints a replication slot the
//! connector NEVER drops (user-owned server resources, `cdc/slot.rs`),
//! so a certification pass would orphan a slot per run on the target
//! server; the CDC wire surface has its own live cell
//! (`test_cdc_wire.rs`). Certification here covers the cursor-
//! incremental read path — the one whose checkpoints the S-clauses
//! certify.
//!
//! Skip-not-fail without a container runtime, like every container
//! suite in this crate.

use rdlt_certify::{
    clause::d::certify as certify_destination, clause::p::DESTINATION_DUAL_ROLE_SKIP,
    clause::p::SOURCE_DUAL_ROLE_SKIP, clause::s::certify as certify_source,
    report::assert_all_pass as assert_certified_all_pass_with_named_skips, target::Target,
};
use rdlt_connector_postgres::fixtures::PostgresContainer;
use serde_json::json;

use super::common::Probe;
use super::support::spawn::built_bin;

/// The SMALL deterministic certification config, used by THIS cell's
/// source arm and nowhere else.
///
/// It is deliberately NOT shared with the kill matrix (Task 4), which
/// defines its own `large_config` in `test_kill_wire.rs` and records
/// why: the kill arms need a read still in flight when the SIGKILL
/// lands, so they want thousands of rows past the kit's 64 KiB window,
/// while this cell wants the opposite — the smallest fixture that still
/// produces real resume points, so a clause failure names a clause
/// rather than a timeout. Two configs because the two suites want
/// opposite sizes, not by oversight. `test_kill_wire.rs`'s module doc
/// cites `small_config` by NAME when it explains that contrast, which
/// is the only cross-file relationship between them — hence the private
/// visibility below.
///
/// Two cursor-incremental streams (BOTH cursored: a snapshot stream
/// never checkpoints —
/// `source/connector.rs` pins "every run is a full read by definition"
/// — and while an undeclared cursor now skips S2 honestly, this cell
/// wants S1 exercised for real, which needs actual checkpoints),
/// with `batch_max_rows: 2` so the five-row stream cuts into three
/// batches and the tracker emits >=2 intermediate checkpoints (one per
/// batch whose watermark advanced), giving S1 real resume points to
/// certify against. No `cdc:` block — the module doc's exclusion.
fn small_config(conn: &str) -> serde_json::Value {
    json!({
        "conn": conn,
        "tables": [
            {"name": "orders", "cursor": {"column": "id"}},
            {"name": "customers", "cursor": {"column": "id"}},
        ],
        "batch_max_rows": 2,
    })
}

/// Seed the two certification tables — small and deterministic: five
/// rows where `batch_max_rows: 2` yields three batches (>=2 intermediate
/// checkpoints), three rows for the second stream.
async fn seed_source_fixture(container: &PostgresContainer) {
    container
        .seed(
            "CREATE TABLE public.orders (id int8 PRIMARY KEY, total int4); \
             INSERT INTO public.orders VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50); \
             CREATE TABLE public.customers (id int8 PRIMARY KEY, name text); \
             INSERT INTO public.customers VALUES (1, 'ada'), (2, 'grace'), (3, 'lin');",
        )
        .await;
}

/// THE SOURCE CELL: the built pg bin certifies all-Pass as a source
/// over the wire — S1/S2/S4 reused against the managed adapter plus the
/// protocol clauses P1–P7 — TWICE in a row against the same target and
/// the same live server (the certification bar's repeated element: a
/// connector must survive being certified again from the state the
/// first certification left behind; the pg source is read-only against
/// its tables, so the second pass proves exactly that). P13 is the
/// dual-role bin's one announced skip: both roles are served, so there
/// is no unserved role to refuse.
#[tokio::test(flavor = "multi_thread")]
async fn the_postgres_source_certifies_all_pass() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    seed_source_fixture(&container).await;
    let target = Target::resolve_path(built_bin(), small_config(&container.connection_string));

    for _attempt in 1..=2 {
        let report = certify_source(&target, &[]).await;

        assert_certified_all_pass_with_named_skips(
            &report,
            &["S1", "S2", "S4", "P1", "P2", "P3", "P4", "P5", "P6", "P7"],
            &[("P13", SOURCE_DUAL_ROLE_SKIP)],
        );
    }
}

/// THE DESTINATION CELL: the built pg bin certifies clean as a
/// destination over the wire — D1–D6 plus D8 LIVE (the pg destination
/// declares the merge capability, so D8 must be a real Pass, never the
/// no-merge Skip the file destination records), the protocol clauses,
/// and the session clauses P8-P12 on raw dials of the live socket
/// (P11 refuses a deliberate two-batch write frame; P12 judges the
/// induced refusals' error-frame text). P13 is the dual-role bin's one
/// announced skip, as on the source cell.
/// The read-back probe is a SEPARATE connection into the scratch
/// dataset — no in-flight-session hazard for its SQL.
#[tokio::test(flavor = "multi_thread")]
async fn the_postgres_destination_certifies_all_pass_with_d8_live() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    // The scratch discipline: a dataset of certification's own — the
    // destination creates the schema, and every table the suite mints
    // (`rdlt_conf_*`, `p10_order_book`) lands inside it, colliding with
    // nothing a sibling suite writes.
    let dataset = "certify_scratch";
    let config = json!({
        "conn": container.connection_string,
        "dataset": dataset,
    });
    let probe = Probe {
        connection_string: container.connection_string.clone(),
        schema: dataset.into(),
    };

    let report =
        certify_destination(&Target::resolve_path(built_bin(), config), Some(&probe)).await;

    assert_certified_all_pass_with_named_skips(
        &report,
        &[
            "D1", "D2", "D3", "D4", "D5", "D6", "D8", "P1", "P2", "P3", "P4", "P7", "P8", "P9",
            "P10", "P11", "P12",
        ],
        &[("P13", DESTINATION_DUAL_ROLE_SKIP)],
    );
}
