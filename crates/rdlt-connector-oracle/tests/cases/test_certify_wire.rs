//! THE CERTIFICATION CELL (042 Task 8): oracle certified over the
//! wire against a live Oracle Free container — the first
//! Arrow-transport source to face the full clause suite remotely. The
//! REAL `rdlt-connector-oracle` bin is spawned by path, and the
//! certify library judges it: the role-generic protocol clauses
//! (P1–P4), the wire clauses on raw frames below the adapters
//! (P3/P5/P6/P7), and the testkit's S-clauses reused against the
//! managed adapter. Both streams declare watermark cursors, so S2
//! judges real checkpoints — never the honest-snapshot skip.
//!
//! DOUBLE skip-not-fail, and BOTH legs live in the fixture's
//! `start()`: no container runtime announces its skip, and no Oracle
//! Client library announces ITS OWN (the driver dlopens libclntsh at
//! RUNTIME — the same absence the spawned bin refuses on, pinned in
//! `test_spawned_bin.rs`). Each reason prints its own line, so a
//! skipped machine says WHICH prerequisite it is missing.

use rdlt_certify::{Target, assert_certified_all_pass, certify_source};
use serde_json::json;

use super::common::{APP_USER, OracleFixture, PASSWORD};
use super::support::spawn::built_bin;

/// The SMALL deterministic certification config, used by THIS cell
/// and nowhere else.
///
/// It is deliberately NOT shared with the kill matrix, which defines
/// its own `large_config` in `test_kill_wire.rs` and records why: the
/// kill arms need a read still in flight when the SIGKILL lands, so
/// they want hundreds of kilobytes past the kit's 64 KiB window,
/// while this cell wants the opposite — the smallest fixture that
/// still produces real resume points, so a clause failure names a
/// clause rather than a timeout. Two configs because the two suites
/// want opposite sizes, not by oversight.
///
/// Two cursor-incremental streams (BOTH cursored: this cell wants S1
/// exercised for real, which needs actual checkpoints — the oracle
/// read checkpoints after every batch whose watermark advanced), with
/// `batch_rows: 2` so the five-row stream cuts into three batches and
/// emits a checkpoint per batch, giving S1 real resume points to
/// certify against.
fn small_config(fixture: &OracleFixture) -> serde_json::Value {
    json!({
        "host": fixture.host,
        "port": fixture.port,
        "service": fixture.flavor.service(),
        "user": APP_USER,
        "password": PASSWORD,
        "tuning": {"batch_rows": 2},
        "streams": [
            {"name": "orders", "table": "certify_orders", "cursor": "id"},
            {"name": "customers", "table": "certify_customers", "cursor": "id"},
        ],
    })
}

/// Seed the two certification tables — small and deterministic: five
/// rows where `batch_rows: 2` yields three batches (a checkpoint
/// each), three rows for the second stream. The cursor columns are
/// NOT NULL — the connector refuses a nullable watermark outright.
async fn seed_source_fixture(fixture: &OracleFixture) {
    fixture
        .seed(&[
            "CREATE TABLE certify_orders (id NUMBER(19) NOT NULL PRIMARY KEY, total NUMBER(10))",
            "INSERT INTO certify_orders \
             SELECT level, level * 10 FROM dual CONNECT BY level <= 5",
            "CREATE TABLE certify_customers \
             (id NUMBER(19) NOT NULL PRIMARY KEY, name VARCHAR2(32))",
            "INSERT INTO certify_customers VALUES (1, 'ada')",
            "INSERT INTO certify_customers VALUES (2, 'grace')",
            "INSERT INTO certify_customers VALUES (3, 'lin')",
        ])
        .await;
}

/// THE CELL: the built oracle bin certifies all-Pass as a source over
/// the wire — S1/S2/S4 reused against the managed adapter plus the
/// protocol clauses P1–P7 — TWICE in a row against the same target
/// and the same live database (the certification bar's repeated
/// element: a connector must survive being certified again from the
/// state the first certification left behind; the oracle source is
/// read-only against its tables, so the second pass proves exactly
/// that).
#[tokio::test(flavor = "multi_thread")]
async fn the_oracle_source_certifies_all_pass() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    seed_source_fixture(&fixture).await;
    let target = Target::resolve_path(built_bin(), small_config(&fixture));

    for _attempt in 1..=2 {
        let report = certify_source(&target, &[]).await;

        assert_certified_all_pass(
            &report,
            &["S1", "S2", "S4", "P1", "P2", "P3", "P4", "P5", "P6", "P7"],
        );
    }
}
