//! THE KILL MATRIX (042 Task 8): the spawned oracle bin SIGKILLed at
//! every K-S boundary against a live Oracle Free container —
//! K-S1/K-S2/K-S3 on the read wire (typed-error-not-hang within the
//! kit's window). Source-only: the crate has no destination half, so
//! the K-D vocabulary has no subject here.
//!
//! DOUBLE skip-not-fail, both legs in the fixture's `start()`: no
//! container runtime announces its skip, and no Oracle Client library
//! (the RUNTIME dlopen the spawned bin itself refuses on) announces
//! its own — each prerequisite prints its own line.
//!
//! THE FIXTURE-SIZE OBLIGATION this cell carries: the kit's source
//! arms dial with a floored 64 KiB h2 window, and a stream the window
//! can swallow WHOLE before the SIGKILL ends cleanly — which the kit
//! reports as an honest Skip, never a vacuous Pass. The certification
//! cell's `small_config` (eight rows) does exactly that, so this
//! matrix rides [`large_config`], sized ~8x past the window with the
//! first checkpoint ~4% in — K-S3 kills with its second checkpoint
//! (and twenty-three more) still to come, the read provably in
//! flight. The no-Skip assertion below is therefore load-bearing: it
//! is what makes an under-sized fixture a FAILURE of this cell rather
//! than a quiet narrowing of the matrix.

use rdlt_certify::{
    clause::k::source as kill_matrix_source,
    report::assert_in_order as assert_all_pass_in_order_with_skip_advice, target::Target,
};
use serde_json::json;

use super::common::{APP_USER, OracleFixture, PASSWORD};
use super::support::spawn::built_bin;

/// The LARGE source config: ONE cursor stream over `k_rows`, sized so
/// the read is provably in flight when the SIGKILL lands. 5,000 rows
/// of ~100 bytes (a NUMBER id plus a 96-char payload) is ~500 KB of
/// row bytes — nearly 8x the kit's 64 KiB window — and
/// `batch_rows: 200` cuts it into 25 batches whose per-batch
/// checkpoints put K-S3's boundary (the FIRST checkpoint) ~4% into
/// the stream, leaving ~480 KB still to flow at its kill.
fn large_config(fixture: &OracleFixture) -> serde_json::Value {
    json!({
        "host": fixture.host,
        "port": fixture.port,
        "service": fixture.flavor.service(),
        "user": APP_USER,
        "password": PASSWORD,
        "tuning": {"batch_rows": 200},
        "streams": [
            {"name": "k_rows", "table": "k_rows", "cursor": "id"},
        ],
    })
}

/// Seed the large source fixture — the row count and payload width
/// [`large_config`]'s sizing math is stated in. The cursor column is
/// NOT NULL (the connector refuses a nullable watermark).
async fn seed_source_fixture(fixture: &OracleFixture) {
    fixture
        .seed(&[
            "CREATE TABLE k_rows (id NUMBER(19) NOT NULL PRIMARY KEY, payload VARCHAR2(96))",
            "INSERT INTO k_rows \
             SELECT level, RPAD('x', 96, 'x') FROM dual CONNECT BY level <= 5000",
        ])
        .await;
}

/// THE MATRIX: every boundary in K order, every arm a real Pass — the
/// killed oracle bin's read wire fails typed within the kit's window,
/// never hangs, and never "completes cleanly despite the kill" (the
/// under-sized-fixture Skip [`large_config`] exists to defeat).
#[tokio::test(flavor = "multi_thread")]
async fn the_source_kill_matrix_passes_at_every_boundary() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    seed_source_fixture(&fixture).await;
    let target = Target::resolve_path(built_bin(), large_config(&fixture));

    let entries = kill_matrix_source(&target).await;

    assert_all_pass_in_order_with_skip_advice(
        &entries,
        &["K-S1", "K-S2", "K-S3"],
        Some(
            "the large fixture must keep the read in flight at the SIGKILL, and a Skip here \
             means it no longer does (enlarge `k_rows` past the kit's read window)",
        ),
    );
}
