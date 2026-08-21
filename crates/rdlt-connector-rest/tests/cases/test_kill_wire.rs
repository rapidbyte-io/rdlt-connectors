//! THE KILL MATRIX (042 Task 5): the spawned rest bin SIGKILLed at
//! every K-S boundary against a LOCAL wiremock stub — K-S1/K-S2/K-S3
//! on the read wire (typed-error-not-hang within the kit's window).
//! Source-only: the crate has no destination half, so the K-D
//! vocabulary has no subject here.
//!
//! THE STUB, not the live API: the `RDLT_NET`-gated PokeAPI cell
//! (`test_pokeapi_live.rs`) is NEVER a kill subject — SIGKILLing reads
//! of a public API would hammer a service that is not ours and prove
//! nothing deterministic. wiremock's `MockServer` binds a real
//! 127.0.0.1 port and serves until its handle drops; this test holds
//! the handle for the whole matrix, so every spawned arm reaches the
//! stub by construction. No container runtime anywhere: this cell
//! never skips.
//!
//! THE FIXTURE-SIZE OBLIGATION this cell carries: the kit's source
//! arms dial with a floored 64 KiB h2 window, and a stream the window
//! can swallow WHOLE before the SIGKILL ends cleanly — which the kit
//! reports as an honest Skip, never a vacuous Pass. The certification
//! cell's `small_config` (nine rows) does exactly that, so this matrix
//! rides [`large_config`], sized ~10x past the window with the first
//! checkpoint ~4% in — K-S3 kills with its second checkpoint (and
//! twenty-three more) still to come, the read provably in flight.
//! Proven red two ways while this cell was written. At one two-row
//! page the "completed cleanly despite the kill" Skip fired once and
//! failed the cell — but that red is TIMING-DEPENDENT: at that size
//! the typed error races the buffered clean end and usually wins
//! (review measured 0/10 reproductions), so do not re-run that shrink
//! expecting red. The DETERMINISTIC red is zero pages (KILL_PAGES=0):
//! the stream ends before the K-S2/K-S3 boundaries are ever reached,
//! both arms Skip with the kit's ended-before-the-boundary diagnosis,
//! and the cell FAILS on the first of them. The no-Skip assertion
//! below is therefore load-bearing: it is what makes an under-sized
//! fixture a FAILURE of this cell rather than a quiet narrowing of
//! the matrix.

use rdlt_certify::{
    clause::k::source as kill_matrix_source,
    report::assert_in_order as assert_all_pass_in_order_with_skip_advice, target::Target,
};
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::support::spawn::built_bin;

/// Pages the stub serves before the empty terminator, and rows per
/// page — the sizing math [`large_config`]'s doc states: 25 pages of
/// 200 rows at ~130 bytes each is ~650 KB of row bytes, ~10x the kit's
/// 64 KiB window, and the per-page checkpoints put K-S3's boundary
/// (the FIRST checkpoint) ~4% into the stream, leaving ~620 KB still
/// to flow at its kill.
const KILL_PAGES: u64 = 25;
const ROWS_PER_PAGE: u64 = 200;

/// The LARGE source config: ONE cursor stream over the stub's `/rows`,
/// page-paginated so every page yields a checkpoint (the cursor is a
/// lexicographic max, hence the zero-padded equal-width ids the stub
/// mints). Sized so the read is provably in flight when the SIGKILL
/// lands — the constants above carry the math.
fn large_config(base_url: &str) -> serde_json::Value {
    json!({
        "base_url": base_url,
        "streams": [
            {
                "name": "k_rows",
                "path": "/rows",
                "pagination": {"type": "page", "page_param": "page"},
                "incremental": {"cursor_field": "id", "start_param": "since"},
            },
        ],
    })
}

/// Mount the kill stub: `KILL_PAGES` full pages then the empty page
/// that terminates `page` pagination. Persistent mocks — each of the
/// three arms spawns a fresh bin and reads from page 1.
async fn mount_large_stub(server: &MockServer) {
    for page in 1..=KILL_PAGES + 1 {
        let rows: Vec<serde_json::Value> = if page <= KILL_PAGES {
            (0..ROWS_PER_PAGE)
                .map(|row| {
                    let id = (page - 1) * ROWS_PER_PAGE + row + 1;
                    json!({"id": format!("{id:08}"), "payload": "x".repeat(96)})
                })
                .collect()
        } else {
            Vec::new()
        };
        Mock::given(method("GET"))
            .and(path("/rows"))
            .and(query_param("page", page.to_string()))
            .respond_with(ResponseTemplate::new(200).set_body_json(rows))
            .mount(server)
            .await;
    }
}

/// THE MATRIX: every boundary in K order, every arm a real Pass — the
/// killed rest bin's read wire fails typed within the kit's window,
/// never hangs, and never "completes cleanly despite the kill" (the
/// under-sized-fixture Skip [`large_config`] exists to defeat).
#[tokio::test(flavor = "multi_thread")]
async fn the_source_kill_matrix_passes_at_every_boundary() {
    let server = MockServer::start().await;
    mount_large_stub(&server).await;
    let target = Target::resolve_path(built_bin(), large_config(&server.uri()));

    let entries = kill_matrix_source(&target).await;

    assert_all_pass_in_order_with_skip_advice(
        &entries,
        &["K-S1", "K-S2", "K-S3"],
        Some(
            "the large fixture must keep the read in flight at the SIGKILL, and a Skip here \
             means it no longer does (raise KILL_PAGES/ROWS_PER_PAGE past the kit's read window)",
        ),
    );
}
