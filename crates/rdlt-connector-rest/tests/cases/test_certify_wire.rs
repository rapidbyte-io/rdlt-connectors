//! THE CERTIFICATION CELL (042 Task 5): rest certified over the wire
//! against a LOCAL wiremock stub — the first HTTP-backed connector to
//! face the full source clause suite remotely. The REAL
//! `rdlt-connector-rest` bin is spawned by path, and the certify
//! library judges it: the role-generic protocol clauses (P1–P4), the
//! wire clauses on raw frames below the adapters (P3/P5/P6/P7), and
//! the testkit's S-clauses reused against the managed adapter. Both
//! streams declare cursors, so S2 judges real checkpoints — never the
//! honest-snapshot skip.
//!
//! THE STUB, not the live API: the `RDLT_NET`-gated PokeAPI cell
//! (`test_pokeapi_live.rs`) stays untouched and is NEVER a
//! certification or kill subject — certification must be
//! deterministic, and a public API is neither deterministic nor ours
//! to hammer. wiremock's `MockServer` binds a real 127.0.0.1 port and
//! serves until its handle drops, so the spawned bin reaches it by
//! construction while this test holds the handle. No container
//! runtime anywhere: this cell never skips.

use rdlt_certify::{
    clause::s::certify as certify_source, report::assert_all_pass as assert_certified_all_pass,
    target::Target,
};
use serde_json::json;
use wiremock::matchers::{method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::support::spawn::built_bin;

/// The SMALL deterministic certification config, used by THIS cell and
/// nowhere else.
///
/// It is deliberately NOT shared with the kill matrix, which defines
/// its own `large_config` in `test_kill_wire.rs` and records why: the
/// kill arms need a read still in flight when the SIGKILL lands, so
/// they want hundreds of kilobytes past the kit's 64 KiB window, while
/// this cell wants the opposite — the smallest fixture that still
/// produces real resume points, so a clause failure names a clause
/// rather than a timeout. Two configs because the two suites want
/// opposite sizes, not by oversight.
///
/// Two cursor-incremental streams (BOTH cursored: this cell wants S1
/// exercised for real, which needs actual checkpoints — a parentless
/// incremental stream checkpoints per page): `orders` paginates so the
/// full read cuts into two pages and the tracker emits two
/// intermediate checkpoints; `customers` is a single request with one
/// checkpoint. Cursor values are equal-width strings because the
/// cursor is a lexicographic max — the documented REST constraint.
fn small_config(base_url: &str) -> serde_json::Value {
    json!({
        "base_url": base_url,
        "streams": [
            {
                "name": "orders",
                "path": "/orders",
                "pagination": {"type": "page", "page_param": "page"},
                "incremental": {"cursor_field": "id", "start_param": "since"},
            },
            {
                "name": "customers",
                "path": "/customers",
                "incremental": {"cursor_field": "id", "start_param": "since"},
            },
        ],
    })
}

/// Mount the certification stub: every page the full reads walk AND
/// every resume window the S1 law re-reads (`since` carries the
/// committed cursor, so each mock's `since` match IS the resume
/// contract: exactly the rows after that cursor, in order). The
/// `query_param_is_missing` guards make matching order-independent.
/// Mocks are persistent — certification reads every stream several
/// times (S1 baseline, one resume per checkpoint, S4, P5) and runs
/// TWICE.
async fn mount_small_stub(server: &MockServer) {
    // `orders`, full read: two pages of two rows, then the empty page
    // that terminates `page` pagination. Checkpoints land at "02", "04".
    let orders_pages = [
        ("1", json!([{"id": "01"}, {"id": "02"}])),
        ("2", json!([{"id": "03"}, {"id": "04"}])),
        ("3", json!([])),
    ];
    for (page, body) in orders_pages {
        Mock::given(method("GET"))
            .and(path("/orders"))
            .and(query_param("page", page))
            .and(query_param_is_missing("since"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(server)
            .await;
    }
    // `orders`, resumed from each checkpoint the full read commits.
    let orders_resumes = [
        ("02", "1", json!([{"id": "03"}, {"id": "04"}])),
        ("02", "2", json!([])),
        ("04", "1", json!([])),
    ];
    for (since, page, body) in orders_resumes {
        Mock::given(method("GET"))
            .and(path("/orders"))
            .and(query_param("since", since))
            .and(query_param("page", page))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(server)
            .await;
    }
    // `customers`: one un-paginated request; checkpoint lands at "03".
    Mock::given(method("GET"))
        .and(path("/customers"))
        .and(query_param_is_missing("since"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!([{"id": "01"}, {"id": "02"}, {"id": "03"}])),
        )
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/customers"))
        .and(query_param("since", "03"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
        .mount(server)
        .await;
}

/// THE CELL: the built rest bin certifies all-Pass as a source over
/// the wire — S1/S2/S4 reused against the managed adapter plus the
/// protocol clauses P1–P7 — TWICE in a row against the same target and
/// the same stub (the certification bar's repeated element: a
/// connector must survive being certified again from the state the
/// first certification left behind; the rest source holds no state
/// outside the engine's cursor, so the second pass proves exactly
/// that).
#[tokio::test(flavor = "multi_thread")]
async fn the_rest_source_certifies_all_pass() {
    let server = MockServer::start().await;
    mount_small_stub(&server).await;
    let target = Target::resolve_path(built_bin(), small_config(&server.uri()));

    for _attempt in 1..=2 {
        let report = certify_source(&target, &[]).await;

        assert_certified_all_pass(
            &report,
            &["S1", "S2", "S4", "P1", "P2", "P3", "P4", "P5", "P6", "P7"],
            &[],
        );
    }
}
