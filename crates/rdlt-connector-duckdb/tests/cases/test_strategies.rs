//! The merge strategies — delete_insert, upsert, scd2 — pinned by what
//! the target database contains afterward, never by the SQL that got it
//! there.
//!
//! The vocabulary, its refusals, and their wordings are sqlcore's shared
//! surface, so these cells prove the v2 wiring hands the shared planner
//! the same truth generation 1 did: same options in, same rows and same
//! typed errors out.

use rdlt_connector_sdk::spi::source::StreamSpec;
use rdlt_engine::config::Config as EngineConfig;
use rdlt_testkit::memory::Source as MemorySource;
use serde_json::json;

use super::common::{
    KeyedFeed, dest_with, drive, flags, ints, keyed, merge_on, refused_at_parse, rows_in, scalar,
    texts, unit,
};

/// (id, note) rows as one keyed unit on stream `items`.
fn items(rows: &[(i64, &str)]) -> KeyedFeed {
    let ids: Vec<_> = rows.iter().map(|r| Some(r.0)).collect();
    let notes: Vec<_> = rows.iter().map(|r| Some(r.1)).collect();
    keyed(
        "items",
        &["id"],
        unit(&[("id", ints(&ids)), ("note", texts(&notes))]),
    )
}

/// (id, note, gone) rows — the hard_delete shape.
fn flagged_items(rows: &[(i64, &str, bool)]) -> KeyedFeed {
    let ids: Vec<_> = rows.iter().map(|r| Some(r.0)).collect();
    let notes: Vec<_> = rows.iter().map(|r| Some(r.1)).collect();
    let gone: Vec<_> = rows.iter().map(|r| Some(r.2)).collect();
    keyed(
        "items",
        &["id"],
        unit(&[
            ("id", ints(&ids)),
            ("note", texts(&notes)),
            ("gone", flags(&gone)),
        ]),
    )
}

/// delete_insert: a redelivered key's old row is gone and its new row is
/// in; unmatched keys survive — totals equal source truth.
#[tokio::test(flavor = "multi_thread")]
async fn delete_insert_swaps_redelivered_keys() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("di.duckdb");
    let dest = dest_with(&file, json!({"merge_strategy": "delete_insert"}));

    drive(
        merge_on("di", &["id"]),
        items(&[(10, "ten"), (20, "twenty")]),
        dest.clone(),
    )
    .await
    .unwrap();
    drive(
        merge_on("di", &["id"]),
        items(&[(20, "twenty-b"), (30, "thirty")]).resuming_after(1),
        dest,
    )
    .await
    .unwrap();

    assert_eq!(rows_in(&file, "items"), 3);
    assert_eq!(
        scalar(&file, "SELECT note FROM items WHERE id = 20"),
        "twenty-b",
        "the redelivered key carries only its new version"
    );
}

/// upsert edits matched keys in place, and hard_delete composes with it:
/// a flagged survivor deletes its key while the rest merge normally.
#[tokio::test(flavor = "multi_thread")]
async fn upsert_edits_in_place_and_hard_delete_composes() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("upsert.duckdb");
    let dest = dest_with(
        &file,
        json!({
            "merge_strategy": "upsert",
            "tables": {"items": {"hard_delete": "gone"}}
        }),
    );

    drive(
        merge_on("up", &["id"]),
        flagged_items(&[(1, "a", false), (2, "b", false), (3, "c", false)]),
        dest.clone(),
    )
    .await
    .unwrap();
    drive(
        merge_on("up", &["id"]),
        flagged_items(&[(1, "a-edited", false), (3, "whatever", true)]).resuming_after(1),
        dest,
    )
    .await
    .unwrap();

    assert_eq!(rows_in(&file, "items"), 2, "the flagged key is deleted");
    assert_eq!(
        scalar(&file, "SELECT note FROM items WHERE id = 1"),
        "a-edited",
        "the matched key updated in place"
    );
    assert_eq!(
        scalar(&file, "SELECT note FROM items WHERE id = 2"),
        "b",
        "the undelivered key kept its row"
    );
}

/// upsert on a stream with NO declared key (shredded identity) is the
/// shared typed refusal, not a silent fallback.
#[tokio::test(flavor = "multi_thread")]
async fn upsert_refuses_a_shredded_stream() {
    let dir = tempfile::tempdir().unwrap();
    let dest = dest_with(
        &dir.path().join("shred.duckdb"),
        json!({"merge_strategy": "upsert"}),
    );
    let source = MemorySource::single_stream(
        StreamSpec::new("docs"), // no primary key → content-hash identity
        vec![json!({"n": 1, "child": {"x": 1}})],
    );
    let err = drive(merge_on("shred", &["n"]), source, dest)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("upsert strategy requires a KEYED")
            && err.contains("shredded streams use delete_insert"),
        "{err}"
    );
}

/// An EXPLICIT strategy under the append write mode is typed (it would
/// be silently inert); the unconfigured default never rejects anything.
#[tokio::test(flavor = "multi_thread")]
async fn explicit_strategy_under_append_is_typed_but_the_default_is_not() {
    let dir = tempfile::tempdir().unwrap();
    let dest = dest_with(
        &dir.path().join("explicit.duckdb"),
        json!({"merge_strategy": "upsert"}),
    );
    let err = drive(EngineConfig::new("append"), items(&[(1, "x")]), dest)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("merge_strategy requires the merge write mode"),
        "{err}"
    );

    let file = dir.path().join("default.duckdb");
    let dest = dest_with(&file, json!({}));
    drive(
        EngineConfig::new("append-default"),
        items(&[(1, "x")]),
        dest,
    )
    .await
    .unwrap();
    assert_eq!(rows_in(&file, "items"), 1, "no options, no objection");
}

/// scd2: a changed key closes its active version and opens a new one; an
/// unchanged key is left alone entirely (no history churn).
#[tokio::test(flavor = "multi_thread")]
async fn scd2_closes_and_reopens_changed_versions_only() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("scd2.duckdb");
    let dest = dest_with(
        &file,
        json!({
            "merge_strategy": "scd2",
            "tables": {"items": {"scd2": {}}}
        }),
    );

    drive(
        merge_on("hist", &["id"]),
        items(&[(1, "first"), (2, "steady")]),
        dest.clone(),
    )
    .await
    .unwrap();
    drive(
        merge_on("hist", &["id"]),
        items(&[(1, "second"), (2, "steady")]).resuming_after(1),
        dest,
    )
    .await
    .unwrap();

    assert_eq!(rows_in(&file, "items"), 3, "one closed + two active");
    assert_eq!(
        scalar(
            &file,
            "SELECT note FROM items WHERE id = 1 AND _rdlt_valid_to IS NULL"
        ),
        "second",
        "the open version is the new value"
    );
    assert_eq!(
        scalar(
            &file,
            "SELECT note FROM items WHERE id = 1 AND _rdlt_valid_to IS NOT NULL"
        ),
        "first",
        "history keeps the closed version"
    );
    assert_eq!(
        scalar(&file, "SELECT count(*)::VARCHAR FROM items WHERE id = 2"),
        "1",
        "the unchanged key still has exactly its original row"
    );
}

/// Every scd2 row written in one commit unit observes the SAME boundary
/// instant — the transaction-stable `now()` the dialect probes pinned.
#[tokio::test(flavor = "multi_thread")]
async fn scd2_writes_one_boundary_instant_per_unit() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("boundary.duckdb");
    let dest = dest_with(
        &file,
        json!({
            "merge_strategy": "scd2",
            "tables": {"items": {"scd2": {}}}
        }),
    );
    let ids: Vec<Option<i64>> = (0..50).map(Some).collect();
    let notes: Vec<String> = (0..50).map(|i| format!("n{i}")).collect();
    let note_refs: Vec<Option<&str>> = notes.iter().map(|n| Some(n.as_str())).collect();
    let feed = keyed(
        "items",
        &["id"],
        unit(&[("id", ints(&ids)), ("note", texts(&note_refs))]),
    );
    drive(merge_on("instant", &["id"]), feed, dest)
        .await
        .unwrap();

    assert_eq!(
        scalar(
            &file,
            "SELECT count(DISTINCT _rdlt_valid_from)::VARCHAR FROM items"
        ),
        "1",
        "50 keys, one commit unit, one validity boundary"
    );
}

/// scd2 `absent: retire`: a full feed missing a previously-active key
/// retires that key's version at the boundary.
#[tokio::test(flavor = "multi_thread")]
async fn scd2_absent_retire_closes_undelivered_keys() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("retire.duckdb");
    let dest = dest_with(
        &file,
        json!({
            "merge_strategy": "scd2",
            "tables": {"items": {"scd2": {"absent": "retire"}}}
        }),
    );

    drive(
        merge_on("retire", &["id"]),
        items(&[(1, "kept"), (2, "doomed")]),
        dest.clone(),
    )
    .await
    .unwrap();
    drive(
        merge_on("retire", &["id"]),
        items(&[(1, "kept")]).resuming_after(1), // id 2 absent from the full feed
        dest,
    )
    .await
    .unwrap();

    assert_eq!(
        scalar(
            &file,
            "SELECT count(*)::VARCHAR FROM items WHERE _rdlt_valid_to IS NULL"
        ),
        "1",
        "only the delivered key remains active"
    );
    assert_eq!(
        scalar(
            &file,
            "SELECT id::VARCHAR FROM items WHERE _rdlt_valid_to IS NOT NULL"
        ),
        "2",
        "the absent key is the retired one"
    );
}

/// upsert against a target already holding duplicate keys cannot create
/// its unique index — the diagnosis names the colliding columns.
#[tokio::test(flavor = "multi_thread")]
async fn upsert_diagnoses_preexisting_duplicate_keys() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("dups.duckdb");
    {
        // Seed duplicates through plain append (no options, no index).
        let seed = dest_with(&file, json!({}));
        drive(
            EngineConfig::new("seed"),
            items(&[(7, "a"), (7, "b")]),
            seed,
        )
        .await
        .unwrap();
    }
    let dest = dest_with(&file, json!({"merge_strategy": "upsert"}));
    let err = drive(merge_on("collide", &["id"]), items(&[(8, "c")]), dest)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("cannot create the unique index") && err.contains("merge key (id)"),
        "{err}"
    );
}

/// scd2 + merge_scope = SCOPED retirement: absence retires a key only
/// inside a delivered scope; other scopes keep their active versions,
/// and no history row is ever destroyed.
#[tokio::test(flavor = "multi_thread")]
async fn scd2_retirement_is_scoped_by_merge_scope() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("scoped.duckdb");
    let dest = dest_with(
        &file,
        json!({
            "merge_strategy": "scd2",
            "tables": {"items": {
                "scd2": {"absent": "retire"},
                "merge_scope": ["region"]
            }}
        }),
    );

    // east: ids 1,2 · west: id 3 — all active after run one.
    let first = keyed(
        "items",
        &["id"],
        unit(&[
            ("id", ints(&[Some(1), Some(2), Some(3)])),
            ("region", texts(&[Some("east"), Some("east"), Some("west")])),
            ("note", texts(&[Some("e1"), Some("e2"), Some("w1")])),
        ]),
    );
    drive(merge_on("scoped", &["id"]), first, dest.clone())
        .await
        .unwrap();

    // Run two delivers ONLY east, without id 2: id 2 retires (absent in
    // a delivered scope), west's id 3 must stay active untouched.
    let second = keyed(
        "items",
        &["id"],
        unit(&[
            ("id", ints(&[Some(1)])),
            ("region", texts(&[Some("east")])),
            ("note", texts(&[Some("e1")])),
        ]),
    )
    .resuming_after(1);
    drive(merge_on("scoped", &["id"]), second, dest)
        .await
        .unwrap();

    assert_eq!(
        scalar(
            &file,
            "SELECT id::VARCHAR FROM items WHERE _rdlt_valid_to IS NOT NULL"
        ),
        "2",
        "only the key absent from a DELIVERED scope retires"
    );
    assert_eq!(
        scalar(
            &file,
            "SELECT string_agg(id::VARCHAR, ',' ORDER BY id) FROM items \
             WHERE _rdlt_valid_to IS NULL"
        ),
        "1,3",
        "the undelivered scope keeps its active version"
    );
    assert_eq!(rows_in(&file, "items"), 3, "scd2 never destroys rows");
}

/// merge_scope with scd2 under `absent: keep` would be silently inert —
/// the document gate refuses it at parse, naming the requirement.
#[test]
fn scd2_merge_scope_without_retire_is_refused_at_parse() {
    let err = refused_at_parse(json!({
        "merge_strategy": "scd2",
        "tables": {"items": {"scd2": {}, "merge_scope": ["region"]}}
    }));
    assert!(
        err.contains("tables.items.merge_scope") && err.contains("absent: retire"),
        "{err}"
    );
}

/// The two scd2 timestamp overrides: `active_record_timestamp` replaces
/// NULL as the open marker, `boundary_timestamp` replaces `now()` — and
/// an injection-shaped timestamp value never reaches SQL.
#[tokio::test(flavor = "multi_thread")]
async fn scd2_timestamp_overrides_hold_and_garbage_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("marker.duckdb");
    let dest = dest_with(
        &file,
        json!({
            "merge_strategy": "scd2",
            "tables": {"items": {"scd2": {
                "active_record_timestamp": "9999-12-31T00:00:00Z",
                "boundary_timestamp": "2031-05-01T00:00:00Z"
            }}}
        }),
    );

    drive(
        merge_on("marker", &["id"]),
        items(&[(1, "v1")]),
        dest.clone(),
    )
    .await
    .unwrap();
    drive(
        merge_on("marker", &["id"]),
        items(&[(1, "v2")]).resuming_after(1),
        dest,
    )
    .await
    .unwrap();

    assert_eq!(
        scalar(
            &file,
            "SELECT note FROM items \
             WHERE _rdlt_valid_to = TIMESTAMPTZ '9999-12-31T00:00:00Z'"
        ),
        "v2",
        "the open version carries the marker"
    );
    assert_eq!(
        scalar(
            &file,
            "SELECT count(*)::VARCHAR FROM items WHERE _rdlt_valid_to IS NULL"
        ),
        "0",
        "no NULL open markers once the marker is configured"
    );
    assert_eq!(
        scalar(
            &file,
            "SELECT note FROM items \
             WHERE _rdlt_valid_to = TIMESTAMPTZ '2031-05-01T00:00:00Z'"
        ),
        "v1",
        "the closed version closed at the caller's boundary"
    );

    let err = refused_at_parse(json!({
        "tables": {"items": {
            "merge_strategy": "scd2",
            "scd2": {"boundary_timestamp": "'); DELETE FROM items; --"}
        }}
    }));
    assert!(
        err.contains("boundary_timestamp") && err.contains("not a"),
        "{err}"
    );
}
