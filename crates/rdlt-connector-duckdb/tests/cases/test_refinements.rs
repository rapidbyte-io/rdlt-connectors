//! The merge refinements: ordered survivor selection (`dedup_sort`) and
//! wholesale scope replacement (`merge_scope`), plus the typed refusals
//! that keep both from failing silently.
//!
//! Outcomes are asserted destination-side; the planning is sqlcore's,
//! shared with postgres, and the wordings asserted here are that shared
//! surface reaching an operator through the v2 shell.

use rdlt_connector_sdk::spi::source::StreamSpec;
use rdlt_testkit::memory::Source as MemorySource;
use serde_json::json;

use super::common::{
    KeyedFeed, dest_with, drive, ints, keyed, merge_on, rows_in, scalar, texts, unit,
};

/// (id, rank, note) rows — the dedup_sort shape.
fn ranked(rows: &[(i64, Option<i64>, &str)]) -> KeyedFeed {
    let ids: Vec<_> = rows.iter().map(|r| Some(r.0)).collect();
    let ranks: Vec<_> = rows.iter().map(|r| r.1).collect();
    let notes: Vec<_> = rows.iter().map(|r| Some(r.2)).collect();
    keyed(
        "items",
        &["id"],
        unit(&[
            ("id", ints(&ids)),
            ("rank", ints(&ranks)),
            ("note", texts(&notes)),
        ]),
    )
}

/// (id, region, note) rows — the merge_scope shape; region NULLable.
fn regional(rows: &[(i64, Option<&str>, &str)]) -> KeyedFeed {
    let ids: Vec<_> = rows.iter().map(|r| Some(r.0)).collect();
    let regions: Vec<_> = rows.iter().map(|r| r.1).collect();
    let notes: Vec<_> = rows.iter().map(|r| Some(r.2)).collect();
    keyed(
        "items",
        &["id"],
        unit(&[
            ("id", ints(&ids)),
            ("region", texts(&regions)),
            ("note", texts(&notes)),
        ]),
    )
}

/// dedup_sort ranks survivors within one load: greatest rank wins under
/// `desc`, any value beats NULL regardless of arrival order, and an
/// all-NULL group falls back to deterministic arrival last-wins.
#[tokio::test(flavor = "multi_thread")]
async fn dedup_sort_picks_ordered_survivors() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("dedup.duckdb");
    let dest = dest_with(
        &file,
        json!({
            "tables": {"items": {"dedup_sort": {"column": "rank", "order": "desc"}}}
        }),
    );
    let feed = ranked(&[
        // id 1: highest rank survives even though it arrived first.
        (1, Some(9), "top"),
        (1, Some(2), "low"),
        // id 2: the ranked row beats the NULL row that arrived after it.
        (2, Some(4), "has-rank"),
        (2, None, "rankless-later"),
        // id 3: all NULL → arrival order decides, last wins.
        (3, None, "earlier"),
        (3, None, "final"),
    ]);
    drive(merge_on("dedup", &["id"]), feed, dest).await.unwrap();

    assert_eq!(rows_in(&file, "items"), 3);
    for (id, survivor) in [(1, "top"), (2, "has-rank"), (3, "final")] {
        assert_eq!(
            scalar(&file, &format!("SELECT note FROM items WHERE id = {id}")),
            survivor,
            "id={id}"
        );
    }
}

/// merge_scope replaces each DELIVERED scope wholesale: undelivered rows
/// inside it disappear, undelivered scopes stay whole, and a NULL scope
/// value is not a scope at all.
#[tokio::test(flavor = "multi_thread")]
async fn merge_scope_replaces_delivered_scopes_wholesale() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("scope.duckdb");
    let dest = dest_with(
        &file,
        json!({"tables": {"items": {"merge_scope": ["region"]}}}),
    );

    drive(
        merge_on("scope", &["id"]),
        regional(&[
            (1, Some("east"), "e1"),
            (2, Some("east"), "e2"),
            (3, Some("west"), "w1"),
            (4, None, "unscoped"),
        ]),
        dest.clone(),
    )
    .await
    .unwrap();
    assert_eq!(rows_in(&file, "items"), 4);

    // Redeliver east alone, and smaller: id 2 must vanish with it.
    drive(
        merge_on("scope", &["id"]),
        regional(&[(1, Some("east"), "e1-b")]).resuming_after(1),
        dest,
    )
    .await
    .unwrap();

    assert_eq!(rows_in(&file, "items"), 3);
    assert_eq!(scalar(&file, "SELECT note FROM items WHERE id = 1"), "e1-b");
    assert_eq!(
        scalar(&file, "SELECT count(*)::VARCHAR FROM items WHERE id = 2"),
        "0",
        "the delivered scope is replaced wholesale — its missing row is gone"
    );
    assert_eq!(
        scalar(&file, "SELECT note FROM items WHERE id = 3"),
        "w1",
        "the undelivered scope is untouched"
    );
    assert_eq!(
        scalar(&file, "SELECT note FROM items WHERE id = 4"),
        "unscoped",
        "NULL is not a scope, so the NULL-scoped row survives"
    );
}

/// A scoped table's feed split over TWO commit units is the shared typed
/// violation, raised on the second unit.
#[tokio::test(flavor = "multi_thread")]
async fn a_scoped_feed_split_across_units_is_typed() {
    let dir = tempfile::tempdir().unwrap();
    let dest = dest_with(
        &dir.path().join("split.duckdb"),
        json!({"tables": {"items": {"merge_scope": ["region"]}}}),
    );
    let feed = keyed(
        "items",
        &["id"],
        unit(&[
            ("id", ints(&[Some(1)])),
            ("region", texts(&[Some("east")])),
            ("note", texts(&[Some("first-unit")])),
        ]),
    )
    .and_unit(unit(&[
        ("id", ints(&[Some(2)])),
        ("region", texts(&[Some("east")])),
        ("note", texts(&[Some("second-unit")])),
    ]));
    let err = drive(merge_on("split", &["id"]), feed, dest)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("merge_scope replacement") && err.contains("SINGLE commit unit"),
        "{err}"
    );
}

/// The refinement-misuse matrix: each misconfiguration is a typed error
/// in sqlcore's shared wording, never a silent no-op.
#[tokio::test(flavor = "multi_thread")]
async fn refinement_misuse_is_typed_never_silent() {
    let dir = tempfile::tempdir().unwrap();

    // dedup_sort on a stream with no declared key.
    let dest = dest_with(
        &dir.path().join("keyless.duckdb"),
        json!({"tables": {"docs": {"dedup_sort": {"column": "rank", "order": "desc"}}}}),
    );
    let source = MemorySource::single_stream(
        StreamSpec::new("docs"),
        vec![json!({"rank": 1, "child": {"x": 1}})],
    );
    let err = drive(merge_on("keyless", &["rank"]), source, dest)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("dedup_sort requires a KEYED"), "{err}");

    // merge_scope naming a column the table does not have.
    let dest = dest_with(
        &dir.path().join("ghost.duckdb"),
        json!({"tables": {"items": {"merge_scope": ["ghost"]}}}),
    );
    let err = drive(
        merge_on("ghost", &["id"]),
        regional(&[(1, Some("east"), "x")]),
        dest,
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(
        err.contains("merge_scope column `ghost` is not a column"),
        "{err}"
    );

    // dedup_sort on a merge-key column — constant per group, orders nothing.
    let dest = dest_with(
        &dir.path().join("keycol.duckdb"),
        json!({"tables": {"items": {"dedup_sort": {"column": "id", "order": "desc"}}}}),
    );
    let err = drive(
        merge_on("keycol", &["id"]),
        regional(&[(1, Some("east"), "x")]),
        dest,
    )
    .await
    .unwrap_err()
    .to_string();
    assert!(err.contains("part of the merge key"), "{err}");
}

/// A NON-boolean hard_delete column deletes on IS NOT NULL — any value
/// in a text `removed_at` kills the key, NULL keeps it.
#[tokio::test(flavor = "multi_thread")]
async fn non_boolean_hard_delete_deletes_on_any_value() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("softflag.duckdb");
    let dest = dest_with(
        &file,
        json!({"tables": {"items": {"hard_delete": "removed_at"}}}),
    );

    let first = keyed(
        "items",
        &["id"],
        unit(&[
            ("id", ints(&[Some(1), Some(2)])),
            ("note", texts(&[Some("survivor"), Some("victim")])),
            ("removed_at", texts(&[None, None])),
        ]),
    );
    drive(merge_on("softflag", &["id"]), first, dest.clone())
        .await
        .unwrap();

    let second = keyed(
        "items",
        &["id"],
        unit(&[
            ("id", ints(&[Some(2)])),
            ("note", texts(&[Some("victim")])),
            ("removed_at", texts(&[Some("2031-05-01")])),
        ]),
    )
    .resuming_after(1);
    drive(merge_on("softflag", &["id"]), second, dest)
        .await
        .unwrap();

    assert_eq!(rows_in(&file, "items"), 1);
    assert_eq!(scalar(&file, "SELECT note FROM items"), "survivor");
}
