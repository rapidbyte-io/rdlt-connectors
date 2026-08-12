//! The 031 round-1 session guards, driven straight through the SPI:
//! the append-only re-ensure rule and the positional write check. Both
//! refuse TYPED where the old behavior would have landed or dropped
//! values silently.

use rdlt_connector_sdk::spi::core::{
    ColumnDef, ColumnType, LoadId, LogicalType, PipelineId, Provenance, TableName, TableSchema,
    WriteMode,
};
use rdlt_connector_sdk::spi::{Destination, OpenContext};
use serde_json::json;

use super::common::{dest_with, ints, texts, unit};

/// A schema of nullable scalar columns, in the given order.
fn table_of(table: &str, columns: &[(&str, LogicalType)]) -> TableSchema {
    TableSchema {
        table: TableName::new(table),
        parent: None,
        columns: columns
            .iter()
            .map(|(name, scalar)| ColumnDef {
                name: (*name).to_owned(),
                column_type: ColumnType::scalar(*scalar),
                nullable: true,
                provenance: Provenance::Inferred,
            })
            .collect(),
    }
}

/// S1: within a session, re-ensure may only ADD columns. A re-ensure
/// whose schema DROPS a previously ensured column is refused typed,
/// naming the dropped column — while a purely additive re-ensure of
/// the same table stays legal.
#[tokio::test]
async fn a_re_ensure_that_drops_an_ensured_column_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("regress.duckdb");
    let dest = dest_with(&file, json!({}));
    let mut session = dest
        .open(OpenContext::new(
            PipelineId::new("guards"),
            LoadId::new("load-1"),
        ))
        .await
        .unwrap();

    let base = &[("id", LogicalType::Int64), ("name", LogicalType::Utf8)];
    session
        .ensure_table(&table_of("events", base), &WriteMode::Append)
        .await
        .expect("first ensure");
    // Additive drift stays legal — the widen/add path is untouched.
    session
        .ensure_table(
            &table_of(
                "events",
                &[
                    ("id", LogicalType::Int64),
                    ("name", LogicalType::Utf8),
                    ("flag", LogicalType::Bool),
                ],
            ),
            &WriteMode::Append,
        )
        .await
        .expect("additive re-ensure");

    // A schema that DROPS previously ensured columns is not evolution.
    let err = session
        .ensure_table(
            &table_of("events", &[("id", LogicalType::Int64)]),
            &WriteMode::Append,
        )
        .await
        .expect_err("a column-dropping re-ensure must refuse")
        .to_string();
    assert!(
        err.contains("re-ensure drops previously ensured columns (flag, name)")
            || err.contains("re-ensure drops previously ensured columns (name, flag)"),
        "names every dropped column: {err}"
    );
    assert!(
        err.contains("two streams colliding on one table"),
        "states the diagnosis: {err}"
    );
}

/// S4: the stage append is positional, so a batch whose columns are
/// reordered against the ensured schema is refused typed at the first
/// divergent position — values must never land in the wrong columns.
#[tokio::test]
async fn a_reordered_batch_is_refused_before_the_positional_append() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("reorder.duckdb");
    let dest = dest_with(&file, json!({}));
    let mut session = dest
        .open(OpenContext::new(
            PipelineId::new("guards"),
            LoadId::new("load-1"),
        ))
        .await
        .unwrap();
    session
        .ensure_table(
            &table_of(
                "kv",
                &[("id", LogicalType::Int64), ("v", LogicalType::Utf8)],
            ),
            &WriteMode::Append,
        )
        .await
        .expect("ensure");

    let table = TableName::new("kv");
    // The ensured order lands fine.
    session
        .write(
            &table,
            unit(&[("id", ints(&[Some(1)])), ("v", texts(&[Some("ok")]))]),
        )
        .await
        .expect("the ensured order appends");

    // The SAME columns, reordered: refused at position 0.
    let err = session
        .write(
            &table,
            unit(&[("v", texts(&[Some("swapped")])), ("id", ints(&[Some(2)]))]),
        )
        .await
        .expect_err("a reordered batch must refuse")
        .to_string();
    assert!(
        err.contains("batch column 0 is `v` but the ensured schema has `id`"),
        "names the first divergent position: {err}"
    );
    assert!(
        err.contains("the stage append is positional"),
        "states why: {err}"
    );
}

/// The registry claim must outlive every SESSION, not just the shell:
/// dropping the shell while a session still writes must not free the
/// path for a second open (031 round 2 — the truncation hazard would
/// reopen exactly for the embedder shapes the registry protects).
#[tokio::test]
async fn the_open_claim_outlives_the_shell_while_a_session_lives() {
    use rdlt_connector_duckdb::destination::{Config, Shell};
    use rdlt_connector_sdk::spi::core::{LoadId, PipelineId, WriteMode};
    use rdlt_connector_sdk::spi::{Destination, OpenContext};
    use rdlt_testkit::schema_for;

    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("held.duckdb");
    let shell = Shell::new(Config::new(&path)).expect("valid");
    let mut session = shell
        .open(OpenContext::new(
            PipelineId::new("claim-life"),
            LoadId::new("load-1"),
        ))
        .await
        .expect("open");
    session
        .ensure_table(&schema_for("rows"), &WriteMode::Append)
        .await
        .expect("ensure");
    drop(shell);

    let err = Shell::new(Config::new(&path))
        .expect_err("the live session still holds the claim")
        .to_string();
    assert!(err.contains("already open in this process"), "{err}");

    drop(session);
    Shell::new(Config::new(&path)).expect("released with the last session");
}

/// The reorder half of the append-only rule: a re-ensure with the
/// SAME columns in a DIFFERENT order is refused — the physical stage
/// keeps creation order and a reorder emits no DDL, so accepting it
/// would let the positional write guard bless value-swapping batches
/// (the /code-review round's trace).
#[tokio::test]
async fn a_re_ensure_that_reorders_columns_is_refused() {
    let dir = tempfile::tempdir().expect("dir");
    let file = dir.path().join("reorder-ensure.duckdb");
    let dest = dest_with(&file, json!({}));
    let mut session = dest
        .open(OpenContext::new(
            PipelineId::new("reorder"),
            LoadId::new("load-1"),
        ))
        .await
        .expect("open");
    session
        .ensure_table(
            &table_of("t", &[("a", LogicalType::Int64), ("b", LogicalType::Utf8)]),
            &WriteMode::Append,
        )
        .await
        .expect("first ensure");
    let err = session
        .ensure_table(
            &table_of("t", &[("b", LogicalType::Utf8), ("a", LogicalType::Int64)]),
            &WriteMode::Append,
        )
        .await
        .expect_err("reorder refused")
        .to_string();
    assert!(
        err.contains("re-ensure reorders previously ensured columns")
            && err.contains("position 0 was `a`, now `b`"),
        "{err}"
    );
}

/// The exact-arity branch of the positional guard, pinned: a batch
/// with FEWER columns than the ensured schema refuses typed, not with
/// the raw appender error.
#[tokio::test]
async fn a_short_batch_is_refused_with_the_arity_message() {
    let dir = tempfile::tempdir().expect("dir");
    let file = dir.path().join("arity.duckdb");
    let dest = dest_with(&file, json!({}));
    let mut session = dest
        .open(OpenContext::new(
            PipelineId::new("arity"),
            LoadId::new("load-1"),
        ))
        .await
        .expect("open");
    session
        .ensure_table(
            &table_of("t", &[("a", LogicalType::Int64), ("b", LogicalType::Utf8)]),
            &WriteMode::Append,
        )
        .await
        .expect("ensure");
    let err = session
        .write(&TableName::new("t"), unit(&[("a", ints(&[Some(1)]))]))
        .await
        .expect_err("short batch refused")
        .to_string();
    assert!(
        err.contains("carries 1 columns but the ensured schema has 2"),
        "{err}"
    );
}

/// The refusal of a second open must leave the FIRST instance's
/// committed data untouched — the registry's whole purpose is that
/// the refused open never becomes the truncating act.
#[tokio::test]
async fn a_refused_second_open_leaves_committed_data_intact() {
    use rdlt_connector_duckdb::destination::{self, Config, Shell};
    use rdlt_connector_sdk::spi::core::{LoadId, PipelineId, TableName, WriteMode};
    use rdlt_connector_sdk::spi::{Destination, OpenContext};
    use rdlt_testkit::{batch_of, commit_meta_for, schema_for};

    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("survivor.duckdb");
    let shell = Shell::new(Config::new(&path)).expect("valid");
    let pipeline = PipelineId::new("survivor");
    let load = LoadId::new("load-1");
    let mut session = shell
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    session
        .ensure_table(&schema_for("rows"), &WriteMode::Append)
        .await
        .expect("ensure");
    session
        .write(&TableName::new("rows"), batch_of(&[1, 2, 3]))
        .await
        .expect("write");
    session
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");

    Shell::new(Config::new(&path)).expect_err("second open refused");

    // The live instance keeps working AND the committed rows survive.
    session
        .write(&TableName::new("rows"), batch_of(&[4]))
        .await
        .expect("the live session still writes");
    session
        .commit(commit_meta_for(&pipeline, &load, 2))
        .await
        .expect("and still commits");
    drop(session);
    drop(shell);
    assert_eq!(
        destination::testhook::count_rows(&Config::new(&path), "rows").expect("count"),
        4,
        "nothing was truncated by the refused open"
    );
}

/// Concurrency smoke for the serialized connect: many simultaneous
/// connects on one path yield exactly ONE holder; after every handle
/// drops, the path opens again.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_connects_admit_exactly_one_holder() {
    use rdlt_connector_duckdb::destination::{Config, Shell};

    let dir = tempfile::tempdir().expect("dir");
    let path = dir.path().join("race.duckdb");
    // Each thread RETURNS its Shell so every claim outlives every
    // check — without that, a fast winner's open-and-drop lets a slow
    // straggler in as a legal sequential re-open and the exactly-one
    // assertion would constrain the scheduler, not the registry.
    let mut handles = Vec::new();
    for _ in 0..8 {
        let path = path.clone();
        handles.push(std::thread::spawn(move || Shell::new(Config::new(&path))));
    }
    let results: Vec<_> = handles
        .into_iter()
        .map(|h| h.join().expect("no panic"))
        .collect();
    let admitted = results.iter().filter(|r| r.is_ok()).count();
    assert_eq!(admitted, 1, "exactly one connect wins the claim");
    drop(results);
    Shell::new(Config::new(&path)).expect("released after all drops");
}
