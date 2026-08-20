//! Crash-recovery facts, driven straight through the SPI: recovery means
//! a FRESH shell and session opened with the SAME load id, and that
//! session must respect what the load's earlier commits already
//! published. Both cells pin durable state — an in-memory guard passes
//! them right up until a recovery constructs the session that has
//! forgotten it.
//!
//! Instance discipline: each "process life" is a scoped Shell, dropped
//! before any read-back — one live instance per file at a time, the
//! documented-safe sequential pattern.

use rdlt_connector_sdk::spi::core::{commit::WriteMode, id::LoadId, id::PipelineId, id::TableName};
use rdlt_connector_sdk::spi::{destination::Destination, destination::OpenContext};
use rdlt_testkit::fixtures::{batch_of, commit_meta_for, schema_for};
use serde_json::json;

use super::common::{dest_with, rows_in};

/// The Replace truncate-once guard is DURABLE: after a crash between two
/// commits of one load, the recovery session (fresh shell, fresh memory,
/// same load) must not re-truncate the rows the first commit published —
/// while the NEXT load's first commit still starts from an empty table.
#[tokio::test]
async fn replace_never_retruncates_what_the_same_load_committed() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("replace.duckdb");
    let pipeline = PipelineId::new("pipe");
    let load = LoadId::new("load-2031");
    let table = TableName::new("events");

    // Process life A: the load's first unit publishes durably.
    {
        let dest = dest_with(&file, json!({}));
        let mut a = dest
            .open(OpenContext::new(pipeline.clone(), load.clone()))
            .await
            .unwrap();
        a.ensure_table(&schema_for("events"), &WriteMode::Replace)
            .await
            .unwrap();
        a.write(&table, batch_of(&[10, 11])).await.unwrap();
        a.commit(commit_meta_for(&pipeline, &load, 1))
            .await
            .unwrap();
    }
    assert_eq!(rows_in(&file, "events"), 2);

    // Process life B: crash recovery — same load, next unit, no memory
    // of A beyond what the database itself carries.
    {
        let dest = dest_with(&file, json!({}));
        let mut b = dest
            .open(OpenContext::new(pipeline.clone(), load.clone()))
            .await
            .unwrap();
        b.ensure_table(&schema_for("events"), &WriteMode::Replace)
            .await
            .unwrap();
        b.write(&table, batch_of(&[12])).await.unwrap();
        b.commit(commit_meta_for(&pipeline, &load, 2))
            .await
            .unwrap();
    }
    assert_eq!(
        rows_in(&file, "events"),
        3,
        "unit 1's rows survive recovery — the guard is durable state, not session memory"
    );

    // A genuinely NEW load still replaces from scratch.
    {
        let next = LoadId::new("load-2032");
        let dest = dest_with(&file, json!({}));
        let mut c = dest
            .open(OpenContext::new(pipeline.clone(), next.clone()))
            .await
            .unwrap();
        c.ensure_table(&schema_for("events"), &WriteMode::Replace)
            .await
            .unwrap();
        c.write(&table, batch_of(&[20])).await.unwrap();
        c.commit(commit_meta_for(&pipeline, &next, 1))
            .await
            .unwrap();
    }
    assert_eq!(rows_in(&file, "events"), 1, "the next load truncates first");
}

/// A redelivered `(load, seq)` routes through the sdk choreography's
/// replay branch — publishing nothing — AND must RE-MARK the scoped
/// table's single-unit discipline, so a later unit for that table in the
/// recovery session errors instead of firing a second scope replacement.
#[tokio::test]
async fn a_replayed_unit_still_counts_as_the_scoped_tables_one_unit() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("replay.duckdb");
    let options = json!({"tables": {"events": {"merge_scope": ["id"]}}});
    let pipeline = PipelineId::new("pipe");
    let load = LoadId::new("load-2031");
    let table = TableName::new("events");
    let merge = WriteMode::Merge {
        key: vec!["id".into()],
    };

    // Process life A: the scoped table's one unit commits; its receipt
    // is durable.
    {
        let dest = dest_with(&file, options.clone());
        let mut a = dest
            .open(OpenContext::new(pipeline.clone(), load.clone()))
            .await
            .unwrap();
        a.ensure_table(&schema_for("events"), &merge).await.unwrap();
        a.write(&table, batch_of(&[1, 2])).await.unwrap();
        let earned = a
            .commit(commit_meta_for(&pipeline, &load, 1))
            .await
            .unwrap();
        assert_eq!(earned.commit_seq, 1);
    }

    // Process life B: recovery redelivers the SAME (load, seq). The sdk
    // choreography finds the earned receipt and takes the replay branch:
    // the redelivered staging is reclaimed, nothing publishes twice.
    {
        let dest = dest_with(&file, options);
        let mut b = dest
            .open(OpenContext::new(pipeline.clone(), load.clone()))
            .await
            .unwrap();
        b.ensure_table(&schema_for("events"), &merge).await.unwrap();
        b.write(&table, batch_of(&[1, 2])).await.unwrap();
        let replayed = b
            .commit(commit_meta_for(&pipeline, &load, 1))
            .await
            .expect("a replayed commit answers with the receipt it already earned");
        assert_eq!(replayed.commit_seq, 1);

        // The replay must have RE-MARKED the discipline: a second unit
        // for the scoped table in this session is the typed violation.
        b.write(&table, batch_of(&[3])).await.unwrap();
        let err = b
            .commit(commit_meta_for(&pipeline, &load, 2))
            .await
            .expect_err("the scoped table already spent its single unit")
            .to_string();
        assert!(
            err.contains("SINGLE commit unit") && err.contains("merge_scope"),
            "{err}"
        );
    }
    assert_eq!(
        rows_in(&file, "events"),
        2,
        "the replay published nothing and the refused unit rolled back"
    );
}
