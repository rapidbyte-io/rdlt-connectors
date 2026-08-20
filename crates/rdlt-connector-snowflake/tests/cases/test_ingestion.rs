//! One real load, end to end: rows leave as parquet parts, land through
//! COPY inside the unit, and the local staging directory is EMPTY once
//! the load settles — the cleanup claim checked rather than trusted.

use rdlt_connector_sdk::spi::source::StreamSpec;
use rdlt_connector_snowflake::destination::{Shell, testhook};
use rdlt_engine::config::Config as EngineConfig;
use rdlt_engine::engine::Engine;
use rdlt_testkit::memory::{Batch as MemoryBatch, Source as MemorySource, Stream as MemoryStream};
use serde_json::json;

use super::common::{config_for, credentials, scratch_schema};

#[tokio::test]
async fn an_append_load_lands_every_row_and_leaves_no_local_residue() {
    let Some(creds) = credentials() else { return };
    let schema = scratch_schema("ing");
    let doc = config_for(&creds, &schema);
    let shell = Shell::from_value(doc.clone()).expect("valid document");
    let config = {
        use rdlt_connector_sdk::config::Document;
        rdlt_connector_snowflake::destination::Config::from_value(doc).expect("valid")
    };

    let source = MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![
            MemoryBatch::new(vec![
                json!({"id": 1, "note": "a"}),
                json!({"id": 2, "note": "b"}),
            ])
            .with_checkpoint(1),
            MemoryBatch::new(vec![json!({"id": 3, "note": "c"})]).with_checkpoint(2),
        ],
    )]);

    let workdir = tempfile::tempdir().expect("workdir");
    let pipeline = "sf-ing";
    let report = Engine::new(
        EngineConfig::new(pipeline).with_workdir(workdir.path().join("wal")),
        source,
        shell,
    )
    .run()
    .await
    .expect("the load settles");
    assert!(!report.tables.is_empty(), "the run reports its table");

    let landed = testhook::rows(
        &config,
        &format!(
            "SELECT COUNT(*) AS N FROM \"{}\".\"{}\".\"EVENTS\"",
            config.database.to_uppercase(),
            schema.to_uppercase()
        ),
        &["n"],
    )
    .await
    .expect("read back");
    assert_eq!(landed[0][0], "3", "every row landed exactly once");

    // The local part directory for this load must be gone or empty:
    // parts are removed after upload, and the directory after settle.
    let local = testhook::local_part_dir(pipeline, report.load_id.as_str());
    let leftover = std::fs::read_dir(&local)
        .map(|entries| entries.count())
        .unwrap_or(0);
    assert_eq!(leftover, 0, "no local residue at {}", local.display());

    let _ = testhook::connect_and_run(
        &config,
        &format!(
            "DROP SCHEMA IF EXISTS \"{}\".\"{}\" CASCADE",
            config.database.to_uppercase(),
            schema.to_uppercase()
        ),
    )
    .await;
}

/// A column that appears MID-UNIT (second batch, before any checkpoint)
/// keeps its data. Pins the generation-1 defect this rewrite found: the
/// COPY's column list was captured on a table's first write and never
/// refreshed, so a mid-unit widened column projected out of the load —
/// every row's value for it landed NULL with matching row counts and no
/// error anywhere.
#[tokio::test]
async fn a_column_added_mid_unit_keeps_its_data() {
    let Some(creds) = credentials() else { return };
    let schema = scratch_schema("widen");
    let doc = config_for(&creds, &schema);
    let config = {
        use rdlt_connector_sdk::config::Document;
        rdlt_connector_snowflake::destination::Config::from_value(doc.clone()).expect("valid")
    };

    // Two batches, ONE checkpoint at the end: both land in the same
    // commit unit, and the second batch carries a column the first did
    // not — the shredder infers it and the engine widens mid-unit.
    let source = MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![
            MemoryBatch::new(vec![json!({"id": 1})]),
            MemoryBatch::new(vec![json!({"id": 2, "extra": "kept"})]).with_checkpoint(1),
        ],
    )]);

    let workdir = tempfile::tempdir().expect("workdir");
    Engine::new(
        EngineConfig::new("sf-widen").with_workdir(workdir.path().join("wal")),
        source,
        Shell::from_value(doc).expect("valid"),
    )
    .run()
    .await
    .expect("the load settles");

    let landed = testhook::rows(
        &config,
        &format!(
            "SELECT \"EXTRA\" AS V FROM \"{}\".\"{}\".\"EVENTS\" WHERE \"ID\" = 2",
            config.database.to_uppercase(),
            schema.to_uppercase()
        ),
        &["v"],
    )
    .await
    .expect("read back");
    assert_eq!(
        landed[0][0], "kept",
        "the mid-unit column's value must survive the COPY"
    );

    let _ = testhook::connect_and_run(
        &config,
        &format!(
            "DROP SCHEMA IF EXISTS \"{}\".\"{}\" CASCADE",
            config.database.to_uppercase(),
            schema.to_uppercase()
        ),
    )
    .await;
}

/// The same mid-unit widening under MERGE mode, where rows travel
/// through the stage TABLE: the review found the just-created stage leg
/// was never folded into the catalog image, so a later same-session
/// evolution skipped the stage's ADD COLUMN (its re-rendered CREATE IF
/// NOT EXISTS is a service no-op) and the COPY named a column the stage
/// never gained. Inherited from generation 1.
#[tokio::test]
async fn a_column_added_mid_unit_reaches_the_merge_stage() {
    let Some(creds) = credentials() else { return };
    let schema = scratch_schema("mwid");
    let mut doc = config_for(&creds, &schema);
    doc["merge_strategy"] = json!("delete_insert");
    let config = {
        use rdlt_connector_sdk::config::Document;
        rdlt_connector_snowflake::destination::Config::from_value(doc.clone()).expect("valid")
    };

    let source = MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events").with_primary_key(["id".to_owned()]),
        vec![
            MemoryBatch::new(vec![json!({"id": 1})]),
            MemoryBatch::new(vec![json!({"id": 2, "extra": "kept"})]).with_checkpoint(1),
        ],
    )]);

    let workdir = tempfile::tempdir().expect("workdir");
    Engine::new(
        EngineConfig::new("sf-mwid")
            .with_workdir(workdir.path().join("wal"))
            .with_write_mode(rdlt_connector_sdk::spi::core::commit::WriteMode::Merge {
                key: vec!["id".to_owned()],
            }),
        source,
        Shell::from_value(doc).expect("valid"),
    )
    .run()
    .await
    .expect("the load settles");

    let landed = testhook::rows(
        &config,
        &format!(
            "SELECT \"EXTRA\" AS V FROM \"{}\".\"{}\".\"EVENTS\" WHERE \"ID\" = 2",
            config.database.to_uppercase(),
            schema.to_uppercase()
        ),
        &["v"],
    )
    .await
    .expect("read back");
    assert_eq!(
        landed[0][0], "kept",
        "the widened column must land through the stage leg"
    );

    let _ = testhook::connect_and_run(
        &config,
        &format!(
            "DROP SCHEMA IF EXISTS \"{}\".\"{}\" CASCADE",
            config.database.to_uppercase(),
            schema.to_uppercase()
        ),
    )
    .await;
}
