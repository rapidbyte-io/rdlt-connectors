//! Full-load semantics against the live service: Replace leaves exactly
//! the newest run's rows, and a repeat load at the same schema issues
//! no schema statements (the read-before-write economy, proven on the
//! real catalog).

use rdlt_connector_sdk::spi::core::commit::WriteMode;
use rdlt_connector_sdk::spi::source::StreamSpec;
use rdlt_connector_snowflake::destination::{Shell, testhook};
use rdlt_engine::config::Config as EngineConfig;
use rdlt_engine::engine::Engine;
use rdlt_testkit::memory::{Batch as MemoryBatch, Source as MemorySource, Stream as MemoryStream};
use serde_json::json;

use super::common::{config_for, credentials, scratch_schema};

fn source_of(rows: Vec<serde_json::Value>) -> MemorySource {
    MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![MemoryBatch::new(rows).with_checkpoint(1)],
    )])
}

#[tokio::test]
async fn a_replace_load_leaves_only_the_newest_rows() {
    let Some(creds) = credentials() else { return };
    let schema = scratch_schema("rep");
    let doc = config_for(&creds, &schema);
    let config = {
        use rdlt_connector_sdk::config::Document;
        rdlt_connector_snowflake::destination::Config::from_value(doc.clone()).expect("valid")
    };

    for (pipeline, rows) in [
        (
            "sf-rep-1",
            vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})],
        ),
        ("sf-rep-2", vec![json!({"id": 10}), json!({"id": 11})]),
    ] {
        let workdir = tempfile::tempdir().expect("workdir");
        Engine::new(
            EngineConfig::new(pipeline)
                .with_workdir(workdir.path().join("wal"))
                .with_write_mode(WriteMode::Replace),
            source_of(rows),
            Shell::from_value(doc.clone()).expect("valid"),
        )
        .run()
        .await
        .expect("the load settles");
    }

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
    assert_eq!(landed[0][0], "2", "only the second run's rows survive");

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

/// A Replace clear survives a MID-UNIT schema evolution of a DIFFERENT
/// table, live. Run 2's `events` batch carries no checkpoint of its
/// own, so the brand-new `arrivals` stream's ensure lands mid-unit
/// AFTER `events` was written and cleared — the rollback takes the
/// DELETE with it, and `events` is never written again before the one
/// publish. Only the owed-re-clear machinery (review round 2) stands
/// between this load and committing run 1's rows alongside its own.
#[tokio::test]
async fn a_replace_clear_survives_another_tables_mid_unit_evolution() {
    let Some(creds) = credentials() else { return };
    let schema = scratch_schema("owed");
    let doc = config_for(&creds, &schema);
    let config = {
        use rdlt_connector_sdk::config::Document;
        rdlt_connector_snowflake::destination::Config::from_value(doc.clone()).expect("valid")
    };

    // Run 1 seeds `events` with the rows a vanished re-clear would
    // leave behind.
    {
        let workdir = tempfile::tempdir().expect("workdir");
        Engine::new(
            EngineConfig::new("sf-owed-1")
                .with_workdir(workdir.path().join("wal"))
                .with_write_mode(WriteMode::Replace),
            source_of(vec![json!({"id": 1}), json!({"id": 2})]),
            Shell::from_value(doc.clone()).expect("valid"),
        )
        .run()
        .await
        .expect("run 1 settles");
    }

    let source = MemorySource::new(vec![
        MemoryStream::new(
            StreamSpec::new("events"),
            vec![MemoryBatch::new(vec![json!({"id": 10})])],
        ),
        // The delay pins the interleaving: the streams race into one
        // channel, and `arrivals` must arrive AFTER `events` was
        // written for its ensure to land mid-unit — without it the pin
        // could pass vacuously on an unlucky schedule.
        MemoryStream::new(
            StreamSpec::new("arrivals"),
            vec![MemoryBatch::new(vec![json!({"id": 100})]).with_checkpoint(1)],
        )
        .batch_delay(std::time::Duration::from_millis(500)),
    ]);
    let workdir = tempfile::tempdir().expect("workdir");
    Engine::new(
        EngineConfig::new("sf-owed-2")
            .with_workdir(workdir.path().join("wal"))
            .with_write_mode(WriteMode::Replace),
        source,
        Shell::from_value(doc).expect("valid"),
    )
    .run()
    .await
    .expect("run 2 settles");

    let landed = testhook::rows(
        &config,
        &format!(
            "SELECT \"ID\" AS I FROM \"{}\".\"{}\".\"EVENTS\" ORDER BY \"ID\"",
            config.database.to_uppercase(),
            schema.to_uppercase()
        ),
        &["i"],
    )
    .await
    .expect("read back");
    assert_eq!(
        landed,
        vec![vec!["10".to_string()]],
        "run 2's one row, and none of run 1's"
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

/// The economy, live: after a load created the table, the same ensure
/// against the REAL catalog renders zero statements.
#[tokio::test]
async fn a_repeat_ensure_against_the_live_catalog_emits_nothing() {
    let Some(creds) = credentials() else { return };
    let schema = scratch_schema("eco");
    let doc = config_for(&creds, &schema);
    let config = {
        use rdlt_connector_sdk::config::Document;
        rdlt_connector_snowflake::destination::Config::from_value(doc.clone()).expect("valid")
    };

    let workdir = tempfile::tempdir().expect("workdir");
    Engine::new(
        EngineConfig::new("sf-eco").with_workdir(workdir.path().join("wal")),
        source_of(vec![json!({"id": 1, "note": "a"})]),
        Shell::from_value(doc).expect("valid"),
    )
    .run()
    .await
    .expect("the load settles");

    // Read the real catalog and re-render the ensure against it.
    let columns = testhook::read_catalog(&config, "events")
        .await
        .expect("catalog read");
    assert!(!columns.is_empty(), "the load created the table");
    let mut catalog = testhook::Catalog::default();
    catalog.observe("events", columns.clone());
    let schema_shape = rdlt_connector_sdk::spi::core::schema::TableSchema {
        table: rdlt_connector_sdk::spi::core::id::TableName::from("events"),
        parent: None,
        columns: columns
            .iter()
            .map(|name| rdlt_connector_sdk::spi::core::schema::Column {
                name: name.to_lowercase(),
                column_type: rdlt_connector_sdk::spi::core::schema::ColumnType::scalar(
                    rdlt_connector_sdk::spi::core::types::LogicalType::Utf8,
                ),
                nullable: true,
                provenance: rdlt_connector_sdk::spi::core::schema::Provenance::Inferred,
            })
            .collect(),
    };
    let again = testhook::ensure_table_sql(
        "sf-eco",
        &schema_shape,
        &rdlt_connector_sdk::spi::core::commit::WriteMode::Append,
        rdlt_connector_snowflake::destination::TableType::Permanent,
        None,
        &catalog,
    );
    assert!(again.is_empty(), "steady state, live: {again:?}");

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
