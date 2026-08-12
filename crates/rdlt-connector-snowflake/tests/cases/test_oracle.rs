//! The differential oracle: the same keyed feeds through this
//! destination and through postgres, whole rows compared — an upsert
//! that inserted right but updated wrong keeps the key set identical
//! and only a full-row comparison catches it. Runs when BOTH a
//! snowflake account and a container runtime are present.

use rdlt_connector_sdk::spi::core::WriteMode;
use rdlt_connector_snowflake::destination::{Shell, testhook};
use rdlt_engine::{Engine, EngineConfig};
use serde_json::json;
use std::collections::BTreeSet;

use super::common::{KeyedSource, config_for, credentials, scratch_schema};

/// Two loads: inserts, then an update + an insert + (for hard-delete
/// arms) a flagged delete.
fn seeded_loads() -> [Vec<(i64, &'static str, bool)>; 2] {
    [
        vec![(1, "one", false), (2, "two", false), (3, "three", false)],
        vec![
            (1, "one-updated", false),
            (4, "four", false),
            (2, "two", true),
        ],
    ]
}

#[tokio::test(flavor = "multi_thread")]
async fn both_destinations_land_the_same_rows_for_every_strategy() {
    let Some(creds) = credentials() else { return };
    let Some(pg) = rdlt_connector_postgres::fixtures::PostgresContainer::start().await else {
        return;
    };

    for (strategy, hard_delete) in [
        ("upsert", false),
        ("upsert", true),
        ("delete_insert", false),
        ("delete_insert", true),
    ] {
        let label = format!("{strategy}{}", if hard_delete { "-hd" } else { "" });
        let schema = scratch_schema(&label.replace('-', "_"));
        let dataset = format!("oracle_{}", label.replace('-', "_"));

        // The same option document on both sides.
        let table_options = if hard_delete {
            json!({"events": {"hard_delete": "gone"}})
        } else {
            json!({})
        };
        let mut sf_doc = config_for(&creds, &schema);
        sf_doc["merge_strategy"] = json!(strategy);
        sf_doc["tables"] = table_options.clone();
        let sf_config = {
            use rdlt_connector_sdk::config::Document;
            rdlt_connector_snowflake::destination::Config::from_value(sf_doc.clone())
                .expect("valid")
        };
        let pg_doc = json!({
            "conn": pg.connection_string,
            "dataset": dataset,
            "merge_strategy": strategy,
            "tables": table_options,
        });

        for (index, rows) in seeded_loads().into_iter().enumerate() {
            let mode = WriteMode::Merge {
                key: vec!["id".to_owned()],
            };
            let sf_workdir = tempfile::tempdir().expect("workdir");
            Engine::new(
                EngineConfig::new(format!("sf-{label}-{index}"))
                    .with_workdir(sf_workdir.path().join("wal"))
                    .with_write_mode(mode.clone()),
                KeyedSource { rows: rows.clone() },
                Shell::from_value(sf_doc.clone()).expect("valid"),
            )
            .run()
            .await
            .unwrap_or_else(|e| panic!("[{label}] snowflake load {index}: {e:?}"));

            let pg_workdir = tempfile::tempdir().expect("workdir");
            Engine::new(
                EngineConfig::new(format!("pg-{label}-{index}"))
                    .with_workdir(pg_workdir.path().join("wal"))
                    .with_write_mode(mode),
                KeyedSource { rows },
                rdlt_connector_postgres::destination::Shell::from_value(pg_doc.clone())
                    .expect("valid"),
            )
            .run()
            .await
            .unwrap_or_else(|e| panic!("[{label}] postgres load {index}: {e:?}"));
        }

        // Whole rows, rendered comparably on both sides.
        let sf_rows: BTreeSet<String> = testhook::rows(
            &sf_config,
            &format!(
                "SELECT \"ID\" AS I, \"NOTE\" AS N FROM \"{}\".\"{}\".\"EVENTS\" ORDER BY 1",
                sf_config.database.to_uppercase(),
                schema.to_uppercase()
            ),
            &["i", "n"],
        )
        .await
        .expect("snowflake read back")
        .into_iter()
        .map(|row| format!("{}|{}", row[0], row[1]))
        .collect();

        let (pg_client, connection) =
            tokio_postgres::connect(&pg.connection_string, tokio_postgres::NoTls)
                .await
                .expect("pg connect");
        tokio::spawn(connection);
        let pg_rows: BTreeSet<String> = pg_client
            .query(
                &format!("SELECT id, note FROM \"{dataset}\".events ORDER BY 1"),
                &[],
            )
            .await
            .expect("pg read back")
            .into_iter()
            .map(|row| {
                let id: i64 = row.get(0);
                let note: String = row.get(1);
                format!("{id}|{note}")
            })
            .collect();

        assert!(
            !pg_rows.is_empty(),
            "[{label}] the reference landed nothing — empty sets also agree"
        );
        assert_eq!(sf_rows, pg_rows, "[{label}] the destinations disagree");

        let _ = testhook::connect_and_run(
            &sf_config,
            &format!(
                "DROP SCHEMA IF EXISTS \"{}\".\"{}\" CASCADE",
                sf_config.database.to_uppercase(),
                schema.to_uppercase()
            ),
        )
        .await;
    }
}
