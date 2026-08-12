//! Postgres destination via testcontainers — the destination conformance
//! verdict plus an end-to-end engine run with flatten lowering and merge.
//! Sibling case files cover native types, merge strategies, merge
//! refinements, the option matrix, and unit-transaction isolation.
//!
//! Requires a container runtime; each test spins up postgres:16-alpine.

use crate::cases::common::Probe;
use async_trait::async_trait;
use rdlt_connector_postgres::destination::Postgres;
use rdlt_connector_postgres::fixtures::PostgresContainer;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::conformance::destination::verify_destination;
use rdlt_testkit::{MemorySource, TableProbe, assert_conformant};
use serde_json::json;

#[tokio::test(flavor = "multi_thread")]
async fn postgres_destination_is_conformant() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let destination = Postgres::new(&connection_string).schema("raw").into_shell();
    let probe = Probe {
        connection_string: connection_string.clone(),
        schema: "raw".into(),
    };
    assert_conformant(
        verify_destination(&destination, &probe)
            .await
            .expecting_no_skips(),
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn flattened_nested_fields_land_and_merge_replaces_child_subtrees() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let destination = Postgres::new(&connection_string).schema("raw").into_shell();

    let source = MemorySource::single_stream(
        rdlt_connector_sdk::spi::StreamSpec::new("users").with_primary_key(["id"]),
        vec![
            json!({"id": 1, "name": "ada", "profile": {"city": "NYC"},
                   "tags": [{"label": "x"}, {"label": "y"}]}),
            json!({"id": 2, "name": "grace", "profile": {"city": "LA"}, "tags": []}),
        ],
    );
    let mut config = EngineConfig::new("pg-e2e");
    config = config.with_write_mode(rdlt_connector_sdk::spi::WriteMode::Merge {
        key: vec!["id".into()],
    });
    let report = Engine::new(config, source, destination.clone())
        .run()
        .await
        .expect("run");
    assert_eq!(report.total_rows(), 4);

    let probe = Probe {
        connection_string: connection_string.clone(),
        schema: "raw".into(),
    };
    assert_eq!(
        probe
            .count(&rdlt_connector_sdk::spi::TableName::new("users"))
            .await
            .expect("probe"),
        2
    );
    assert_eq!(
        probe
            .count(&rdlt_connector_sdk::spi::TableName::new("users__tags"))
            .await
            .expect("probe"),
        2
    );

    // Flatten lowering: nested field arrived as a flat, prefixed column.
    let client = crate::cases::common::connect(&connection_string).await;
    let city: String = client
        .query_one("SELECT profile__city FROM raw.users WHERE id = 1", &[])
        .await
        .expect("flattened column query")
        .get(0);
    assert_eq!(city, "NYC");

    // Merge run 2: user 1 updated with one new tag — subtree replaced.
    let source = MemorySource::single_stream(
        rdlt_connector_sdk::spi::StreamSpec::new("users").with_primary_key(["id"]),
        vec![
            json!({"id": 1, "name": "ada lovelace", "profile": {"city": "London"},
                    "tags": [{"label": "z"}]}),
        ],
    );
    let mut config = EngineConfig::new("pg-e2e");
    config = config.with_write_mode(rdlt_connector_sdk::spi::WriteMode::Merge {
        key: vec!["id".into()],
    });
    Engine::new(config, source, destination.clone())
        .run()
        .await
        .expect("run 2");

    assert_eq!(
        probe
            .count(&rdlt_connector_sdk::spi::TableName::new("users"))
            .await
            .expect("probe"),
        2
    );
    let labels: Vec<String> = client
        .query("SELECT label FROM raw.users__tags ORDER BY label", &[])
        .await
        .expect("labels")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        labels,
        vec!["z"],
        "x/y subtree replaced; grace had no children"
    );
}

/// Keyed STRUCTURED merge (contract merge-structured.md) — no `_rdlt_id`
/// exists, the declared key drives delete+insert, update-heavy runs converge.
#[tokio::test(flavor = "multi_thread")]
async fn keyed_merge_converges_to_one_row_per_key_with_updated_values() {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use rdlt_connector_sdk::spi::{ConnectorSpec, ReadRequest, Source, SourceError, StreamSpec};

    struct KeyedArrowSource {
        batch: RecordBatch,
    }

    #[async_trait]
    impl Source for KeyedArrowSource {
        fn spec(&self) -> ConnectorSpec {
            ConnectorSpec::new("keyed-arrow-test", "0.0.0")
        }

        async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
            Ok(vec![
                StreamSpec::new("metrics")
                    .with_structured()
                    .with_primary_key(["id"]),
            ])
        }

        async fn read(&self, mut request: ReadRequest) -> Result<(), SourceError> {
            let _ = request.out.arrow(self.batch.clone()).await;
            Ok(())
        }
    }

    fn batch(ids: &[i64], names: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .expect("batch")
    }

    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let destination = Postgres::new(&connection_string).schema("raw").into_shell();
    let merge_config = || {
        let mut config = EngineConfig::new("pg-kmerge");
        config = config.with_write_mode(rdlt_connector_sdk::spi::WriteMode::Merge {
            key: vec!["id".into()],
        });
        config
    };

    let source = KeyedArrowSource {
        batch: batch(&[1, 2], &["a", "b"]),
    };
    Engine::new(merge_config(), source, destination.clone())
        .run()
        .await
        .expect("keyed merge run 1");

    // Run 2 updates key 2 and adds key 3.
    let source = KeyedArrowSource {
        batch: batch(&[2, 3], &["b2", "c"]),
    };
    Engine::new(merge_config(), source, destination.clone())
        .run()
        .await
        .expect("keyed merge run 2");

    let client = crate::cases::common::connect(&connection_string).await;
    let count: i64 = client
        .query_one("SELECT count(*) FROM raw.metrics", &[])
        .await
        .expect("count")
        .get(0);
    assert_eq!(count, 3, "one row per key after update-heavy run");
    let name: String = client
        .query_one("SELECT name FROM raw.metrics WHERE id = 2", &[])
        .await
        .expect("updated row")
        .get(0);
    assert_eq!(name, "b2", "merge took the updated value");
}
