//! scd2 against a live server (contract scd2.md): version history, point-in-time
//! correctness, absence policies, redelivery stability, rejections.

use rdlt_testkit::memory::Source as MemorySource;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use rdlt_connector_postgres::destination::{
    AbsentPolicy, DestinationOptions, MergeStrategy, Postgres, Scd2Options, TableOptions,
};
use rdlt_connector_postgres::fixtures::PostgresContainer;
use rdlt_connector_sdk::spi::core::{id::LoadId, id::PipelineId};
use rdlt_connector_sdk::spi::{
    core::cursor::Cursor, destination::Destination as _, destination::OpenContext,
    error::SourceError, source::ReadRequest, source::Source, source::StreamSpec,
    spec::ConnectorSpec,
};
use rdlt_engine::config::Config as EngineConfig;
use rdlt_engine::engine::Engine;

struct DimSource {
    batch: RecordBatch,
}

#[async_trait]
impl Source for DimSource {
    /// In-memory: the rows are already here, so there is nothing to
    /// reach and nothing that could be misconfigured. Answering Ok is
    /// the honest answer for this double, not a stub — a probe that
    /// passes what the read then fails is what the clause forbids.
    async fn check(&self) -> Result<(), SourceError> {
        Ok(())
    }
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("dim-test", "0.0.0")
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(vec![
            StreamSpec::new("dims")
                .with_structured()
                .with_primary_key(["id"]),
        ])
    }

    async fn read(&self, mut request: ReadRequest) -> Result<(), SourceError> {
        let _ = request.out.arrow(self.batch.clone()).await;
        let _ = request.out.checkpoint(Cursor::new(1u64)).await;
        Ok(())
    }
}

fn batch(rows: &[(i64, &str)]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch")
}

fn scd2_destination(
    connection_string: &str,
    schema: &str,
    absent: AbsentPolicy,
) -> rdlt_connector_postgres::destination::Shell {
    Postgres::new(connection_string)
        .schema(schema)
        .options(DestinationOptions {
            merge_strategy: Some(MergeStrategy::DeleteInsert),
            tables: [(
                "dims".to_string(),
                TableOptions {
                    merge_strategy: Some(MergeStrategy::Scd2),
                    scd2: Some(Scd2Options {
                        absent,
                        ..Scd2Options::default()
                    }),
                    ..TableOptions::default()
                },
            )]
            .into_iter()
            .collect(),
        })
        .expect("valid options")
        .into_shell()
}

async fn run(destination: rdlt_connector_postgres::destination::Shell, rows: &[(i64, &str)]) {
    let mut config = EngineConfig::new("scd2");
    config = config.with_write_mode(rdlt_connector_sdk::spi::core::commit::WriteMode::Merge {
        key: vec!["id".into()],
    });
    Engine::new(config, DimSource { batch: batch(rows) }, destination)
        .run()
        .await
        .expect("scd2 run");
}

/// The two-column dimension schema the session-level cells ensure by hand,
/// where no engine is in the loop to infer it.
fn dims_schema() -> rdlt_connector_sdk::spi::core::schema::TableSchema {
    rdlt_connector_sdk::spi::core::schema::TableSchema {
        table: rdlt_connector_sdk::spi::core::id::TableName::new("dims"),
        parent: None,
        columns: vec![
            rdlt_connector_sdk::spi::core::schema::Column {
                name: "id".into(),
                column_type: rdlt_connector_sdk::spi::core::schema::ColumnType::scalar(
                    rdlt_connector_sdk::spi::core::types::LogicalType::Int64,
                ),
                nullable: false,
                provenance: rdlt_connector_sdk::spi::core::schema::Provenance::Hinted,
            },
            rdlt_connector_sdk::spi::core::schema::Column {
                name: "name".into(),
                column_type: rdlt_connector_sdk::spi::core::schema::ColumnType::scalar(
                    rdlt_connector_sdk::spi::core::types::LogicalType::Utf8,
                ),
                nullable: true,
                provenance: rdlt_connector_sdk::spi::core::schema::Provenance::Hinted,
            },
        ],
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn three_rounds_produce_correct_history_and_point_in_time() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let client = crate::cases::common::connect(&connection_string).await;
    let now = || async {
        let instant: chrono::DateTime<chrono::Utc> = client
            .query_one("SELECT now()", &[])
            .await
            .expect("now")
            .get(0);
        instant
    };

    // Round 1: both keys active.
    run(
        scd2_destination(&connection_string, "hist", AbsentPolicy::Keep),
        &[(1, "a"), (2, "b")],
    )
    .await;
    let after_first_round = now().await;
    // Round 2 (S3): key 1 CHANGED (retire + new version); key 2 UNCHANGED
    // (no churn version).
    run(
        scd2_destination(&connection_string, "hist", AbsentPolicy::Keep),
        &[(1, "a2"), (2, "b")],
    )
    .await;
    // Round 3 (S6 keep): key 2 absent — keeps its active version.
    run(
        scd2_destination(&connection_string, "hist", AbsentPolicy::Keep),
        &[(1, "a3")],
    )
    .await;

    // Version counts: key 1 has 3, key 2 has exactly 1 (S3 skip-unchanged).
    let counts: Vec<(i64, i64)> = client
        .query(
            "SELECT id, count(*) FROM hist.dims GROUP BY id ORDER BY id",
            &[],
        )
        .await
        .expect("counts")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(counts, vec![(1, 3), (2, 1)], "S3: versions only on change");

    // S7: exactly one active per key.
    let active: Vec<(i64, String)> = client
        .query(
            "SELECT id, name FROM hist.dims WHERE _rdlt_valid_to IS NULL ORDER BY id",
            &[],
        )
        .await
        .expect("active")
        .into_iter()
        .map(|row| (row.get(0), row.get(1)))
        .collect();
    assert_eq!(
        active,
        vec![(1, "a3".to_string()), (2, "b".to_string())],
        "one active version per key; absent key kept"
    );

    // S7: ranges non-overlapping AND contiguous per key.
    let overlaps: i64 = client
        .query_one(
            "SELECT count(*) FROM hist.dims x JOIN hist.dims y
               ON x.id = y.id AND x._rdlt_valid_from < y._rdlt_valid_from
              AND COALESCE(x._rdlt_valid_to, 'infinity') > y._rdlt_valid_from",
            &[],
        )
        .await
        .expect("overlap check")
        .get(0);
    assert_eq!(overlaps, 0, "S7: no overlapping validity ranges");
    let gaps: i64 = client
        .query_one(
            "SELECT count(*) FROM hist.dims x
             WHERE x._rdlt_valid_to IS NOT NULL
               AND NOT EXISTS (
                 SELECT 1 FROM hist.dims y
                 WHERE y.id = x.id AND y._rdlt_valid_from = x._rdlt_valid_to)",
            &[],
        )
        .await
        .expect("gap check")
        .get(0);
    assert_eq!(gaps, 0, "S7: retirement boundaries are contiguous");

    // Point-in-time between rounds 1 and 2: key 1 was still 'a'.
    let as_of: String = client
        .query_one(
            "SELECT name FROM hist.dims
             WHERE id = 1 AND _rdlt_valid_from <= $1
               AND COALESCE(_rdlt_valid_to, 'infinity') > $1",
            &[&after_first_round],
        )
        .await
        .expect("as-of")
        .get(0);
    assert_eq!(as_of, "a", "S7: point-in-time answers from history");
}

#[tokio::test(flavor = "multi_thread")]
async fn absent_retire_closes_missing_keys() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    run(
        scd2_destination(&connection_string, "ret", AbsentPolicy::Retire),
        &[(1, "a"), (2, "b")],
    )
    .await;
    run(
        scd2_destination(&connection_string, "ret", AbsentPolicy::Retire),
        &[(1, "a2")],
    )
    .await;

    let client = crate::cases::common::connect(&connection_string).await;
    let active: Vec<i64> = client
        .query(
            "SELECT id FROM ret.dims WHERE _rdlt_valid_to IS NULL ORDER BY id",
            &[],
        )
        .await
        .expect("active")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(active, vec![1], "S6 retire: absent key 2 closed");
    let second_key: i64 = client
        .query_one(
            "SELECT count(*) FROM ret.dims WHERE id = 2 AND _rdlt_valid_to IS NOT NULL",
            &[],
        )
        .await
        .expect("key2")
        .get(0);
    assert_eq!(second_key, 1, "key 2's version retired, not deleted");
}

#[tokio::test(flavor = "multi_thread")]
async fn redelivery_adds_zero_versions() {
    use rdlt_connector_sdk::spi::core::{commit::Counters as CommitCounters, state::StateDoc};
    use rdlt_connector_sdk::spi::{core::commit::CommitMeta, core::commit::WriteMode};

    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let destination = scd2_destination(&connection_string, "redel", AbsentPolicy::Keep);
    let pipeline = PipelineId::new("redel");
    let mut session = destination
        .open(OpenContext::new(pipeline.clone(), LoadId::new("rd-load")))
        .await
        .expect("open");
    let table_schema = dims_schema();
    let mode = WriteMode::Merge {
        key: vec!["id".into()],
    };
    session
        .ensure_table(&table_schema, &mode)
        .await
        .expect("ensure");
    session
        .write(&table_schema.table, batch(&[(1, "a")]))
        .await
        .expect("write");
    let meta = CommitMeta {
        load_id: LoadId::new("rd-load"),
        commit_seq: 0,
        state: StateDoc::new(pipeline.clone(), env!("CARGO_PKG_VERSION")),
        counters: CommitCounters::default(),
    };
    session.commit(meta.clone()).await.expect("commit 1");
    // S5: the SAME (load_id, commit_seq) redelivered — receipts short-circuit,
    // zero new versions even though the stage was re-written.
    session
        .write(&table_schema.table, batch(&[(1, "a")]))
        .await
        .expect("re-write (redelivery)");
    session.commit(meta).await.expect("redelivered commit");

    let client = crate::cases::common::connect(&connection_string).await;
    let versions: i64 = client
        .query_one("SELECT count(*) FROM redel.dims", &[])
        .await
        .expect("versions")
        .get(0);
    assert_eq!(versions, 1, "S5: redelivery minted no versions");
}

#[tokio::test(flavor = "multi_thread")]
async fn rejections_are_typed_at_ensure() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();

    // Validity-name collision with a stream column (S1).
    let destination = Postgres::new(&connection_string)
        .schema("bad")
        .options(DestinationOptions {
            tables: [(
                "dims".to_string(),
                TableOptions {
                    merge_strategy: Some(MergeStrategy::Scd2),
                    scd2: Some(Scd2Options {
                        valid_from: "name".into(), // collides
                        ..Scd2Options::default()
                    }),
                    ..TableOptions::default()
                },
            )]
            .into_iter()
            .collect(),
            ..DestinationOptions::default()
        })
        .expect("options parse")
        .into_shell();
    let mut config = EngineConfig::new("bad");
    config = config.with_write_mode(rdlt_connector_sdk::spi::core::commit::WriteMode::Merge {
        key: vec!["id".into()],
    });
    let error = Engine::new(
        config,
        DimSource {
            batch: batch(&[(1, "a")]),
        },
        destination,
    )
    .run()
    .await
    .expect_err("collision must be rejected");
    let message = error.to_string();
    assert!(
        message.contains("name") && message.contains("collides"),
        "{message}"
    );

    // scd2 on a SHREDDED stream (S1): typed at ensure.
    use serde_json::json;
    let destination = Postgres::new(&connection_string)
        .schema("badsh")
        .options(DestinationOptions {
            merge_strategy: Some(MergeStrategy::Scd2),
            ..DestinationOptions::default()
        })
        .expect("options parse")
        .into_shell();
    let mut config = EngineConfig::new("badsh");
    config = config.with_write_mode(rdlt_connector_sdk::spi::core::commit::WriteMode::Merge {
        key: vec!["id".into()],
    });
    let source = MemorySource::single_stream(
        StreamSpec::new("users").with_primary_key(["id"]),
        vec![json!({"id": 1})],
    );
    let error = Engine::new(config, source, destination)
        .run()
        .await
        .expect_err("scd2 on shredded must be rejected");
    let message = error.to_string();
    assert!(message.contains("KEYED structured"), "{message}");
}

/// Review F2 (contract S6 as amended): `absent: retire` compares against ONE
/// commit unit's stage — a load split across units would mass-retire earlier
/// units' keys, so a second unit under retire fails typed instead.
#[tokio::test(flavor = "multi_thread")]
async fn absent_retire_rejects_multi_unit_loads() {
    use rdlt_connector_sdk::spi::core::{commit::Counters as CommitCounters, state::StateDoc};
    use rdlt_connector_sdk::spi::{core::commit::CommitMeta, core::commit::WriteMode};

    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let destination = scd2_destination(&connection_string, "multiunit", AbsentPolicy::Retire);
    let pipeline = PipelineId::new("multiunit");
    let mut session = destination
        .open(OpenContext::new(pipeline.clone(), LoadId::new("mu-load")))
        .await
        .expect("open");
    let table_schema = dims_schema();
    let mode = WriteMode::Merge {
        key: vec!["id".into()],
    };
    let meta = |commit_seq: u64| CommitMeta {
        load_id: LoadId::new("mu-load"),
        commit_seq,
        state: StateDoc::new(pipeline.clone(), env!("CARGO_PKG_VERSION")),
        counters: CommitCounters::default(),
    };
    session
        .ensure_table(&table_schema, &mode)
        .await
        .expect("ensure");
    session
        .write(&table_schema.table, batch(&[(1, "a")]))
        .await
        .expect("write unit 0");
    session.commit(meta(0)).await.expect("first unit commits");
    // Unit 1 of the SAME load: must fail typed, not corrupt history.
    session
        .write(&table_schema.table, batch(&[(2, "b")]))
        .await
        .expect("write unit 1");
    let error = session
        .commit(meta(1))
        .await
        .expect_err("second unit under absent: retire must be rejected");
    let message = error.to_string();
    assert!(
        message.contains("SINGLE commit unit") && message.contains("commit thresholds"),
        "{message}"
    );
}

/// CUSTOM validity column names flow end to end —
/// the configured names appear on the target and carry the history.
#[tokio::test(flavor = "multi_thread")]
async fn custom_validity_column_names_flow_end_to_end() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let destination = Postgres::new(&connection_string)
        .schema("scd2c")
        .options(DestinationOptions {
            merge_strategy: Some(MergeStrategy::Scd2),
            tables: [(
                "dims".to_string(),
                TableOptions {
                    merge_strategy: Some(MergeStrategy::Scd2),
                    scd2: Some(Scd2Options {
                        valid_from: "row_since".into(),
                        valid_to: "row_until".into(),
                        ..Scd2Options::default()
                    }),
                    ..TableOptions::default()
                },
            )]
            .into_iter()
            .collect(),
        })
        .expect("options")
        .into_shell();
    run(destination.clone(), &[(1, "v1")]).await;
    run(destination, &[(1, "v2")]).await;

    let client = crate::cases::common::connect(&connection_string).await;
    let versions: i64 = client
        .query_one("SELECT count(*) FROM scd2c.dims", &[])
        .await
        .expect("count")
        .get(0);
    let active: String = client
        .query_one("SELECT name FROM scd2c.dims WHERE row_until IS NULL", &[])
        .await
        .expect("one active row via the CUSTOM column")
        .get(0);
    let retired: i64 = client
        .query_one(
            "SELECT count(*) FROM scd2c.dims WHERE row_until IS NOT NULL AND row_since IS NOT NULL",
            &[],
        )
        .await
        .expect("retired")
        .get(0);
    assert_eq!((versions, active.as_str(), retired), (2, "v2", 1));
}
