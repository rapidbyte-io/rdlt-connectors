//! Destination option edge cases the strategy and refinement suites do not
//! reach: the schema default, cross-mode strategy rejection, and the
//! non-bool hard-delete flag — plus, in the [`schema`] module, the generated
//! JSON Schema for the options document itself.

use rdlt_connector_postgres::destination::{
    DestinationOptions, MergeStrategy, Postgres, TableOptions,
};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::MemorySource;
use serde_json::json;

use rdlt_connector_postgres::fixtures::PostgresContainer;

/// `dataset` default — omitted, tables land in `public` (observed,
/// not inferred; PM3).
#[tokio::test(flavor = "multi_thread")]
async fn default_dataset_is_public() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let destination = Postgres::new(&connection_string).into_shell(); // no .schema(...)
    let source = MemorySource::single_stream(
        rdlt_connector_sdk::spi::StreamSpec::new("things").with_primary_key(["id"]),
        vec![json!({"id": 1, "v": "a"})],
    );
    Engine::new(EngineConfig::new("dflt-ds"), source, destination)
        .run()
        .await
        .expect("run");
    let client = crate::cases::common::connect(&connection_string).await;
    let landed: i64 = client
        .query_one("SELECT count(*) FROM public.things", &[])
        .await
        .expect("public table")
        .get(0);
    assert_eq!(landed, 1, "omitted dataset lands in the `public` schema");
}

/// An EXPLICITLY configured merge_strategy under
/// append/replace is a typed error; the unconfigured default never
/// rejects.
#[tokio::test(flavor = "multi_thread")]
async fn explicit_strategy_under_non_merge_mode_is_typed() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let source = || {
        MemorySource::single_stream(
            rdlt_connector_sdk::spi::StreamSpec::new("things").with_primary_key(["id"]),
            vec![json!({"id": 1, "v": "a"})],
        )
    };
    let run = |mode: rdlt_connector_sdk::spi::WriteMode, options: DestinationOptions| {
        let destination = Postgres::new(&connection_string)
            .schema("r5")
            .options(options)
            .expect("options")
            .into_shell();
        let mut config = EngineConfig::new("r5");
        config = config.with_write_mode(mode);
        Engine::new(config, source(), destination).run()
    };

    // Destination-wide explicit strategy under APPEND: typed.
    let error = run(
        rdlt_connector_sdk::spi::WriteMode::Append,
        DestinationOptions {
            merge_strategy: Some(MergeStrategy::Upsert),
            ..DestinationOptions::default()
        },
    )
    .await
    .expect_err("explicit strategy under append")
    .to_string();
    assert!(error.contains("merge_strategy"), "{error}");
    assert!(error.contains("requires the merge write mode"), "{error}");

    // Per-table explicit strategy under REPLACE: typed too.
    let error = run(
        rdlt_connector_sdk::spi::WriteMode::Replace,
        DestinationOptions {
            tables: [(
                "things".to_string(),
                TableOptions {
                    merge_strategy: Some(MergeStrategy::DeleteInsert),
                    ..TableOptions::default()
                },
            )]
            .into_iter()
            .collect(),
            ..DestinationOptions::default()
        },
    )
    .await
    .expect_err("per-table explicit strategy under replace")
    .to_string();
    assert!(error.contains("`things`"), "{error}");

    // UNCONFIGURED default: append works exactly as before.
    run(
        rdlt_connector_sdk::spi::WriteMode::Append,
        DestinationOptions::default(),
    )
    .await
    .expect("default options never reject append");
}

/// `hard_delete` on a NON-boolean column — M4's other arm: the flag
/// fires on `IS NOT NULL` (any value), keeps on NULL.
#[tokio::test(flavor = "multi_thread")]
async fn non_bool_hard_delete_flag_uses_is_not_null() {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
    use arrow_schema::{DataType, Field, Schema, TimeUnit};
    use async_trait::async_trait;
    use rdlt_connector_sdk::spi::{ConnectorSpec, ReadRequest, Source, SourceError, StreamSpec};

    struct TimestampFlaggedSource {
        batch: RecordBatch,
    }

    #[async_trait]
    impl Source for TimestampFlaggedSource {
        fn spec(&self) -> ConnectorSpec {
            ConnectorSpec::new("ts-flagged", "0.0.0")
        }
        async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
            Ok(vec![
                StreamSpec::new("ev")
                    .with_structured()
                    .with_primary_key(["id"]),
            ])
        }
        async fn read(&self, mut request: ReadRequest) -> Result<(), SourceError> {
            let _ = request.out.arrow(self.batch.clone()).await;
            Ok(())
        }
    }

    fn batch(rows: &[(i64, &str, Option<i64>)]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, true),
                Field::new(
                    "deleted_at",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    true,
                ),
            ])),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|row| row.0).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|row| row.1).collect::<Vec<_>>(),
                )),
                Arc::new(TimestampMicrosecondArray::from(
                    rows.iter().map(|row| row.2).collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("batch")
    }

    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let destination = Postgres::new(&connection_string)
        .schema("nbhd")
        .options(DestinationOptions {
            merge_strategy: Some(MergeStrategy::Upsert),
            tables: [(
                "ev".to_string(),
                TableOptions {
                    hard_delete: Some("deleted_at".into()),
                    ..TableOptions::default()
                },
            )]
            .into_iter()
            .collect(),
        })
        .expect("options")
        .into_shell();
    let run = |rows: &[(i64, &str, Option<i64>)]| {
        let mut config = EngineConfig::new("nbhd");
        config = config.with_write_mode(rdlt_connector_sdk::spi::WriteMode::Merge {
            key: vec!["id".into()],
        });
        Engine::new(
            config,
            TimestampFlaggedSource { batch: batch(rows) },
            destination.clone(),
        )
        .run()
    };
    run(&[(1, "a", None), (2, "b", None)]).await.expect("seed");
    // A deletion TIMESTAMP (non-bool) fires the flag; NULL keeps.
    run(&[(1, "a2", Some(1_700_000_000_000_000)), (2, "b2", None)])
        .await
        .expect("flagged");
    let client = crate::cases::common::connect(&connection_string).await;
    let names: Vec<String> = client
        .query("SELECT name FROM nbhd.ev ORDER BY id", &[])
        .await
        .expect("rows")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(
        names,
        vec!["b2"],
        "non-bool flag: IS NOT NULL deletes, NULL merges normally (M4)"
    );
}

/// `parts` is REFUSED here, not accepted and ignored.
///
/// This destination writes rows into tables — there is no output file
/// for a part size to describe. `deny_unknown_fields` already does the
/// refusing; the pin exists so that removing it, or adding a field
/// that shadows it, fails a test rather than silently making a
/// meaningless setting look effective.
#[test]
fn part_sizing_is_refused_because_there_are_no_files() {
    use rdlt_connector_sdk::config::Document;

    let err = rdlt_connector_postgres::destination::Config::from_value(json!({
        "conn": "host=localhost",
        "parts": {"target_bytes": 134217728},
    }))
    .expect_err("refused")
    .to_string();
    assert!(err.contains("parts"), "the refusal names the field: {err}");
    assert!(err.contains("unknown field"), "{err}");
}

/// The generated JSON Schema for the destination OPTIONS document — the
/// artifact an editor or a config linter consumes, distinct from
/// `destination::config_schema()` which describes the whole destination
/// block. Each cell asks the schema and the parser the same question, so a
/// drift between the two shows up as a disagreement rather than as a
/// document that validates and then fails to load.
mod schema {
    use jsonschema::validator_for;
    use rdlt_connector_postgres::destination::DestinationOptions;
    use serde_json::json;

    fn schema() -> serde_json::Value {
        serde_json::to_value(schemars::schema_for!(DestinationOptions)).expect("schema serializes")
    }

    #[test]
    fn documented_example_validates_and_parses() {
        let validator = validator_for(&schema()).expect("generated schema compiles");
        let example = json!({
            "merge_strategy": "upsert",
            "tables": {
                "customers": {"merge_strategy": "scd2",
                               "scd2": {"absent": "retire",
                                        "valid_from": "_rdlt_valid_from",
                                        "valid_to": "_rdlt_valid_to"}},
                "orders": {"hard_delete": "is_deleted",
                            "dedup_sort": {"column": "seq", "order": "desc"},
                            "merge_scope": ["day", "tenant"]}
            }
        });
        assert!(
            validator.is_valid(&example),
            "example must validate: {:?}",
            validator.iter_errors(&example).next()
        );
        DestinationOptions::from_value(example).expect("schema-valid example parses");
    }

    #[test]
    fn refinement_options_round_trip_the_schema() {
        // Both refinement options in the generated schema; bad
        // `order` tokens and unknown sub-fields fail schema AND parser.
        let validator = validator_for(&schema()).expect("schema compiles");
        for bad in [
            json!({"tables": {"t": {"dedup_sort": {"column": "seq", "order": "downwards"}}}}),
            json!({"tables": {"t": {"dedup_sort": {"column": "seq"}}}}),
            json!({"tables": {"t": {"dedup_sort": {"column": "seq", "order": "desc",
                                                     "nulls": "first"}}}}),
            json!({"tables": {"t": {"merge_scope": "day"}}}),
        ] {
            assert!(!validator.is_valid(&bad), "schema must reject: {bad}");
            assert!(
                DestinationOptions::from_value(bad.clone()).is_err(),
                "parser agrees: {bad}"
            );
        }
    }

    #[test]
    fn unknown_fields_and_contradictions_fail_both_layers() {
        let validator = validator_for(&schema()).expect("schema compiles");
        // Unknown field: schema AND parser agree.
        let bad = json!({"merge_stratgy": "upsert"});
        assert!(!validator.is_valid(&bad));
        assert!(DestinationOptions::from_value(bad).is_err());
        // Unknown strategy value.
        let bad = json!({"merge_strategy": "replace"});
        assert!(!validator.is_valid(&bad));
        assert!(DestinationOptions::from_value(bad).is_err());
        // Schema-valid but semantically contradictory (S8): the VALIDATOR
        // accepts the shape; the parser's validate() names the field.
        let contradiction = json!({
            "tables": {"t": {"merge_strategy": "scd2", "hard_delete": "gone"}}
        });
        assert!(validator.is_valid(&contradiction), "shape is legal");
        let error = DestinationOptions::from_value(contradiction).unwrap_err();
        assert!(error.contains("tables.t.hard_delete"), "{error}");
    }
}
