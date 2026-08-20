//! Merge refinements against a live server: ordered survivor selection
//! (dedup_sort), scope-key replacement (merge_scope), the per-table
//! single-unit rule, and the open-time validation matrix.

use rdlt_testkit::memory::Source as MemorySource;
use std::sync::Arc;

use arrow_array::{BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use rdlt_connector_postgres::destination::{
    DedupSort, DestinationOptions, MergeStrategy, Postgres, SortOrder, TableOptions,
};
use rdlt_connector_sdk::spi::{
    core::cursor::Cursor, error::SourceError, source::ReadRequest, source::Source,
    source::StreamSpec, spec::ConnectorSpec,
};
use rdlt_engine::config::Config as EngineConfig;
use rdlt_engine::engine::Engine;

use crate::cases::common;
use rdlt_connector_postgres::fixtures::PostgresContainer;

/// (id, day, seq, name, deleted) — id is the identity key, day the
/// scope column, seq the dedup-sort column.
type Row = (i64, Option<i64>, Option<i64>, &'static str, Option<bool>);

fn batch(rows: &[Row]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("day", DataType::Int64, true),
            Field::new("seq", DataType::Int64, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("deleted", DataType::Boolean, true),
        ])),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.3).collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                rows.iter().map(|row| row.4).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch")
}

/// Pushes each batch with its own checkpoint — under the default
/// commit policy every batch is its OWN COMMIT UNIT (the multi-unit
/// cells depend on this).
struct UnitsSource {
    units: Vec<RecordBatch>,
}

#[async_trait]
impl Source for UnitsSource {
    /// In-memory: the rows are already here, so there is nothing to
    /// reach and nothing that could be misconfigured. Answering Ok is
    /// the honest answer for this double, not a stub — a probe that
    /// passes what the read then fails is what the clause forbids.
    async fn check(&self) -> Result<(), SourceError> {
        Ok(())
    }
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("refinements-test", "0.0.0")
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(vec![
            StreamSpec::new("events")
                .with_structured()
                .with_primary_key(["id"]),
        ])
    }

    async fn read(&self, mut request: ReadRequest) -> Result<(), SourceError> {
        for (index, unit) in self.units.iter().enumerate() {
            let _ = request.out.arrow(unit.clone()).await;
            let _ = request.out.checkpoint(Cursor::new(index as u64 + 1)).await;
        }
        Ok(())
    }
}

/// The knobs one cell turns on, in the spelling a cell reads best in — the
/// full destination options are built from them by [`destination`].
#[derive(Clone, Copy, Default)]
struct Refinements {
    strategy: Option<MergeStrategy>,
    dedup: Option<(&'static str, SortOrder)>,
    merge_scope: Option<&'static [&'static str]>,
    hard_delete: bool,
    scd2_retire: bool,
}

fn destination(
    connection_string: &str,
    schema: &str,
    refinements: Refinements,
) -> rdlt_connector_postgres::destination::Shell {
    Postgres::new(connection_string)
        .schema(schema)
        .options(DestinationOptions {
            merge_strategy: refinements.strategy,
            tables: [(
                "events".to_string(),
                TableOptions {
                    hard_delete: refinements.hard_delete.then(|| "deleted".into()),
                    dedup_sort: refinements.dedup.map(|(column, order)| DedupSort {
                        column: column.into(),
                        order,
                    }),
                    merge_scope: refinements
                        .merge_scope
                        .map(|columns| columns.iter().map(|name| name.to_string()).collect()),
                    scd2: refinements.scd2_retire.then(|| {
                        rdlt_connector_postgres::destination::Scd2Options {
                            absent: rdlt_connector_postgres::destination::AbsentPolicy::Retire,
                            ..rdlt_connector_postgres::destination::Scd2Options::default()
                        }
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

async fn run(
    connection_string: &str,
    schema: &str,
    refinements: Refinements,
    units: Vec<Vec<Row>>,
) {
    let mut config = EngineConfig::new(format!("mr-{schema}"));
    config = config.with_write_mode(rdlt_connector_sdk::spi::core::commit::WriteMode::Merge {
        key: vec!["id".into()],
    });
    let units = units.iter().map(|unit| batch(unit)).collect();
    Engine::new(
        config,
        UnitsSource { units },
        destination(connection_string, schema, refinements),
    )
    .run()
    .await
    .expect("merge run");
}

/// `(id, day, seq, name)` rows of `<schema>.events`, id-ordered.
async fn rows(
    connection_string: &str,
    schema: &str,
) -> Vec<(i64, Option<i64>, Option<i64>, String)> {
    let client = crate::cases::common::connect(connection_string).await;
    client
        .query(
            &format!("SELECT id, day, seq, name FROM \"{schema}\".events ORDER BY id, day, seq"),
            &[],
        )
        .await
        .expect("rows")
        .into_iter()
        .map(|row| (row.get(0), row.get(1), row.get(2), row.get::<_, String>(3)))
        .collect()
}

async fn run_expect_error(
    connection_string: &str,
    schema: &str,
    refinements: Refinements,
    units: Vec<Vec<Row>>,
) -> String {
    let mut config = EngineConfig::new(format!("mr-{schema}"));
    config = config.with_write_mode(rdlt_connector_sdk::spi::core::commit::WriteMode::Merge {
        key: vec!["id".into()],
    });
    let units = units.iter().map(|unit| batch(unit)).collect();
    Engine::new(
        config,
        UnitsSource { units },
        destination(connection_string, schema, refinements),
    )
    .run()
    .await
    .expect_err("run should fail")
    .to_string()
}

// ---- ordered survivor selection (dedup_sort) ----

#[tokio::test(flavor = "multi_thread")]
async fn dedup_sort_orders_survivors_not_arrival() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    // Wrong arrival order: the newest version arrives FIRST.
    let load: Vec<Vec<Row>> = vec![vec![
        (1, None, Some(5), "newest", None),
        (1, None, Some(3), "older", None),
    ]];

    // desc: greatest seq survives, despite arriving first.
    let desc = Refinements {
        dedup: Some(("seq", SortOrder::Desc)),
        ..Refinements::default()
    };
    run(&connection_string, "mr_desc", desc, load.clone()).await;
    assert_eq!(
        rows(&connection_string, "mr_desc").await,
        vec![(1, None, Some(5), "newest".into())]
    );

    // asc: least seq survives.
    let asc = Refinements {
        dedup: Some(("seq", SortOrder::Asc)),
        ..Refinements::default()
    };
    run(&connection_string, "mr_asc", asc, load.clone()).await;
    assert_eq!(
        rows(&connection_string, "mr_asc").await,
        vec![(1, None, Some(3), "older".into())]
    );

    // Absent the option, arrival-order last-wins is UNCHANGED.
    run(
        &connection_string,
        "mr_absent",
        Refinements::default(),
        load.clone(),
    )
    .await;
    assert_eq!(
        rows(&connection_string, "mr_absent").await,
        vec![(1, None, Some(3), "older".into())]
    );

    // The same desc rule under the UPSERT arm — one shared shape.
    let upsert = Refinements {
        strategy: Some(MergeStrategy::Upsert),
        dedup: Some(("seq", SortOrder::Desc)),
        ..Refinements::default()
    };
    run(&connection_string, "mr_upsert", upsert, load).await;
    assert_eq!(
        rows(&connection_string, "mr_upsert").await,
        vec![(1, None, Some(5), "newest".into())]
    );
}

// ---- scope-key replacement (merge_scope) ----

#[tokio::test(flavor = "multi_thread")]
async fn merge_scope_replaces_delivered_scopes_only() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let refinements = Refinements {
        merge_scope: Some(&["day"]),
        ..Refinements::default()
    };
    // Seed two scopes.
    run(
        &connection_string,
        "mr_scope",
        refinements,
        vec![vec![
            (1, Some(1), None, "d1-a", None),
            (2, Some(1), None, "d1-b", None),
            (3, Some(2), None, "d2-a", None),
        ]],
    )
    .await;
    // Re-deliver day 1 WITHOUT id 2, with id 1 updated; day 2 untouched.
    run(
        &connection_string,
        "mr_scope",
        refinements,
        vec![vec![(1, Some(1), None, "d1-a2", None)]],
    )
    .await;
    assert_eq!(
        rows(&connection_string, "mr_scope").await,
        vec![
            (1, Some(1), None, "d1-a2".into()),
            (3, Some(2), None, "d2-a".into()),
        ],
        "undelivered row in the delivered scope is GONE; day 2 intact"
    );

    // Review F8: the scope columns get a supporting index automatically
    // (the scope delete must never seq-scan the target).
    assert_eq!(
        common::scalar::<i64>(
            &connection_string,
            "SELECT count(*) FROM pg_indexes WHERE schemaname = 'mr_scope' \
             AND tablename = 'events' AND indexname LIKE 'rdlt_ix%' \
             AND indexdef LIKE '%(day)%'",
        )
        .await,
        1,
        "merge_scope scope index auto-ensured"
    );

    // An unseen scope simply lands; replay is idempotent.
    let unseen: Vec<Vec<Row>> = vec![vec![(9, Some(9), None, "d9", None)]];
    run(&connection_string, "mr_scope", refinements, unseen.clone()).await;
    run(&connection_string, "mr_scope", refinements, unseen).await;
    assert_eq!(
        rows(&connection_string, "mr_scope").await,
        vec![
            (1, Some(1), None, "d1-a2".into()),
            (3, Some(2), None, "d2-a".into()),
            (9, Some(9), None, "d9".into()),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_row_moving_scopes_lands_once_and_null_scope_rows_still_merge() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let refinements = Refinements {
        merge_scope: Some(&["day"]),
        ..Refinements::default()
    };
    run(
        &connection_string,
        "mr_move",
        refinements,
        vec![vec![
            (1, Some(1), None, "in-d1", None),
            (2, None, None, "no-scope", None),
        ]],
    )
    .await;
    // id 1 MOVES from day 1 to day 2 — held once, in its new scope
    // the NULL-scope row is untouched by scope deletion and
    // still merges by identity.
    run(
        &connection_string,
        "mr_move",
        refinements,
        vec![vec![
            (1, Some(2), None, "in-d2", None),
            (2, None, None, "no-scope-v2", None),
        ]],
    )
    .await;
    assert_eq!(
        rows(&connection_string, "mr_move").await,
        vec![
            (1, Some(2), None, "in-d2".into()),
            (2, None, None, "no-scope-v2".into()),
        ]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_scope_requires_a_single_commit_unit() {
    // The NON-OPTIONAL cell (plan rule; the 008 S6/F2 lesson, sharpened
    // by this feature's own crash sweep): "the batch is the complete
    // truth for its scope" only holds when the scope's truth arrives in
    // ONE commit unit — a crash-resumed load is a NEW load delivering a
    // PARTIAL feed, indistinguishable destination-side from a fresh one.
    // Multi-unit scoped loads are therefore a TYPED error, never silent
    // partial replacement.
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let refinements = Refinements {
        merge_scope: Some(&["day"]),
        ..Refinements::default()
    };
    run(
        &connection_string,
        "mr_units",
        refinements,
        vec![vec![(99, Some(1), None, "stale", None)]],
    )
    .await;
    let error = run_expect_error(
        &connection_string,
        "mr_units",
        refinements,
        vec![
            vec![(1, Some(1), None, "u1-a", None)],
            vec![(2, Some(1), None, "u2-a", None)],
        ],
    )
    .await;
    assert!(
        error.contains("SINGLE commit unit") && error.contains("commit thresholds"),
        "{error}"
    );

    // Recovery: the same feed in one unit converges.
    run(
        &connection_string,
        "mr_units",
        refinements,
        vec![vec![
            (1, Some(1), None, "u1-a", None),
            (2, Some(1), None, "u2-a", None),
        ]],
    )
    .await;
    assert_eq!(
        rows(&connection_string, "mr_units").await,
        vec![
            (1, Some(1), None, "u1-a".into()),
            (2, Some(1), None, "u2-a".into()),
        ],
        "stale row gone exactly once; the full-feed retry converges"
    );

    // A later unit with NOTHING staged for the scoped table is fine —
    // multi-unit pipelines where the scoped table fits unit 1 work.
    run(
        &connection_string,
        "mr_units",
        refinements,
        vec![
            vec![
                (1, Some(1), None, "v2", None),
                (2, Some(1), None, "u2-a", None),
            ],
            vec![],
        ],
    )
    .await;
    assert_eq!(
        rows(&connection_string, "mr_units").await,
        vec![
            (1, Some(1), None, "v2".into()),
            (2, Some(1), None, "u2-a".into()),
        ]
    );
}

// ---- per-table single-unit rule + composition pins ----

#[tokio::test(flavor = "multi_thread")]
async fn a_leading_empty_unit_does_not_reject_a_later_scoped_replace() {
    // Review F2: the single-unit rule is PER TABLE — other streams'
    // checkpoints split the LOAD without splitting this table's feed. A
    // leading empty unit (another stream committed first) must not
    // reject the scoped table.
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let refinements = Refinements {
        merge_scope: Some(&["day"]),
        ..Refinements::default()
    };
    run(
        &connection_string,
        "mr_lead_empty",
        refinements,
        vec![vec![(99, Some(1), None, "stale", None)]],
    )
    .await;
    run(
        &connection_string,
        "mr_lead_empty",
        refinements,
        vec![vec![], vec![(1, Some(1), None, "fresh", None)]],
    )
    .await;
    assert_eq!(
        rows(&connection_string, "mr_lead_empty").await,
        vec![(1, Some(1), None, "fresh".into())],
        "scope replaced from the table's FIRST STAGED unit, wherever it lands"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn scd2_retire_shares_the_per_table_single_unit_rule() {
    // One rule, both consumers. Retire tolerates units where the table
    // stages nothing (an empty stage must not read as "every key absent"
    // = mass retirement), and rejects a split feed typed.
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let refinements = Refinements {
        strategy: Some(MergeStrategy::Scd2),
        scd2_retire: true,
        ..Refinements::default()
    };
    let full: Vec<Vec<Row>> = vec![vec![(1, None, None, "a", None), (2, None, None, "b", None)]];
    run(
        &connection_string,
        "mr_scd2_units",
        refinements,
        full.clone(),
    )
    .await;
    // Trailing empty unit: fine — and it retires NOTHING.
    run(
        &connection_string,
        "mr_scd2_units",
        refinements,
        vec![full[0].clone(), vec![]],
    )
    .await;
    assert_eq!(
        common::scalar::<i64>(
            &connection_string,
            "SELECT count(*) FROM mr_scd2_units.events WHERE _rdlt_valid_to IS NULL",
        )
        .await,
        2,
        "empty unit retired nothing"
    );
    // Split feed: typed, names the single-unit rule.
    let error = run_expect_error(
        &connection_string,
        "mr_scd2_units",
        refinements,
        vec![
            vec![(1, None, None, "a2", None)],
            vec![(2, None, None, "b2", None)],
        ],
    )
    .await;
    assert!(error.contains("SINGLE commit unit"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn dedup_sort_survivor_drives_scd2_change_detection() {
    // The scd2 arm consumes the SAME deduped shape —
    // the ordered survivor decides the active version.
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let refinements = Refinements {
        strategy: Some(MergeStrategy::Scd2),
        dedup: Some(("seq", SortOrder::Desc)),
        ..Refinements::default()
    };
    // Wrong arrival order: the survivor (seq=5) becomes the active row.
    run(
        &connection_string,
        "mr_scd2_dedup",
        refinements,
        vec![vec![
            (1, None, Some(5), "newest", None),
            (1, None, Some(3), "older", None),
        ]],
    )
    .await;
    assert_eq!(
        common::scalar::<String>(
            &connection_string,
            "SELECT name FROM mr_scd2_dedup.events WHERE _rdlt_valid_to IS NULL",
        )
        .await,
        "newest",
        "the ordered survivor is the active version"
    );
    // A later load creates history; the stale-arrival version never
    // polluted it.
    run(
        &connection_string,
        "mr_scd2_dedup",
        refinements,
        vec![vec![(1, None, Some(9), "newer-still", None)]],
    )
    .await;
    assert_eq!(
        common::scalar::<i64>(
            &connection_string,
            "SELECT count(*) FROM mr_scd2_dedup.events"
        )
        .await,
        2,
        "exactly two versions ever existed"
    );
}

// ---- open-time validation matrix ----

#[tokio::test(flavor = "multi_thread")]
async fn refinement_options_validate_typed_at_open() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let one_row: Vec<Vec<Row>> = vec![vec![(1, Some(1), Some(1), "x", None)]];

    // Nonexistent columns: table AND column named, before any data moves.
    let error = run_expect_error(
        &connection_string,
        "mr_bad_dedup",
        Refinements {
            dedup: Some(("nope", SortOrder::Desc)),
            ..Refinements::default()
        },
        one_row.clone(),
    )
    .await;
    assert!(
        error.contains("`nope`") && error.contains("`events`"),
        "{error}"
    );

    let error = run_expect_error(
        &connection_string,
        "mr_bad_scope",
        Refinements {
            merge_scope: Some(&["ghost"]),
            ..Refinements::default()
        },
        one_row.clone(),
    )
    .await;
    assert!(
        error.contains("`ghost`") && error.contains("`events`"),
        "{error}"
    );

    // The hard_delete flag is neither an ordering column nor a scope.
    let error = run_expect_error(
        &connection_string,
        "mr_flag_dedup",
        Refinements {
            dedup: Some(("deleted", SortOrder::Desc)),
            hard_delete: true,
            ..Refinements::default()
        },
        one_row.clone(),
    )
    .await;
    assert!(
        error.contains("hard_delete") && error.contains("`deleted`"),
        "{error}"
    );

    let error = run_expect_error(
        &connection_string,
        "mr_flag_scope",
        Refinements {
            merge_scope: Some(&["deleted"]),
            hard_delete: true,
            ..Refinements::default()
        },
        one_row.clone(),
    )
    .await;
    assert!(error.contains("not a scope"), "{error}");

    // Review F4: a merge-key column is constant per identity group — the
    // ordering could never pick a survivor; silent no-op forbidden.
    let error = run_expect_error(
        &connection_string,
        "mr_key_dedup",
        Refinements {
            dedup: Some(("id", SortOrder::Desc)),
            ..Refinements::default()
        },
        one_row.clone(),
    )
    .await;
    assert!(error.contains("part of the merge key"), "{error}");

    // Review F5: the options under a non-merge write mode are rejected,
    // never silently inert (the 008 F6 lesson).
    let mut config = EngineConfig::new("mr-inert");
    config = config.with_write_mode(rdlt_connector_sdk::spi::core::commit::WriteMode::Append);
    let units = one_row.iter().map(|unit| batch(unit)).collect();
    let error = Engine::new(
        config,
        UnitsSource { units },
        destination(
            &connection_string,
            "mr_inert",
            Refinements {
                merge_scope: Some(&["day"]),
                ..Refinements::default()
            },
        ),
    )
    .run()
    .await
    .expect_err("inert option must be rejected")
    .to_string();
    assert!(error.contains("requires the merge write mode"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn refinement_options_reject_shredded_streams() {
    use serde_json::json;

    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    for (schema, table_options, needle) in [
        (
            "mr_sh_dedup",
            TableOptions {
                dedup_sort: Some(DedupSort {
                    column: "seq".into(),
                    order: SortOrder::Desc,
                }),
                ..TableOptions::default()
            },
            "dedup_sort requires a KEYED structured",
        ),
        (
            "mr_sh_scope",
            TableOptions {
                merge_scope: Some(vec!["day".into()]),
                ..TableOptions::default()
            },
            "merge_scope requires a KEYED structured",
        ),
    ] {
        let destination = Postgres::new(&connection_string)
            .schema(schema)
            .options(DestinationOptions {
                tables: [("users".to_string(), table_options)].into_iter().collect(),
                ..DestinationOptions::default()
            })
            .expect("options")
            .into_shell();
        let mut config = EngineConfig::new(schema);
        config = config.with_write_mode(rdlt_connector_sdk::spi::core::commit::WriteMode::Merge {
            key: vec!["id".into()],
        });
        let source = MemorySource::single_stream(
            rdlt_connector_sdk::spi::source::StreamSpec::new("users").with_primary_key(["id"]),
            vec![json!({"id": 1, "seq": 2, "day": 3})],
        );
        let error = Engine::new(config, source, destination)
            .run()
            .await
            .expect_err("shredded stream must reject the option")
            .to_string();
        assert!(error.contains(needle), "{error}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_scope_composes_with_upsert_hard_delete_and_dedup_sort() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let refinements = Refinements {
        strategy: Some(MergeStrategy::Upsert),
        dedup: Some(("seq", SortOrder::Desc)),
        merge_scope: Some(&["day"]),
        hard_delete: true,
        ..Refinements::default()
    };
    run(
        &connection_string,
        "mr_compose",
        refinements,
        vec![vec![
            (1, Some(1), Some(1), "keep-old", None),
            (2, Some(1), Some(1), "stale", None),
        ]],
    )
    .await;
    // Day 1 re-delivered: id 2 not re-delivered (scope-dies), id 1
    // arrives twice in wrong order (survivor by seq), id 3 arrives
    // flagged (hard-delete wins over insert).
    run(
        &connection_string,
        "mr_compose",
        refinements,
        vec![vec![
            (1, Some(1), Some(9), "newest", None),
            (1, Some(1), Some(5), "older", None),
            (3, Some(1), Some(1), "kill", Some(true)),
        ]],
    )
    .await;
    assert_eq!(
        rows(&connection_string, "mr_compose").await,
        vec![(1, Some(1), Some(9), "newest".into())],
        "scope delete + ordered survivor + hard delete compose"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn dedup_sort_survivor_drives_hard_delete() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let refinements = Refinements {
        dedup: Some(("seq", SortOrder::Desc)),
        hard_delete: true,
        ..Refinements::default()
    };
    // Seed the key, then a load where the NEWEST version is flagged
    // deleted but an OLDER unflagged version arrives after it.
    run(
        &connection_string,
        "mr_flag",
        refinements,
        vec![vec![(1, None, Some(1), "seed", None)]],
    )
    .await;
    run(
        &connection_string,
        "mr_flag",
        refinements,
        vec![vec![
            (1, None, Some(5), "kill", Some(true)),
            (1, None, Some(3), "stale", None),
        ]],
    )
    .await;
    assert_eq!(
        rows(&connection_string, "mr_flag").await,
        vec![],
        "the SURVIVOR's flag decides — the row is gone"
    );

    // Under asc the unflagged older version survives instead.
    let asc = Refinements {
        dedup: Some(("seq", SortOrder::Asc)),
        hard_delete: true,
        ..Refinements::default()
    };
    run(
        &connection_string,
        "mr_flag_asc",
        asc,
        vec![vec![(1, None, Some(1), "seed", None)]],
    )
    .await;
    run(
        &connection_string,
        "mr_flag_asc",
        asc,
        vec![vec![
            (1, None, Some(5), "kill", Some(true)),
            (1, None, Some(3), "stale", None),
        ]],
    )
    .await;
    assert_eq!(
        rows(&connection_string, "mr_flag_asc").await,
        vec![(1, None, Some(3), "stale".into())]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn dedup_sort_null_and_tie_policy_is_deterministic() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let refinements = Refinements {
        dedup: Some(("seq", SortOrder::Desc)),
        ..Refinements::default()
    };
    // NULL loses to a value in EITHER direction.
    run(
        &connection_string,
        "mr_null",
        refinements,
        vec![vec![
            (1, None, None, "null-seq", None),
            (1, None, Some(3), "valued", None),
        ]],
    )
    .await;
    assert_eq!(
        rows(&connection_string, "mr_null").await,
        vec![(1, None, Some(3), "valued".into())]
    );

    // All NULL: deterministic last-wins.
    run(
        &connection_string,
        "mr_all_null",
        refinements,
        vec![vec![
            (1, None, None, "first", None),
            (1, None, None, "last", None),
        ]],
    )
    .await;
    assert_eq!(
        rows(&connection_string, "mr_all_null").await,
        vec![(1, None, None, "last".into())]
    );

    // Tie: arrival breaks it, replay converges to the same survivor
    // — a second identical run moves the state nowhere.
    let tie: Vec<Vec<Row>> = vec![vec![
        (1, None, Some(5), "first", None),
        (1, None, Some(5), "last", None),
    ]];
    run(&connection_string, "mr_tie", refinements, tie.clone()).await;
    assert_eq!(
        rows(&connection_string, "mr_tie").await,
        vec![(1, None, Some(5), "last".into())]
    );
    run(&connection_string, "mr_tie", refinements, tie).await;
    assert_eq!(
        rows(&connection_string, "mr_tie").await,
        vec![(1, None, Some(5), "last".into())]
    );
}
