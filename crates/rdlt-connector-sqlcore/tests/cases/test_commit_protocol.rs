//! The commit-protocol pins, lifted verbatim from the planner's former
//! inline module so the planner file can be reshaped under an EXTERNAL net.

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use rdlt_connector::WriteMode;
    use rdlt_connector::core::LogicalType;
    use rdlt_connector::core::schema::ColumnDef;
    use rdlt_connector::core::{TableName, TableSchema, schema::system_columns};
    use rdlt_connector_sqlcore::options::{
        AbsentPolicy, DestinationOptions, MergeStrategy, Scd2Options, TableOptions,
    };
    use rdlt_connector_sqlcore::plan::single_unit_violation;
    use rdlt_connector_sqlcore::protocol::*;

    use crate::cases::common::{self, merge};

    fn col(name: &str) -> ColumnDef {
        common::col(name, LogicalType::Int64)
    }

    /// A keyed structured schema (no `_rdlt_id`): merge goes by declared key.
    fn keyed_schema(table: &str) -> TableSchema {
        TableSchema {
            table: TableName::new(table),
            parent: None,
            columns: vec![col("id"), col("day"), col("name")],
        }
    }

    fn tables(
        entries: Vec<(&str, TableSchema, WriteMode)>,
    ) -> BTreeMap<TableName, (TableSchema, WriteMode)> {
        entries
            .into_iter()
            .map(|(t, s, m)| (TableName::new(t), (s, m)))
            .collect()
    }

    /// No target cleared yet — the state every staged-path pin assumes.
    static NO_CLEARED: BTreeSet<TableName> = BTreeSet::new();

    fn ctx<'a>(
        replayed: bool,
        load_committed_before: bool,
        done: &'a BTreeSet<TableName>,
        staged: &'a BTreeSet<TableName>,
    ) -> CommitContext<'a> {
        CommitContext {
            replayed,
            load_committed_before,
            single_unit_done: done,
            staged_nonempty: staged,
            full_load_publish: FullLoadPublish::Staged,
            cleared_targets: &NO_CLEARED,
        }
    }

    /// The direct-to-target counterpart of [`ctx`].
    fn direct_ctx<'a>(
        replayed: bool,
        load_committed_before: bool,
        done: &'a BTreeSet<TableName>,
        staged: &'a BTreeSet<TableName>,
        cleared: &'a BTreeSet<TableName>,
    ) -> CommitContext<'a> {
        CommitContext {
            full_load_publish: FullLoadPublish::DirectToTarget,
            cleared_targets: cleared,
            ..ctx(replayed, load_committed_before, done, staged)
        }
    }

    fn t(name: &str) -> TableName {
        TableName::new(name)
    }

    fn empty() -> BTreeSet<TableName> {
        BTreeSet::new()
    }

    /// Which tables get a staged-emptiness PROBE, and which get MARKED as
    /// single-unit, are two different conjunctions — and every term in them was
    /// unpinned.
    ///
    /// The probe exists to answer "did this table's full feed arrive in one
    /// unit?", which is only a question for a merge whose semantics read the
    /// stage as the complete truth. Widening either conjunction to a disjunction
    /// makes ordinary Append and plain-merge tables get probed and marked, which
    /// turns a correct multi-unit load into a single-unit VIOLATION and fails a
    /// run that should succeed.
    #[test]
    fn only_full_feed_merges_are_probed() {
        let opts = DestinationOptions {
            tables: [(
                "scoped".to_string(),
                TableOptions {
                    merge_scope: Some(vec!["day".to_string()]),
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        let tbls = tables(vec![
            // Full-feed merge: scoped, so the stage must be the whole truth.
            ("scoped", keyed_schema("scoped"), merge(&["id"])),
            // Merge WITHOUT scope or retire: an ordinary incremental merge.
            ("plain", keyed_schema("plain"), merge(&["id"])),
            // Not a merge at all.
            ("events", keyed_schema("events"), WriteMode::Append),
        ]);

        let targets: Vec<String> = staged_probe_targets(&tbls, &opts)
            .into_iter()
            .map(|t| t.as_str().to_owned())
            .collect();
        assert_eq!(
            targets,
            vec!["scoped".to_string()],
            "only a full-feed merge is probed: an Append table has no stage \
             semantics to check, and a plain merge is legitimately incremental"
        );
    }

    /// A single-unit violation names the rule that FIRED, not merely a rule that
    /// applies.
    ///
    /// A scd2 table with `absent: retire` AND a `merge_scope` satisfies both
    /// conditions, but retirement is the stricter one and it governs — so the
    /// error must NOT report itself as scoped. That flag is what tells an
    /// operator which constraint to relax; naming the wrong one sends them to
    /// change a setting that was never the cause.
    #[test]
    fn a_single_unit_violation_names_the_rule_that_fired() {
        let with = |scope: bool, retire: bool| {
            let opts = DestinationOptions {
                tables: [(
                    "dims".to_string(),
                    TableOptions {
                        merge_strategy: Some(MergeStrategy::Scd2),
                        merge_scope: scope.then(|| vec!["day".to_string()]),
                        scd2: Some(Scd2Options {
                            absent: if retire {
                                AbsentPolicy::Retire
                            } else {
                                AbsentPolicy::Keep
                            },
                            ..Default::default()
                        }),
                        ..Default::default()
                    },
                )]
                .into_iter()
                .collect(),
                ..Default::default()
            };
            let tbls = tables(vec![("dims", keyed_schema("dims"), merge(&["id"]))]);
            // Already marked in an earlier unit AND staging again: a second
            // delivery of what must be a complete feed.
            let done: BTreeSet<TableName> = [t("dims")].into_iter().collect();
            let staged: BTreeSet<TableName> = [t("dims")].into_iter().collect();
            match plan_commit(&tbls, &opts, &ctx(false, false, &done, &staged)) {
                Err(CommitError::SingleUnit { scoped, .. }) => scoped,
                other => panic!("expected a single-unit violation, got {other:?}"),
            }
        };

        assert!(
            !with(true, true),
            "retire governs: a scoped scd2 table that also retires is NOT a scope violation"
        );
        assert!(with(true, false), "scope alone is a scope violation");
        assert!(!with(false, true), "retire alone is not scoped");
    }

    /// The single-unit MARK requires all three of: full-feed semantics, not
    /// already marked in an earlier unit, and something actually staged.
    ///
    /// Each term removes a different false positive. Drop the "already done"
    /// term and a table is re-marked every unit; drop the "staged non-empty"
    /// term and an empty unit marks a table that delivered nothing, which then
    /// reads as its complete feed.
    #[test]
    fn the_single_unit_mark_needs_every_term() {
        let opts = DestinationOptions {
            tables: [(
                "scoped".to_string(),
                TableOptions {
                    merge_scope: Some(vec!["day".to_string()]),
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        let tbls = tables(vec![
            ("scoped", keyed_schema("scoped"), merge(&["id"])),
            ("plain", keyed_schema("plain"), merge(&["id"])),
            ("events", keyed_schema("events"), WriteMode::Append),
        ]);

        // Staged and not yet done: the one table that gets marked.
        let done = empty();
        let staged: BTreeSet<TableName> =
            [t("scoped"), t("plain"), t("events")].into_iter().collect();
        let script = plan_commit(&tbls, &opts, &ctx(false, false, &done, &staged)).unwrap();
        assert_eq!(
            script.marks,
            vec![t("scoped")],
            "only the full-feed table is marked, even though all three staged"
        );

        // Nothing staged for it: nothing to call a complete feed.
        let empty_staged = empty();
        let script = plan_commit(&tbls, &opts, &ctx(false, false, &done, &empty_staged)).unwrap();
        assert!(
            script.marks.is_empty(),
            "an empty unit marks nothing: {:?}",
            script.marks
        );

        // THE REPLAY PATH keeps its own copy of this conjunction, and it is the
        // one that matters most: a redelivered unit must still count toward the
        // single-unit discipline without re-running any merge SQL. Marking a
        // plain incremental merge here would make a later legitimate unit look
        // like a second delivery of a complete feed.
        let script = plan_commit(&tbls, &opts, &ctx(true, true, &done, &staged)).unwrap();
        assert_eq!(
            script.marks,
            vec![t("scoped")],
            "on replay too, only the full-feed table is marked"
        );

        // …and a table already marked in an earlier unit is not marked twice.
        let already: BTreeSet<TableName> = [t("scoped")].into_iter().collect();
        let script = plan_commit(&tbls, &opts, &ctx(true, true, &already, &staged)).unwrap();
        assert!(
            script.marks.is_empty(),
            "an already-counted table is not re-marked: {:?}",
            script.marks
        );
    }

    // ---- Direct-to-target pins (feature 019 US5). ADDITIVE: every staged pin
    // above keeps its exact expected program, because a destination that
    // stages is unaffected by this option existing. ----

    #[test]
    fn pin_direct_append_publishes_nothing() {
        let tbls = tables(vec![("events", keyed_schema("events"), WriteMode::Append)]);
        let opts = DestinationOptions::default();
        let (done, staged, cleared) = (empty(), empty(), empty());
        let script = plan_commit(
            &tbls,
            &opts,
            &direct_ctx(false, false, &done, &staged, &cleared),
        )
        .unwrap();
        // The rows are already in the target and there is no stage to
        // truncate; only the whole-unit state + receipt remain.
        assert_eq!(script.steps, vec![Step::UpsertState, Step::InsertReceipt]);
        assert!(script.marks.is_empty());
    }

    #[test]
    fn pin_direct_replace_clears_at_write_not_at_publish() {
        let tbls = tables(vec![("events", keyed_schema("events"), WriteMode::Replace)]);
        let opts = DestinationOptions::default();
        let (done, staged, cleared) = (empty(), empty(), empty());
        let ctx = direct_ctx(false, false, &done, &staged, &cleared);

        // The clear is planned for the FIRST write of the load…
        assert_eq!(
            prepare_target(&tbls, &ctx, &t("events")),
            vec![Step::ClearTarget { table: t("events") }]
        );
        // …and therefore not for the publish, which carries no data step.
        let script = plan_commit(&tbls, &opts, &ctx).unwrap();
        assert_eq!(script.steps, vec![Step::UpsertState, Step::InsertReceipt]);
    }

    /// The clear guard is per-(load, TARGET), and nothing else suppresses it.
    ///
    /// This pin previously asserted that `load_committed_before` also
    /// suppressed the clear. It does not, and asserting that it did was
    /// pinning a defect: a Replace table registered in unit 1 but first
    /// written in unit 2 found the load already committed, skipped its
    /// TRUNCATE, and appended to the PREVIOUS load's rows. The executor now
    /// seeds `cleared_targets` from a durable per-target record instead.
    #[test]
    fn pin_direct_replace_clears_exactly_once_per_target() {
        let tbls = tables(vec![("events", keyed_schema("events"), WriteMode::Replace)]);
        let (done, staged) = (empty(), empty());
        let cleared_none = empty();
        let cleared_events: BTreeSet<TableName> = [t("events")].into_iter().collect();

        // Known cleared — whether from this session or seeded from the durable
        // record — means no second clear.
        assert!(
            prepare_target(
                &tbls,
                &direct_ctx(false, false, &done, &staged, &cleared_events),
                &t("events")
            )
            .is_empty()
        );
        // An earlier unit of this load having committed says NOTHING about
        // whether THIS target was cleared, so it must not suppress.
        assert_eq!(
            prepare_target(
                &tbls,
                &direct_ctx(false, true, &done, &staged, &cleared_none),
                &t("events")
            ),
            vec![Step::ClearTarget { table: t("events") }],
            "a committed unit elsewhere in the load must not skip this target's clear"
        );
    }

    #[test]
    fn pin_direct_append_never_clears() {
        let tbls = tables(vec![("events", keyed_schema("events"), WriteMode::Append)]);
        let (done, staged, cleared) = (empty(), empty(), empty());
        assert!(
            prepare_target(
                &tbls,
                &direct_ctx(false, false, &done, &staged, &cleared),
                &t("events")
            )
            .is_empty()
        );
    }

    /// Merge is untouched by the option: it stages either way, so its program
    /// and its stage truncation are identical on both paths.
    #[test]
    fn pin_direct_merge_is_identical_to_staged() {
        let tbls = tables(vec![("events", keyed_schema("events"), merge(&["id"]))]);
        let opts = DestinationOptions::default();
        let (done, staged, cleared) = (empty(), empty(), empty());
        let direct = plan_commit(
            &tbls,
            &opts,
            &direct_ctx(false, false, &done, &staged, &cleared),
        )
        .unwrap();
        let stagedd = plan_commit(&tbls, &opts, &ctx(false, false, &done, &staged)).unwrap();
        assert_eq!(direct.steps, stagedd.steps);
        assert!(
            prepare_target(
                &tbls,
                &direct_ctx(false, false, &done, &staged, &cleared),
                &t("events")
            )
            .is_empty(),
            "a merge table has nothing to prepare — it stages"
        );
    }

    /// A mixed session truncates only the stages that exist.
    #[test]
    fn pin_direct_mixed_truncates_only_merge_stages() {
        let tbls = tables(vec![
            ("evt", keyed_schema("evt"), WriteMode::Replace),
            ("usr", keyed_schema("usr"), merge(&["id"])),
        ]);
        let opts = DestinationOptions::default();
        let (done, staged, cleared) = (empty(), empty(), empty());
        let script = plan_commit(
            &tbls,
            &opts,
            &direct_ctx(false, false, &done, &staged, &cleared),
        )
        .unwrap();
        assert_eq!(
            script.steps,
            vec![
                Step::MergeArm {
                    table: t("usr"),
                    arm: MergeArm::KeyedDeleteInsert,
                },
                Step::TruncateStage { table: t("usr") },
                Step::UpsertState,
                Step::InsertReceipt,
            ]
        );
    }

    /// A REPLAYED unit truncates only real stages too.
    #[test]
    fn pin_direct_replay_truncates_only_merge_stages() {
        let tbls = tables(vec![
            ("evt", keyed_schema("evt"), WriteMode::Replace),
            ("usr", keyed_schema("usr"), merge(&["id"])),
        ]);
        let opts = DestinationOptions::default();
        let (done, staged, cleared) = (empty(), empty(), empty());
        let script = plan_commit(
            &tbls,
            &opts,
            &direct_ctx(true, false, &done, &staged, &cleared),
        )
        .unwrap();
        assert_eq!(script.steps, vec![Step::TruncateStage { table: t("usr") }]);
    }

    #[test]
    fn pin_append_only() {
        let tbls = tables(vec![("events", keyed_schema("events"), WriteMode::Append)]);
        let opts = DestinationOptions::default();
        let (done, staged) = (empty(), empty());
        let script = plan_commit(&tbls, &opts, &ctx(false, false, &done, &staged)).unwrap();
        assert_eq!(
            script.steps,
            vec![
                Step::InsertSelect { table: t("events") },
                Step::TruncateStage { table: t("events") },
                Step::UpsertState,
                Step::InsertReceipt,
            ]
        );
        assert!(script.marks.is_empty());
    }

    #[test]
    fn pin_replace_first_and_later_unit() {
        let tbls = tables(vec![("events", keyed_schema("events"), WriteMode::Replace)]);
        let opts = DestinationOptions::default();
        let (done, staged) = (empty(), empty());
        // First unit of the load: clear the target once.
        let first = plan_commit(&tbls, &opts, &ctx(false, false, &done, &staged)).unwrap();
        assert_eq!(
            first.steps,
            vec![
                Step::ClearTarget { table: t("events") },
                Step::InsertSelect { table: t("events") },
                Step::TruncateStage { table: t("events") },
                Step::UpsertState,
                Step::InsertReceipt,
            ]
        );
        // A later unit (load already committed once): NO clear.
        let later = plan_commit(&tbls, &opts, &ctx(false, true, &done, &staged)).unwrap();
        assert_eq!(
            later.steps,
            vec![
                Step::InsertSelect { table: t("events") },
                Step::TruncateStage { table: t("events") },
                Step::UpsertState,
                Step::InsertReceipt,
            ]
        );
    }

    #[test]
    fn pin_merge_upsert_and_scd2_arms() {
        let opts = DestinationOptions {
            merge_strategy: Some(MergeStrategy::Upsert),
            tables: [(
                "dims".to_string(),
                TableOptions {
                    merge_strategy: Some(MergeStrategy::Scd2),
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
        };
        let tbls = tables(vec![
            ("events", keyed_schema("events"), merge(&["id"])),
            ("dims", keyed_schema("dims"), merge(&["id"])),
        ]);
        let (done, staged) = (empty(), empty());
        let script = plan_commit(&tbls, &opts, &ctx(false, false, &done, &staged)).unwrap();
        assert_eq!(
            script.steps,
            vec![
                // BTreeMap order: dims before events.
                Step::MergeArm {
                    table: t("dims"),
                    arm: MergeArm::Scd2(Scd2Options::default()),
                },
                Step::MergeArm {
                    table: t("events"),
                    arm: MergeArm::KeyedUpsert,
                },
                Step::TruncateStage { table: t("dims") },
                Step::TruncateStage { table: t("events") },
                Step::UpsertState,
                Step::InsertReceipt,
            ]
        );
    }

    #[test]
    fn pin_scoped_merge_publishes_and_marks_when_staged() {
        let opts = DestinationOptions {
            tables: [(
                "events".to_string(),
                TableOptions {
                    merge_scope: Some(vec!["day".to_string()]),
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        let tbls = tables(vec![("events", keyed_schema("events"), merge(&["id"]))]);
        let done = empty();
        let staged: BTreeSet<TableName> = [t("events")].into_iter().collect();
        let script = plan_commit(&tbls, &opts, &ctx(false, false, &done, &staged)).unwrap();
        assert_eq!(
            script.steps,
            vec![
                Step::ScopeReplace {
                    table: t("events"),
                    scope: vec!["day".to_string()],
                },
                Step::MergeArm {
                    table: t("events"),
                    arm: MergeArm::KeyedDeleteInsert,
                },
                Step::TruncateStage { table: t("events") },
                Step::UpsertState,
                Step::InsertReceipt,
            ]
        );
        assert_eq!(script.marks, vec![t("events")]);
    }

    #[test]
    fn pin_scoped_merge_skips_when_stage_empty() {
        let opts = DestinationOptions {
            tables: [(
                "events".to_string(),
                TableOptions {
                    merge_scope: Some(vec!["day".to_string()]),
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        let tbls = tables(vec![("events", keyed_schema("events"), merge(&["id"]))]);
        let (done, staged) = (empty(), empty()); // stage empty this unit
        let script = plan_commit(&tbls, &opts, &ctx(false, false, &done, &staged)).unwrap();
        // No publish for the skipped table — just the stage truncate + persist.
        assert_eq!(
            script.steps,
            vec![
                Step::TruncateStage { table: t("events") },
                Step::UpsertState,
                Step::InsertReceipt,
            ]
        );
        assert!(script.marks.is_empty());
    }

    #[test]
    fn pin_single_unit_violation() {
        let opts = DestinationOptions {
            tables: [(
                "events".to_string(),
                TableOptions {
                    merge_scope: Some(vec!["day".to_string()]),
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        let tbls = tables(vec![("events", keyed_schema("events"), merge(&["id"]))]);
        let done: BTreeSet<TableName> = [t("events")].into_iter().collect(); // already published
        let staged: BTreeSet<TableName> = [t("events")].into_iter().collect();
        let err = plan_commit(&tbls, &opts, &ctx(false, false, &done, &staged)).unwrap_err();
        assert_eq!(
            err,
            CommitError::SingleUnit {
                table: "events".to_string(),
                scoped: true,
            }
        );
        // The Display text matches the shared violation message verbatim.
        assert_eq!(err.to_string(), single_unit_violation("events", true));
    }

    #[test]
    fn pin_replayed_unit_truncates_and_recounts() {
        let opts = DestinationOptions {
            tables: [(
                "events".to_string(),
                TableOptions {
                    merge_scope: Some(vec!["day".to_string()]),
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        };
        let tbls = tables(vec![("events", keyed_schema("events"), merge(&["id"]))]);
        let done = empty();
        let staged: BTreeSet<TableName> = [t("events")].into_iter().collect();
        let script = plan_commit(&tbls, &opts, &ctx(true, true, &done, &staged)).unwrap();
        // Replay: only stage truncation, no publish/state/receipt; the
        // redelivered full-feed unit is still counted.
        assert_eq!(
            script.steps,
            vec![Step::TruncateStage { table: t("events") }]
        );
        assert_eq!(script.marks, vec![t("events")]);
    }

    #[test]
    fn pin_shredded_upsert_is_unsupported() {
        let mut schema = keyed_schema("events");
        schema.columns.insert(0, col(system_columns::ID));
        let opts = DestinationOptions {
            merge_strategy: Some(MergeStrategy::Upsert),
            ..Default::default()
        };
        let tbls = tables(vec![("events", schema, merge(&[]))]);
        let (done, staged) = (empty(), empty());
        let err = plan_commit(&tbls, &opts, &ctx(false, false, &done, &staged)).unwrap_err();
        assert_eq!(
            err,
            CommitError::ArmUnsupported {
                table: "events".to_string(),
                what: "upsert on a shredded stream",
            }
        );
    }
}
