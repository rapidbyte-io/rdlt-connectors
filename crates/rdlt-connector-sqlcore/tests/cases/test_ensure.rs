//! The ensure-step pins, lifted from the module's former inline tests.

#[cfg(test)]
mod tests {
    use rdlt_connector::core::commit::WriteMode;
    use rdlt_connector::core::schema::TableSchema;
    use rdlt_connector::core::{id::TableName, schema::Column, types::LogicalType};

    use crate::cases::common::{col, merge};
    use rdlt_connector_sqlcore::ensure::*;
    use rdlt_connector_sqlcore::options::DestinationOptions;
    use rdlt_connector_sqlcore::options::MergeStrategy;
    use rdlt_connector_sqlcore::protocol::FullLoadPublish;

    fn schema(columns: Vec<Column>) -> TableSchema {
        TableSchema {
            table: TableName::from("events"),
            parent: None,
            columns,
        }
    }

    #[test]
    fn append_on_a_direct_destination_plans_one_leg() {
        let plan = schema_steps(
            &schema(vec![col("id", LogicalType::Int64)]),
            &WriteMode::Append,
            FullLoadPublish::DirectToTarget,
            None,
        );
        assert_eq!(
            plan,
            vec![
                EnsureStep::Table { leg: Leg::Target },
                EnsureStep::Column {
                    leg: Leg::Target,
                    column: 0
                },
            ]
        );
    }

    #[test]
    fn append_on_a_staged_destination_plans_both_legs() {
        // The same mode, a different publish path — and the stage leg appears.
        // This is the rule the commit planner also consults; they must agree.
        let plan = schema_steps(
            &schema(vec![col("id", LogicalType::Int64)]),
            &WriteMode::Append,
            FullLoadPublish::Staged,
            None,
        );
        assert_eq!(
            plan.iter()
                .filter(|s| matches!(s, EnsureStep::Table { .. }))
                .count(),
            2
        );
    }

    #[test]
    fn merge_always_stages_whatever_the_publish_path() {
        for publish in [FullLoadPublish::DirectToTarget, FullLoadPublish::Staged] {
            assert!(
                uses_stage(&merge(&["id"]), publish),
                "merge stages under {publish:?}"
            );
        }
    }

    #[test]
    fn a_changed_type_plans_a_widen_directly_after_its_column() {
        let before = schema(vec![col("id", LogicalType::Int64)]);
        let after = schema(vec![col("id", LogicalType::Utf8)]);
        let plan = schema_steps(
            &after,
            &WriteMode::Append,
            FullLoadPublish::DirectToTarget,
            Some(&before),
        );
        assert_eq!(
            plan,
            vec![
                EnsureStep::Table { leg: Leg::Target },
                EnsureStep::Column {
                    leg: Leg::Target,
                    column: 0
                },
                EnsureStep::Widen {
                    leg: Leg::Target,
                    column: 0
                },
            ]
        );
    }

    #[test]
    fn an_unchanged_type_plans_no_widen() {
        let same = schema(vec![col("id", LogicalType::Int64)]);
        let plan = schema_steps(
            &same,
            &WriteMode::Append,
            FullLoadPublish::DirectToTarget,
            Some(&same),
        );
        assert!(!plan.iter().any(|s| matches!(s, EnsureStep::Widen { .. })));
    }

    #[test]
    fn scd2_plans_both_validity_columns_before_any_index() {
        let options = DestinationOptions {
            merge_strategy: Some(MergeStrategy::Scd2),
            ..DestinationOptions::default()
        };
        let plan = merge_steps(
            &options,
            &schema(vec![col("id", LogicalType::Int64)]),
            &merge(&["id"]),
        )
        .expect("valid options");
        let from = plan
            .iter()
            .position(|s| matches!(s, EnsureStep::Validity(Validity::From)))
            .expect("valid_from");
        let to = plan
            .iter()
            .position(|s| matches!(s, EnsureStep::Validity(Validity::To)))
            .expect("valid_to");
        let first_index = plan
            .iter()
            .position(|s| matches!(s, EnsureStep::Index(_)))
            .unwrap_or(usize::MAX);
        assert!(from < to && to < first_index, "{plan:?}");
    }

    /// 031 review N1: a merge key naming a column the schema does not
    /// carry is refused AT VALIDATION with one shared wording — before
    /// this check, a ghost key sailed through and surfaced as whatever
    /// statement first bound it, destination by destination.
    #[test]
    fn a_merge_key_column_absent_from_the_schema_is_refused() {
        let err = merge_steps(
            &DestinationOptions::default(),
            &schema(vec![col("id", LogicalType::Int64)]),
            &merge(&["ghost"]),
        )
        .expect_err("a ghost merge-key column is a config error");
        assert_eq!(
            err.to_string(),
            "merge key column `ghost` is not a column of table `events`"
        );
    }

    #[test]
    fn a_non_merge_mode_plans_nothing_but_still_validates() {
        let plan = merge_steps(
            &DestinationOptions::default(),
            &schema(vec![col("id", LogicalType::Int64)]),
            &WriteMode::Append,
        )
        .expect("default options are valid");
        assert!(plan.is_empty());

        let refused = DestinationOptions {
            merge_strategy: Some(MergeStrategy::Upsert),
            ..DestinationOptions::default()
        };
        merge_steps(
            &refused,
            &schema(vec![col("id", LogicalType::Int64)]),
            &WriteMode::Append,
        )
        .expect_err("a merge strategy under Append is a config error");
    }
}
