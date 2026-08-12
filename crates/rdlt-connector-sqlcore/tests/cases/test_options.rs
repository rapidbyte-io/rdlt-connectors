//! The options-validation pins, lifted from the module's former inline tests.

#[cfg(test)]
mod tests {
    use rdlt_connector_sqlcore::options::*;

    /// EITHER validity column being blank is an error, not both.
    ///
    /// These name the columns scd2 writes its validity window into. A blank one
    /// produces DDL naming an empty identifier — and the check reads as a pair,
    /// so it is easy to weaken into "only reject when BOTH are blank", which
    /// lets exactly the half-configured case through.
    #[test]
    fn either_blank_scd2_validity_column_is_refused() {
        let scd2 = |from: &str, to: &str| Scd2Options {
            valid_from: from.to_string(),
            valid_to: to.to_string(),
            ..Default::default()
        };
        let check = |from: &str, to: &str| {
            let opts = TableOptions {
                merge_strategy: Some(MergeStrategy::Scd2),
                scd2: Some(scd2(from, to)),
                ..Default::default()
            };
            let dest = DestinationOptions {
                tables: [("dims".to_string(), opts)].into_iter().collect(),
                ..Default::default()
            };
            dest.validate()
        };

        assert!(
            check("valid_from", "valid_to").is_ok(),
            "both named is fine"
        );
        assert!(
            check("", "valid_to").is_err(),
            "a blank valid_from is refused"
        );
        assert!(
            check("valid_from", "").is_err(),
            "a blank valid_to is refused"
        );
        assert!(check("", "").is_err(), "both blank is refused");
        assert!(check("   ", "valid_to").is_err(), "whitespace is blank");
    }

    #[test]
    fn validation_matrix_names_the_field() {
        // scd2 options without the strategy.
        let bad = DestinationOptions::from_value(serde_json::json!({
            "tables": {"t": {"scd2": {}}}
        }))
        .unwrap_err();
        assert!(
            bad.contains("tables.t.scd2") && bad.contains("merge_strategy"),
            "{bad}"
        );

        // hard_delete + scd2 is a contradiction and must be rejected.
        let bad = DestinationOptions::from_value(serde_json::json!({
            "tables": {"t": {"merge_strategy": "scd2", "hard_delete": "deleted"}}
        }))
        .unwrap_err();
        assert!(
            bad.contains("tables.t.hard_delete") && bad.contains("scd2"),
            "{bad}"
        );

        // Empty hard_delete column.
        let bad = DestinationOptions::from_value(serde_json::json!({
            "tables": {"t": {"hard_delete": "  "}}
        }))
        .unwrap_err();
        assert!(bad.contains("empty column name"), "{bad}");

        // Identical validity columns.
        let bad = DestinationOptions::from_value(serde_json::json!({
            "tables": {"t": {"merge_strategy": "scd2",
                              "scd2": {"valid_from": "v", "valid_to": "v"}}}
        }))
        .unwrap_err();
        assert!(bad.contains("must differ"), "{bad}");

        // Unknown strategy dies at serde; unknown fields likewise.
        assert!(
            DestinationOptions::from_value(serde_json::json!({"merge_strategy": "replace"}))
                .is_err()
        );
        assert!(DestinationOptions::from_value(serde_json::json!({"nope": 1})).is_err());

        // Refinement-option shape matrix at the parse layer.
        let bad = DestinationOptions::from_value(serde_json::json!({
            "tables": {"t": {"dedup_sort": {"column": " ", "order": "desc"}}}
        }))
        .unwrap_err();
        assert!(bad.contains("tables.t.dedup_sort"), "{bad}");
        // order is REQUIRED — no implicit survivor direction.
        assert!(
            DestinationOptions::from_value(serde_json::json!({
                "tables": {"t": {"dedup_sort": {"column": "seq"}}}
            }))
            .is_err()
        );
        for (scope, needle) in [
            (serde_json::json!([]), "empty column list"),
            (serde_json::json!([" "]), "empty column name"),
            (serde_json::json!(["day", "day"]), "listed twice"),
        ] {
            let bad = DestinationOptions::from_value(serde_json::json!({
                "tables": {"t": {"merge_scope": scope}}
            }))
            .unwrap_err();
            assert!(bad.contains(needle), "{bad}");
        }
        // merge_scope + scd2 under KEEP is the inert-option rejection; under
        // RETIRE it is VALID (scoped retirement).
        let bad = DestinationOptions::from_value(serde_json::json!({
            "tables": {"t": {"merge_strategy": "scd2", "merge_scope": ["day"]}}
        }))
        .unwrap_err();
        assert!(
            bad.contains("tables.t.merge_scope") && bad.contains("absent: retire"),
            "{bad}"
        );
        DestinationOptions::from_value(serde_json::json!({
            "tables": {"t": {"merge_strategy": "scd2", "merge_scope": ["day"],
                              "scd2": {"absent": "retire"}}}
        }))
        .expect("scoped retirement is valid");

        // marker/boundary must be real timestamps — injection shapes
        // are typed errors, never SQL.
        for field in ["active_record_timestamp", "boundary_timestamp"] {
            let bad = DestinationOptions::from_value(serde_json::json!({
                "tables": {"t": {"merge_strategy": "scd2",
                                  "scd2": {field: "'); DROP TABLE t; --"}}}
            }))
            .unwrap_err();
            assert!(bad.contains(field) && bad.contains("not a"), "{bad}");
        }
        DestinationOptions::from_value(serde_json::json!({
            "tables": {"t": {"merge_strategy": "scd2",
                              "scd2": {"active_record_timestamp": "9999-12-31T00:00:00Z",
                                       "boundary_timestamp": "2026-07-22T00:00:00Z"}}}
        }))
        .expect("valid marker + boundary literals");
        // Zone-less literals resolve per session TimeZone — rejected.
        for zoneless in ["9999-12-31", "2026-07-22 00:00:00"] {
            let bad = DestinationOptions::from_value(serde_json::json!({
                "tables": {"t": {"merge_strategy": "scd2",
                                  "scd2": {"boundary_timestamp": zoneless}}}
            }))
            .unwrap_err();
            assert!(bad.contains("zone-explicit"), "{bad}");
        }
        // boundary == marker: closed rows would read as active.
        let bad = DestinationOptions::from_value(serde_json::json!({
            "tables": {"t": {"merge_strategy": "scd2",
                              "scd2": {"active_record_timestamp": "9999-12-31T00:00:00Z",
                                       "boundary_timestamp": "9999-12-31T00:00:00Z"}}}
        }))
        .unwrap_err();
        assert!(bad.contains("equals active_record_timestamp"), "{bad}");

        // Valid: destination default upsert + per-table scd2.
        let ok = DestinationOptions::from_value(serde_json::json!({
            "merge_strategy": "upsert",
            "tables": {
                "dims": {"merge_strategy": "scd2", "scd2": {"absent": "retire"}},
                "facts": {"hard_delete": "is_deleted",
                           "dedup_sort": {"column": "seq", "order": "desc"},
                           "merge_scope": ["day", "tenant"]}
            }
        }))
        .expect("valid options");
        assert_eq!(ok.strategy_for("dims"), MergeStrategy::Scd2);
        assert_eq!(ok.strategy_for("facts"), MergeStrategy::Upsert);
        assert_eq!(ok.strategy_for("other"), MergeStrategy::Upsert);
        assert_eq!(ok.hard_delete_for("facts"), Some("is_deleted"));
        assert_eq!(ok.scd2_for("dims").absent, AbsentPolicy::Retire);
        let dedup = ok.dedup_sort_for("facts").expect("dedup_sort");
        assert_eq!(
            (dedup.column.as_str(), dedup.order),
            ("seq", SortOrder::Desc)
        );
        assert_eq!(
            ok.merge_scope_for("facts"),
            Some(&["day".to_string(), "tenant".to_string()][..])
        );
        assert_eq!(ok.dedup_sort_for("other"), None);
    }
}
