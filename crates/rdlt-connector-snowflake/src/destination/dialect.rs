//! The shared merge seam, spelled for this service.
//!
//! Every merge DECISION — arms, survivors, columns — belongs to the
//! shared planner; only spelling lives here. This is the first dialect
//! that cannot ride the seam's defaults, because three things the other
//! SQL destinations lean on simply do not exist:
//!
//! - no `DISTINCT ON` — the survivor query is `QUALIFY ROW_NUMBER()`;
//! - no `ON CONFLICT`, and nothing to arbitrate one anyway (no enforced
//!   uniqueness) — the upsert is `MERGE INTO`;
//! - no transaction-stable clock — the scd2 boundary reads the instant
//!   the unit captured (`$rdlt_tx_ts`, set by the unit module) instead
//!   of calling a clock that moves per statement.

use rdlt_connector_sqlcore::{MergeDialect, Upsert, UpsertAction, names};

use super::ddl::quote;

/// The alias a MERGE gives its incoming rows. `EXCLUDED` is
/// `ON CONFLICT`'s pseudo-table and does not exist here; the shared
/// assignment list is built from whatever the dialect names.
const INCOMING: &str = "SRC";

pub(super) struct Dialect;

impl MergeDialect for Dialect {
    fn quote(&self, ident: &str) -> String {
        quote(ident)
    }

    fn arrival_order(&self) -> String {
        quote(names::ARRIVAL_COL)
    }

    fn tx_timestamp(&self) -> &'static str {
        // The unit's captured instant — NOT CURRENT_TIMESTAMP(), which
        // moves between statements here (see the unit module).
        concat!("$", "rdlt_tx_ts")
    }

    fn clear_table(&self, table: &str) -> String {
        // DELETE, never TRUNCATE: truncation is DDL on this service and
        // would commit the open unit, publishing a cleared table before
        // its replacement rows land.
        format!("DELETE FROM {table}")
    }

    fn dedup_subquery(&self, identity: &str, sort_prefix: &str, stage: &str) -> String {
        // QUALIFY filters after the window function — one pass over the
        // stage, where a DISTINCT ON substitute would need a self-join
        // against a grouped subquery.
        format!(
            "(SELECT * FROM {stage} QUALIFY ROW_NUMBER() OVER \
             (PARTITION BY {identity} ORDER BY {sort_prefix}{} DESC) = 1)",
            self.arrival_order()
        )
    }

    fn flag_set(&self, column: &str) -> String {
        // `IS TRUE` does not compile on this service — a hard error, so
        // the shared default cannot be inherited.
        format!("{column} = TRUE")
    }

    fn flag_unset(&self, column: &str) -> String {
        // NULL-safe where `<> TRUE` is not: a NULL flag means "not
        // deleted", and a predicate that evaluates to NULL would drop
        // the row from the keep set — deleting it.
        format!("{column} IS DISTINCT FROM TRUE")
    }

    fn incoming_ref(&self) -> &'static str {
        INCOMING
    }

    fn upsert_stmt(&self, upsert: &Upsert<'_>) -> String {
        let Upsert {
            target,
            columns,
            cols_sql,
            source,
            keep,
            keys,
            ..
        } = upsert;

        // Key equality with `=`, deliberately not NULL-safe: a NULL
        // merge key is refused long before a row is written, so a NULL
        // reaching this join means that guard failed — and NULL-matching
        // would hide the failure instead of surfacing it.
        let on = keys
            .iter()
            .map(|key| format!("TGT.{key} = {INCOMING}.{key}"))
            .collect::<Vec<_>>()
            .join(" AND ");

        let values = columns
            .iter()
            .map(|column| format!("{INCOMING}.{column}"))
            .collect::<Vec<_>>()
            .join(", ");

        // The MATCHED arm disappears entirely when there is nothing to
        // assign (every column in the key): `UPDATE SET` with an empty
        // list is a syntax error, not a no-op.
        let matched = match &upsert.action {
            UpsertAction::Update { set } => format!(" WHEN MATCHED THEN UPDATE SET {set}"),
            UpsertAction::Nothing => String::new(),
        };

        format!(
            "MERGE INTO {target} TGT USING \
             (SELECT {cols_sql} FROM {source} deduped{keep}) {INCOMING} ON {on}{matched} \
             WHEN NOT MATCHED THEN INSERT ({cols_sql}) VALUES ({values})"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn columns() -> Vec<String> {
        vec![quote("id"), quote("region"), quote("note")]
    }

    fn render(action: UpsertAction<'_>, keys: &[String], columns: &[String]) -> String {
        Dialect.upsert_stmt(&Upsert {
            target: "\"DB\".\"S\".\"EVENTS\"",
            columns,
            cols_sql: "\"ID\", \"REGION\", \"NOTE\"",
            source: "(SELECT * FROM STG)",
            keep: "",
            keys,
            key_list: "\"ID\"",
            action,
        })
    }

    /// The full upsert spelling, pinned end to end.
    #[test]
    fn the_upsert_is_a_merge_because_there_is_no_on_conflict() {
        let keys = vec![quote("id")];
        let cols = columns();
        let sql = render(
            UpsertAction::Update {
                set: "\"NOTE\" = SRC.\"NOTE\"",
            },
            &keys,
            &cols,
        );
        assert_eq!(
            sql,
            "MERGE INTO \"DB\".\"S\".\"EVENTS\" TGT USING \
             (SELECT \"ID\", \"REGION\", \"NOTE\" FROM (SELECT * FROM STG) deduped) SRC \
             ON TGT.\"ID\" = SRC.\"ID\" \
             WHEN MATCHED THEN UPDATE SET \"NOTE\" = SRC.\"NOTE\" \
             WHEN NOT MATCHED THEN INSERT (\"ID\", \"REGION\", \"NOTE\") \
             VALUES (SRC.\"ID\", SRC.\"REGION\", SRC.\"NOTE\")"
        );
    }

    /// Composite keys join on every column.
    #[test]
    fn a_composite_key_matches_on_every_column() {
        let keys = vec![quote("id"), quote("region")];
        let cols = columns();
        let sql = render(UpsertAction::Nothing, &keys, &cols);
        assert!(
            sql.contains("ON TGT.\"ID\" = SRC.\"ID\" AND TGT.\"REGION\" = SRC.\"REGION\""),
            "{sql}"
        );
    }

    /// All-key tables lose the MATCHED arm entirely (empty UPDATE SET is
    /// a syntax error).
    #[test]
    fn nothing_to_update_emits_no_matched_arm_at_all() {
        let keys = columns();
        let cols = columns();
        let sql = render(UpsertAction::Nothing, &keys, &cols);
        assert!(!sql.contains("WHEN MATCHED"), "{sql}");
        assert!(sql.contains("WHEN NOT MATCHED THEN INSERT"), "{sql}");
    }

    /// The hard-delete keep filters the SOURCE subquery, not the target.
    #[test]
    fn the_keep_predicate_rides_the_source_subquery() {
        let keys = vec![quote("id")];
        let cols = columns();
        let sql = Dialect.upsert_stmt(&Upsert {
            target: "\"T\"",
            columns: &cols,
            cols_sql: "\"ID\"",
            source: "(SELECT * FROM STG)",
            keep: " WHERE \"DELETED\" IS NOT TRUE",
            keys: &keys,
            key_list: "\"ID\"",
            action: UpsertAction::Nothing,
        });
        assert!(
            sql.contains("deduped WHERE \"DELETED\" IS NOT TRUE) SRC"),
            "{sql}"
        );
    }

    /// The survivor query: QUALIFY, never DISTINCT ON; arrival is the
    /// tie-break, after any declared sort.
    #[test]
    fn the_survivor_subquery_uses_qualify_with_arrival_as_tie_break() {
        let bare = Dialect.dedup_subquery("\"ID\"", "", "\"STG\"");
        assert_eq!(
            bare,
            "(SELECT * FROM \"STG\" QUALIFY ROW_NUMBER() OVER \
             (PARTITION BY \"ID\" ORDER BY \"__RDLT_ARRIVAL\" DESC) = 1)"
        );
        let sorted = Dialect.dedup_subquery("\"ID\"", "\"V\" DESC NULLS LAST, ", "\"STG\"");
        assert!(
            sorted.contains("ORDER BY \"V\" DESC NULLS LAST, \"__RDLT_ARRIVAL\" DESC"),
            "{sorted}"
        );
    }

    /// The flag spellings: `= TRUE` (IS TRUE does not compile here) and
    /// the NULL-safe unset.
    #[test]
    fn the_flag_predicates_use_the_service_spellings() {
        assert_eq!(Dialect.flag_set("\"D\""), "\"D\" = TRUE");
        assert_eq!(Dialect.flag_unset("\"D\""), "\"D\" IS DISTINCT FROM TRUE");
    }

    /// Clears delete; the boundary reads the captured instant.
    #[test]
    fn clears_delete_and_the_boundary_reads_the_captured_instant() {
        assert_eq!(Dialect.clear_table("\"T\""), "DELETE FROM \"T\"");
        assert_eq!(Dialect.tx_timestamp(), "$rdlt_tx_ts");
        assert_eq!(
            format!("${}", super::super::unit::TX_TIMESTAMP_VAR),
            Dialect.tx_timestamp(),
            "the dialect reads exactly the variable the unit captures"
        );
    }

    /// The identifier policy flows through the seam.
    #[test]
    fn identifiers_and_arrival_follow_the_quoted_upper_policy() {
        assert_eq!(Dialect.quote("events"), "\"EVENTS\"");
        assert_eq!(Dialect.arrival_order(), "\"__RDLT_ARRIVAL\"");
    }
}
