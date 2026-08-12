//! Golden-SQL pins (contract shared-merge-core.md).
//!
//! These tests bind the EXACT statement text the postgres destination
//! generates for a representative plan matrix. They were captured against the
//! pre-extraction code and MUST NOT change across the rdlt-connector-sqlcore
//! extraction — if a pin has to change, that is a behavioral edit, not an
//! extraction, and the extraction stops (tasks.md implementation strategy).
//!
//! No database: the builders are pure. Behavior (what the SQL does) is pinned
//! by the 006/008/010 conformance + sweep suites; THIS suite pins the text.

use rdlt_connector_postgres::destination::{AbsentPolicy, DedupSort, Scd2Options, SortOrder};
use rdlt_connector_postgres::testsupport::destination::Dialect;
use rdlt_connector_sdk::spi::core::schema::ColumnDef;
use rdlt_connector_sdk::spi::core::{ColumnType, LogicalType, Provenance, TableName, TableSchema};
use rdlt_connector_sqlcore::plan::{
    identity_delete_insert_sql, keyed_delete_insert_sql, keyed_upsert_sql, scd2_merge_sql,
    scope_replace_sql,
};
use rdlt_connector_sqlcore::{HardDelete, MergePlan};

fn column(name: &str, scalar: LogicalType) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        column_type: ColumnType::Scalar { scalar },
        nullable: true,
        provenance: Provenance::Inferred,
    }
}

/// A keyed structured table: id/day keys + data columns + a bool flag.
fn keyed_schema() -> TableSchema {
    TableSchema {
        table: TableName::from("events"),
        parent: None,
        columns: vec![
            column("id", LogicalType::Int64),
            column("day", LogicalType::Int64),
            column("name", LogicalType::Utf8),
            column("seq", LogicalType::Int64),
            column("deleted", LogicalType::Bool),
            column("_rdlt_load_id", LogicalType::Utf8),
        ],
    }
}

fn plan<'a>(
    table_schema: &'a TableSchema,
    key: &'a [String],
    hard_delete: Option<&str>,
    dedup: Option<&'a DedupSort>,
) -> MergePlan<'a> {
    MergePlan {
        dialect: &Dialect,
        target_sql: "\"events\"",
        stage_sql: "\"_rdlt_stage_feedcafe\"",
        cols_sql: "\"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\"",
        schema: table_schema,
        key,
        root_stage_sql: "\"_rdlt_stage_feedcafe\"".into(),
        is_child: false,
        hard_delete: hard_delete.map(|flag| HardDelete::new(flag, table_schema, &Dialect)),
        dedup_sort: dedup,
        merge_scope: None,
    }
}

#[test]
fn pin_scope_replace() {
    let sql = scope_replace_sql(
        &Dialect,
        "\"events\"",
        "\"_rdlt_stage_feedcafe\"",
        &["day".into(), "tenant".into()],
    );
    assert_eq!(
        sql,
        "DELETE FROM \"events\" WHERE (\"day\", \"tenant\") IN (\n             SELECT \"day\", \"tenant\" FROM \"_rdlt_stage_feedcafe\" WHERE \"day\" IS NOT NULL AND \"tenant\" IS NOT NULL)"
    );
}

#[test]
fn pin_keyed_delete_insert_plain() {
    let table_schema = keyed_schema();
    let key = vec!["id".to_string()];
    let statements = keyed_delete_insert_sql(&plan(&table_schema, &key, None, None));
    assert_eq!(statements, vec!["DELETE FROM \"events\" WHERE (\"id\") IN (SELECT \"id\" FROM \"_rdlt_stage_feedcafe\")".to_string(), "INSERT INTO \"events\" (\"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\") SELECT \"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\" FROM (SELECT DISTINCT ON (\"id\") * FROM \"_rdlt_stage_feedcafe\" ORDER BY \"id\", \"__rdlt_arrival\" DESC) deduped".to_string()]);
}

#[test]
fn pin_keyed_delete_insert_with_dedup_and_hard_delete() {
    let table_schema = keyed_schema();
    let key = vec!["id".to_string()];
    let dedup = DedupSort {
        column: "seq".into(),
        order: SortOrder::Desc,
    };
    let statements =
        keyed_delete_insert_sql(&plan(&table_schema, &key, Some("deleted"), Some(&dedup)));
    assert_eq!(statements, vec!["DELETE FROM \"events\" WHERE (\"id\") IN (SELECT \"id\" FROM \"_rdlt_stage_feedcafe\")".to_string(), "INSERT INTO \"events\" (\"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\") SELECT \"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\" FROM (SELECT DISTINCT ON (\"id\") * FROM \"_rdlt_stage_feedcafe\" ORDER BY \"id\", \"seq\" DESC NULLS LAST, \"__rdlt_arrival\" DESC) deduped WHERE \"deleted\" IS NOT TRUE".to_string()]);
}

#[test]
fn pin_keyed_upsert_plain() {
    let table_schema = keyed_schema();
    let key = vec!["id".to_string(), "day".to_string()];
    let statements = keyed_upsert_sql(&plan(&table_schema, &key, None, None));
    assert_eq!(statements, vec!["INSERT INTO \"events\" (\"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\") SELECT \"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\" FROM (SELECT DISTINCT ON (\"id\", \"day\") * FROM \"_rdlt_stage_feedcafe\" ORDER BY \"id\", \"day\", \"__rdlt_arrival\" DESC) deduped ON CONFLICT (\"id\", \"day\") DO UPDATE SET \"name\" = EXCLUDED.\"name\", \"seq\" = EXCLUDED.\"seq\", \"deleted\" = EXCLUDED.\"deleted\", \"_rdlt_load_id\" = EXCLUDED.\"_rdlt_load_id\"".to_string()]);
}

#[test]
fn pin_keyed_upsert_with_hard_delete_asc_dedup() {
    let table_schema = keyed_schema();
    let key = vec!["id".to_string()];
    let dedup = DedupSort {
        column: "seq".into(),
        order: SortOrder::Asc,
    };
    let statements = keyed_upsert_sql(&plan(&table_schema, &key, Some("deleted"), Some(&dedup)));
    assert_eq!(
        statements,
        vec!["CREATE TEMP TABLE rdlt_dd_949c09887f1d4674 ON COMMIT DROP AS SELECT * FROM (SELECT DISTINCT ON (\"id\") * FROM \"_rdlt_stage_feedcafe\" ORDER BY \"id\", \"seq\" ASC NULLS LAST, \"__rdlt_arrival\" DESC) deduped".to_string(), "DELETE FROM \"events\" WHERE (\"id\") IN (SELECT \"id\" FROM rdlt_dd_949c09887f1d4674 d WHERE \"deleted\" IS TRUE)".to_string(), "INSERT INTO \"events\" (\"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\") SELECT \"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\" FROM rdlt_dd_949c09887f1d4674 deduped WHERE \"deleted\" IS NOT TRUE ON CONFLICT (\"id\") DO UPDATE SET \"day\" = EXCLUDED.\"day\", \"name\" = EXCLUDED.\"name\", \"seq\" = EXCLUDED.\"seq\", \"deleted\" = EXCLUDED.\"deleted\", \"_rdlt_load_id\" = EXCLUDED.\"_rdlt_load_id\"".to_string()]
    );
}

#[test]
fn pin_identity_delete_insert_root_with_hard_delete() {
    let mut table_schema = keyed_schema();
    table_schema
        .columns
        .insert(0, column("_rdlt_id", LogicalType::Utf8));
    let key: Vec<String> = vec![];
    let statements = identity_delete_insert_sql(&plan(&table_schema, &key, Some("deleted"), None));
    assert_eq!(statements, vec!["DELETE FROM \"events\" WHERE \"_rdlt_id\" IN (SELECT \"_rdlt_id\" FROM \"_rdlt_stage_feedcafe\")".to_string(), "INSERT INTO \"events\" (\"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\") SELECT \"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\" FROM (SELECT DISTINCT ON (\"_rdlt_id\") * FROM \"_rdlt_stage_feedcafe\" ORDER BY \"_rdlt_id\", \"__rdlt_arrival\" DESC) deduped WHERE \"deleted\" IS NOT TRUE".to_string()]);
}

#[test]
fn pin_scd2_keep_and_retire() {
    let table_schema = keyed_schema();
    let key = vec!["id".to_string()];
    let scd2 = Scd2Options::default();
    let statements = scd2_merge_sql(&plan(&table_schema, &key, None, None), &scd2);
    assert_eq!(statements, vec!["CREATE TEMP TABLE rdlt_dd_949c09887f1d4674 ON COMMIT DROP AS SELECT * FROM (SELECT DISTINCT ON (\"id\") * FROM \"_rdlt_stage_feedcafe\" ORDER BY \"id\", \"__rdlt_arrival\" DESC) deduped".to_string(), "UPDATE \"events\" t SET \"_rdlt_valid_to\" = now() FROM rdlt_dd_949c09887f1d4674 d WHERE t.\"_rdlt_valid_to\" IS NULL AND t.\"id\" = d.\"id\" AND (t.\"day\" IS DISTINCT FROM d.\"day\" OR t.\"name\" IS DISTINCT FROM d.\"name\" OR t.\"seq\" IS DISTINCT FROM d.\"seq\" OR t.\"deleted\" IS DISTINCT FROM d.\"deleted\")".to_string(), "INSERT INTO \"events\" (\"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\", \"_rdlt_valid_from\", \"_rdlt_valid_to\") SELECT \"id\", \"day\", \"name\", \"seq\", \"deleted\", \"_rdlt_load_id\", now(), NULL FROM rdlt_dd_949c09887f1d4674 d WHERE NOT EXISTS ( SELECT 1 FROM \"events\" t WHERE t.\"_rdlt_valid_to\" IS NULL AND t.\"id\" = d.\"id\")".to_string()]);

    let retire = Scd2Options {
        absent: AbsentPolicy::Retire,
        ..Scd2Options::default()
    };
    let statements = scd2_merge_sql(&plan(&table_schema, &key, None, None), &retire);
    // Four now, not three: the dedup is materialized once up front and the
    // three statements name it. All three used to re-sort the whole stage.
    assert_eq!(statements.len(), 4);
    assert_eq!(statements[0], "CREATE TEMP TABLE rdlt_dd_949c09887f1d4674 ON COMMIT DROP AS SELECT * FROM (SELECT DISTINCT ON (\"id\") * FROM \"_rdlt_stage_feedcafe\" ORDER BY \"id\", \"__rdlt_arrival\" DESC) deduped".to_string());
    assert_eq!(
        statements[3],
        "UPDATE \"events\" t SET \"_rdlt_valid_to\" = now() WHERE t.\"_rdlt_valid_to\" IS NULL AND (\"id\") NOT IN (SELECT \"id\" FROM rdlt_dd_949c09887f1d4674 d)"
    );
}

/// Later additions — new pins; the pins above are
/// UNTOUCHED (default scd2 output stayed byte-identical).
#[test]
fn pin_scd2_markers_and_boundary() {
    let table_schema = keyed_schema();
    let key = vec!["id".to_string()];
    let scd2 = Scd2Options {
        absent: AbsentPolicy::Retire,
        active_record_timestamp: Some("9999-12-31T00:00:00Z".into()),
        boundary_timestamp: Some("2026-07-22T00:00:00Z".into()),
        ..Scd2Options::default()
    };
    let mut markers_plan = plan(&table_schema, &key, None, None);
    markers_plan.cols_sql = "\"id\", \"day\", \"name\", \"_rdlt_load_id\"";
    let markers_schema = TableSchema {
        table: TableName::from("events"),
        parent: None,
        columns: vec![
            column("id", LogicalType::Int64),
            column("day", LogicalType::Int64),
            column("name", LogicalType::Utf8),
            column("_rdlt_load_id", LogicalType::Utf8),
        ],
    };
    markers_plan.schema = &markers_schema;
    let statements = scd2_merge_sql(&markers_plan, &scd2);
    assert_eq!(
        statements,
        vec![
            PIN_CREATE.to_string(),
            PIN_M0.to_string(),
            PIN_M1.to_string(),
            PIN_M2.to_string(),
        ]
    );
}

#[test]
fn pin_scd2_scoped_retirement() {
    let table_schema = TableSchema {
        table: TableName::from("events"),
        parent: None,
        columns: vec![
            column("id", LogicalType::Int64),
            column("day", LogicalType::Int64),
            column("name", LogicalType::Utf8),
            column("_rdlt_load_id", LogicalType::Utf8),
        ],
    };
    let key = vec!["id".to_string()];
    let scope = vec!["day".to_string()];
    let mut scoped_plan = plan(&table_schema, &key, None, None);
    scoped_plan.cols_sql = "\"id\", \"day\", \"name\", \"_rdlt_load_id\"";
    scoped_plan.merge_scope = Some(&scope);
    let scd2 = Scd2Options {
        absent: AbsentPolicy::Retire,
        ..Scd2Options::default()
    };
    let statements = scd2_merge_sql(&scoped_plan, &scd2);
    assert_eq!(statements.last().unwrap(), PIN_SCOPED);
}

/// The dedup, evaluated ONCE per publish. Every scd2 pin below now names
/// this relation instead of re-interpolating (and re-sorting) the stage.
const PIN_CREATE: &str = "CREATE TEMP TABLE rdlt_dd_949c09887f1d4674 ON COMMIT DROP AS SELECT * FROM (SELECT DISTINCT ON (\"id\") * FROM \"_rdlt_stage_feedcafe\" ORDER BY \"id\", \"__rdlt_arrival\" DESC) deduped";
const PIN_M0: &str = "UPDATE \"events\" t SET \"_rdlt_valid_to\" = '2026-07-22T00:00:00Z' FROM rdlt_dd_949c09887f1d4674 d WHERE (t.\"_rdlt_valid_to\" IS NULL OR t.\"_rdlt_valid_to\" = '9999-12-31T00:00:00Z') AND t.\"id\" = d.\"id\" AND (t.\"day\" IS DISTINCT FROM d.\"day\" OR t.\"name\" IS DISTINCT FROM d.\"name\")";
const PIN_M1: &str = "INSERT INTO \"events\" (\"id\", \"day\", \"name\", \"_rdlt_load_id\", \"_rdlt_valid_from\", \"_rdlt_valid_to\") SELECT \"id\", \"day\", \"name\", \"_rdlt_load_id\", '2026-07-22T00:00:00Z', '9999-12-31T00:00:00Z' FROM rdlt_dd_949c09887f1d4674 d WHERE NOT EXISTS ( SELECT 1 FROM \"events\" t WHERE (t.\"_rdlt_valid_to\" IS NULL OR t.\"_rdlt_valid_to\" = '9999-12-31T00:00:00Z') AND t.\"id\" = d.\"id\")";
const PIN_M2: &str = "UPDATE \"events\" t SET \"_rdlt_valid_to\" = '2026-07-22T00:00:00Z' WHERE (t.\"_rdlt_valid_to\" IS NULL OR t.\"_rdlt_valid_to\" = '9999-12-31T00:00:00Z') AND (\"id\") NOT IN (SELECT \"id\" FROM rdlt_dd_949c09887f1d4674 d)";
const PIN_SCOPED: &str = "UPDATE \"events\" t SET \"_rdlt_valid_to\" = now() WHERE t.\"_rdlt_valid_to\" IS NULL AND (\"id\") NOT IN (SELECT \"id\" FROM rdlt_dd_949c09887f1d4674 d) AND (\"day\") IN (SELECT \"day\" FROM \"_rdlt_stage_feedcafe\" WHERE \"day\" IS NOT NULL)";
