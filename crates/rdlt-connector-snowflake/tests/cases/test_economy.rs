//! Statement economy, pinned as data: the rendered ensure DDL is
//! compared without an account (executing it needs one; comparing it
//! needs nothing), and the read-before-write discipline shows up as
//! statements that simply do not exist.

use rdlt_connector_sdk::spi::core::{
    commit::WriteMode, id::TableName, schema::Column, schema::ColumnType, schema::Provenance,
    schema::TableSchema, types::LogicalType,
};
use rdlt_connector_snowflake::destination::TableType;
use rdlt_connector_snowflake::destination::testhook::{
    Catalog, ensure_merge_sql, ensure_table_sql,
};

fn schema(columns: &[(&str, bool)]) -> TableSchema {
    TableSchema {
        table: TableName::from("events"),
        parent: None,
        columns: columns
            .iter()
            .map(|(name, nullable)| Column {
                name: (*name).to_owned(),
                column_type: ColumnType::scalar(LogicalType::Int64),
                nullable: *nullable,
                provenance: Provenance::Inferred,
            })
            .collect(),
    }
}

/// A fresh table costs one CREATE; the SAME ensure against the folded
/// image costs zero statements — the steady state every load after the
/// first lives in.
#[test]
fn the_second_ensure_of_an_unchanged_table_costs_nothing() {
    let events = schema(&[("id", false), ("v", true)]);
    let mut catalog = Catalog::default();
    catalog.observe("events", std::collections::BTreeSet::new());

    let first = ensure_table_sql(
        "p",
        &events,
        &WriteMode::Append,
        TableType::Permanent,
        None,
        &catalog,
    );
    assert_eq!(first.len(), 1, "one CREATE: {first:?}");

    catalog.record_created("events", &["id".to_owned(), "v".to_owned()]);
    let second = ensure_table_sql(
        "p",
        &events,
        &WriteMode::Append,
        TableType::Permanent,
        None,
        &catalog,
    );
    assert!(second.is_empty(), "steady state emits nothing: {second:?}");
}

/// Merge pays for its stage twin exactly once too, and the phase-2
/// ensure never emits an index on a service that would ignore one.
#[test]
fn merge_ensures_settle_to_zero_statements_as_well() {
    let events = schema(&[("id", false)]);
    let mode = WriteMode::Merge {
        key: vec!["id".to_owned()],
    };
    let options = rdlt_connector_sqlcore::DestinationOptions {
        merge_strategy: Some(rdlt_connector_sqlcore::MergeStrategy::Upsert),
        ..Default::default()
    };
    let mut catalog = Catalog::default();
    catalog.observe("events", std::collections::BTreeSet::new());
    catalog.observe(
        rdlt_connector_sqlcore::names::stage_table("p", "events").as_str(),
        std::collections::BTreeSet::new(),
    );

    let first = ensure_table_sql("p", &events, &mode, TableType::Permanent, None, &catalog);
    assert_eq!(first.len(), 2, "target and stage: {first:?}");

    catalog.record_created("events", &["id".to_owned()]);
    catalog.record_created(
        rdlt_connector_sqlcore::names::stage_table("p", "events").as_str(),
        &["id".to_owned()],
    );
    let second = ensure_table_sql("p", &events, &mode, TableType::Permanent, None, &catalog);
    assert!(second.is_empty(), "{second:?}");

    let merge = ensure_merge_sql(&options, &events, &mode, &catalog).expect("valid options");
    assert!(
        !merge.iter().any(|s| s.contains("INDEX")),
        "no index, ever: {merge:?}"
    );
}
