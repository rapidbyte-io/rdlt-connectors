//! Golden text pin for the NON-MERGE publish path.
//!
//! `test_golden_sql.rs` pins the merge statements and is deliberately
//! untouched by this story — merge still stages and its SQL is unchanged.
//! What changed is the full-load path, which no longer emits any publish
//! statement at all: Append and Replace rows COPY straight into the target
//! inside a unit transaction. This file pins what that path DOES say, and
//! what it no longer says.
//!
//! No database: every string here is produced by a pure builder.

use rdlt_connector_postgres::testsupport::destination::{
    Dialect, UNIT_BEGIN, UNIT_COMMIT, UNIT_ROLLBACK, UNIT_WORK_MEM,
};
use rdlt_connector_sdk::spi::core::commit::WriteMode;
use rdlt_connector_sdk::spi::core::schema::Column;
use rdlt_connector_sdk::spi::core::{
    id::TableName, schema::ColumnType, schema::Provenance, schema::TableSchema, types::LogicalType,
};
use rdlt_connector_sqlcore::{
    CommitContext, DestinationOptions, FullLoadPublish, Step, column_list, insert_select_sql,
    plan_commit, prepare_target, quote_identifier,
};

use rdlt_connector_sqlcore::MergeDialect;
use std::collections::{BTreeMap, BTreeSet};

fn schema(table: &str) -> TableSchema {
    TableSchema {
        table: TableName::from(table),
        parent: None,
        columns: vec![
            Column {
                name: "id".into(),
                column_type: ColumnType::Scalar {
                    scalar: LogicalType::Int64,
                },
                nullable: true,
                provenance: Provenance::Inferred,
            },
            Column {
                name: "name".into(),
                column_type: ColumnType::Scalar {
                    scalar: LogicalType::Utf8,
                },
                nullable: true,
                provenance: Provenance::Inferred,
            },
        ],
    }
}

fn tables(mode: WriteMode) -> BTreeMap<TableName, (TableSchema, WriteMode)> {
    [(TableName::from("events"), (schema("events"), mode))]
        .into_iter()
        .collect()
}

fn direct(cleared: &BTreeSet<TableName>, load_committed_before: bool) -> CommitContext<'_> {
    static EMPTY: BTreeSet<TableName> = BTreeSet::new();
    CommitContext {
        replayed: false,
        load_committed_before,
        single_unit_done: &EMPTY,
        staged_nonempty: &EMPTY,
        full_load_publish: FullLoadPublish::DirectToTarget,
        cleared_targets: cleared,
    }
}

/// The unit transaction's own statements. The isolation level is stated
/// explicitly rather than inherited from a server default a deployment could
/// change — see the module doc on `destination`.
#[test]
fn unit_transaction_statements() {
    assert_eq!(UNIT_BEGIN, "BEGIN ISOLATION LEVEL READ COMMITTED");
    assert_eq!(UNIT_COMMIT, "COMMIT");
    assert_eq!(UNIT_ROLLBACK, "ROLLBACK");
    // SET LOCAL, never a bare SET: the scope is what makes it safe to set
    // unasked. A bare SET would leak into every later unit on this connection
    // and into anything else sharing it.
    assert_eq!(UNIT_WORK_MEM, "SET LOCAL work_mem = '64MB'");
    assert!(
        UNIT_WORK_MEM.starts_with("SET LOCAL "),
        "work_mem must be transaction-scoped"
    );
}

/// The Replace clear, rendered exactly as the unit issues it before the first
/// COPY. TRUNCATE (not DELETE) is the choice being pinned: it is what makes
/// the clear cheap, and what makes it take ACCESS EXCLUSIVE.
#[test]
fn replace_clears_with_truncate() {
    let cleared = BTreeSet::new();
    let steps = prepare_target(
        &tables(WriteMode::Replace),
        &direct(&cleared, false),
        &TableName::from("events"),
    );
    assert_eq!(
        steps,
        vec![Step::ClearTarget {
            table: TableName::from("events")
        }]
    );
    assert_eq!(
        Dialect.clear_table(&quote_identifier("events")),
        r#"TRUNCATE TABLE "events""#
    );
}

/// The statement this story DELETES. `insert_select_sql` still exists — merge
/// destinations and the staged path use it — but no direct-path plan reaches
/// it, which is the whole point: the rows were never anywhere else.
#[test]
fn the_direct_path_emits_no_insert_select() {
    let cleared = BTreeSet::new();
    for mode in [WriteMode::Append, WriteMode::Replace] {
        let table_set = tables(mode);
        let script = plan_commit(
            &table_set,
            &DestinationOptions::default(),
            &direct(&cleared, false),
        )
        .expect("plan");
        assert_eq!(
            script.steps,
            vec![Step::UpsertState, Step::InsertReceipt],
            "a direct full-load unit publishes only state + receipt"
        );
    }
    // What it would have said, kept here so the diff is legible: this exact
    // statement ran once per table per unit and wrote every row a second time.
    assert_eq!(
        insert_select_sql(
            &quote_identifier("events"),
            &column_list(&schema("events")),
            &quote_identifier("stage")
        ),
        r#"INSERT INTO "events" ("id", "name") SELECT "id", "name" FROM "stage""#
    );
}
