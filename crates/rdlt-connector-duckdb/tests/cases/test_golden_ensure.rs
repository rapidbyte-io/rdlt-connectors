//! The nine golden-DDL facts, pinned as rendered statement data.
//!
//! Everything here drives `testhook::{ensure_table_sql, ensure_merge_sql}`
//! — the seam that renders the ensure phases WITHOUT a connection — so
//! these cells compare exact SQL text, byte for byte where the contract
//! inventory quotes a shape. Several pin where this dialect deliberately
//! DIFFERS from postgres (both legs always exist, SET DATA TYPE without
//! USING, validity columns without NOT NULL); a refactor that
//! "harmonized" the two would break real databases.

use rdlt_connector_duckdb::destination::testhook::{ensure_merge_sql, ensure_table_sql};
use rdlt_connector_duckdb::destination::{DestinationOptions, MergeStrategy};
use rdlt_connector_sdk::spi::core::naming::ident_hash;
use rdlt_connector_sdk::spi::core::{
    ColumnDef, ColumnType, LogicalType, Provenance, TableName, TableSchema, WriteMode,
};
use rdlt_connector_sqlcore::names;

fn field(name: &str, scalar: LogicalType) -> ColumnDef {
    ColumnDef {
        name: name.to_owned(),
        column_type: ColumnType::scalar(scalar),
        nullable: true,
        provenance: Provenance::Inferred,
    }
}

fn table(name: &str, columns: Vec<ColumnDef>) -> TableSchema {
    TableSchema {
        table: TableName::new(name),
        parent: None,
        columns,
    }
}

/// The working schema: a key plus a Json payload, so the stage-shape
/// divergence (Json stages as VARCHAR) is visible in every rendering.
fn orders() -> TableSchema {
    table(
        "orders",
        vec![
            field("id", LogicalType::Int64),
            field("payload", LogicalType::Json),
        ],
    )
}

fn stage_of(name: &str) -> String {
    format!("_rdlt_stage_{}", ident_hash(name, 16))
}

fn strategy(s: MergeStrategy) -> DestinationOptions {
    DestinationOptions {
        merge_strategy: Some(s),
        ..DestinationOptions::default()
    }
}

fn keyed_on_id() -> WriteMode {
    WriteMode::Merge {
        key: vec!["id".to_owned()],
    }
}

/// Fact 1 — BOTH legs are always created, whatever the write mode will
/// be: every mode here publishes through a stage. The stage leg is TEMP
/// and stage-shaped: Json lowers to VARCHAR there (the Arrow appender
/// writes Utf8; the publish INSERT…SELECT casts, which validates).
#[test]
fn every_ensure_creates_target_and_temp_stage_with_stage_shapes() {
    let ddl = ensure_table_sql(&orders(), None);
    let creates: Vec<&String> = ddl.iter().filter(|s| s.starts_with("CREATE")).collect();
    assert_eq!(creates.len(), 2, "exactly two legs: {ddl:?}");
    assert_eq!(
        creates[0],
        "CREATE TABLE IF NOT EXISTS \"orders\" (\"id\" BIGINT, \"payload\" JSON)"
    );
    assert_eq!(
        *creates[1],
        format!(
            "CREATE TEMP TABLE IF NOT EXISTS \"{}\" (\"id\" BIGINT, \"payload\" VARCHAR)",
            stage_of("orders")
        )
    );
}

/// Fact 2 — column ensures come in target-leg order first, then the
/// stage leg, each `ADD COLUMN IF NOT EXISTS` in its leg's shape.
#[test]
fn columns_ensure_on_the_target_leg_before_the_stage_leg() {
    let ddl = ensure_table_sql(&orders(), None);
    let adds: Vec<&String> = ddl
        .iter()
        .filter(|s| s.contains("ADD COLUMN IF NOT EXISTS"))
        .collect();
    assert_eq!(adds.len(), 4, "two columns on each leg: {ddl:?}");
    assert_eq!(
        adds[0],
        "ALTER TABLE \"orders\" ADD COLUMN IF NOT EXISTS \"id\" BIGINT"
    );
    assert_eq!(
        adds[1],
        "ALTER TABLE \"orders\" ADD COLUMN IF NOT EXISTS \"payload\" JSON"
    );
    let stage = stage_of("orders");
    assert!(
        adds[2].contains(&stage) && adds[3].contains(&stage),
        "the stage leg trails: {adds:?}"
    );
    assert_eq!(
        *adds[3],
        format!("ALTER TABLE \"{stage}\" ADD COLUMN IF NOT EXISTS \"payload\" VARCHAR")
    );
}

/// Fact 3 — a type change renders as `SET DATA TYPE` on both legs and
/// NEVER carries a USING cast: DuckDB migrates existing rows itself
/// (deliberate divergence from postgres).
#[test]
fn a_widen_is_set_data_type_on_both_legs_with_no_using() {
    let before = table("orders", vec![field("id", LogicalType::Int64)]);
    let after = table("orders", vec![field("id", LogicalType::Utf8)]);
    let ddl = ensure_table_sql(&after, Some(&before));
    let widens: Vec<&String> = ddl.iter().filter(|s| s.contains("ALTER COLUMN")).collect();
    assert_eq!(widens.len(), 2, "target and stage widen: {ddl:?}");
    assert_eq!(
        widens[0],
        "ALTER TABLE \"orders\" ALTER COLUMN \"id\" SET DATA TYPE VARCHAR"
    );
    assert_eq!(
        *widens[1],
        format!(
            "ALTER TABLE \"{}\" ALTER COLUMN \"id\" SET DATA TYPE VARCHAR",
            stage_of("orders")
        )
    );
    assert!(
        widens.iter().all(|s| !s.contains("USING")),
        "this dialect never emits a USING cast: {widens:?}"
    );
}

/// Fact 4 — re-ensuring the identical schema widens NOTHING.
#[test]
fn an_unchanged_schema_never_renders_a_widen() {
    let same = orders();
    let ddl = ensure_table_sql(&same, Some(&same));
    assert!(
        !ddl.iter().any(|s| s.contains("ALTER COLUMN")),
        "no type moved, so no widen may render: {ddl:?}"
    );
}

/// Fact 5 — the scd2 validity columns carry the exact frozen tails and
/// NO NOT NULL: DuckDB rejects ADD COLUMN with that constraint, the
/// insert arm always supplies the boundary, and DEFAULT now() backfills
/// a table adopting scd2.
#[test]
fn scd2_validity_columns_render_exact_tails_without_not_null() {
    let plain = table(
        "orders",
        vec![
            field("id", LogicalType::Int64),
            field("v", LogicalType::Utf8),
        ],
    );
    let stmts = ensure_merge_sql(&strategy(MergeStrategy::Scd2), &plain, &keyed_on_id())
        .expect("scd2 under merge is valid");
    let validity: Vec<&str> = stmts
        .iter()
        .map(|(sql, _)| sql.as_str())
        .filter(|s| s.contains("ADD COLUMN"))
        .collect();
    assert_eq!(validity.len(), 2, "{stmts:?}");
    assert_eq!(
        validity[0],
        "ALTER TABLE \"orders\" ADD COLUMN IF NOT EXISTS \"_rdlt_valid_from\" \
         TIMESTAMPTZ DEFAULT now()"
    );
    assert_eq!(
        validity[1],
        "ALTER TABLE \"orders\" ADD COLUMN IF NOT EXISTS \"_rdlt_valid_to\" TIMESTAMPTZ"
    );
    assert!(
        stmts.iter().all(|(sql, _)| !sql.contains("NOT NULL")),
        "DuckDB rejects the constraint: {stmts:?}"
    );
}

/// Fact 6 — the upsert arbiter index is preceded by dropping its
/// LEGACY name (databases written before the unique prefix used the
/// plain formula); the drop comes first, and only the CREATE carries
/// the key columns that route the duplicate-key diagnosis.
#[test]
fn the_unique_index_drops_its_legacy_name_first() {
    let key = vec!["id".to_owned()];
    let stmts = ensure_merge_sql(&strategy(MergeStrategy::Upsert), &orders(), &keyed_on_id())
        .expect("upsert under merge is valid");
    let drop_at = stmts
        .iter()
        .position(|(sql, _)| sql.starts_with("DROP INDEX IF EXISTS"))
        .expect("the legacy drop renders: {stmts:?}");
    let create_at = stmts
        .iter()
        .position(|(sql, _)| sql.contains("CREATE UNIQUE INDEX"))
        .expect("the unique index renders");
    assert!(drop_at < create_at, "drop precedes create: {stmts:?}");
    assert_eq!(
        stmts[drop_at].0,
        format!(
            "DROP INDEX IF EXISTS \"{}\"",
            names::index_name(false, "orders", &key)
        ),
        "the legacy name is the PLAIN formula over the same key"
    );
    assert_eq!(stmts[drop_at].1, None, "the drop routes no diagnosis");
    assert_eq!(
        stmts[create_at].0,
        format!(
            "CREATE UNIQUE INDEX IF NOT EXISTS \"{}\" ON \"orders\" (\"id\")",
            names::index_name(true, "orders", &key)
        )
    );
    assert_eq!(
        stmts[create_at].1.as_deref(),
        Some(&key[..]),
        "only the create carries the key columns"
    );
}

/// Fact 7 — the default strategy (delete_insert) renders exactly one
/// PLAIN supporting index and no drop: the key must be findable but
/// never declared unique, so there is no legacy unique name to retire.
#[test]
fn the_default_strategy_renders_one_plain_index_and_no_drop() {
    let key = vec!["id".to_owned()];
    let stmts = ensure_merge_sql(&DestinationOptions::default(), &orders(), &keyed_on_id())
        .expect("the default is always valid");
    assert_eq!(stmts.len(), 1, "one supporting index only: {stmts:?}");
    assert_eq!(
        stmts[0].0,
        format!(
            "CREATE INDEX IF NOT EXISTS \"{}\" ON \"orders\" (\"id\")",
            names::index_name(false, "orders", &key)
        )
    );
    assert_eq!(stmts[0].1, None, "a plain index routes no diagnosis");
}

/// Fact 8 — a non-merge mode renders NOTHING in phase 2 but the
/// options still pass through validation (a bad document would error
/// even under Append).
#[test]
fn non_merge_modes_render_nothing_yet_still_validate() {
    for mode in [WriteMode::Append, WriteMode::Replace] {
        let stmts = ensure_merge_sql(&DestinationOptions::default(), &orders(), &mode)
            .expect("default options validate under every mode");
        assert!(stmts.is_empty(), "{mode:?} has no merge DDL: {stmts:?}");
    }
}

/// Fact 9 — an EXPLICIT merge strategy under Append is a typed refusal
/// naming the table: the None/Some distinction on merge_strategy is
/// load-bearing.
#[test]
fn an_explicit_strategy_under_append_is_refused_naming_the_table() {
    let err = ensure_merge_sql(
        &strategy(MergeStrategy::Upsert),
        &orders(),
        &WriteMode::Append,
    )
    .expect_err("merge-only options need the merge write mode");
    let text = err.to_string();
    assert!(text.contains("orders"), "names the table: {text}");
    assert!(
        text.contains("merge write mode"),
        "the shared validator wording: {text}"
    );
}
