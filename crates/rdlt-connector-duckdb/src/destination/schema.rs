//! Type lowering and the two ensure phases, RENDERED as statement
//! data separately from execution — the seam the golden-SQL pins
//! read, so the DDL this destination would run is testable without a
//! connection.

use rdlt_connector_sdk::spi::core::{
    commit::WriteMode, schema::ColumnType, schema::TableSchema, types::LogicalType,
};
use rdlt_connector_sqlcore::ensure::{self, EnsureStep, Leg, Validity};
use rdlt_connector_sqlcore::plan::ValidateError;
use rdlt_connector_sqlcore::protocol::FullLoadPublish;
use rdlt_connector_sqlcore::{DestinationOptions, names, quote_identifier};

/// The one injection-safety rule, shared with sqlcore's default hook.
pub(crate) fn quote(ident: &str) -> String {
    quote_identifier(ident)
}

/// A table's stage twin: `_rdlt_stage_{16-hex}` of the table name.
/// NOT pipeline-scoped — safe ONLY because stages are TEMP tables,
/// which are session-scoped; a redesign that staged into real tables
/// would collide across pipelines and must re-derive this name.
pub(crate) fn stage_name(table: &str) -> String {
    format!(
        "{}{}",
        names::STAGE_PREFIX,
        rdlt_connector_sdk::spi::core::schema::ident_hash(table, 16)
    )
}

/// Lower one column type. The stage leg differs in ONE row: Json
/// stages as VARCHAR (the Arrow appender writes Utf8) and the publish
/// `INSERT … SELECT` applies DuckDB's implicit VARCHAR→JSON cast,
/// which VALIDATES the document on the way through.
pub(crate) fn sql_type(column: &ColumnType, is_stage: bool) -> String {
    match column {
        ColumnType::Scalar { scalar } => scalar_type(scalar, is_stage),
        ColumnType::Struct { fields } => {
            let inner = fields
                .iter()
                .map(|f| format!("{} {}", quote(&f.name), sql_type(&f.column_type, is_stage)))
                .collect::<Vec<_>>()
                .join(", ");
            format!("STRUCT({inner})")
        }
        ColumnType::ScalarList { item } => format!("{}[]", scalar_type(item, is_stage)),
    }
}

fn scalar_type(scalar: &LogicalType, is_stage: bool) -> String {
    match scalar {
        LogicalType::Bool => "BOOLEAN".into(),
        LogicalType::Int64 => "BIGINT".into(),
        LogicalType::Float64 => "DOUBLE".into(),
        LogicalType::Decimal { precision, scale } => format!("DECIMAL({precision},{scale})"),
        LogicalType::Json if is_stage => "VARCHAR".into(),
        LogicalType::Json => "JSON".into(),
        // Uuid travels as text for portability with the hex _rdlt_id
        // convention.
        LogicalType::Utf8 | LogicalType::Uuid => "VARCHAR".into(),
        LogicalType::Binary => "BLOB".into(),
        LogicalType::TimestampTz => "TIMESTAMP WITH TIME ZONE".into(),
        LogicalType::TimestampNaive => "TIMESTAMP".into(),
        LogicalType::Date => "DATE".into(),
        LogicalType::Time => "TIME".into(),
    }
}

/// `CREATE [TEMP ]TABLE IF NOT EXISTS` carrying every column in
/// schema order; the stage leg is TEMP and stage-shaped. The rendered
/// text is golden-pinned — the statement grows in place, one clause at
/// a time.
pub(crate) fn create_table_sql(name: &str, schema: &TableSchema, temp: bool) -> String {
    let mut sql = String::from("CREATE ");
    if temp {
        sql.push_str("TEMP ");
    }
    sql.push_str("TABLE IF NOT EXISTS ");
    sql.push_str(&quote(name));
    sql.push_str(" (");
    for (position, column) in schema.columns.iter().enumerate() {
        if position > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&quote(&column.name));
        sql.push(' ');
        sql.push_str(&sql_type(&column.column_type, temp));
    }
    sql.push(')');
    sql
}

fn leg_name(leg: Leg, schema: &TableSchema) -> (String, bool) {
    match leg {
        Leg::Target => (schema.table.as_str().to_owned(), false),
        Leg::Stage => (stage_name(schema.table.as_str()), true),
    }
}

/// Phase 1: the schema DDL. Driven Append+Staged DELIBERATELY — every
/// write mode here publishes through a stage, so both legs always
/// exist (golden-pinned; differs from postgres). `previous` is the
/// schema THIS SESSION last ensured — the widen is a within-run rule.
pub(crate) fn table_ddl(schema: &TableSchema, previous: Option<&TableSchema>) -> Vec<String> {
    ensure::schema_steps(
        schema,
        &WriteMode::Append,
        FullLoadPublish::Staged,
        previous,
    )
    .into_iter()
    .map(|step| match step {
        EnsureStep::Table { leg } => {
            let (name, temp) = leg_name(leg, schema);
            create_table_sql(&name, schema, temp)
        }
        EnsureStep::Column { leg, column } => {
            let (name, temp) = leg_name(leg, schema);
            let col = &schema.columns[column];
            format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}",
                quote(&name),
                quote(&col.name),
                sql_type(&col.column_type, temp)
            )
        }
        // DuckDB's ALTER … SET DATA TYPE migrates existing rows —
        // no USING clause (golden-pinned; differs from postgres).
        EnsureStep::Widen { leg, column } => {
            let (name, temp) = leg_name(leg, schema);
            let col = &schema.columns[column];
            format!(
                "ALTER TABLE {} ALTER COLUMN {} SET DATA TYPE {}",
                quote(&name),
                quote(&col.name),
                sql_type(&col.column_type, temp)
            )
        }
        other => unreachable!("schema_steps never emits {other:?}"),
    })
    .collect()
}

/// One phase-2 statement with the unique-index key columns that route
/// the duplicate-key diagnosis when it fails.
pub(crate) type EnsureStatement = (String, Option<Vec<String>>);

/// Phase 2: validity columns and indexes. Option validation lives in
/// sqlcore's merge_steps; non-merge modes validate and emit nothing.
pub(crate) fn merge_ddl(
    options: &DestinationOptions,
    schema: &TableSchema,
    mode: &WriteMode,
) -> Result<Vec<EnsureStatement>, ValidateError> {
    let table = schema.table.as_str();
    let scd2 = options.scd2_for(table);
    let mut out = Vec::new();
    for step in ensure::merge_steps(options, schema, mode)? {
        match step {
            // Target only, NO NOT NULL (DuckDB rejects ADD COLUMN with
            // a NOT NULL constraint; the insert arm always supplies the
            // boundary, and DEFAULT now() backfills a table adopting
            // scd2). Golden-pinned tails.
            EnsureStep::Validity(validity) => {
                let (column, tail) = match validity {
                    Validity::From => (scd2.valid_from.clone(), "TIMESTAMPTZ DEFAULT now()"),
                    Validity::To => (scd2.valid_to.clone(), "TIMESTAMPTZ"),
                };
                out.push((
                    format!(
                        "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {tail}",
                        quote(table),
                        quote(&column)
                    ),
                    None,
                ));
            }
            EnsureStep::Index(spec) => {
                if spec.unique {
                    // Databases written before the unique prefix named
                    // unique indexes with the PLAIN formula; drop that
                    // name first so an upgraded database doesn't carry
                    // two identical unique ART indexes forever. A
                    // persisted-format migration — keep it. Text
                    // byte-identical to before; only the renderer moved
                    // to `names::drop_index_sql` so `load.rs`'s
                    // pre-ALTER legacy-name drop (037 US5 fix round 2,
                    // F5) reuses it instead of a third hand-written copy.
                    out.push((
                        names::drop_index_sql(false, table, &spec.columns, quote),
                        None,
                    ));
                }
                let key = spec.unique.then(|| spec.columns.clone());
                out.push((
                    names::create_index_sql(spec.unique, table, &spec.columns, quote),
                    key,
                ));
            }
            other => unreachable!("merge_steps never emits {other:?}"),
        }
    }
    Ok(out)
}
