//! What the session has ensured, and the DDL that ensured it: the type
//! lowering, the two ensure phases (rendered as data, executed here), and
//! the ensured-tables map every later decision consults.
//!
//! Rendering is separated from execution so the emitted statement sequence
//! is testable without a server — the statements are compared as data, and
//! only their execution needs a connection.

use std::collections::BTreeMap;

use rdlt_connector_sdk::spi::core::{
    id::PipelineId, id::TableName, schema::Column, schema::ColumnType, types::LogicalType,
};
use rdlt_connector_sdk::spi::{
    core::commit::WriteMode, core::schema::TableSchema, error::DestinationError,
};
use rdlt_connector_sqlcore::ensure::{self, EnsureStep, Leg, Validity};
use rdlt_connector_sqlcore::plan::ValidateError;
use rdlt_connector_sqlcore::{FullLoadPublish, quote_identifier};

use super::config::DestinationOptions;
use super::errors::{Phase, classify_statement, fatal};
use crate::session::Connection;

/// Stage names are pipeline-scoped and hashed: scoping stops one pipeline's
/// `open` from truncating another's live staged rows in a shared schema, and
/// hashing bounds the identifier under Postgres's 63-byte limit, where
/// silent truncation would otherwise cut off exactly the disambiguation
/// suffix.
pub fn stage_prefix(pipeline: &PipelineId) -> String {
    format!(
        "{}{}_",
        rdlt_connector_sqlcore::names::STAGE_PREFIX,
        rdlt_connector_sdk::spi::core::schema::ident_hash(pipeline.as_str(), 8)
    )
}

pub fn stage_name(pipeline: &PipelineId, table: &TableName) -> String {
    rdlt_connector_sqlcore::names::stage_table(pipeline.as_str(), table.as_str())
}

/// Postgres SQL type per logical column type.
pub fn sql_type(column_type: &ColumnType) -> String {
    match column_type {
        ColumnType::Scalar { scalar } => match scalar {
            LogicalType::Bool => "BOOLEAN".into(),
            LogicalType::Int64 => "BIGINT".into(),
            LogicalType::Float64 => "DOUBLE PRECISION".into(),
            LogicalType::Utf8 => "TEXT".into(),
            LogicalType::Uuid => "UUID".into(),
            LogicalType::Json => "JSONB".into(),
            LogicalType::Binary => "BYTEA".into(),
            LogicalType::TimestampTz => "TIMESTAMPTZ".into(),
            LogicalType::TimestampNaive => "TIMESTAMP".into(),
            LogicalType::Date => "DATE".into(),
            LogicalType::Time => "TIME".into(),
            LogicalType::Decimal { precision, scale } => {
                format!("NUMERIC({precision},{scale})")
            }
        },
        // structs/lists never reach a structless destination (engine-
        // lowered).
        ColumnType::Struct { .. } | ColumnType::ScalarList { .. } => "TEXT".into(),
    }
}

/// One column definition for CREATE TABLE. NOT NULL applies to the TARGET
/// table only (CREATE-time; migrations stay additive) — stage tables keep
/// everything nullable.
pub fn render_column_definition(column: &Column, for_target: bool) -> String {
    let not_null = if for_target && !column.nullable {
        " NOT NULL"
    } else {
        ""
    };
    format!(
        "{} {}{}",
        quote_identifier(&column.name),
        sql_type(&column.column_type),
        not_null
    )
}

/// One ensure statement plus what the caller must know to classify its
/// failure. A unique-index failure is the only one carrying a special
/// diagnosis, so it is the only kind distinguished here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EnsureStatement {
    pub sql: String,
    pub unique_index: Option<Vec<String>>,
}

impl EnsureStatement {
    fn plain(sql: String) -> Self {
        Self {
            sql,
            unique_index: None,
        }
    }
}

/// Phase 1 — the table statements: create each leg, then add and widen its
/// columns. Infallible by construction, and deliberately independent of the
/// option validation that follows: the tables are created BEFORE the
/// options are checked, and reordering that would move where a bad config
/// fails.
///
/// `previous` is the schema THIS SESSION last ensured, never the live
/// catalog — that is what makes the widen a within-run rule.
pub fn table_ddl_statements(
    pipeline: &PipelineId,
    table_schema: &TableSchema,
    mode: &WriteMode,
    previous: Option<&TableSchema>,
) -> Vec<String> {
    // Only merge tables get a stage leg here — this destination COPYs
    // Append and Replace straight into the target inside the unit
    // transaction, so an UNLOGGED twin and the sequence that comes with it
    // would be built, written and truncated for nothing. That is what
    // `DirectToTarget` says.
    let plan = ensure::schema_steps(
        table_schema,
        mode,
        FullLoadPublish::DirectToTarget,
        previous,
    );
    let leg_name = |leg: Leg| match leg {
        Leg::Target => table_schema.table.as_str().to_owned(),
        Leg::Stage => stage_name(pipeline, &table_schema.table),
    };
    let mut statements = Vec::new();
    for step in plan {
        match step {
            EnsureStep::Table { leg } => {
                let is_stage = leg == Leg::Stage;
                let mut columns = table_schema
                    .columns
                    .iter()
                    .map(|column| render_column_definition(column, !is_stage))
                    .collect::<Vec<_>>()
                    .join(", ");
                if is_stage {
                    // Arrival order for deterministic merge dedup.
                    //
                    // `CACHE 32` rather than a plain BIGSERIAL's cache of 1,
                    // and it is worth real time: the sequence is drawn once
                    // per staged row, and at a million rows that draw was
                    // measured at 215 ms of a 642 ms stage COPY — a third of
                    // it. Caching recovers 122 ms; the number plateaus at
                    // 32, with 1000 no better.
                    //
                    // Caching costs only CONTIGUITY, which nothing here
                    // reads: the column orders rows within one stage table,
                    // and the dedup asks which value is larger, never
                    // whether values are adjacent. The engine opens its
                    // destination sessions one after another, so there is no
                    // concurrent writer to interleave blocks with.
                    //
                    // Spelled as an identity column because that is one
                    // statement that both creates the sequence and sets its
                    // cache. An already-existing stage table keeps whatever
                    // it was created with; `IF NOT EXISTS` leaves it alone,
                    // and a stage holding uncached arrival values is
                    // correct, merely slower.
                    columns.push_str(&format!(
                        ", {} bigint GENERATED BY DEFAULT AS IDENTITY (CACHE 32)",
                        quote_identifier(rdlt_connector_sqlcore::names::ARRIVAL_COL)
                    ));
                }
                let unlogged = if is_stage { "UNLOGGED " } else { "" };
                statements.push(format!(
                    "CREATE {unlogged}TABLE IF NOT EXISTS {} ({columns})",
                    quote_identifier(&leg_name(leg))
                ));
            }
            EnsureStep::Column { leg, column } => statements.push(format!(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}",
                quote_identifier(&leg_name(leg)),
                quote_identifier(&table_schema.columns[column].name),
                sql_type(&table_schema.columns[column].column_type)
            )),
            // Postgres needs the cast spelled out; the plan says only that a
            // widen must happen.
            EnsureStep::Widen { leg, column } => statements.push(format!(
                "ALTER TABLE {} ALTER COLUMN {} TYPE {} USING {}::{}",
                quote_identifier(&leg_name(leg)),
                quote_identifier(&table_schema.columns[column].name),
                sql_type(&table_schema.columns[column].column_type),
                quote_identifier(&table_schema.columns[column].name),
                sql_type(&table_schema.columns[column].column_type)
            )),
            _ => unreachable!("phase 1 plans relations and columns only"),
        }
    }
    statements
}

/// Phase 2 — option validation, then the scd2 validity columns and the
/// index plan. Runs AFTER phase 1 has been applied, preserving the frozen
/// failure point. Non-merge modes validate and return nothing to execute.
pub fn merge_ensure_statements(
    options: &DestinationOptions,
    table_schema: &TableSchema,
    mode: &WriteMode,
) -> Result<Vec<EnsureStatement>, ValidateError> {
    let table = table_schema.table.as_str();
    let scd2 = options.scd2_for(table);
    let mut statements = Vec::new();
    for step in ensure::merge_steps(options, table_schema, mode)? {
        match step {
            // Validity columns on the TARGET only (the stage carries the
            // stream's shape); additive for pre-existing scd2 tables.
            EnsureStep::Validity(which) => {
                let (column, definition) = match which {
                    Validity::From => (&scd2.valid_from, "TIMESTAMPTZ NOT NULL DEFAULT now()"),
                    Validity::To => (&scd2.valid_to, "TIMESTAMPTZ"),
                };
                statements.push(EnsureStatement::plain(format!(
                    "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {definition}",
                    quote_identifier(table),
                    quote_identifier(column)
                )));
            }
            EnsureStep::Index(spec) => statements.push(EnsureStatement {
                sql: rdlt_connector_sqlcore::names::create_index_sql(
                    spec.unique,
                    table,
                    &spec.columns,
                    quote_identifier,
                ),
                unique_index: spec.unique.then_some(spec.columns),
            }),
            _ => unreachable!("phase 2 plans validity columns and indexes only"),
        }
    }
    Ok(statements)
}

/// The ensured-tables record: what this session has been told about each
/// table, which every later planning decision consults.
#[derive(Debug, Default)]
pub(super) struct Catalog {
    pub(super) tables: BTreeMap<TableName, (TableSchema, WriteMode)>,
}

impl Catalog {
    /// Run both ensure phases for one table and record it. Failures
    /// classify by SQLSTATE; a 23505 on a unique merge-identity index gets
    /// the shared duplicate-key diagnosis naming the key columns.
    pub(super) async fn ensure(
        &mut self,
        connection: &Connection,
        pipeline: &PipelineId,
        options: &DestinationOptions,
        table_schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        let previous = self
            .tables
            .get(&table_schema.table)
            .map(|(schema, _)| schema.clone());
        for sql in table_ddl_statements(pipeline, table_schema, mode, previous.as_ref()) {
            connection
                .batch_execute(&sql)
                .await
                .map_err(|e| classify_statement(Phase::Ensure, e))?;
        }
        for statement in merge_ensure_statements(options, table_schema, mode)
            .map_err(|e| fatal(Phase::Ensure, e))?
        {
            if let Err(e) = connection.batch_execute(&statement.sql).await {
                // Pre-existing duplicate keys under upsert — typed, naming
                // the key columns.
                if let Some(columns) = &statement.unique_index
                    && e.as_db_error()
                        .is_some_and(|db| db.code().code() == "23505")
                {
                    return Err(fatal(
                        Phase::Ensure,
                        rdlt_connector_sqlcore::names::duplicate_merge_key_diagnosis(
                            table_schema.table.as_str(),
                            columns,
                            &crate::session::describe(&e),
                        ),
                    ));
                }
                return Err(classify_statement(Phase::Ensure, e));
            }
        }
        self.tables.insert(
            table_schema.table.clone(),
            (table_schema.clone(), mode.clone()),
        );
        Ok(())
    }

    /// The root a child table's merge reads through (sqlcore's rule).
    pub(super) fn root_of(&self, table: &TableName) -> TableName {
        rdlt_connector_sqlcore::root_of(&self.tables, table)
    }
}
