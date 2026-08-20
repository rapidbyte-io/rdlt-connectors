//! Identifier spelling, the type map, and ensure rendering.
//!
//! One rule governs every identifier this crate emits: **quoted upper
//! case**. Unquoted names fold upper here, and a quoted lower-case name
//! is a DIFFERENT object (probing produced `EVENTS` and `events`
//! coexisting) — quoted-upper is also exactly what an unquoted user
//! query resolves to, so `select * from events` in a worksheet finds
//! what rdlt wrote. Quoting alone would strand lower-case objects no
//! unquoted query reaches; uppercasing alone would break on reserved
//! words and specials.
//!
//! Ensure renders against the [`Catalog`](super::catalog::Catalog)
//! image so only genuinely missing work becomes a statement — the
//! read-before-write economy the catalog module explains.

use rdlt_connector_sdk::spi::core::{
    commit::WriteMode, id::TableName, schema::ColumnType, schema::TableSchema, types::LogicalType,
};
use rdlt_connector_sqlcore::ensure::{self, EnsureStep, Leg, Validity};
use rdlt_connector_sqlcore::plan::ValidateError;
use rdlt_connector_sqlcore::{DestinationOptions, FullLoadPublish};

use super::catalog::Catalog;
use super::config::TableType;

/// Quote an identifier the one way this crate spells them.
pub(super) fn quote(name: &str) -> String {
    // An embedded quote doubles. The engine's normalisation makes one
    // vanishingly rare, but silently emitting broken SQL for it would
    // cost more than the two characters handling it costs.
    format!("\"{}\"", name.to_uppercase().replace('"', "\"\""))
}

/// The service type for a logical column type. Closed by construction:
/// nested shapes cannot arrive (a destination that does not advertise
/// structs receives them lowered), and everything else maps to a
/// documented type.
pub(super) fn sql_type(ty: &ColumnType) -> String {
    match ty {
        ColumnType::Scalar { scalar } => match scalar {
            LogicalType::Bool => "BOOLEAN".into(),
            // Exactly i64's range: NUMBER defaults to precision 38, and
            // being explicit keeps a value that fits the engine from
            // landing in a wider column than the schema declares.
            LogicalType::Int64 => "NUMBER(19,0)".into(),
            LogicalType::Float64 => "FLOAT".into(),
            LogicalType::Utf8 => "VARCHAR".into(),
            // No native UUID; the canonical text form round-trips and is
            // what the service's ecosystem uses.
            LogicalType::Uuid => "VARCHAR(36)".into(),
            LogicalType::Json => "VARIANT".into(),
            LogicalType::Binary => "BINARY".into(),
            // Zone-carrying and naive are DIFFERENT types here;
            // collapsing them would silently relocate every timestamp.
            LogicalType::TimestampTz => "TIMESTAMP_TZ".into(),
            LogicalType::TimestampNaive => "TIMESTAMP_NTZ".into(),
            LogicalType::Date => "DATE".into(),
            LogicalType::Time => "TIME".into(),
            LogicalType::Decimal { precision, scale } => format!("NUMBER({precision},{scale})"),
        },
        ColumnType::Struct { .. } | ColumnType::ScalarList { .. } => "VARCHAR".into(),
    }
}

/// The merge-stage TABLE for a target — the shared naming rule, so two
/// pipelines writing one table in one schema cannot truncate each
/// other's staged rows.
pub(super) fn stage_name(pipeline: &str, table: &TableName) -> String {
    rdlt_connector_sqlcore::names::stage_table(pipeline, table.as_str())
}

/// A structural flag on a rendered phase-1 statement — never derived
/// from the statement's TEXT (message-sniffing), always from the
/// [`EnsureStep`] that produced it. `Widen` is the one shape the
/// service can refuse at execution (cross-type, not the in-place
/// VARCHAR-length/NUMBER-precision case): the phase-1 loop uses this
/// to enrich exactly that failure with the manual-migration advice
/// and leave every other statement's error untouched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StmtKind {
    /// A CREATE or ADD COLUMN — never refused for a type reason.
    Plain,
    /// An ALTER COLUMN SET DATA TYPE — the one statement the service
    /// can refuse when the change crosses a type family.
    Widen,
}

/// Phase 1: bring the target (and, for merge, its stage twin) into line
/// with the schema — emitting only what the catalog image lacks.
pub(super) fn table_ddl_stmts(
    pipeline: &str,
    schema: &TableSchema,
    mode: &WriteMode,
    table_type: TableType,
    previous: Option<&TableSchema>,
    catalog: &Catalog,
) -> Vec<(String, StmtKind)> {
    // Merge is the only staged mode on this direct-publish path, so it
    // alone gets a stage leg.
    let plan = ensure::schema_steps(schema, mode, FullLoadPublish::DirectToTarget, previous);
    let leg_name = |leg: Leg| match leg {
        Leg::Target => schema.table.as_str().to_owned(),
        Leg::Stage => stage_name(pipeline, &schema.table),
    };
    let transient = match table_type {
        TableType::Transient => "TRANSIENT ",
        TableType::Permanent => "",
    };
    let mut out = Vec::new();
    for step in plan {
        match step {
            EnsureStep::Table { leg } => {
                let name = leg_name(leg);
                if catalog.exists(&name) {
                    continue;
                }
                let mut columns: Vec<String> = schema
                    .columns
                    .iter()
                    .map(|column| {
                        // NOT NULL only on the target: the stage accepts
                        // whatever arrives — the merge decides what
                        // survives, and a stage constraint would reject
                        // rows the dedup was about to discard anyway.
                        let null = if leg == Leg::Target && !column.nullable {
                            " NOT NULL"
                        } else {
                            ""
                        };
                        format!(
                            "{} {}{null}",
                            quote(&column.name),
                            sql_type(&column.column_type)
                        )
                    })
                    .collect();
                if leg == Leg::Stage {
                    // Arrival order for deterministic dedup. An identity
                    // COLUMN, not a sequence object: it lives and dies
                    // with the table, so a stage cannot outlive its
                    // counter or inherit a stale one.
                    columns.push(format!(
                        "{} NUMBER AUTOINCREMENT",
                        quote(rdlt_connector_sqlcore::names::ARRIVAL_COL)
                    ));
                }
                out.push((
                    format!(
                        "CREATE {transient}TABLE IF NOT EXISTS {} ({})",
                        quote(&name),
                        columns.join(", ")
                    ),
                    StmtKind::Plain,
                ));
            }
            EnsureStep::Column { leg, column } => {
                let name = leg_name(leg);
                let def = &schema.columns[column];
                // The economy this module exists for: a column the image
                // already holds costs nothing. An absent TABLE also
                // skips — its CREATE above carries every column.
                if catalog.has_column(&name, &def.name) || !catalog.exists(&name) {
                    continue;
                }
                out.push((
                    format!(
                        "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {}",
                        quote(&name),
                        quote(&def.name),
                        sql_type(&def.column_type)
                    ),
                    StmtKind::Plain,
                ));
            }
            EnsureStep::Widen { leg, column } => {
                // The service widens in place only along VARCHAR length
                // and NUMBER precision; a cross-type widen (NUMBER to
                // FLOAT/VARCHAR/VARIANT) is refused at execution with a
                // compilation error — loud, and a recorded limitation
                // (generation-1 parity), never a silent misload.
                let def = &schema.columns[column];
                out.push((
                    format!(
                        "ALTER TABLE {} ALTER COLUMN {} SET DATA TYPE {}",
                        quote(&leg_name(leg)),
                        quote(&def.name),
                        sql_type(&def.column_type)
                    ),
                    StmtKind::Widen,
                ));
            }
            _ => unreachable!("phase 1 plans relations and columns only"),
        }
    }
    out
}

/// Phase 2: option validation plus the scd2 validity columns.
///
/// The shared plan's INDEX steps are dropped, deliberately and
/// entirely: this service enforces no unique constraints and needs no
/// supporting index to find rows — an emitted index would spend time to
/// do nothing.
pub(super) fn merge_ensure_stmts(
    options: &DestinationOptions,
    schema: &TableSchema,
    mode: &WriteMode,
    catalog: &Catalog,
) -> Result<Vec<String>, ValidateError> {
    let table = schema.table.as_str();
    let scd2 = options.scd2_for(table);
    let mut out = Vec::new();
    for step in ensure::merge_steps(options, schema, mode)? {
        match step {
            EnsureStep::Validity(which) => {
                let (column, ty) = match which {
                    Validity::From => (&scd2.valid_from, "TIMESTAMP_TZ"),
                    Validity::To => (&scd2.valid_to, "TIMESTAMP_TZ"),
                };
                if catalog.has_column(table, column) {
                    continue;
                }
                // No NOT NULL even on valid_from: the insert arm always
                // supplies the boundary, and ADDING a NOT NULL column to
                // a table that has rows is refused by the service.
                out.push(format!(
                    "ALTER TABLE {} ADD COLUMN IF NOT EXISTS {} {ty}",
                    quote(table),
                    quote(column)
                ));
            }
            EnsureStep::Index(_) => {}
            _ => unreachable!("phase 2 plans validity columns and indexes only"),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use rdlt_connector_sdk::spi::core::{schema::Column, schema::Provenance};

    use super::*;

    fn column(name: &str, scalar: LogicalType, nullable: bool) -> Column {
        Column {
            name: name.to_owned(),
            column_type: ColumnType::Scalar { scalar },
            nullable,
            provenance: Provenance::Inferred,
        }
    }

    fn events() -> TableSchema {
        TableSchema {
            table: TableName::from("events"),
            parent: None,
            columns: vec![
                column("id", LogicalType::Int64, false),
                column("note", LogicalType::Utf8, true),
            ],
        }
    }

    fn merge_mode() -> WriteMode {
        WriteMode::Merge {
            key: vec!["id".to_owned()],
        }
    }

    /// Quoted upper, with embedded quotes doubled — both halves of the
    /// rule, including the reserved-word case.
    #[test]
    fn identifiers_are_quoted_upper_case() {
        assert_eq!(quote("events"), "\"EVENTS\"");
        assert_eq!(quote("Mixed_Case"), "\"MIXED_CASE\"");
        assert_eq!(quote("select"), "\"SELECT\"");
        assert_eq!(quote("with space"), "\"WITH SPACE\"");
        assert_eq!(quote("has\"quote"), "\"HAS\"\"QUOTE\"");
    }

    /// The whole map, pinned — including the explicit NUMBER(19,0) and
    /// the tz/naive distinction.
    #[test]
    fn every_logical_type_maps_to_a_documented_service_type() {
        for (scalar, expected) in [
            (LogicalType::Bool, "BOOLEAN"),
            (LogicalType::Int64, "NUMBER(19,0)"),
            (LogicalType::Float64, "FLOAT"),
            (LogicalType::Utf8, "VARCHAR"),
            (LogicalType::Uuid, "VARCHAR(36)"),
            (LogicalType::Json, "VARIANT"),
            (LogicalType::Binary, "BINARY"),
            (LogicalType::TimestampTz, "TIMESTAMP_TZ"),
            (LogicalType::TimestampNaive, "TIMESTAMP_NTZ"),
            (LogicalType::Date, "DATE"),
            (LogicalType::Time, "TIME"),
        ] {
            assert_eq!(sql_type(&ColumnType::Scalar { scalar }), expected);
        }
        assert_eq!(
            sql_type(&ColumnType::Scalar {
                scalar: LogicalType::Decimal {
                    precision: 18,
                    scale: 4
                }
            }),
            "NUMBER(18,4)"
        );
        assert_ne!(
            sql_type(&ColumnType::scalar(LogicalType::TimestampTz)),
            sql_type(&ColumnType::scalar(LogicalType::TimestampNaive)),
            "collapsing tz and naive would relocate every timestamp"
        );
    }

    /// Unknown table: one CREATE carrying every column, target NOT NULL
    /// honored.
    #[test]
    fn an_unknown_table_is_created_with_every_column() {
        let sql = table_ddl_stmts(
            "p",
            &events(),
            &WriteMode::Append,
            TableType::Permanent,
            None,
            &Catalog::default(),
        );
        assert_eq!(
            sql,
            vec![(
                "CREATE TABLE IF NOT EXISTS \"EVENTS\" \
                 (\"ID\" NUMBER(19,0) NOT NULL, \"NOTE\" VARCHAR)"
                    .to_owned(),
                StmtKind::Plain
            )]
        );
    }

    /// The economy claim itself: a matching table costs zero statements.
    #[test]
    fn a_table_that_already_matches_costs_no_statements_at_all() {
        let mut catalog = Catalog::default();
        catalog.observe(
            "events",
            ["ID".to_owned(), "NOTE".to_owned()].into_iter().collect(),
        );
        let sql = table_ddl_stmts(
            "p",
            &events(),
            &WriteMode::Append,
            TableType::Permanent,
            None,
            &catalog,
        );
        assert!(sql.is_empty(), "steady state emits nothing: {sql:?}");
    }

    /// A partial image yields exactly the missing column.
    #[test]
    fn only_the_genuinely_missing_column_is_added() {
        let mut catalog = Catalog::default();
        catalog.observe("events", ["ID".to_owned()].into_iter().collect());
        let sql = table_ddl_stmts(
            "p",
            &events(),
            &WriteMode::Append,
            TableType::Permanent,
            None,
            &catalog,
        );
        assert_eq!(
            sql,
            vec![(
                "ALTER TABLE \"EVENTS\" ADD COLUMN IF NOT EXISTS \"NOTE\" VARCHAR".to_owned(),
                StmtKind::Plain
            )]
        );
    }

    /// Merge builds the stage twin: arrival autoincrement, no NOT NULL.
    #[test]
    fn merge_creates_a_stage_leg_with_an_autoincrement_arrival_column() {
        let sql = table_ddl_stmts(
            "p",
            &events(),
            &merge_mode(),
            TableType::Permanent,
            None,
            &Catalog::default(),
        );
        assert_eq!(sql.len(), 2, "target and stage: {sql:?}");
        let stage = &sql[1].0;
        assert!(
            stage.contains("__RDLT_ARRIVAL\" NUMBER AUTOINCREMENT"),
            "{stage}"
        );
        assert!(
            !stage.contains("NOT NULL"),
            "the stage accepts what arrives: {stage}"
        );
    }

    /// Direct-publish modes build no stage twin.
    #[test]
    fn append_builds_no_stage_leg() {
        let sql = table_ddl_stmts(
            "p",
            &events(),
            &WriteMode::Append,
            TableType::Permanent,
            None,
            &Catalog::default(),
        );
        assert!(
            !sql.iter().any(|(s, _)| s.contains("__RDLT_ARRIVAL")),
            "{sql:?}"
        );
    }

    /// One table_type choice governs every created table.
    #[test]
    fn transient_applies_to_every_table_this_pipeline_creates() {
        let sql = table_ddl_stmts(
            "p",
            &events(),
            &merge_mode(),
            TableType::Transient,
            None,
            &Catalog::default(),
        );
        assert!(
            sql.iter()
                .all(|(s, _)| s.contains("CREATE TRANSIENT TABLE")),
            "both legs: {sql:?}"
        );
    }

    /// A widening becomes ALTER COLUMN SET DATA TYPE.
    #[test]
    fn a_widened_column_sets_its_data_type() {
        let mut catalog = Catalog::default();
        catalog.observe("events", ["ID".to_owned()].into_iter().collect());
        let narrow = TableSchema {
            table: TableName::from("events"),
            parent: None,
            columns: vec![column("id", LogicalType::Int64, false)],
        };
        let wide = TableSchema {
            table: TableName::from("events"),
            parent: None,
            columns: vec![column("id", LogicalType::Utf8, false)],
        };
        let sql = table_ddl_stmts(
            "p",
            &wide,
            &WriteMode::Append,
            TableType::Permanent,
            Some(&narrow),
            &catalog,
        );
        assert_eq!(
            sql,
            vec![(
                "ALTER TABLE \"EVENTS\" ALTER COLUMN \"ID\" SET DATA TYPE VARCHAR".to_owned(),
                StmtKind::Widen
            )],
            "flagged structurally, from the EnsureStep — not sniffed from the text"
        );
    }

    /// scd2 validity columns: TIMESTAMP_TZ, no NOT NULL, and never an
    /// index.
    #[test]
    fn scd2_adds_validity_columns_and_never_an_index() {
        let options = DestinationOptions {
            merge_strategy: Some(rdlt_connector_sqlcore::MergeStrategy::Scd2),
            ..DestinationOptions::default()
        };
        let mut catalog = Catalog::default();
        catalog.observe("events", ["ID".to_owned()].into_iter().collect());
        let sql = merge_ensure_stmts(&options, &events(), &merge_mode(), &catalog)
            .expect("valid options");
        assert_eq!(sql.len(), 2, "two validity columns, nothing else: {sql:?}");
        for stmt in &sql {
            assert!(stmt.contains("TIMESTAMP_TZ"), "{stmt}");
            assert!(!stmt.contains("NOT NULL"), "{stmt}");
            assert!(!stmt.contains("INDEX"), "{stmt}");
        }
    }

    /// The index drop covers non-scd2 merges too.
    #[test]
    fn no_index_is_ever_emitted() {
        let options = DestinationOptions {
            merge_strategy: Some(rdlt_connector_sqlcore::MergeStrategy::Upsert),
            ..DestinationOptions::default()
        };
        let sql = merge_ensure_stmts(&options, &events(), &merge_mode(), &Catalog::default())
            .expect("valid options");
        assert!(!sql.iter().any(|s| s.contains("INDEX")), "{sql:?}");
    }

    /// The shared validation still fires through this phase — a merge
    /// strategy under Append refuses, naming the table.
    #[test]
    fn merge_only_options_are_refused_under_append_with_the_shared_error() {
        let options = DestinationOptions {
            merge_strategy: Some(rdlt_connector_sqlcore::MergeStrategy::Upsert),
            ..DestinationOptions::default()
        };
        let err = merge_ensure_stmts(&options, &events(), &WriteMode::Append, &Catalog::default())
            .expect_err("refused");
        assert!(format!("{err}").contains("events"), "{err}");
    }
}
