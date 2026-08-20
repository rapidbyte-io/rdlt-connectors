//! The closed engine→Iceberg type mapping, and drift.
//!
//! CLOSED means an unmappable column is a typed error naming the
//! column, never a guess — the engine's type enum is non_exhaustive
//! upstream, and a new variant must land here as a refusal.
//!
//! Field IDs are assigned sequentially from 1, DEPTH-FIRST, at
//! creation only; the catalog renumbers LEVEL-ORDER on create, which
//! is exactly why the drift comparison below ignores IDs entirely and
//! compares STRUCTURE — comparing IDs made the second load of a
//! struct-bearing table read as contradictory drift in generation 1.

use iceberg::spec::{ListType, NestedField, PrimitiveType, Schema, StructType, Type};
use rdlt_connector_sdk::spi::core::{
    schema::Column, schema::ColumnType, schema::TableSchema, types::LogicalType,
};
use rdlt_connector_sdk::spi::error::DestinationError;

use super::client::fatal;

/// Map one scalar type. `Json` maps to string — Iceberg v2 has no JSON
/// type (the variant type is future work; the capability says so).
fn scalar_type(table: &str, column: &str, scalar: LogicalType) -> Result<Type, DestinationError> {
    Ok(Type::Primitive(match scalar {
        LogicalType::Bool => PrimitiveType::Boolean,
        LogicalType::Int64 => PrimitiveType::Long,
        LogicalType::Float64 => PrimitiveType::Double,
        LogicalType::Decimal { precision, scale } => PrimitiveType::Decimal {
            precision: precision as u32,
            scale: scale as u32,
        },
        LogicalType::Utf8 => PrimitiveType::String,
        LogicalType::Binary => PrimitiveType::Binary,
        LogicalType::TimestampTz => PrimitiveType::Timestamptz,
        LogicalType::TimestampNaive => PrimitiveType::Timestamp,
        LogicalType::Date => PrimitiveType::Date,
        LogicalType::Time => PrimitiveType::Time,
        LogicalType::Uuid => PrimitiveType::Uuid,
        LogicalType::Json => PrimitiveType::String,
        #[allow(unreachable_patterns)]
        other => {
            return Err(fatal(format!(
                "table `{table}` column `{column}`: engine type {other:?} has no iceberg mapping \
                 (the engine-type → iceberg-type mapping is closed)"
            )));
        }
    }))
}

/// One column as a field, consuming IDs from the running counter —
/// list elements and struct children draw from the same counter, which
/// is what "depth-first" means concretely.
fn build_field(
    table: &str,
    column: &Column,
    next_id: &mut i32,
) -> Result<NestedField, DestinationError> {
    let id = *next_id;
    *next_id += 1;
    let field_type = match &column.column_type {
        ColumnType::Scalar { scalar } => scalar_type(table, &column.name, *scalar)?,
        ColumnType::ScalarList { item } => {
            let element = scalar_type(table, &column.name, *item)?;
            let element_id = *next_id;
            *next_id += 1;
            // The element is created NON-required: the engine's list
            // type carries no item nullability, and optional is the
            // shape that can hold whatever arrives.
            Type::List(ListType {
                element_field: NestedField::list_element(element_id, element, false).into(),
            })
        }
        ColumnType::Struct { fields } => {
            let mut children = Vec::with_capacity(fields.len());
            for child in fields {
                children.push(build_field(table, child, next_id)?.into());
            }
            Type::Struct(StructType::new(children))
        }
    };
    Ok(if column.nullable {
        NestedField::optional(id, &column.name, field_type)
    } else {
        NestedField::required(id, &column.name, field_type)
    })
}

/// The engine schema as an Iceberg schema, for CREATE.
pub(super) fn to_iceberg_schema(schema: &TableSchema) -> Result<Schema, DestinationError> {
    let table = schema.table.as_str();
    let mut next_id = 1i32;
    let mut fields = Vec::with_capacity(schema.columns.len());
    for column in &schema.columns {
        fields.push(build_field(table, column, &mut next_id)?.into());
    }
    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|e| fatal(format!("table `{table}`: building iceberg schema: {e}")))
}

/// How a wanted column can disagree with the live table — each is a
/// typed refusal, because contradictory drift is never applied.
pub(super) enum Drift {
    /// The scalar/shape differs.
    Type { wanted: String, live: String },
    /// A nested field was added, removed or renamed.
    NestedFields { wanted: String, live: String },
    /// The live table requires a value this stream may not supply.
    Nullability,
}

impl Drift {
    /// The frozen rendering, embedded by the caller's context frame.
    pub(super) fn detail(&self) -> String {
        match self {
            Drift::Type { wanted, live } => {
                format!("stream type {wanted} conflicts with the table's {live}")
            }
            Drift::NestedFields { wanted, live } => format!(
                "stream shape {wanted} conflicts with the table's {live} — a nested field was \
                 added, removed or renamed, which additive evolution cannot express"
            ),
            Drift::Nullability => "the table requires a value this stream may not supply".into(),
        }
    }
}

/// Compare a wanted field against the live one — STRUCTURALLY, ids
/// ignored (the catalog renumbers level-order against our depth-first
/// assignment). Nullability is ASYMMETRIC on purpose: a live optional
/// column accepting a required stream is exactly what additive
/// evolution creates; the reverse loses data.
pub(super) fn compare_field(wanted: &NestedField, live: &NestedField) -> Option<Drift> {
    if live.required && !wanted.required {
        return Some(Drift::Nullability);
    }
    compare_type(&wanted.field_type, &live.field_type)
}

fn type_drift(wanted: &Type, live: &Type) -> Drift {
    Drift::Type {
        wanted: wanted.to_string(),
        live: live.to_string(),
    }
}

fn nested_drift(wanted: &Type, live: &Type) -> Drift {
    Drift::NestedFields {
        wanted: wanted.to_string(),
        live: live.to_string(),
    }
}

fn compare_type(wanted: &Type, live: &Type) -> Option<Drift> {
    match (wanted, live) {
        (Type::Primitive(w), Type::Primitive(l)) => (w != l).then(|| type_drift(wanted, live)),
        (Type::Struct(w), Type::Struct(l)) => {
            if w.fields().len() != l.fields().len() {
                return Some(nested_drift(wanted, live));
            }
            // By NAME, not position: the catalog owns the order.
            for wanted_child in w.fields() {
                let Some(live_child) = l.fields().iter().find(|f| f.name == wanted_child.name)
                else {
                    return Some(nested_drift(wanted, live));
                };
                if let Some(drift) = compare_field(wanted_child, live_child) {
                    return Some(drift);
                }
            }
            None
        }
        (Type::List(w), Type::List(l)) => compare_field(&w.element_field, &l.element_field),
        (Type::Map(w), Type::Map(l)) => compare_field(&w.key_field, &l.key_field)
            .or_else(|| compare_field(&w.value_field, &l.value_field)),
        _ => Some(type_drift(wanted, live)),
    }
}

#[cfg(test)]
mod tests {
    use rdlt_connector_sdk::spi::core::{id::TableName, schema::Provenance};

    use super::*;

    fn column(name: &str, column_type: ColumnType, nullable: bool) -> Column {
        Column {
            name: name.into(),
            column_type,
            nullable,
            provenance: Provenance::Inferred,
        }
    }

    fn table_of(columns: Vec<Column>) -> TableSchema {
        TableSchema {
            table: TableName::from("t"),
            parent: None,
            columns,
        }
    }

    /// The whole closed table, pinned — including Json→String.
    #[test]
    fn every_logical_type_maps_to_its_iceberg_type() {
        for (scalar, expected) in [
            (LogicalType::Bool, PrimitiveType::Boolean),
            (LogicalType::Int64, PrimitiveType::Long),
            (LogicalType::Float64, PrimitiveType::Double),
            (LogicalType::Utf8, PrimitiveType::String),
            (LogicalType::Binary, PrimitiveType::Binary),
            (LogicalType::TimestampTz, PrimitiveType::Timestamptz),
            (LogicalType::TimestampNaive, PrimitiveType::Timestamp),
            (LogicalType::Date, PrimitiveType::Date),
            (LogicalType::Time, PrimitiveType::Time),
            (LogicalType::Uuid, PrimitiveType::Uuid),
            (LogicalType::Json, PrimitiveType::String),
        ] {
            let ty = scalar_type("t", "c", scalar).expect("mapped");
            assert_eq!(ty, Type::Primitive(expected));
        }
        assert_eq!(
            scalar_type(
                "t",
                "c",
                LogicalType::Decimal {
                    precision: 18,
                    scale: 4
                }
            )
            .expect("mapped"),
            Type::Primitive(PrimitiveType::Decimal {
                precision: 18,
                scale: 4
            })
        );
    }

    /// Depth-first IDs from one running counter: a struct's children
    /// and a list's element consume IDs before the next top-level
    /// column.
    #[test]
    fn field_ids_run_depth_first_from_one() {
        let schema = to_iceberg_schema(&table_of(vec![
            column(
                "s",
                ColumnType::Struct {
                    fields: vec![
                        column("a", ColumnType::scalar(LogicalType::Int64), true),
                        column("b", ColumnType::scalar(LogicalType::Utf8), true),
                    ],
                },
                true,
            ),
            column(
                "l",
                ColumnType::ScalarList {
                    item: LogicalType::Int64,
                },
                true,
            ),
            column("z", ColumnType::scalar(LogicalType::Bool), true),
        ]))
        .expect("schema");
        // s=1 (a=2, b=3), l=4 (element=5), z=6.
        assert_eq!(schema.field_by_name("s").expect("s").id, 1);
        assert_eq!(schema.field_by_name("l").expect("l").id, 4);
        assert_eq!(schema.field_by_name("z").expect("z").id, 6);
    }

    /// The drift rules: ids ignored, structs by name, nullability
    /// asymmetric, and each refusal's frozen wording.
    #[test]
    fn drift_ignores_ids_compares_names_and_is_nullability_asymmetric() {
        let wanted = NestedField::required(1, "c", Type::Primitive(PrimitiveType::Long));
        let live_renumbered = NestedField::required(99, "c", Type::Primitive(PrimitiveType::Long));
        assert!(
            compare_field(&wanted, &live_renumbered).is_none(),
            "ids ignored"
        );

        let live_optional = NestedField::optional(2, "c", Type::Primitive(PrimitiveType::Long));
        assert!(
            compare_field(&wanted, &live_optional).is_none(),
            "live optional accepts a required stream (additive evolution creates this)"
        );
        let wanted_optional = NestedField::optional(1, "c", Type::Primitive(PrimitiveType::Long));
        let live_required = NestedField::required(2, "c", Type::Primitive(PrimitiveType::Long));
        let drift = compare_field(&wanted_optional, &live_required).expect("drift");
        assert_eq!(
            drift.detail(),
            "the table requires a value this stream may not supply"
        );

        let live_string = NestedField::required(3, "c", Type::Primitive(PrimitiveType::String));
        let drift = compare_field(&wanted, &live_string).expect("drift");
        assert_eq!(
            drift.detail(),
            "stream type long conflicts with the table's string"
        );
    }

    /// Struct drift: same fields in a different ORDER is fine; a
    /// renamed child is nested-field drift with the frozen advice.
    #[test]
    fn struct_children_compare_by_name_not_position() {
        let wanted = NestedField::optional(
            1,
            "s",
            Type::Struct(StructType::new(vec![
                NestedField::optional(2, "a", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(3, "b", Type::Primitive(PrimitiveType::String)).into(),
            ])),
        );
        let live_swapped = NestedField::optional(
            9,
            "s",
            Type::Struct(StructType::new(vec![
                NestedField::optional(21, "b", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(22, "a", Type::Primitive(PrimitiveType::Long)).into(),
            ])),
        );
        assert!(compare_field(&wanted, &live_swapped).is_none());

        let live_renamed = NestedField::optional(
            9,
            "s",
            Type::Struct(StructType::new(vec![
                NestedField::optional(21, "a", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(22, "renamed", Type::Primitive(PrimitiveType::String)).into(),
            ])),
        );
        let drift = compare_field(&wanted, &live_renamed).expect("drift");
        assert!(
            drift
                .detail()
                .contains("which additive evolution cannot express"),
            "{}",
            drift.detail()
        );
    }

    /// Schema-build failures carry the frozen context frame.
    #[test]
    fn a_schema_that_cannot_build_names_the_table() {
        let err = to_iceberg_schema(&table_of(vec![
            column("dup", ColumnType::scalar(LogicalType::Int64), true),
            column("dup", ColumnType::scalar(LogicalType::Int64), true),
        ]))
        .expect_err("duplicate names cannot build");
        assert!(
            format!("{err}").contains("table `t`: building iceberg schema: "),
            "{err}"
        );
    }
}
