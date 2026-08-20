//! The partition vocabulary made concrete: config transforms → an
//! Iceberg partition spec at CREATE, and the fixed-at-creation check
//! every later ensure runs against the live table.
//!
//! Partition specs never evolve here — the config either matches what
//! the table was created with, or the refusal tells the operator to
//! drop the table or align the config. One transform mapping is shared
//! by both sides, so the seven-arm match exists exactly once.

use iceberg::spec::{Schema, Transform, UnboundPartitionField, UnboundPartitionSpec};
use rdlt_connector_sdk::spi::error::DestinationError;

use super::client::fatal;
use super::config::{PartitionField, PartitionTransform};

/// The config vocabulary → the library transform. Shared by spec
/// construction and the live drift check.
fn to_transform(transform: PartitionTransform) -> Transform {
    match transform {
        PartitionTransform::Identity => Transform::Identity,
        PartitionTransform::Year => Transform::Year,
        PartitionTransform::Month => Transform::Month,
        PartitionTransform::Day => Transform::Day,
        PartitionTransform::Hour => Transform::Hour,
        PartitionTransform::Bucket(n) => Transform::Bucket(n),
        PartitionTransform::Truncate(w) => Transform::Truncate(w),
    }
}

/// The partition field's NAME, by the Java convention: identity keeps
/// the column's own name, everything else appends its transform
/// (`_day`, `_bucket`, `_trunc`, …).
fn field_name(field: &PartitionField) -> String {
    match field.transform {
        PartitionTransform::Identity => field.column.clone(),
        PartitionTransform::Year => format!("{}_year", field.column),
        PartitionTransform::Month => format!("{}_month", field.column),
        PartitionTransform::Day => format!("{}_day", field.column),
        PartitionTransform::Hour => format!("{}_hour", field.column),
        PartitionTransform::Bucket(_) => format!("{}_bucket", field.column),
        PartitionTransform::Truncate(_) => format!("{}_trunc", field.column),
    }
}

/// Build the spec for CREATE — `None` when unpartitioned.
///
/// Polaris parses the create payload STRICTLY: the spec id and every
/// per-field field id must be present (probed live in 016 — omitting
/// either is a 400). Partition field ids follow the Iceberg
/// convention, ascending from 1000.
pub(super) fn to_partition_spec(
    context: &str,
    schema: &Schema,
    fields: &[PartitionField],
) -> Result<Option<UnboundPartitionSpec>, DestinationError> {
    if fields.is_empty() {
        return Ok(None);
    }
    let mut builder = UnboundPartitionSpec::builder().with_spec_id(0);
    for (field_id, field) in (1000..).zip(fields.iter()) {
        let source = schema.field_by_name(&field.column).ok_or_else(|| {
            fatal(format!(
                "{context}: partition_by names unknown column `{}`",
                field.column
            ))
        })?;
        let transform = to_transform(field.transform);
        let unbound = UnboundPartitionField::builder()
            .source_id(source.id)
            .field_id(field_id)
            .name(field_name(field))
            .transform(transform)
            .build();
        builder = builder.add_partition_fields([unbound]).map_err(|e| {
            fatal(format!(
                "{context}: partition field `{}` ({transform}): {e}",
                field.column
            ))
        })?;
    }
    Ok(Some(builder.build()))
}

/// The fixed-at-creation check: the configured (column, transform)
/// pairs must equal the live table's default spec, in order. The
/// refusal renders both sides so the operator sees the disagreement
/// without opening the catalog.
pub(super) fn check_live_spec(
    context: &str,
    table: &iceberg::table::Table,
    fields: &[PartitionField],
) -> Result<(), DestinationError> {
    let schema = table.metadata().current_schema();
    let live: Vec<(String, Transform)> = table
        .metadata()
        .default_partition_spec()
        .fields()
        .iter()
        .map(|f| {
            // The live side resolves its source column by id; a column
            // the schema no longer names renders as `#{id}` rather
            // than failing the check it is part of.
            let column = schema
                .field_by_id(f.source_id)
                .map(|s| s.name.clone())
                .unwrap_or_else(|| format!("#{}", f.source_id));
            (column, f.transform)
        })
        .collect();
    let wanted: Vec<(String, Transform)> = fields
        .iter()
        .map(|f| (f.column.clone(), to_transform(f.transform)))
        .collect();
    if live != wanted {
        return Err(fatal(format!(
            "{context}: configured partition_by [{}] does not match the live table's partition \
             spec [{}] — partition specs are fixed at creation (drop the table or align the \
             config)",
            render(&wanted),
            render(&live)
        )));
    }
    Ok(())
}

/// `column:transform` pairs joined by `", "` — Display, never Debug.
fn render(pairs: &[(String, Transform)]) -> String {
    pairs
        .iter()
        .map(|(column, transform)| format!("{column}:{transform}"))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(test)]
mod tests {
    use rdlt_connector_sdk::spi::core::{
        id::TableName, schema::Column, schema::ColumnType, schema::Provenance, schema::TableSchema,
        types::LogicalType,
    };

    use super::super::schema::to_iceberg_schema;
    use super::*;

    fn field(column: &str, transform: PartitionTransform) -> PartitionField {
        PartitionField {
            column: column.into(),
            transform,
        }
    }

    fn schema() -> Schema {
        to_iceberg_schema(&TableSchema {
            table: TableName::from("t"),
            parent: None,
            columns: ["region", "ts"]
                .into_iter()
                .map(|name| Column {
                    name: name.into(),
                    column_type: ColumnType::scalar(if name == "ts" {
                        LogicalType::TimestampTz
                    } else {
                        LogicalType::Utf8
                    }),
                    nullable: false,
                    provenance: Provenance::Inferred,
                })
                .collect(),
        })
        .expect("schema")
    }

    /// Spec id 0, field ids from 1000, and the naming convention —
    /// the strict-parser facts Polaris probed live.
    #[test]
    fn the_spec_carries_ids_and_java_convention_names() {
        let spec = to_partition_spec(
            "table `t`",
            &schema(),
            &[
                field("region", PartitionTransform::Identity),
                field("ts", PartitionTransform::Day),
            ],
        )
        .expect("builds")
        .expect("present");
        assert_eq!(spec.spec_id(), Some(0));
        let fields = spec.fields();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].field_id, Some(1000));
        assert_eq!(fields[0].name, "region", "identity keeps the column name");
        assert_eq!(fields[1].field_id, Some(1001));
        assert_eq!(fields[1].name, "ts_day");
    }

    /// Each transform's name suffix, in one sweep.
    #[test]
    fn every_transform_names_by_the_convention() {
        for (transform, expected) in [
            (PartitionTransform::Identity, "c"),
            (PartitionTransform::Year, "c_year"),
            (PartitionTransform::Month, "c_month"),
            (PartitionTransform::Day, "c_day"),
            (PartitionTransform::Hour, "c_hour"),
            (PartitionTransform::Bucket(16), "c_bucket"),
            (PartitionTransform::Truncate(8), "c_trunc"),
        ] {
            assert_eq!(field_name(&field("c", transform)), expected);
        }
    }

    /// An unpartitioned table builds no spec at all.
    #[test]
    fn no_fields_means_no_spec() {
        assert!(
            to_partition_spec("table `t`", &schema(), &[])
                .expect("ok")
                .is_none()
        );
    }

    /// An unknown column refuses with the frozen spelling.
    #[test]
    fn an_unknown_column_is_typed_never_guessed() {
        let err = to_partition_spec(
            "table `t`",
            &schema(),
            &[field("nope", PartitionTransform::Day)],
        )
        .expect_err("refused");
        assert!(
            format!("{err}").contains("table `t`: partition_by names unknown column `nope`"),
            "{err}"
        );
    }
}
