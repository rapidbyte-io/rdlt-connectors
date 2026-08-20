//! Shared scaffolding: the one spelling of a test column, a keyed merge mode,
//! and a parent-linked table shell, so each case file builds its fixtures the
//! same way.

use rdlt_connector::core::commit::WriteMode;
use rdlt_connector::core::{
    id::TableName, schema::Column, schema::ColumnType, schema::ParentLink, schema::Provenance,
    schema::TableSchema, types::LogicalType,
};

/// A nullable inferred scalar column — the only column shape the pins need.
pub fn col(name: &str, scalar: LogicalType) -> Column {
    Column {
        name: name.to_owned(),
        column_type: ColumnType::Scalar { scalar },
        nullable: true,
        provenance: Provenance::Inferred,
    }
}

/// A merge write mode over the named key columns.
pub fn merge(keys: &[&str]) -> WriteMode {
    WriteMode::Merge {
        key: keys.iter().map(|k| k.to_string()).collect(),
    }
}

/// A column-less table shell, optionally linked to a parent one hop up —
/// enough schema for the root-resolution pins.
pub fn linked_table(name: &str, parent: Option<&str>) -> TableSchema {
    TableSchema {
        table: TableName::from(name),
        parent: parent.map(|p| ParentLink {
            parent: TableName::from(p),
            depth: 1,
        }),
        columns: Vec::new(),
    }
}
