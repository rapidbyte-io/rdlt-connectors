//! Shared representative fixtures: the column mixes the benches and fuzz
//! entries build from — ONE table of literals instead of a copy per caller.

use crate::types::{Column, Kind};

fn column(name: &str, kind: Kind, not_null: bool) -> Column {
    Column {
        name: name.into(),
        kind,
        not_null,
    }
}

/// The bench column mix: int8 pk, int4, float8, text, timestamptz, bool,
/// uuid, jsonb — a representative OLTP row.
pub(crate) fn bench_columns() -> Vec<Column> {
    vec![
        column("id", Kind::Int64, true),
        column("small", Kind::Int32, false),
        column("ratio", Kind::Float64, false),
        column("name", Kind::Text, false),
        column("at", Kind::TimestampTz, false),
        column("ok", Kind::Bool, false),
        column("token", Kind::Uuid, false),
        column("doc", Kind::Jsonb, false),
    ]
}

/// The fuzz column mix — every wire-decode family in one plan.
pub(crate) fn fuzz_columns() -> Vec<Column> {
    vec![
        column("a", Kind::Int64, true),
        column("b", Kind::Text, false),
        column(
            "c",
            Kind::Decimal {
                precision: 10,
                scale: 2,
            },
            false,
        ),
        column("d", Kind::TimestampTz, false),
        column("e", Kind::Uuid, false),
        column("f", Kind::Jsonb, false),
        column("g", Kind::Bytea, false),
        column("h", Kind::Bool, false),
    ]
}
