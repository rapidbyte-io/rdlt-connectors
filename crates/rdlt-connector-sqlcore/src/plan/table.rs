//! Per-table plan inputs: the shred root, the publish column list, and the
//! resolved merge context every SQL destination threads through the arms.

use std::collections::BTreeMap;

use rdlt_connector::core::{TableName, TableSchema};

use crate::dialect::quote_identifier;
use crate::options::{AbsentPolicy, DestinationOptions, MergeStrategy, Scd2Options};

/// The parent-chain depth ceiling for [`root_of`]: shred nesting never
/// approaches this (the engine bounds nesting far below), so the loop is a
/// guard against a cyclic `parent` link — reaching it yields the deepest node
/// walked, never a hang.
const ROOT_DEPTH_BOUND: usize = 64;

/// Walk a table's `parent` chain to its shred root — the table itself when it
/// has no parent. Bounded by `ROOT_DEPTH_BOUND`. Generic over the map's
/// value tail so it serves any `(TableSchema, _)` session table map.
pub fn root_of<V>(tables: &BTreeMap<TableName, (TableSchema, V)>, table: &TableName) -> TableName {
    let mut current = table.clone();
    for _ in 0..ROOT_DEPTH_BOUND {
        match tables.get(&current).and_then(|(s, _)| s.parent.as_ref()) {
            Some(link) => current = link.parent.clone(),
            None => break,
        }
    }
    current
}

/// Quoted, comma-joined logical columns — publishes are ALWAYS by name: the
/// persistent target's column order is historical while the stage carries this
/// run's order, so a positional `SELECT *` would corrupt or break on drift.
pub fn column_list(schema: &TableSchema) -> String {
    column_list_with(schema, quote_identifier)
}

/// The publish column list under a destination's OWN quoting.
///
/// [`column_list`] bakes in the double-quote rule the first two destinations
/// share, which reads as neutral and is not: a destination that folds
/// identifiers to upper case, or quotes differently at all, would get a list
/// that disagrees with every other statement it emits — and the disagreement
/// would surface as a missing-column error at run time, far from the cause.
pub fn column_list_with(schema: &TableSchema, quote: impl Fn(&str) -> String) -> String {
    schema
        .columns
        .iter()
        .map(|c| quote(&c.name))
        .collect::<Vec<_>>()
        .join(", ")
}

/// A table's resolved merge configuration — the per-table state a publish
/// threads through the strategy arms (strategy, scope columns, scd2 settings,
/// retire flag). Resolved ONCE from the destination options; every SQL
/// destination reads the same shape.
#[derive(Debug)]
pub struct MergeContext<'a> {
    pub strategy: MergeStrategy,
    /// merge_scope scope columns, when configured.
    pub scoped: Option<&'a [String]>,
    /// scd2 settings, resolved only under the scd2 strategy.
    pub scd2: Option<Scd2Options>,
    /// scd2 `absent: retire` — full-feed absence semantics.
    pub retire: bool,
}

impl<'a> MergeContext<'a> {
    pub fn resolve(options: &'a DestinationOptions, table: &str) -> Self {
        let strategy = options.strategy_for(table);
        let scoped = options.merge_scope_for(table);
        let scd2 = (strategy == MergeStrategy::Scd2).then(|| options.scd2_for(table));
        let retire = scd2
            .as_ref()
            .is_some_and(|s| s.absent == AbsentPolicy::Retire);
        Self {
            strategy,
            scoped,
            scd2,
            retire,
        }
    }

    /// Whether this table is under the single-commit-unit discipline: scope
    /// replacement and absent-retire each read the stage as "the complete
    /// truth", sound only when the table's full feed arrives in one unit.
    pub fn full_feed(&self) -> bool {
        self.scoped.is_some() || self.retire
    }
}
