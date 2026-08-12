//! The sqlcore dialect seam — DuckDB keeps every default hook (each
//! backed by a feasibility probe in the test suite) and overrides
//! exactly two facts of its own.

use rdlt_connector_sqlcore::MergeDialect;

/// DuckDB's answers to the shared merge core.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct DuckDialect;

impl MergeDialect for DuckDialect {
    /// Temp stages carry no arrival column; `rowid` reflects append
    /// order, giving the dedup tie-breaker a deterministic last-wins.
    fn arrival_order(&self) -> String {
        "rowid".to_owned()
    }

    /// Temp-table stages reject TRUNCATE; one spelling clears both a
    /// stage and a Replace target.
    fn clear_table(&self, table: &str) -> String {
        format!("DELETE FROM {table}")
    }
}
