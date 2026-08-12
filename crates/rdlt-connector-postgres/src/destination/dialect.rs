//! The postgres [`MergeDialect`]: the EXTRACTION SOURCE — every hook keeps
//! the trait default (the defaults ARE this destination's text,
//! golden-pinned in the golden-SQL suite) except the arrival column, which
//! is the stage's real identity column, and the two overrides below.

use rdlt_connector_sqlcore::{MergeDialect, quote_identifier};

#[derive(Debug, Clone, Copy)]
pub struct Dialect;

impl MergeDialect for Dialect {
    fn arrival_order(&self) -> String {
        quote_identifier(rdlt_connector_sqlcore::names::ARRIVAL_COL)
    }

    fn clear_table(&self, table: &str) -> String {
        format!("TRUNCATE TABLE {table}")
    }

    /// `ON COMMIT DROP` ties the relation's life to the publish transaction,
    /// which is exactly its useful span: it exists for the arm's statements
    /// and is gone before anything else could observe or collide with it —
    /// including a retry of this same unit.
    fn materialize_dedup(&self, name: &str, subquery: &str) -> Option<String> {
        Some(format!(
            "CREATE TEMP TABLE {name} ON COMMIT DROP AS SELECT * FROM {subquery} deduped"
        ))
    }
}
