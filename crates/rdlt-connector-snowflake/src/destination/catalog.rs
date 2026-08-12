//! What the account has been observed to hold.
//!
//! Ensure here READS before it writes: the other SQL destinations
//! re-assert every column each load because a local round trip is free,
//! but against a SaaS warehouse each statement is a network round trip.
//! So a session reads a table's columns once, folds everything it later
//! creates into the same image, and the ensure renderer consults the
//! image to emit only what is genuinely missing — a steady-state load
//! issues no schema statements at all.

use std::collections::{BTreeMap, BTreeSet};

use rdlt_connector_sdk::spi::DestinationError;

use super::client::Executor;
use super::ddl::quote;

/// The catalog's image of a name is UPPER case — what
/// `INFORMATION_SCHEMA` holds after the service's folding.
pub(super) fn catalog_name(name: &str) -> String {
    name.to_uppercase()
}

/// One session's image of the catalog.
///
/// Absent from the map means "never read"; present-but-empty means
/// "read, and the table does not exist". The two answer different
/// questions: the first is a reason to look, the second a reason to
/// create.
#[derive(Debug, Default, Clone)]
pub struct Catalog {
    tables: BTreeMap<String, BTreeSet<String>>,
}

impl Catalog {
    /// Record a read's outcome: the table's columns, empty when absent.
    pub fn observe(&mut self, table: &str, columns: BTreeSet<String>) {
        self.tables.insert(catalog_name(table), columns);
    }

    /// Was this table read (or created) this session?
    pub fn is_known(&self, table: &str) -> bool {
        self.tables.contains_key(&catalog_name(table))
    }

    /// Does the table exist, as far as this session has seen?
    pub(super) fn exists(&self, table: &str) -> bool {
        self.tables
            .get(&catalog_name(table))
            .is_some_and(|columns| !columns.is_empty())
    }

    /// Does the table carry this column, as far as this session has
    /// seen?
    pub(super) fn has_column(&self, table: &str, column: &str) -> bool {
        self.tables
            .get(&catalog_name(table))
            .is_some_and(|columns| columns.contains(&catalog_name(column)))
    }

    /// Fold a just-created table into the image, so a second ensure at
    /// the same schema version emits nothing. EXTENDS rather than
    /// replaces: columns never leave a table under additive evolution,
    /// and replacing wiped columns this session recorded that the
    /// schema does not carry (the scd2 validity pair), making a
    /// re-ensure re-render their ALTERs — and, mid-unit, end a unit for
    /// work the service would not perform.
    pub fn record_created(&mut self, table: &str, columns: &[String]) {
        let entry = self.tables.entry(catalog_name(table)).or_default();
        entry.extend(columns.iter().map(|c| catalog_name(c)));
    }

    /// Fold a just-added column into the image.
    pub fn record_column(&mut self, table: &str, column: &str) {
        self.tables
            .entry(catalog_name(table))
            .or_default()
            .insert(catalog_name(column));
    }
}

/// The one read: a table's columns from `INFORMATION_SCHEMA`.
///
/// Names are BOUND, never interpolated. `INFORMATION_SCHEMA` belongs to
/// a DATABASE — qualifying it with the schema names a database that
/// does not exist (the service says so in those words); the schema is a
/// filter column, not a qualifier.
pub(super) fn describe_sql(database: &str) -> String {
    format!(
        "SELECT COLUMN_NAME FROM {}.INFORMATION_SCHEMA.COLUMNS \
         WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?",
        quote(database)
    )
}

/// Read one table's columns, in the catalog's own upper-case image.
pub(super) async fn observe_table(
    executor: &dyn Executor,
    database: &str,
    schema: &str,
    table: &str,
) -> Result<BTreeSet<String>, DestinationError> {
    let rows = executor
        .rows(
            &describe_sql(database),
            &[&catalog_name(schema), &catalog_name(table)],
            &["COLUMN_NAME"],
        )
        .await?;
    Ok(rows.into_iter().map(|mut row| row.remove(0)).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The three states an image distinguishes: unread, read-and-absent,
    /// read-and-present.
    #[test]
    fn unread_absent_and_present_are_three_different_answers() {
        let mut catalog = Catalog::default();
        assert!(!catalog.is_known("events"), "never read");
        assert!(!catalog.exists("events"));

        catalog.observe("events", BTreeSet::new());
        assert!(catalog.is_known("events"), "read, absent");
        assert!(!catalog.exists("events"));

        catalog.observe("events", ["ID".to_owned()].into_iter().collect());
        assert!(catalog.exists("events"), "read, present");
        assert!(catalog.has_column("events", "id"), "case-folded lookup");
        assert!(!catalog.has_column("events", "note"));
    }

    /// Folding a creation or an added column keeps later ensures silent.
    #[test]
    fn recorded_work_counts_as_observed() {
        let mut catalog = Catalog::default();
        catalog.record_created("events", &["id".to_owned(), "note".to_owned()]);
        assert!(catalog.exists("events"));
        assert!(catalog.has_column("events", "NOTE"), "upper image");

        catalog.record_column("events", "added");
        assert!(catalog.has_column("events", "added"));
    }

    /// The describe query binds both names and qualifies only the
    /// database.
    #[test]
    fn the_describe_query_binds_names_and_qualifies_the_database_only() {
        let sql = describe_sql("raw");
        assert!(sql.contains("\"RAW\".INFORMATION_SCHEMA.COLUMNS"), "{sql}");
        assert_eq!(sql.matches('?').count(), 2, "schema and table bound: {sql}");
        assert!(
            !sql.contains("\"RAW\".\""),
            "the schema must not qualify the path: {sql}"
        );
    }
}
