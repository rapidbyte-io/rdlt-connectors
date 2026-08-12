//! The supporting-index plan: which merge-identity/scope indexes a table needs.
//! The plan names each index's shape; the destination owns the SQL text (via
//! [`crate::names::index_name`] + its own `CREATE INDEX`).

use rdlt_connector::core::schema::system_columns;

use crate::options::{DestinationOptions, MergeStrategy};

/// One planned supporting index: `unique` distinguishes the upsert merge-key
/// index (which enforces the conflict target) from the plain lookup indexes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSpec {
    pub unique: bool,
    pub columns: Vec<String>,
}

impl IndexSpec {
    fn plain(columns: Vec<String>) -> Self {
        Self {
            unique: false,
            columns,
        }
    }

    fn unique(columns: Vec<String>) -> Self {
        Self {
            unique: true,
            columns,
        }
    }
}

/// Index plan: identity indexes per table kind, plus the scope index.
pub fn index_plan(
    options: &DestinationOptions,
    table: &str,
    key: &[String],
    has_identity: bool,
    is_child: bool,
) -> Vec<IndexSpec> {
    let strategy = options.strategy_for(table);
    let mut indexes: Vec<IndexSpec> = Vec::new();
    if has_identity {
        indexes.push(IndexSpec::plain(vec![system_columns::ID.to_string()]));
        if is_child {
            indexes.push(IndexSpec::plain(vec![system_columns::ROOT_ID.to_string()]));
        }
    } else {
        match strategy {
            MergeStrategy::Upsert => indexes.push(IndexSpec::unique(key.to_vec())),
            MergeStrategy::DeleteInsert => indexes.push(IndexSpec::plain(key.to_vec())),
            MergeStrategy::Scd2 => {
                // (key…, valid_to): active-version lookups + retire.
                let scd2 = options.scd2_for(table);
                let mut cols = key.to_vec();
                cols.push(scd2.valid_to.clone());
                indexes.push(IndexSpec::plain(cols));
            }
        }
        // The scope delete probes by the scope columns — without this index
        // it seq-scans the whole target every load.
        if let Some(scope) = options.merge_scope_for(table) {
            indexes.push(IndexSpec::plain(scope.to_vec()));
        }
    }
    indexes
}
