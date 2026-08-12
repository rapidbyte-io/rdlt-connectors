//! The shared merge planning: survivor selection, scope replacement,
//! strategy arms, hard-delete decisions, open-time validation, index
//! plans, and the per-table plan inputs (root walk, column list, merge
//! resolution). Every statement's text is produced through the
//! [`MergeDialect`](crate::MergeDialect) seam; the postgres crate's
//! golden-SQL suite pins that text byte-for-byte, so a change that alters
//! emitted SQL is caught there.

mod arms;
mod index;
mod table;
mod validate;

pub use arms::{
    HardDelete, MergePlan, identity_delete_insert_sql, keyed_delete_insert_sql, keyed_upsert_sql,
    scd2_merge_sql, scope_replace_sql, single_unit_violation,
};
pub use index::IndexSpec;
pub use table::{MergeContext, column_list, column_list_with, root_of};
pub use validate::ValidateError;

pub(crate) use index::index_plan;
pub(crate) use validate::{TableFacts, validate_merge, validate_non_merge};
