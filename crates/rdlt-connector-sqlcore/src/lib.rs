//! # rdlt-connector-sqlcore — the shared merge-planning core
//!
//! ONE core for every SQL destination: the destination options vocabulary +
//! validation ([`options`]), the plan shapes — dedup/survivor ordering, scope
//! replacement, strategy arms, hard-delete decisions, index plans ([`plan`])
//! — and the [`MergeDialect`] seam through which destinations own SQL TEXT and
//! nothing else.
//!
//! The plan shapes are shared by the postgres and DuckDB destinations; the
//! postgres crate's golden-SQL suite pins them byte-for-byte, so any change
//! here that alters emitted SQL is caught there.
//!
//! ONE SPELLING PER ITEM: the re-export list below is the canonical
//! vocabulary — an item listed here is spelled at the crate root everywhere;
//! everything else is reached by its module path. Do not import a root item
//! through its module.
//!
//! The primary workflow is planning a commit unit. [`plan_commit`] is a
//! PURE function, so the whole flow runs without a database: describe the
//! session tables, resolve the options, state the transaction facts, and
//! receive the exact ordered step program the destination will execute:
//!
//! ```
//! use std::collections::{BTreeMap, BTreeSet};
//!
//! use rdlt_connector::core::commit::WriteMode;
//! use rdlt_connector::core::{id::TableName, schema::TableSchema};
//! use rdlt_connector_sqlcore::{CommitContext, DestinationOptions, FullLoadPublish, Step, plan_commit};
//!
//! let mut tables = BTreeMap::new();
//! tables.insert(
//!     TableName::new("events"),
//!     (
//!         TableSchema { table: TableName::new("events"), parent: None, columns: vec![] },
//!         WriteMode::Append,
//!     ),
//! );
//! let empty = BTreeSet::new();
//! let script = plan_commit(
//!     &tables,
//!     &DestinationOptions::default(),
//!     &CommitContext {
//!         replayed: false,
//!         load_committed_before: false,
//!         single_unit_done: &empty,
//!         staged_nonempty: &empty,
//!         full_load_publish: FullLoadPublish::Staged,
//!         cleared_targets: &empty,
//!     },
//! )
//! .unwrap();
//! assert_eq!(
//!     script.steps,
//!     vec![
//!         Step::InsertSelect { table: TableName::new("events") },
//!         Step::TruncateStage { table: TableName::new("events") },
//!         Step::UpsertState,
//!         Step::InsertReceipt,
//!     ],
//! );
//! ```

mod dialect;
pub mod ensure;
pub mod names;
pub mod options;
pub mod plan;
pub mod protocol;

pub use dialect::{MergeDialect, Upsert, UpsertAction, quote_identifier};
pub use options::{
    AbsentPolicy, DedupSort, DestinationOptions, MergeStrategy, Scd2Options, SortOrder,
    TableOptions,
};
pub use plan::{HardDelete, MergeContext, MergePlan, column_list, column_list_with, root_of};
pub use protocol::{
    CommitContext, CommitError, CommitScript, FullLoadPublish, MergeArm, Step, build_merge_plan,
    insert_select_sql, plan_commit, prepare_target, render_arm, staged_probe_targets,
};
