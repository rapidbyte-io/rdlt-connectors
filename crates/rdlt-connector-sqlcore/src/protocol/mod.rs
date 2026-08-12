//! The load-commit protocol planner: the correctness-critical half of the
//! commit unit. [`plan_commit`] is a PURE function — no driver types cross
//! into it (Principle III) — that decides, from the session tables, the
//! destination options, and the transaction facts a destination has already
//! gathered, the exact ordered [`Step`] program a publish executes. The
//! destinations own only EXECUTION: they run each step's SQL through their own
//! connection + [`MergeDialect`](crate::MergeDialect) seam.
//!
//! The single-unit discipline and scope-replacement ordering are planner
//! decisions here; an executor may not reorder or re-decide them. Golden pins
//! freeze the emitted script for the representative plan matrix
//! (`tests/cases/test_commit_protocol.rs`), and the consuming destinations pin
//! the rendered SQL byte-for-byte from outside.

mod render;
mod script;
mod step;
pub mod unit;

pub use render::{build_merge_plan, insert_select_sql, render_arm};
pub use script::{
    CommitContext, CommitError, CommitScript, plan_commit, prepare_target, staged_probe_targets,
};
pub use step::{FullLoadPublish, MergeArm, Step};
