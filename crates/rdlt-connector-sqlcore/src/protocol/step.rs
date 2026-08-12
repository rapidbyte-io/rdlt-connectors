//! The step vocabulary: what a commit publish can execute, as data. The
//! planner emits these; an executor renders each through its dialect seam and
//! runs it — it may not reorder or re-decide them.

use rdlt_connector::core::TableName;

use crate::options::Scd2Options;

/// One executable step of a commit publish, in emitted order. Every step names
/// the table it acts on (except the whole-unit state/receipt steps); the
/// executor renders each step's SQL through its dialect and runs it in the
/// publish transaction. The strategy arm carries its RESOLVED shape, never raw
/// SQL — the text is still produced through the dialect seam at execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// Clear a Replace target before its rows land — once per (load, target).
    /// On the staged path the planner emits it in the publish transaction,
    /// ahead of that target's `InsertSelect`; on the direct path
    /// [`prepare_target`](crate::prepare_target) emits it as the unit transaction's first statement,
    /// ahead of the first `COPY` into the target.
    ClearTarget { table: TableName },
    /// Append/Replace publish: by-name `INSERT … SELECT` of the staged rows.
    InsertSelect { table: TableName },
    /// Scope replacement before a merge arm (non-scd2 scoped merge): delete the
    /// delivered scopes from the target.
    ScopeReplace {
        table: TableName,
        scope: Vec<String>,
    },
    /// A resolved merge strategy arm.
    MergeArm { table: TableName, arm: MergeArm },
    /// Truncate a stage table — after ALL tables publish (child merges read the
    /// root's stage), and on the replay path.
    TruncateStage { table: TableName },
    /// Upsert the state document (fresh unit only — same transaction as data).
    UpsertState,
    /// Insert the commit receipt (fresh unit only) — the idempotence key.
    InsertReceipt,
}

/// A resolved merge strategy arm — the planner's arm selection, with the scd2
/// settings resolved BY CONSTRUCTION so the executor never re-derives them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeArm {
    /// Keyed structured delete-insert.
    KeyedDeleteInsert,
    /// Keyed structured upsert (conflict-update on the merge key).
    KeyedUpsert,
    /// Shredded identity delete-insert (subtree replacement by root id).
    IdentityDeleteInsert,
    /// SCD2 retire-then-insert with the resolved settings.
    Scd2(Scd2Options),
}

/// How a destination lands FULL-LOAD rows — the Append and Replace modes.
/// Merge is unaffected either way: it genuinely needs the stage, because its
/// arms join delivered rows against the target.
///
/// This is a property of the DESTINATION, not user configuration: a driver
/// picks the one its engine supports and passes it at every planning call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FullLoadPublish {
    /// Rows land in a stage table; the publish transaction moves them into the
    /// target with `INSERT … SELECT`. Every row is therefore written twice.
    Staged,
    /// Rows land in the target directly, inside the unit transaction. The
    /// target is cleared (Replace only) as that transaction's first statement,
    /// so readers still see the old contents until the unit commits.
    DirectToTarget,
}
