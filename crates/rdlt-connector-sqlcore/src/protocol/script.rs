//! The planner: from session tables, destination options, and the
//! transaction facts, decide the exact ordered step program a publish
//! executes. [`plan_commit`] is PURE — no driver types cross into it — and
//! the single-unit discipline and scope-replacement ordering are decided HERE;
//! an executor may not reorder or re-decide them.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use rdlt_connector::core::commit::WriteMode;
use rdlt_connector::core::{id::TableName, schema::TableSchema};

use crate::options::{DestinationOptions, MergeStrategy};
use crate::plan::{MergeContext, TableFacts, single_unit_violation};

use super::step::{FullLoadPublish, MergeArm, Step};

/// The transaction facts a destination gathers before planning: whether this
/// exact `(load_id, commit_seq)` already committed, whether any unit of this
/// load committed (the Replace once-per-load guard), which tables the
/// single-unit discipline has already counted this load, which full-feed
/// tables staged a non-empty unit (see [`staged_probe_targets`]), how the
/// destination lands full-load rows, and which targets THIS load has already
/// cleared.
///
/// `cleared_targets` is the whole Replace guard on the direct path, and the
/// executor is responsible for seeding it durably — see
/// [`crate::names::CLEARED_TABLE`]. `load_committed_before` answers a
/// different question (did ANY unit of this load commit) and is used only by
/// the staged path, where the clear is planned for every Replace table at
/// once.
#[derive(Debug, Clone, Copy)]
pub struct CommitContext<'a> {
    pub replayed: bool,
    pub load_committed_before: bool,
    pub single_unit_done: &'a BTreeSet<TableName>,
    pub staged_nonempty: &'a BTreeSet<TableName>,
    pub full_load_publish: FullLoadPublish,
    pub cleared_targets: &'a BTreeSet<TableName>,
}

impl CommitContext<'_> {
    /// Whether `table` still round-trips through a stage table — the ensure
    /// planner's rule, consulted here so the two planners cannot drift.
    fn uses_stage(&self, mode: &WriteMode) -> bool {
        crate::ensure::uses_stage(mode, self.full_load_publish)
    }
}

/// What must run against `table` before the FIRST row of this unit is written
/// into it — the direct-to-target counterpart of the publish steps, hoisted to
/// write time because by commit time the rows are already there.
///
/// Empty for every table that still stages, for Append (which never clears),
/// and for a Replace target this load has already cleared. When it is not
/// empty it is `ClearTarget`, and the caller MUST run it inside the same
/// transaction as the writes that follow: outside one, a crash between the
/// clear and the rows would leave the target empty.
pub fn prepare_target(
    tables: &BTreeMap<TableName, (TableSchema, WriteMode)>,
    ctx: &CommitContext<'_>,
    table: &TableName,
) -> Vec<Step> {
    let Some((_, mode)) = tables.get(table) else {
        return Vec::new();
    };
    if ctx.uses_stage(mode) || !matches!(mode, WriteMode::Replace) {
        return Vec::new();
    }
    // Once per (load, target). `cleared_targets` carries BOTH halves now: the
    // executor seeds it from a durable record before asking, so a
    // crash-recovery session with fresh memory sees what earlier units did.
    //
    // It deliberately does NOT consult `load_committed_before`. That is a
    // per-LOAD fact, and using it here answered the wrong question: a Replace
    // table registered in unit 1 but first written in unit 2 found the load
    // already committed, skipped its clear, and appended to the PREVIOUS
    // load's rows. Pinned in the postgres crate's
    // `direct_publish_guarantees` suite.
    if ctx.cleared_targets.contains(table) {
        return Vec::new();
    }
    vec![Step::ClearTarget {
        table: table.clone(),
    }]
}

/// The planner's output: the ordered in-transaction step program, plus the
/// tables whose single-unit discipline the executor marks AFTER the transaction
/// commits (a rolled-back unit never counts).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitScript {
    pub steps: Vec<Step>,
    pub marks: Vec<TableName>,
}

/// A planner decision that fails the unit — surfaced typed so the destination
/// maps it to a fatal `DestinationError` at its SPI boundary with the exact current
/// text (the SPI conversion stays at the destination).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitError {
    /// A full-feed table (merge_scope replacement or scd2 `absent: retire`)
    /// published a second non-empty unit within one load.
    SingleUnit { table: String, scoped: bool },
    /// A merge arm impossible for the stream shape (shredded upsert / scd2).
    /// `ensure_table` rejects these, so reaching it means a driver bypassed
    /// validation — kept as a construction guard.
    ArmUnsupported { table: String, what: &'static str },
}

impl fmt::Display for CommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CommitError::SingleUnit { table, scoped } => {
                write!(f, "{}", single_unit_violation(table, *scoped))
            }
            CommitError::ArmUnsupported { table, what } => {
                write!(f, "table `{table}`: {what}")
            }
        }
    }
}

impl std::error::Error for CommitError {}

/// The single-unit discipline decision for one full-feed table this unit.
enum SingleUnit {
    /// Nothing staged this unit — skip the table's publish (an empty stage
    /// must not read as "every key absent" = mass retirement).
    SkipEmpty,
    /// The table already published a non-empty unit this load — rule violated.
    Violation,
    /// First non-empty unit — mark the table done after the tx commits.
    Mark,
}

fn check_single_unit(already_done: bool, staged: bool) -> SingleUnit {
    if !staged {
        SingleUnit::SkipEmpty
    } else if already_done {
        SingleUnit::Violation
    } else {
        SingleUnit::Mark
    }
}

/// The full-feed merge tables whose stage the executor must probe for emptiness
/// before planning — scope replacement and absent-retire read the stage as the
/// complete truth, so an empty stage means "skip this table this unit", not
/// "mass retirement". The executor probes exactly these and passes the
/// non-empty ones in [`CommitContext::staged_nonempty`]; every other table's stage
/// row count never affects the plan.
pub fn staged_probe_targets<'a>(
    tables: &'a BTreeMap<TableName, (TableSchema, WriteMode)>,
    options: &DestinationOptions,
) -> Vec<&'a TableName> {
    tables
        .iter()
        .filter(|(table, (_, mode))| {
            matches!(mode, WriteMode::Merge { .. })
                && MergeContext::resolve(options, table.as_str()).full_feed()
        })
        .map(|(table, _)| table)
        .collect()
}

fn select_arm(
    table: &str,
    has_identity: bool,
    strategy: MergeStrategy,
    options: &DestinationOptions,
) -> Result<MergeArm, CommitError> {
    Ok(match (has_identity, strategy) {
        (false, MergeStrategy::DeleteInsert) => MergeArm::KeyedDeleteInsert,
        (false, MergeStrategy::Upsert) => MergeArm::KeyedUpsert,
        (true, MergeStrategy::DeleteInsert) => MergeArm::IdentityDeleteInsert,
        // `scd2_for` is TOTAL (it defaults when unset), so the scd2 arm's
        // settings are resolved by construction — no Option to unwrap.
        (false, MergeStrategy::Scd2) => MergeArm::Scd2(options.scd2_for(table)),
        (true, MergeStrategy::Upsert) => {
            return Err(CommitError::ArmUnsupported {
                table: table.to_owned(),
                what: "upsert on a shredded stream",
            });
        }
        (true, MergeStrategy::Scd2) => {
            return Err(CommitError::ArmUnsupported {
                table: table.to_owned(),
                what: "scd2 on a shredded stream",
            });
        }
    })
}

/// Plan the ordered step program for a commit unit — the ONE place the
/// commit-unit decisions and ordering live. Pure: given the same tables,
/// options, and facts, it emits the same script byte-for-byte.
pub fn plan_commit(
    tables: &BTreeMap<TableName, (TableSchema, WriteMode)>,
    options: &DestinationOptions,
    ctx: &CommitContext<'_>,
) -> Result<CommitScript, CommitError> {
    let mut steps = Vec::new();
    let mut marks = Vec::new();

    if ctx.replayed {
        // Replay of a unit that DID commit server-side: the merge SQL never
        // re-runs, but the single-unit discipline still counts each redelivered
        // full-feed unit, and the stages get truncated.
        for (table, (_, mode)) in tables {
            if !matches!(mode, WriteMode::Merge { .. }) {
                continue;
            }
            let merge_context = MergeContext::resolve(options, table.as_str());
            if merge_context.full_feed()
                && !ctx.single_unit_done.contains(table)
                && ctx.staged_nonempty.contains(table)
            {
                marks.push(table.clone());
            }
        }
        for (table, (_, mode)) in tables {
            if ctx.uses_stage(mode) {
                steps.push(Step::TruncateStage {
                    table: table.clone(),
                });
            }
        }
        return Ok(CommitScript { steps, marks });
    }

    // Fresh unit: publish every table, then truncate stages, then persist state
    // + receipt in the SAME transaction (so state and data become visible
    // atomically).
    for (table, (schema, mode)) in tables {
        // A schema without the per-row identity column is a STRUCTURED stream's
        // table — merge (if requested) goes by key. One predicate, owned by
        // the per-table facts, so the planner and validation cannot disagree.
        let has_identity = TableFacts::of(schema).has_identity;
        match mode {
            // Append and Replace publish nothing here on the direct path: the
            // rows are already in the target and the clear (Replace only)
            // happened as the unit transaction's first statement, via
            // `prepare_target`. See `FullLoadPublish`.
            WriteMode::Append if !ctx.uses_stage(mode) => {}
            WriteMode::Replace if !ctx.uses_stage(mode) => {}
            WriteMode::Append => steps.push(Step::InsertSelect {
                table: table.clone(),
            }),
            WriteMode::Replace => {
                // Truncate at most once per LOAD, guarded durably from the
                // receipt log — a crash-recovery session must never re-truncate
                // rows an earlier commit already published.
                if !ctx.load_committed_before {
                    steps.push(Step::ClearTarget {
                        table: table.clone(),
                    });
                }
                steps.push(Step::InsertSelect {
                    table: table.clone(),
                });
            }
            WriteMode::Merge { .. } => {
                let merge_context = MergeContext::resolve(options, table.as_str());
                // Single-unit discipline, PER TABLE: a unit where this table
                // stages NOTHING is skipped outright; a second non-empty unit
                // violates.
                if merge_context.full_feed() {
                    match check_single_unit(
                        ctx.single_unit_done.contains(table),
                        ctx.staged_nonempty.contains(table),
                    ) {
                        SingleUnit::SkipEmpty => continue,
                        SingleUnit::Violation => {
                            return Err(CommitError::SingleUnit {
                                table: table.as_str().to_owned(),
                                // Name the rule that FIRED — under scd2 the
                                // absent-retire rule governs even when a
                                // merge_scope scopes it.
                                scoped: merge_context.scoped.is_some() && !merge_context.retire,
                            });
                        }
                        SingleUnit::Mark => marks.push(table.clone()),
                    }
                }
                // Scope replacement runs BEFORE the strategy arm. NOT for scd2:
                // there the merge_scope scopes RETIREMENT inside the arm —
                // deleting scope rows would destroy history.
                if let Some(scope) = merge_context.scoped
                    && merge_context.strategy != MergeStrategy::Scd2
                {
                    steps.push(Step::ScopeReplace {
                        table: table.clone(),
                        scope: scope.to_vec(),
                    });
                }
                let arm = select_arm(
                    table.as_str(),
                    has_identity,
                    merge_context.strategy,
                    options,
                )?;
                steps.push(Step::MergeArm {
                    table: table.clone(),
                    arm,
                });
            }
        }
    }
    // Truncate stages only after ALL tables published: child-table merges read
    // the ROOT's stage for their delete-by-root-id subquery. Tables that never
    // staged have no stage to truncate.
    for (table, (_, mode)) in tables {
        if ctx.uses_stage(mode) {
            steps.push(Step::TruncateStage {
                table: table.clone(),
            });
        }
    }
    steps.push(Step::UpsertState);
    steps.push(Step::InsertReceipt);
    Ok(CommitScript { steps, marks })
}
