//! What a destination must make true about a table before rows are written to
//! it — as an ordered list of DECISIONS, never as SQL.
//!
//! The three SQL destinations do not share a statement program and never
//! will: two of them re-assert every column on every load while a third reads
//! the catalog once and emits nothing when nothing is missing, and only two
//! of them have indexes at all. What they do share is *which facts must
//! hold*, and in what order. That is what lives here; each destination lowers
//! these steps to its own grammar.
//!
//! The split matters beyond tidiness. A rule that lives in one planner is
//! stated once and inherited; the same rule copied into three executors
//! drifts silently, which is exactly how a stage table and a commit plan come
//! to disagree about whether a table is staged at all.

use rdlt_connector::core::{commit::WriteMode, schema::TableSchema};

use crate::options::{DestinationOptions, MergeStrategy};
use crate::plan::{
    IndexSpec, TableFacts, ValidateError, index_plan, validate_merge, validate_non_merge,
};
use crate::protocol::FullLoadPublish;

/// Which physical relation a step concerns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Leg {
    /// The published table a reader queries.
    Target,
    /// The staging twin rows land in first.
    Stage,
}

/// One scd2 validity column, in emission order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Validity {
    /// When the version became current.
    From,
    /// When it stopped being current; open on the active row.
    To,
}

/// One thing that must be true before rows are written. `column` indexes
/// `schema.columns`, so a step stays valid only against the schema it was
/// planned from.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum EnsureStep {
    /// This leg's relation must exist, carrying every column of the schema.
    Table { leg: Leg },
    /// This column must exist on this leg.
    Column { leg: Leg, column: usize },
    /// This column's type changed WITHIN this session and must widen.
    Widen { leg: Leg, column: usize },
    /// An scd2 validity column, on the target only — the stage carries the
    /// stream's shape, not the history's.
    Validity(Validity),
    /// A supporting or arbiter index, on the target only.
    Index(IndexSpec),
}

/// Whether a table round-trips through a stage relation.
///
/// The commit planner and the ensure planner MUST agree on this: a table the
/// commit plan publishes from a stage but ensure never created a stage for
/// fails at write time, and the reverse builds a stage that is written,
/// truncated, and never read. One rule, consulted twice.
pub fn uses_stage(mode: &WriteMode, full_load_publish: FullLoadPublish) -> bool {
    matches!(mode, WriteMode::Merge { .. }) || full_load_publish == FullLoadPublish::Staged
}

/// Phase 1 — the relations and their columns.
///
/// Deliberately infallible and deliberately separate from the option
/// validation in [`merge_steps`]: today's destinations create tables BEFORE
/// checking options, so a planner that validated first would move where a bad
/// configuration fails.
///
/// `previous` is the schema THIS SESSION last ensured, never the live catalog.
/// That is what makes widening a within-run rule: a type that changed between
/// runs is the catalog's business, and a type that changed mid-run is this
/// session's.
pub fn schema_steps(
    schema: &TableSchema,
    mode: &WriteMode,
    full_load_publish: FullLoadPublish,
    previous: Option<&TableSchema>,
) -> Vec<EnsureStep> {
    let mut legs = vec![Leg::Target];
    if uses_stage(mode, full_load_publish) {
        legs.push(Leg::Stage);
    }
    let mut out = Vec::new();
    for leg in legs {
        out.push(EnsureStep::Table { leg });
        for (column, def) in schema.columns.iter().enumerate() {
            out.push(EnsureStep::Column { leg, column });
            if let Some(prev) = previous
                && let Some(old) = prev.column(&def.name)
                && old.column_type != def.column_type
            {
                out.push(EnsureStep::Widen { leg, column });
            }
        }
    }
    out
}

/// Phase 2 — option validation, then the scd2 validity columns and the index
/// plan. Runs only after phase 1 has been APPLIED.
///
/// A non-merge mode still validates: the merge-only options are refused here,
/// with the same typed error on every destination.
pub fn merge_steps(
    options: &DestinationOptions,
    schema: &TableSchema,
    mode: &WriteMode,
) -> Result<Vec<EnsureStep>, ValidateError> {
    let table = schema.table.as_str();
    let WriteMode::Merge { key } = mode else {
        validate_non_merge(options, table)?;
        return Ok(Vec::new());
    };
    let facts = TableFacts::of(schema);
    validate_merge(options, table, key, &facts)?;
    let mut out = Vec::new();
    if options.strategy_for(table) == MergeStrategy::Scd2 {
        out.push(EnsureStep::Validity(Validity::From));
        out.push(EnsureStep::Validity(Validity::To));
    }
    out.extend(
        index_plan(options, table, key, facts.has_identity, facts.is_child)
            .into_iter()
            .map(EnsureStep::Index),
    );
    Ok(out)
}
