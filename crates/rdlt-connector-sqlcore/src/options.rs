//! The destination options vocabulary + parse-layer validation. The serde
//! spellings are frozen: both the postgres and DuckDB destinations parse the
//! identical YAML/JSON shape, so a rename here would diverge them. Strategy
//! selection NEVER crosses the connector SPI: the engine's WriteMode stays
//! frozen; these options choose how a DESTINATION executes Merge.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// How Merge executes at this destination.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum MergeStrategy {
    /// Atomic delete-then-insert.
    #[default]
    DeleteInsert,
    /// Matched keys update in place via conflict-update; requires a unique
    /// index on the merge identity (auto-ensured).
    Upsert,
    /// Full version history with validity columns.
    Scd2,
}

/// SCD2 settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Scd2Options {
    /// Validity-start column added to the target table.
    #[serde(default = "default_valid_from")]
    pub valid_from: String,
    /// Validity-end column; the ACTIVE version carries
    /// `active_record_timestamp` (default: NULL).
    #[serde(default = "default_valid_to")]
    pub valid_to: String,
    /// What happens to active keys ABSENT from a load (see [`AbsentPolicy`]).
    #[serde(default)]
    pub absent: AbsentPolicy,
    /// The OPEN-version marker written to
    /// `valid_to` instead of NULL — an RFC3339 literal like
    /// `9999-12-31T00:00:00Z` (some BI tools cannot range-query NULLs).
    /// Active-version predicates treat NULL AND the marker as open, so a
    /// table that predates the option (NULL-open rows) keeps working; new
    /// versions open with the marker. Must differ from `boundary_timestamp`.
    #[serde(default)]
    pub active_record_timestamp: Option<String>,
    /// A CALLER-SUPPLIED RFC3339 boundary used
    /// instead of the transaction timestamp for close/open/retire.
    #[serde(default)]
    pub boundary_timestamp: Option<String>,
}

impl Default for Scd2Options {
    fn default() -> Self {
        Self {
            valid_from: default_valid_from(),
            valid_to: default_valid_to(),
            absent: AbsentPolicy::default(),
            active_record_timestamp: None,
            boundary_timestamp: None,
        }
    }
}

/// A timestamp OPTION value must be a real, ZONE-EXPLICIT timestamp before
/// it may appear as a SQL literal (validated here, quoted at generation —
/// never raw user text in a statement). RFC3339 with offset ONLY
/// (`9999-12-31T00:00:00Z`): these literals are compared for EQUALITY
/// against TIMESTAMPTZ columns, and a zone-less literal resolves per
/// session/system TimeZone — the same marker would mean different instants
/// on different machines, silently corrupting scd2 history.
pub(crate) fn validate_timestamp_literal(value: &str) -> bool {
    chrono::DateTime::parse_from_rfc3339(value).is_ok()
}

fn default_valid_from() -> String {
    "_rdlt_valid_from".into()
}

fn default_valid_to() -> String {
    "_rdlt_valid_to".into()
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum AbsentPolicy {
    /// Absent keys keep their active version (incremental feeds are partial).
    #[default]
    Keep,
    /// Absent keys retire at the boundary (full-feed semantics).
    Retire,
}

/// Ordered in-load survivor selection: among same-identity rows within one
/// load, the row this column ranks first survives. Values beat NULL; ties keep
/// the deterministic arrival-order last-wins. `order` is REQUIRED —
/// survivor selection is too important for an implicit default.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DedupSort {
    pub column: String,
    pub order: SortOrder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SortOrder {
    /// Least value survives.
    Asc,
    /// Greatest value survives.
    Desc,
}

/// Per-table overrides.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TableOptions {
    #[serde(default)]
    pub merge_strategy: Option<MergeStrategy>,
    /// CDC-style deletion flag column: flagged rows DELETE their key
    /// instead of merging. Boolean columns compare `= TRUE`, other types
    /// `IS NOT NULL`. Not valid with scd2.
    #[serde(default)]
    pub hard_delete: Option<String>,
    #[serde(default)]
    pub scd2: Option<Scd2Options>,
    /// Ordered survivor selection within one load (see [`DedupSort`]).
    #[serde(default)]
    pub dedup_sort: Option<DedupSort>,
    /// Scope replacement: a non-unique column set;
    /// a merge load replaces every scope present in its delivered rows
    /// (undelivered rows in those scopes disappear), leaving other scopes
    /// untouched. NULL is not a scope. Keyed structured tables only; not
    /// valid with scd2.
    #[serde(default)]
    pub merge_scope: Option<Vec<String>>,
}

/// Destination-wide defaults + per-table overrides.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DestinationOptions {
    /// Destination-wide strategy. `None` = the `delete_insert` default —
    /// and the distinction is LOAD-BEARING: an EXPLICIT strategy under an
    /// append/replace write mode is a typed error at open, while the
    /// unconfigured default never rejects.
    #[serde(default)]
    pub merge_strategy: Option<MergeStrategy>,
    #[serde(default)]
    pub tables: BTreeMap<String, TableOptions>,
}

impl DestinationOptions {
    /// The embedder entry point (mirrors the sources' from_value contract).
    pub fn from_value(value: serde_json::Value) -> Result<Self, String> {
        let options: Self = serde_json::from_value(value).map_err(|e| e.to_string())?;
        options.validate()?;
        Ok(options)
    }

    /// Validation errors NAME the offending field (FR-012). One `check_*` fn
    /// per rule group, run in the order the messages were frozen.
    pub fn validate(&self) -> Result<(), String> {
        for (table, opts) in &self.tables {
            let strategy = opts
                .merge_strategy
                .or(self.merge_strategy)
                .unwrap_or_default();
            check_hard_delete(table, opts, strategy)?;
            check_dedup_sort(table, opts)?;
            check_merge_scope(table, opts, strategy)?;
            check_scd2(table, opts, strategy)?;
        }
        Ok(())
    }

    pub fn strategy_for(&self, table: &str) -> MergeStrategy {
        self.explicit_strategy_for(table).unwrap_or_default()
    }

    /// The strategy the user EXPLICITLY configured for this table (per-table
    /// override, else the destination-wide setting), or `None` when both are
    /// unconfigured.
    pub fn explicit_strategy_for(&self, table: &str) -> Option<MergeStrategy> {
        self.tables
            .get(table)
            .and_then(|t| t.merge_strategy)
            .or(self.merge_strategy)
    }

    pub fn hard_delete_for(&self, table: &str) -> Option<&str> {
        self.tables
            .get(table)
            .and_then(|t| t.hard_delete.as_deref())
    }

    pub fn scd2_for(&self, table: &str) -> Scd2Options {
        self.tables
            .get(table)
            .and_then(|t| t.scd2.clone())
            .unwrap_or_default()
    }

    pub fn dedup_sort_for(&self, table: &str) -> Option<&DedupSort> {
        self.tables.get(table).and_then(|t| t.dedup_sort.as_ref())
    }

    pub fn merge_scope_for(&self, table: &str) -> Option<&[String]> {
        self.tables
            .get(table)
            .and_then(|t| t.merge_scope.as_deref())
    }
}

fn check_hard_delete(
    table: &str,
    opts: &TableOptions,
    strategy: MergeStrategy,
) -> Result<(), String> {
    if let Some(col) = &opts.hard_delete {
        if col.trim().is_empty() {
            return Err(format!("tables.{table}.hard_delete: empty column name"));
        }
        if strategy == MergeStrategy::Scd2 {
            return Err(format!(
                "tables.{table}.hard_delete: not valid with merge_strategy scd2 \
                 (deletion-as-retirement is future work)"
            ));
        }
    }
    Ok(())
}

fn check_dedup_sort(table: &str, opts: &TableOptions) -> Result<(), String> {
    if let Some(dedup) = &opts.dedup_sort
        && dedup.column.trim().is_empty()
    {
        return Err(format!("tables.{table}.dedup_sort: empty column name"));
    }
    Ok(())
}

fn check_merge_scope(
    table: &str,
    opts: &TableOptions,
    strategy: MergeStrategy,
) -> Result<(), String> {
    let Some(scope) = &opts.merge_scope else {
        return Ok(());
    };
    if scope.is_empty() {
        return Err(format!("tables.{table}.merge_scope: empty column list"));
    }
    if scope.iter().any(|c| c.trim().is_empty()) {
        return Err(format!("tables.{table}.merge_scope: empty column name"));
    }
    let mut seen = std::collections::BTreeSet::new();
    if let Some(dup) = scope.iter().find(|c| !seen.insert(c.as_str())) {
        return Err(format!(
            "tables.{table}.merge_scope: column `{dup}` listed twice"
        ));
    }
    // merge_scope COMPOSES with scd2 as SCOPED RETIREMENT — absent keys retire
    // only within delivered scopes. That reading requires `absent: retire`;
    // under `keep` the option would be silently inert, so reject it rather than
    // ignore it.
    if strategy == MergeStrategy::Scd2 {
        let retire =
            opts.scd2.as_ref().map(|s| s.absent).unwrap_or_default() == AbsentPolicy::Retire;
        if !retire {
            return Err(format!(
                "tables.{table}.merge_scope: with merge_strategy scd2 this \
                 scopes RETIREMENT and requires scd2 {{absent: retire}} \
                 — under `keep` it would be silently inert"
            ));
        }
    }
    Ok(())
}

fn check_scd2(table: &str, opts: &TableOptions, strategy: MergeStrategy) -> Result<(), String> {
    let Some(scd2) = &opts.scd2 else {
        return Ok(());
    };
    if strategy != MergeStrategy::Scd2 {
        return Err(format!(
            "tables.{table}.scd2: options present but merge_strategy is \
             {strategy:?} — set merge_strategy: scd2"
        ));
    }
    if scd2.valid_from.trim().is_empty() || scd2.valid_to.trim().is_empty() {
        return Err(format!(
            "tables.{table}.scd2: validity column names must not be empty"
        ));
    }
    if scd2.valid_from == scd2.valid_to {
        return Err(format!(
            "tables.{table}.scd2: valid_from and valid_to must differ"
        ));
    }
    for (field, value) in [
        (
            "active_record_timestamp",
            scd2.active_record_timestamp.as_deref(),
        ),
        ("boundary_timestamp", scd2.boundary_timestamp.as_deref()),
    ] {
        if let Some(value) = value
            && !validate_timestamp_literal(value)
        {
            return Err(format!(
                "tables.{table}.scd2.{field}: `{value}` is not a \
                 zone-explicit timestamp (RFC3339 with offset, \
                 e.g. `9999-12-31T00:00:00Z` — zone-less \
                 literals resolve per session TimeZone)"
            ));
        }
    }
    // A boundary EQUAL to the open marker makes every closed row satisfy the
    // active predicate — reject.
    if let (Some(marker), Some(boundary)) = (
        scd2.active_record_timestamp.as_deref(),
        scd2.boundary_timestamp.as_deref(),
    ) && marker == boundary
    {
        return Err(format!(
            "tables.{table}.scd2: boundary_timestamp equals \
             active_record_timestamp — closed versions would \
             read as ACTIVE; pick a marker outside the data's \
             time range (e.g. 9999-12-31T00:00:00Z)"
        ));
    }
    Ok(())
}
