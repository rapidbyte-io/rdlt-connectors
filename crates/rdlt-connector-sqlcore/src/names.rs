//! The SQL-destination protocol naming vocabulary — the `_rdlt_` naming
//! conventions for persisted state, defined ONCE so no destination
//! hand-types a prefix.
//!
//! These names are PERSISTED-FORMAT IDENTITY, not configuration: `read_state`
//! must find the table a previous run wrote, so a runtime-configurable prefix
//! would orphan state (cursor loss → double loading) the moment it changed.
//! A product-wide rename (e.g. platform branding) is a deliberate one-time
//! compile-time decision made HERE, before production data exists — never a
//! per-pipeline option. Tests deliberately keep literal spellings: they pin
//! the on-disk contract and catch an accidental rename.
//!
//! (System COLUMN names — `_rdlt_id` etc. — live in
//! `rdlt_core::schema::system`: the engine writes those. scd2
//! validity defaults live in [`crate::options`] and ARE user-overridable —
//! they name user-visible data columns, not protocol state.)

/// State document table — state commits atomically with the data it describes.
pub const STATE_TABLE: &str = "_rdlt_state";

/// Commit receipt table — idempotence key is (load_id, commit_seq).
pub const COMMITS_TABLE: &str = "_rdlt_commits";

/// Targets a load has already cleared — the durable half of the
/// once-per-(load, target) Replace guard, for destinations that write full
/// loads STRAIGHT INTO the target.
///
/// A staged destination does not need it: there the clear is planned inside
/// the publish transaction for every Replace table at once, so "did this load
/// clear this target" is answered by "did any unit of this load commit".
/// Writing directly, the clear happens at a target's FIRST WRITE, and those
/// firsts are spread across commit units — so the question becomes per-target
/// and needs its own record.
///
/// The record is written in the SAME transaction as the TRUNCATE it describes,
/// so the two cannot disagree: either both are durable or neither happened.
pub const CLEARED_TABLE: &str = "_rdlt_cleared";

/// Stage table name prefix; destinations append their own scoping/hash
/// suffixes.
pub const STAGE_PREFIX: &str = "_rdlt_stage_";

/// Arrival-order column on STAGE tables: what makes merge dedup deterministic
/// ("last wins" for real). Excluded from publish column lists because it is
/// not part of the logical schema.
///
/// A persisted identity like the table names above — a destination that
/// spelled it differently would not find its own staged order after a
/// restart.
pub const ARRIVAL_COL: &str = "__rdlt_arrival";

/// A stage table's name: pipeline-scoped and hashed.
///
/// Scoping stops one pipeline's open from truncating another's live staged
/// rows in a shared schema. Hashing bounds the identifier under the tightest
/// destination limit (Postgres allows 63 bytes), where silent truncation would
/// otherwise cut off exactly the disambiguating suffix and make two pipelines
/// collide precisely when their names are most similar.
pub fn stage_table(pipeline: &str, table: &str) -> String {
    format!(
        "{STAGE_PREFIX}{}_{}",
        rdlt_connector::core::schema::ident_hash(pipeline, 8),
        rdlt_connector::core::schema::ident_hash(table, 16)
    )
}

/// Merge-identity index prefixes (auto-ensured per load): plain and unique.
pub(crate) const INDEX_PREFIX: &str = "rdlt_ix";
pub(crate) const UNIQUE_INDEX_PREFIX: &str = "rdlt_ux";

/// Deterministic supporting-index name — the ONE hash formula every SQL
/// destination shares: the unique/plain prefix plus a bounded hash of
/// `table:col,col`. The hash bounds the identifier under each destination's
/// length limit, and the determinism makes `CREATE INDEX IF NOT EXISTS`
/// idempotent across sessions.
pub fn index_name(unique: bool, table: &str, columns: &[String]) -> String {
    let prefix = if unique {
        UNIQUE_INDEX_PREFIX
    } else {
        INDEX_PREFIX
    };
    format!(
        "{prefix}_{}",
        rdlt_connector::core::schema::ident_hash(&format!("{table}:{}", columns.join(",")), 16)
    )
}

/// The supporting-index DDL, built once for every SQL destination.
///
/// The name formula already lived here; the STATEMENT around it was copied into
/// both executors, differing only in which `quote` they reached for. Two copies
/// of one statement is how the emitted SQL drifts apart without anyone deciding
/// that it should — and index DDL is exactly where that matters, because
/// `IF NOT EXISTS` makes a drifted name silently create a SECOND index rather
/// than fail.
///
/// `quote` is passed in because identifier quoting is the one thing a
/// destination genuinely owns.
pub fn create_index_sql(
    unique: bool,
    table: &str,
    columns: &[String],
    quote: impl Fn(&str) -> String,
) -> String {
    let name = index_name(unique, table, columns);
    let cols = columns
        .iter()
        .map(|c| quote(c))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CREATE {}INDEX IF NOT EXISTS {} ON {} ({cols})",
        if unique { "UNIQUE " } else { "" },
        quote(&name),
        quote(table),
    )
}

/// The mirror of [`create_index_sql`]: `DROP INDEX IF EXISTS` for the SAME
/// deterministic name formula. A caller that needs to clear an index out of
/// an ALTER's way — or retire a legacy naming spelling — reaches for this
/// instead of hand-writing the DROP text a second time, for the identical
/// reason `create_index_sql` exists: two copies of one statement is how the
/// emitted SQL drifts.
pub fn drop_index_sql(
    unique: bool,
    table: &str,
    columns: &[String],
    quote: impl Fn(&str) -> String,
) -> String {
    format!(
        "DROP INDEX IF EXISTS {}",
        quote(&index_name(unique, table, columns))
    )
}

/// The diagnosis for a unique index that cannot be created because the target
/// already holds duplicate keys.
///
/// Shared because it is ADVICE, not a message: it names the strategy that
/// requires the index, the columns that collide, and the two ways out. Both
/// destinations had their own copy of that sentence, so a correction to one
/// would have left the other telling operators something different about the
/// same failure.
pub fn duplicate_merge_key_diagnosis(table: &str, columns: &[String], cause: &str) -> String {
    format!(
        "table `{table}`: cannot create the unique index the upsert strategy \
         requires — existing rows duplicate the merge key ({}); deduplicate the \
         table or use delete_insert: {cause}",
        columns.join(", ")
    )
}
