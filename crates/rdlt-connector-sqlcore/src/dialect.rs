//! The dialect seam: destinations own SQL TEXT only. Every hook receives the
//! shape's full semantic inputs and returns SQL; shapes, ordering,
//! survivorship, and unit rules live in [`crate::plan`].
//!
//! The default hook bodies emit standard SQL that both current destinations
//! accept. A destination that cannot honor a shape's exact semantics must
//! surface a TYPED capability gap at validation time — never override a hook
//! with an approximation that silently changes the result.

/// Identifier quoting — the ONE injection-safety implementation for every SQL
/// destination: wrap in double quotes and double any embedded quote. Both
/// current destinations quote identically; the [`MergeDialect::quote`] hook
/// defaults to this, and each destination's own DDL/publish quoting delegates
/// here too, so the escaping rule exists in exactly one place.
pub fn quote_identifier(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// SQL-text generation hooks for the shared merge shapes.
///
/// `Send + Sync`: plans borrow dialects across await points in async sessions.
pub trait MergeDialect: Send + Sync {
    /// Identifier quoting. Both current destinations double-quote with `"`
    /// doubling — the shared [`quote_identifier`] rule.
    fn quote(&self, ident: &str) -> String {
        quote_identifier(ident)
    }

    /// The stage's arrival-order expression (a quoted real column or a
    /// pseudo-column) — the deterministic last-wins tie-breaker.
    fn arrival_order(&self) -> String;

    /// Transaction-stable timestamp expression (the scd2 boundary): every
    /// statement in one commit unit must observe the same instant.
    fn tx_timestamp(&self) -> &'static str {
        "now()"
    }

    /// Remove every row of a table — used to clear a Replace target (first unit
    /// of a load) and to truncate a stage after publish. `table` is already
    /// quoted. The two current destinations differ only here: Postgres
    /// `TRUNCATE TABLE`, DuckDB `DELETE FROM` (temp tables reject TRUNCATE).
    fn clear_table(&self, table: &str) -> String;

    /// The dedup/survivor subquery: one surviving row per identity
    /// group. `sort_prefix` is either empty (arrival last-wins) or
    /// `"col" DIR NULLS LAST, ` from a declared dedup_sort — values beat
    /// NULL, arrival stays the trailing tie-breaker.
    /// Materialize the dedup result once per publish, for dialects that can.
    ///
    /// The scd2 arm references the dedup subquery in THREE statements and the
    /// hard-delete upsert arm in two. Interpolated, the server evaluates —
    /// and therefore re-SORTS — the whole stage once per statement. Named
    /// once, it sorts once.
    ///
    /// `None`, the default, keeps the subquery inline: a dialect that cannot
    /// hold a transaction-scoped temporary relation must not pretend to. The
    /// statement returned MUST create something that disappears with the
    /// transaction, because the publish is the only thing that scopes it.
    fn materialize_dedup(&self, _name: &str, _subquery: &str) -> Option<String> {
        None
    }

    fn dedup_subquery(&self, identity: &str, sort_prefix: &str, stage: &str) -> String {
        format!(
            "(SELECT DISTINCT ON ({identity}) * FROM {stage} ORDER BY {identity}, {sort_prefix}{} DESC)",
            self.arrival_order()
        )
    }

    /// Is this boolean flag SET? A NULL flag is not set.
    ///
    /// `IS TRUE` reads as standard and is not universally accepted — Snowflake
    /// rejects it outright — so the spelling belongs to the dialect. Both this
    /// and [`MergeDialect::flag_unset`] must stay NULL-safe: the flag arrives
    /// from user data and is free to be NULL, and a predicate that evaluated
    /// to NULL there would silently match nothing.
    fn flag_set(&self, column: &str) -> String {
        format!("{column} IS TRUE")
    }

    /// Is this boolean flag NOT set? NULL counts as not set.
    fn flag_unset(&self, column: &str) -> String {
        format!("{column} IS NOT TRUE")
    }

    /// How the incoming row is referred to in an update assignment.
    ///
    /// `EXCLUDED` is the pseudo-table an `ON CONFLICT … DO UPDATE` exposes. A
    /// dialect with no such clause names its source relation instead, and the
    /// assignment list is built from THIS rather than from the assumption that
    /// every dialect has Postgres's spelling.
    fn incoming_ref(&self) -> &'static str {
        "EXCLUDED"
    }

    /// The upsert statement: insert the surviving rows, updating matched keys
    /// in place.
    ///
    /// The default is the `ON CONFLICT` form the first two destinations share.
    /// A dialect without it — Snowflake has no `ON CONFLICT`, and no enforced
    /// unique constraint for one to key on — overrides this with `MERGE INTO`.
    fn upsert_stmt(&self, upsert: &Upsert<'_>) -> String {
        let Upsert {
            target,
            cols_sql,
            source,
            keep,
            key_list,
            ..
        } = upsert;
        let action = match &upsert.action {
            UpsertAction::Nothing => "DO NOTHING".to_owned(),
            UpsertAction::Update { set } => format!("DO UPDATE SET {set}"),
        };
        format!(
            "INSERT INTO {target} ({cols_sql}) SELECT {cols_sql} FROM {source} deduped{keep} \
             ON CONFLICT ({key_list}) {action}"
        )
    }
}

/// What an upsert does with a row whose key is already present.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UpsertAction<'a> {
    /// Overwrite the non-key columns from the incoming row.
    ///
    /// `set` is a rendered assignment list built through
    /// [`MergeDialect::incoming_ref`], so it already speaks the dialect's own
    /// name for the incoming row.
    Update {
        /// `"col" = <incoming>."col", …`
        set: &'a str,
    },
    /// Keep the row that is already there.
    ///
    /// Arises when every column is part of the merge key: there is nothing
    /// left to update, and a dialect must express that rather than emitting an
    /// assignment list that happens to be empty.
    Nothing,
}

/// Everything a dialect needs to render an upsert.
///
/// A struct rather than eight positional arguments, and — the point — the
/// DECISION rather than another dialect's syntax. The action used to arrive as
/// the literal text `DO UPDATE SET …`, which a dialect without `ON CONFLICT`
/// could only consume by taking that string apart: pattern-matching one
/// dialect's SQL to produce another's, with nothing to catch the day the
/// wording changed.
#[derive(Debug)]
pub struct Upsert<'a> {
    /// The target table, quoted.
    pub target: &'a str,
    /// Publish columns, quoted, in order.
    pub columns: &'a [String],
    /// The same columns, comma-joined.
    pub cols_sql: &'a str,
    /// The relation holding the surviving rows — a subquery or a materialized
    /// name; both accept an alias.
    pub source: &'a str,
    /// A trailing `WHERE …` restricting which survivors are inserted (the
    /// hard-delete keep predicate), or empty.
    pub keep: &'a str,
    /// Merge-key columns, quoted.
    pub keys: &'a [String],
    /// The same keys, comma-joined.
    pub key_list: &'a str,
    /// What to do about a key that already exists.
    pub action: UpsertAction<'a>,
}
