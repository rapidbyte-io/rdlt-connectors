//! The strategy arms + their building blocks: hard-delete semantics, the
//! survivor/dedup subquery, scope replacement, the keyed/identity/scd2 shapes,
//! and the single-commit-unit message. Every statement's text is produced
//! through the [`MergeDialect`] seam; the postgres crate's golden-SQL suite
//! pins that text byte-for-byte, so a change here that alters emitted SQL is
//! caught there.

use rdlt_connector::core::{TableSchema, schema::system_columns};

use crate::dialect::{MergeDialect, Upsert, UpsertAction};
use crate::options::{AbsentPolicy, DedupSort, Scd2Options, SortOrder};

/// Hard-delete flag semantics: boolean columns compare `IS TRUE`,
/// other types `IS NOT NULL` — both NULL-safe on the KEEP side.
#[derive(Debug)]
pub struct HardDelete {
    pub(crate) flagged: String,
    pub(crate) keep: String,
}

impl HardDelete {
    pub fn new(column: &str, root_schema: &TableSchema, dialect: &dyn MergeDialect) -> Self {
        use rdlt_connector::core::{ColumnType, LogicalType};
        let is_bool = matches!(
            root_schema.column(column).map(|c| &c.column_type),
            Some(ColumnType::Scalar {
                scalar: LogicalType::Bool
            })
        );
        let col = dialect.quote(column);
        if is_bool {
            Self {
                flagged: dialect.flag_set(&col),
                keep: dialect.flag_unset(&col),
            }
        } else {
            Self {
                flagged: format!("{col} IS NOT NULL"),
                keep: format!("{col} IS NULL"),
            }
        }
    }
}

/// Everything a strategy arm needs about one table's publish.
pub struct MergePlan<'a> {
    pub dialect: &'a dyn MergeDialect,
    /// Fields suffixed `_sql` hold pre-rendered, already-quoted SQL operands the
    /// arm interpolates verbatim; unsuffixed fields (`key`, `merge_scope`) carry
    /// raw column names the arm quotes itself.
    pub target_sql: &'a str,
    pub stage_sql: &'a str,
    pub cols_sql: &'a str,
    pub schema: &'a TableSchema,
    pub key: &'a [String],
    pub root_stage_sql: String,
    pub is_child: bool,
    pub hard_delete: Option<HardDelete>,
    /// Ordered in-load survivor selection; None keeps arrival-order last-wins.
    pub dedup_sort: Option<&'a DedupSort>,
    /// The table's merge_scope columns. For
    /// non-scd2 strategies the caller runs scope REPLACEMENT before the arm;
    /// for scd2 this SCOPES RETIREMENT instead (retire absent keys only
    /// within delivered scopes).
    pub merge_scope: Option<&'a [String]>,
}

impl std::fmt::Debug for MergePlan<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergePlan")
            .field("target_sql", &self.target_sql)
            .finish_non_exhaustive()
    }
}

impl MergePlan<'_> {
    fn quote(&self, ident: &str) -> String {
        self.dialect.quote(ident)
    }

    /// The hard-delete survivor filter, or nothing when the option is off.
    fn keep_clause(&self) -> String {
        self.hard_delete
            .as_ref()
            .map(|hd| format!(" WHERE {}", hd.keep))
            .unwrap_or_default()
    }

    fn key_list(&self) -> String {
        self.key
            .iter()
            .map(|k| self.quote(k))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// In-batch dedup over the stage: arrival-order last-wins,
    /// or ordered survivor selection when `dedup_sort`
    /// is declared. Values beat NULL (`NULLS LAST` both directions); the
    /// arrival order stays as the trailing tie-breaker, so ties and
    /// all-NULL groups keep the deterministic last-wins. EVERY strategy's
    /// survivor decision flows through here.
    fn deduped(&self, identity: &str) -> String {
        let sort = match self.dedup_sort {
            Some(d) => {
                let dir = match d.order {
                    SortOrder::Asc => "ASC",
                    SortOrder::Desc => "DESC",
                };
                format!("{} {dir} NULLS LAST, ", self.quote(&d.column))
            }
            None => String::new(),
        };
        self.dialect.dedup_subquery(identity, &sort, self.stage_sql)
    }

    /// The dedup result, evaluated ONCE when the dialect can materialize it.
    ///
    /// Returns the statements to run first (empty when it cannot) and the SQL
    /// to reference in place of the subquery. Both forms are usable wherever
    /// the subquery was — a table accepts the same alias a subquery requires —
    /// so callers interpolate the second value exactly as they used to.
    ///
    /// Only worth doing where the arm references it MORE THAN ONCE; a
    /// single-reference arm would pay for a temporary relation to save
    /// nothing.
    fn dedup_once(&self, identity: &str) -> (Vec<String>, String) {
        let subquery = self.deduped(identity);
        // Derived from the stage name, which is already unique per (pipeline,
        // table) and length-bounded — so two tables publishing in one
        // transaction cannot collide, and the name cannot outgrow the
        // identifier limit.
        let name = format!(
            "rdlt_dd_{}",
            rdlt_connector::core::naming::ident_hash(self.stage_sql, 16)
        );
        match self.dialect.materialize_dedup(&name, &subquery) {
            Some(statement) => (vec![statement], name),
            None => (Vec::new(), subquery),
        }
    }

    /// `SET c = EXCLUDED.c, …` over the non-identity columns.
    fn update_set(&self, identity: &[String]) -> String {
        self.schema
            .columns
            .iter()
            .filter(|c| !identity.contains(&c.name))
            .map(|c| {
                format!(
                    "{q} = {incoming}.{q}",
                    q = self.quote(&c.name),
                    incoming = self.dialect.incoming_ref()
                )
            })
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// Roots flagged for hard deletion — decided from the DEDUPED last-wins
    /// root row. Reading the RAW stage instead would disagree with survival
    /// when a root is flagged then re-created in the same load. Takes the
    /// resolved [`HardDelete`] directly, so the child hard-delete arm needs no
    /// "is it present?" unwrap — the flag's presence is proven by the caller's
    /// match.
    fn flagged_roots(&self, hd: &HardDelete) -> String {
        let id = self.quote(system_columns::ID);
        // Through the DIALECT seam, not a hand-rolled copy of it. This arm used
        // to spell `DISTINCT ON` itself, which meant a dialect overriding
        // `dedup_subquery` — because it has no DISTINCT ON, or spells last-wins
        // differently — had its override silently ignored HERE while being
        // honoured everywhere else. One survivor decision, one seam.
        //
        // No sort prefix: hard-delete flagging reads the arrival-order last-wins
        // root, which is what it did before and what the golden pins hold. A
        // `dedup_sort` would change WHICH row is consulted, so applying it here
        // is a behaviour change, not a refactor.
        let deduped = self.dialect.dedup_subquery(&id, "", &self.root_stage_sql);
        format!("(SELECT {id} FROM {deduped} d WHERE {})", hd.flagged)
    }
}

/// Scope replacement: delete every target row whose scope matches a DELIVERED
/// stage scope, inside the publish transaction, in the load's FIRST commit
/// unit only (the caller enforces the single-unit rule). NULL is not a scope:
/// stage rows with any NULL scope column are excluded explicitly, and
/// target-side row comparison is never TRUE against NULL. Committed-unit
/// redelivery exits before merge SQL, so the delete can never double-fire.
pub fn scope_replace_sql(
    dialect: &dyn MergeDialect,
    target: &str,
    stage: &str,
    scope: &[String],
) -> String {
    let (cols, not_null) = scope_predicate(scope, |c| dialect.quote(c));
    format!(
        "DELETE FROM {target} WHERE ({cols}) IN (
             SELECT {cols} FROM {stage} WHERE {not_null})"
    )
}

/// Keyed structured delete-insert, with the hard-delete arm.
///
/// NULL keys: the `(key) IN (...)` predicate is NULL-blind by SQL semantics,
/// but NULL merge-key VALUES cannot reach this code — the engine rejects
/// them typed at write time (the `rdlt-engine` load path's
/// structured_merge_keys guard, conformance-pinned). Direct SPI drivers
/// bypassing the engine inherit that same obligation.
pub fn keyed_delete_insert_sql(plan: &MergePlan<'_>) -> Vec<String> {
    let (target, stage, cols) = (plan.target_sql, plan.stage_sql, plan.cols_sql);
    let key_list = plan.key_list();
    let keep = plan.keep_clause();
    vec![
        format!("DELETE FROM {target} WHERE ({key_list}) IN (SELECT {key_list} FROM {stage})"),
        format!(
            "INSERT INTO {target} ({cols}) SELECT {cols} FROM {} deduped{keep}",
            plan.deduped(&key_list),
        ),
    ]
}

/// Keyed structured upsert: conflict-update on the merge key.
pub fn keyed_upsert_sql(plan: &MergePlan<'_>) -> Vec<String> {
    let (target, cols) = (plan.target_sql, plan.cols_sql);
    let key_list = plan.key_list();
    // Two references ONLY when hard-delete is configured (the delete and the
    // upsert); without it there is a single reference and materializing would
    // cost a temporary relation to save nothing.
    let (mut out, deduped) = if plan.hard_delete.is_some() {
        plan.dedup_once(&key_list)
    } else {
        (Vec::new(), plan.deduped(&key_list))
    };
    if let Some(hd) = &plan.hard_delete {
        out.push(format!(
            "DELETE FROM {target} WHERE ({key_list}) IN \
             (SELECT {key_list} FROM {deduped} d WHERE {})",
            hd.flagged
        ));
    }
    let keep = plan.keep_clause();
    let set = plan.update_set(plan.key);
    let action = if set.is_empty() {
        UpsertAction::Nothing
    } else {
        UpsertAction::Update { set: &set }
    };
    let columns: Vec<String> = plan
        .schema
        .columns
        .iter()
        .map(|c| plan.quote(&c.name))
        .collect();
    let keys: Vec<String> = plan.key.iter().map(|k| plan.quote(k)).collect();
    out.push(plan.dialect.upsert_stmt(&Upsert {
        target,
        columns: &columns,
        cols_sql: cols,
        source: &deduped,
        keep: &keep,
        keys: &keys,
        key_list: &key_list,
        action,
    }));
    out
}

/// Shredded identity delete-insert, with the hard-delete arm.
pub fn identity_delete_insert_sql(plan: &MergePlan<'_>) -> Vec<String> {
    let (target, cols) = (plan.target_sql, plan.cols_sql);
    let id = plan.quote(system_columns::ID);
    let id_col = if plan.is_child {
        plan.quote(system_columns::ROOT_ID)
    } else {
        id.clone()
    };
    // Hard delete: flagged ROOTS drop from the root insert; their
    // children drop by root-id membership.
    let keep = match (&plan.hard_delete, plan.is_child) {
        (Some(hd), false) => format!(" WHERE {}", hd.keep),
        (Some(hd), true) => format!(
            " WHERE {} NOT IN {}",
            plan.quote(system_columns::ROOT_ID),
            plan.flagged_roots(hd)
        ),
        (None, _) => String::new(),
    };
    vec![
        // Subtree replacement by root id + DETERMINISTIC in-batch dedup:
        // arrival order breaks ties, so "last wins" is real.
        format!(
            "DELETE FROM {target} WHERE {id_col} IN (SELECT {id} FROM {})",
            plan.root_stage_sql
        ),
        format!(
            "INSERT INTO {target} ({cols}) SELECT {cols} FROM {} deduped{keep}",
            plan.deduped(&id),
        ),
    ]
}

/// SCD2: retire-changed-then-insert with NULL-safe column-wise change
/// detection. One boundary per commit unit: the dialect's timestamp
/// expression is TRANSACTION-stable, so every statement in this publish sees
/// the same instant; redelivery re-executes nothing.
pub fn scd2_merge_sql(plan: &MergePlan<'_>, scd2: &Scd2Options) -> Vec<String> {
    let (target, cols) = (plan.target_sql, plan.cols_sql);
    // A caller-supplied boundary beats the transaction timestamp. The
    // value was VALIDATED as a timestamp at the options layer — the quoted
    // literal here can never carry raw user SQL.
    let now = match &scd2.boundary_timestamp {
        Some(boundary) => sql_literal(boundary),
        None => plan.dialect.tx_timestamp().to_string(),
    };
    let key_list = plan.key_list();
    // Referenced by the retire, insert and (scoped) retirement statements —
    // three full sorts of the stage when interpolated.
    let (mut out, deduped) = plan.dedup_once(&key_list);
    let vf = plan.quote(&scd2.valid_from);
    let vt = plan.quote(&scd2.valid_to);
    // The OPEN-version marker — NULL by default, a validated timestamp
    // literal when `active_record_timestamp` is set. The active predicate is
    // MARKER-TOLERANT (`IS NULL OR = marker`): a table whose history predates
    // the option holds NULL-open rows, and treating only the marker as open
    // would orphan them — duplicate actives, retirement blind spots. New
    // versions open with the marker; NULL-open rows close normally as their
    // keys change.
    let (open_value, is_active) = match &scd2.active_record_timestamp {
        Some(marker) => {
            let lit = sql_literal(marker);
            let is_active = format!("(t.{vt} IS NULL OR t.{vt} = {lit})");
            (lit, is_active)
        }
        None => ("NULL".to_string(), format!("t.{vt} IS NULL")),
    };
    let key_match = plan
        .key
        .iter()
        .map(|k| format!("t.{q} = d.{q}", q = plan.quote(k)))
        .collect::<Vec<_>>()
        .join(" AND ");
    // Change detection: NULL-safe, over the DATA columns — the key is
    // the identity and the load-id changes every load by construction.
    let changed = plan
        .schema
        .columns
        .iter()
        .filter(|c| !plan.key.contains(&c.name) && c.name != system_columns::LOAD_ID)
        .map(|c| format!("t.{q} IS DISTINCT FROM d.{q}", q = plan.quote(&c.name)))
        .collect::<Vec<_>>()
        .join(" OR ");

    // Retire: active versions whose key arrives with DIFFERENT values.
    if !changed.is_empty() {
        out.push(format!(
            "UPDATE {target} t SET {vt} = {now} \
             FROM {deduped} d WHERE {is_active} AND {key_match} AND ({changed})"
        ));
    }
    // Insert: staged rows with NO remaining active version (changed
    // keys were just retired; unchanged keys still hold their identical
    // active version and are SKIPPED — no churn).
    out.push(format!(
        "INSERT INTO {target} ({cols}, {vf}, {vt}) \
         SELECT {cols}, {now}, {open_value} FROM {deduped} d \
         WHERE NOT EXISTS ( \
             SELECT 1 FROM {target} t WHERE {is_active} AND {key_match})"
    ));
    // Full-feed absence semantics on request: with a merge_scope, retirement is
    // SCOPED to the delivered scopes (NULL is not a scope); without one, every
    // absent active key retires.
    if scd2.absent == AbsentPolicy::Retire {
        let scope_clause = match plan.merge_scope {
            // Mutation note: the emptiness guard is an equivalent mutant —
            // validation rejects an empty `merge_scope` before a plan is ever
            // built ("merge_scope: empty column list"), so `Some(empty)` cannot
            // reach here. Kept because a scope clause built from no columns
            // would be malformed SQL, and the guard says so locally rather than
            // relying on a check three modules away.
            Some(scope) if !scope.is_empty() => {
                let (cols, not_null) = scope_predicate(scope, |c| plan.quote(c));
                format!(
                    " AND ({cols}) IN (SELECT {cols} FROM {} WHERE {not_null})",
                    plan.stage_sql
                )
            }
            _ => String::new(),
        };
        out.push(format!(
            "UPDATE {target} t SET {vt} = {now} \
             WHERE {is_active} AND ({key_list}) NOT IN \
                   (SELECT {key_list} FROM {deduped} d){scope_clause}"
        ));
    }
    out
}

/// The per-table single-commit-unit violation: scoped merge_scope replacement
/// and scd2 `absent: retire` share one rule and one message.
pub fn single_unit_violation(table: &str, scoped: bool) -> String {
    let what = if scoped {
        "merge_scope replacement"
    } else {
        "scd2 `absent: retire`"
    };
    format!(
        "table `{table}`: {what} requires the table's full \
         feed in a SINGLE commit unit — \
         raise the engine commit thresholds and re-run; the \
         fixed configuration re-delivers the full feed and \
         converges"
    )
}

/// Quote a validated timestamp value as a SQL string literal. The value was
/// validated at the options layer, so the literal can never carry raw SQL —
/// and the quoting rule exists ONCE.
fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// The scope columns as a quoted list and their NULL-is-not-a-scope filter —
/// the one spelling of the scope predicate, shared by scope replacement and
/// scoped scd2 retirement.
fn scope_predicate(scope: &[String], quote: impl Fn(&str) -> String) -> (String, String) {
    let cols = scope
        .iter()
        .map(|c| quote(c))
        .collect::<Vec<_>>()
        .join(", ");
    let not_null = scope
        .iter()
        .map(|c| format!("{} IS NOT NULL", quote(c)))
        .collect::<Vec<_>>()
        .join(" AND ");
    (cols, not_null)
}
