//! Execution helpers: the destinations render step SQL through these, so the
//! by-name publish text and the arm→SQL dispatch each exist once.

use rdlt_connector::core::{TableName, TableSchema};

use crate::dialect::MergeDialect;
use crate::options::DestinationOptions;
use crate::plan::{
    HardDelete, MergePlan, identity_delete_insert_sql, keyed_delete_insert_sql, keyed_upsert_sql,
    scd2_merge_sql,
};

use super::step::MergeArm;

/// Append/Replace publish text — by-name `INSERT … SELECT`, identical across
/// destinations (no dialect divergence). Operands are already quoted.
pub fn insert_select_sql(target: &str, cols: &str, stage: &str) -> String {
    format!("INSERT INTO {target} ({cols}) SELECT {cols} FROM {stage}")
}

/// Assemble the [`MergePlan`] for a merge arm — the shared 10-field shape +
/// hard-delete resolution. The destination supplies its dialect and the
/// (differently-scoped) stage identifiers; every merge decision the plan needs
/// is resolved here from the options.
#[allow(clippy::too_many_arguments)]
pub fn build_merge_plan<'a>(
    dialect: &'a dyn MergeDialect,
    options: &'a DestinationOptions,
    table: &'a TableName,
    schema: &'a TableSchema,
    key: &'a [String],
    target_sql: &'a str,
    stage_sql: &'a str,
    cols_sql: &'a str,
    root: &TableName,
    root_stage_sql: String,
    root_schema: Option<&TableSchema>,
) -> MergePlan<'a> {
    MergePlan {
        dialect,
        target_sql,
        stage_sql,
        cols_sql,
        schema,
        key,
        root_stage_sql,
        is_child: table != root,
        hard_delete: options
            .hard_delete_for(root.as_str())
            .and_then(|col| Some(HardDelete::new(col, root_schema?, dialect))),
        dedup_sort: options.dedup_sort_for(table.as_str()),
        merge_scope: options.merge_scope_for(table.as_str()),
    }
}

/// Render a resolved arm to its statements through the plan's dialect seam.
pub fn render_arm(plan: &MergePlan<'_>, arm: &MergeArm) -> Vec<String> {
    match arm {
        MergeArm::KeyedDeleteInsert => keyed_delete_insert_sql(plan),
        MergeArm::KeyedUpsert => keyed_upsert_sql(plan),
        MergeArm::IdentityDeleteInsert => identity_delete_insert_sql(plan),
        MergeArm::Scd2(scd2) => scd2_merge_sql(plan, scd2),
    }
}
