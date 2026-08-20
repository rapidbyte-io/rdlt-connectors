//! Execute one planned [`Step`] inside the OPEN unit transaction.
//!
//! Every decision and the ordering come from the sqlcore planner; this
//! renders each step's SQL through the dialect seam and the shared
//! renderers, and executes it. Because the connection, the catalog and the
//! options arrive as separate borrows, no field-splitting free function is
//! needed — the structure IS the disjointness.
//!
//! Failures CLASSIFY by SQLSTATE (the shared rule with the duckdb
//! executor): environmental errors ride the engine's retry budget, while
//! deterministic classes (22/23/42) — a duplicate receipt's unique
//! violation included, which is the idempotence-anomaly signal — fail
//! loudly instead of burning retries.

use std::collections::BTreeMap;

use rdlt_connector_sdk::spi::core::{id::PipelineId, id::TableName};
use rdlt_connector_sdk::spi::{
    core::commit::CommitMeta, core::commit::WriteMode, error::DestinationError,
};
use rdlt_connector_sqlcore::plan::scope_replace_sql;
use rdlt_connector_sqlcore::{
    MergeDialect, Step, build_merge_plan, column_list, quote_identifier, render_arm,
};

use super::catalog::{Catalog, stage_name};
use super::config::DestinationOptions;
use super::dialect::Dialect;
use super::errors::{Phase, classify_statement, fatal};
use crate::session::Connection;

pub(super) async fn run_step(
    connection: &Connection,
    pipeline: &PipelineId,
    catalog: &Catalog,
    options: &DestinationOptions,
    roots: &BTreeMap<TableName, TableName>,
    meta: &CommitMeta,
    step: &Step,
) -> Result<(), DestinationError> {
    match step {
        // Unreachable in THIS executor and deliberately not carried as dead
        // SQL. Postgres always plans `FullLoadPublish::DirectToTarget`, so a
        // full load's rows are already in the target and its clear happened
        // at the first write. Reaching either arm means the planner changed
        // without this executor being taught, which is worth saying loudly
        // rather than silently re-running a publish that already happened.
        Step::ClearTarget { table } | Step::InsertSelect { table } => {
            return Err(fatal(
                Phase::Commit,
                format!(
                    "internal: staged publish step planned for `{table}`, but this \
                     destination writes full loads directly"
                ),
            ));
        }
        Step::ScopeReplace { table, scope } => {
            let target = quote_identifier(table.as_str());
            let stage = quote_identifier(&stage_name(pipeline, table));
            connection
                .batch_execute(&scope_replace_sql(&Dialect, &target, &stage, scope))
                .await
                .map_err(|e| classify_statement(Phase::Commit, e))?;
        }
        Step::MergeArm { table, arm } => {
            let (table_schema, mode) = &catalog.tables[table];
            let WriteMode::Merge { key } = mode else {
                // The planner emits MergeArm only for merge tables.
                return Err(fatal(
                    Phase::Commit,
                    format!("internal: merge arm planned for non-merge table `{table}`"),
                ));
            };
            let root = &roots[table];
            let target = quote_identifier(table.as_str());
            let stage = quote_identifier(&stage_name(pipeline, table));
            let columns = column_list(table_schema);
            let root_stage = quote_identifier(&stage_name(pipeline, root));
            let root_schema = catalog.tables.get(root).map(|(schema, _)| schema);
            let plan = build_merge_plan(
                &Dialect,
                options,
                table,
                table_schema,
                key,
                &target,
                &stage,
                &columns,
                root,
                root_stage,
                root_schema,
            );
            for sql in render_arm(&plan, arm) {
                connection
                    .batch_execute(&sql)
                    .await
                    .map_err(|e| classify_statement(Phase::Commit, e))?;
            }
        }
        Step::TruncateStage { table } => {
            connection
                .batch_execute(
                    &Dialect.clear_table(&quote_identifier(&stage_name(pipeline, table))),
                )
                .await
                .map_err(|e| classify_statement(Phase::Commit, e))?;
        }
        Step::UpsertState => {
            // State travels in the SAME transaction as the data.
            let document =
                serde_json::to_string(&meta.state).map_err(|e| fatal(Phase::Commit, e))?;
            connection
                .execute(
                    &format!(
                        "INSERT INTO {} VALUES ($1, $2)
             ON CONFLICT (pipeline) DO UPDATE SET doc = EXCLUDED.doc",
                        rdlt_connector_sqlcore::names::STATE_TABLE
                    ),
                    &[&meta.state.pipeline.as_str(), &document],
                )
                .await
                .map_err(|e| classify_statement(Phase::Commit, e))?;
        }
        Step::InsertReceipt => {
            connection
                .execute(
                    &format!(
                        "INSERT INTO {} VALUES ($1, $2)",
                        rdlt_connector_sqlcore::names::COMMITS_TABLE
                    ),
                    &[&meta.load_id.as_str(), &(meta.commit_seq as i64)],
                )
                .await
                .map_err(|e| classify_statement(Phase::Commit, e))?;
        }
    }
    Ok(())
}
