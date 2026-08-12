//! The destination connector: the framework face (the sdk's
//! `DestinationConnector`; the SPI comes from [`super::Shell`]) and
//! `connect` — session bookkeeping tables, the search path, and the
//! reclamation obligation.

use async_trait::async_trait;
use rdlt_connector_sdk::destination::DestinationConnector;
use rdlt_connector_sdk::spi::core::naming::IdentRules;
use rdlt_connector_sdk::spi::{DestinationCapabilities, DestinationError, OpenContext};
use rdlt_connector_sqlcore::quote_identifier;

use super::catalog::{Catalog, stage_prefix};
use super::config::{self, Config, Postgres};
use super::errors::{Phase, driver_transient};
use super::load::Load;
use super::unit::Unit;
use crate::session;

#[async_trait]
impl DestinationConnector for Postgres {
    // Reverse-DNS, not bare `postgres` — the same id the source half
    // reports, for the same reason: NAME is what the wire handshake
    // reports and the client strictly verifies, and D-039-1 derives the
    // binary name from its last segment (see `source::connector`).
    const NAME: &'static str = "io.rapidbyte.postgres";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");

    type Config = Config;
    type Backend = Load;

    fn assemble(config: Config) -> Result<Self, config::ConfigError> {
        Self::from_config(config)
    }

    fn config_schema() -> Option<serde_json::Value> {
        Some(config::config_schema())
    }

    fn capabilities(&self) -> DestinationCapabilities {
        DestinationCapabilities::default()
            // Merge is native (the sqlcore strategy arms); structs and
            // scalar lists are not — the engine flattens and shreds at the
            // seam. Json and decimal pass through untouched as JSONB and
            // NUMERIC.
            .with_merge(true)
            .with_structs(false)
            .with_scalar_lists(false)
            .with_json_type(true)
            .with_decimal(true)
            .with_ident_rules(IdentRules { max_len: 63 })
    }

    async fn connect(&self, context: &OpenContext) -> Result<Load, DestinationError> {
        let connection = session::establish(
            &self.connection_string,
            self.tls.as_ref(),
            session::Profile::Plain,
        )
        .await
        .map_err(|e| {
            // The inner failure alone, through this half's phase taxonomy —
            // the session enum's own framing would double the "connect"
            // spelling.
            let detail = e.detail();
            if e.is_transient() {
                super::errors::transient(Phase::Connect, detail)
            } else {
                super::errors::fatal(Phase::Connect, detail)
            }
        })?;
        let schema = quote_identifier(&self.schema);
        connection
            .batch_execute(&format!(
                "CREATE SCHEMA IF NOT EXISTS {schema};
                 SET search_path TO {schema};
                 CREATE TABLE IF NOT EXISTS {state} (pipeline TEXT PRIMARY KEY, doc TEXT);
                 CREATE TABLE IF NOT EXISTS {commits} (
                     load_id TEXT, commit_seq BIGINT, PRIMARY KEY (load_id, commit_seq));
                 CREATE TABLE IF NOT EXISTS {cleared} (
                     load_id TEXT, table_name TEXT, PRIMARY KEY (load_id, table_name));",
                state = rdlt_connector_sqlcore::names::STATE_TABLE,
                commits = rdlt_connector_sqlcore::names::COMMITS_TABLE,
                cleared = rdlt_connector_sqlcore::names::CLEARED_TABLE
            ))
            .await
            .map_err(|e| driver_transient(Phase::Connect, e))?;

        // Staged data from THIS PIPELINE's dead sessions becomes
        // invisible/reclaimable. Scoped by pipeline-hash prefix: other
        // pipelines sharing the schema keep their live staged rows.
        let prefix_pattern = format!("{}%", stage_prefix(&context.pipeline).replace('_', "\\_"));
        let stale: Vec<String> = connection
            .query(
                "SELECT tablename FROM pg_tables
                 WHERE schemaname = $1 AND tablename LIKE $2",
                &[&self.schema, &prefix_pattern],
            )
            .await
            .map_err(|e| driver_transient(Phase::Connect, e))?
            .into_iter()
            .map(|row| row.get::<_, String>(0))
            .collect();
        for table in stale {
            connection
                .batch_execute(&format!("TRUNCATE TABLE {}", quote_identifier(&table)))
                .await
                .map_err(|e| driver_transient(Phase::Connect, e))?;
        }

        Ok(Load {
            connection,
            pipeline: context.pipeline.clone(),
            load_id: context.load_id.clone(),
            catalog: Catalog::default(),
            options: self.options.clone(),
            unit: Unit::default(),
            cleared_targets: std::collections::BTreeSet::new(),
            single_unit_done: std::collections::BTreeSet::new(),
        })
    }
}
