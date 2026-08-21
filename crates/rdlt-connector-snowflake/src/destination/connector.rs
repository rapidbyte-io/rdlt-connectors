//! The framework face: [`Snowflake`] implements the sdk's
//! `DestinationConnector`, and `connect` does everything that must
//! happen before any unit can open — the bookkeeping tables, the
//! staging object, and both halves of the reclamation obligation.

use async_trait::async_trait;
use rdlt_connector_sdk::destination::DestinationConnector;
use rdlt_connector_sdk::spi::core::schema::IdentRules;
use rdlt_connector_sdk::spi::{
    destination::Capabilities as DestinationCapabilities, destination::OpenContext,
    error::DestinationError,
};

use super::client;
use super::config::{Config, ConfigError, TableType, config_schema};
use super::load::Load;
use super::stage::Stage;
use super::{catalog, ddl};

/// The crash points this destination arms — exported so the sweep
/// iterates exactly this list: a point in the code but not the sweep is
/// a protocol edge nobody ever crashes at, and that failure mode is
/// silence.
pub const FAIL_POINTS: &[&str] = &[
    // A part exists on local disk and nowhere else; the residue is
    // local and only its own load may reclaim it.
    "sf.stage.write",
    // Uploaded and unrecorded: REMOTE residue no load statement will
    // name — a separate point because it leaves different debris in a
    // different place with a different reclaimer.
    "sf.stage.upload",
    // Rows, receipt and state written; none durable.
    "sf.unit.publish",
    // Durable, and the caller is about to be told otherwise.
    "sf.receipt.visible",
];

/// The Snowflake destination.
#[derive(Debug, Clone)]
pub struct Snowflake {
    config: Config,
}

#[async_trait]
impl DestinationConnector for Snowflake {
    // Reverse-DNS, not bare `snowflake` (039 T6): NAME is the connector
    // id the wire handshake reports and the client verifies by STRICT
    // equality against a `Requirement.id` — and D-039-1 keys
    // discovery on the id's last segment (`io.rapidbyte.snowflake` →
    // binary `rdlt-connector-snowflake` on PATH).
    const NAME: &'static str = "io.rapidbyte.snowflake";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");

    type Config = Config;
    type Backend = Load;

    fn assemble(config: Config) -> Result<Self, ConfigError> {
        Ok(Self { config })
    }

    fn config_schema() -> Option<serde_json::Value> {
        Some(config_schema())
    }

    fn capabilities(&self) -> DestinationCapabilities {
        // No structs/lists: VARIANT could carry them, but a capability
        // this crate has not read back and verified is a capability it
        // does not claim — the engine lowers nested shapes at the seam.
        DestinationCapabilities::default()
            .with_merge(true)
            .with_json_type(true)
            .with_decimal(true)
            .with_ident_rules(IdentRules::default())
    }

    async fn connect(&self, context: &OpenContext) -> Result<Load, DestinationError> {
        let executor = client::connect(&self.config).await?;
        let qualified = |table: &str| {
            format!(
                "{}.{}.{}",
                ddl::quote(&self.config.database),
                ddl::quote(&self.config.schema),
                ddl::quote(table)
            )
        };

        // Bookkeeping first, and OUTSIDE any unit: these are DDL, and
        // DDL commits whatever transaction is open. The table_type
        // choice covers rdlt's own tables too.
        let transient = match self.config.table_type {
            TableType::Transient => "TRANSIENT ",
            TableType::Permanent => "",
        };
        for sql in [
            format!(
                "CREATE SCHEMA IF NOT EXISTS {}.{}",
                ddl::quote(&self.config.database),
                ddl::quote(&self.config.schema)
            ),
            format!(
                "CREATE {transient}TABLE IF NOT EXISTS {} ({} VARCHAR PRIMARY KEY, {} VARCHAR)",
                qualified(rdlt_connector_sqlcore::names::STATE_TABLE),
                ddl::quote("pipeline"),
                ddl::quote("doc")
            ),
            format!(
                "CREATE {transient}TABLE IF NOT EXISTS {} ({} VARCHAR, {} NUMBER, \
                 PRIMARY KEY ({}, {}))",
                qualified(rdlt_connector_sqlcore::names::COMMITS_TABLE),
                ddl::quote("load_id"),
                ddl::quote("commit_seq"),
                ddl::quote("load_id"),
                ddl::quote("commit_seq")
            ),
            format!(
                "CREATE {transient}TABLE IF NOT EXISTS {} ({} VARCHAR, {} VARCHAR, \
                 PRIMARY KEY ({}, {}))",
                qualified(rdlt_connector_sqlcore::names::CLEARED_TABLE),
                ddl::quote("load_id"),
                ddl::quote("table_name"),
                ddl::quote("load_id"),
                ddl::quote("table_name")
            ),
        ] {
            executor.execute(&sql).await?;
        }

        // The staging object — always, there is no second transport and
        // nothing for a user to configure. `IF NOT EXISTS`: a second
        // session of the pipeline finds the first one's object instead
        // of discarding its staged parts.
        let stage = Stage::new(context.pipeline.as_str(), context.load_id.as_str());
        executor
            .execute(&Stage::create_sql(&qualified(stage.name())))
            .await?;
        // The open contract's reclamation, in both places debris can
        // live: THIS load's local directory (unconditionally — no
        // receipt names those files), and remote parts old enough that
        // no live load can still own them.
        stage.reclaim_local();
        stage
            .reclaim_remote(&*executor, &qualified(stage.name()))
            .await;

        // The durable half of the Replace once-per-load guard: what a
        // COMMITTED unit of THIS load already cleared. A recovery
        // session's memory is empty, and without this seed its next
        // write would re-clear — DELETE — rows the earlier unit
        // committed.
        let cleared = super::load::read_cleared_targets(
            &*executor,
            &qualified(rdlt_connector_sqlcore::names::CLEARED_TABLE),
            &context.load_id,
        )
        .await?;

        Ok(Load {
            config: self.config.clone(),
            executor,
            pipeline: context.pipeline.clone(),
            load_id: context.load_id.clone(),
            catalog: catalog::Catalog::default(),
            tables: std::collections::BTreeMap::new(),
            cleared,
            cleared_in_unit: std::collections::BTreeSet::new(),
            reclear_owed: std::collections::BTreeSet::new(),
            unit: super::unit::Unit::default(),
            single_unit_done: std::collections::BTreeSet::new(),
            stage,
            pending: std::collections::BTreeMap::new(),
            parts: self.config.parts.unwrap_or_default(),
            open: std::collections::BTreeMap::new(),
            part_events: context.part_events.clone(),
        })
    }
}

/// Seams the live cells need and nothing else may use: they provoke
/// real service errors to check classification, script one session to
/// observe transactional facts, and compare rendered DDL as data. Not a
/// public API.
#[doc(hidden)]
pub mod testhook {
    use rdlt_connector_sdk::spi::core::{commit::WriteMode, schema::TableSchema};
    use rdlt_connector_sdk::spi::error::DestinationError;

    use super::super::client::Executor as _;
    use super::super::config::{Config, TableType};
    use super::super::unit::DmlOnly;
    use super::super::{catalog, client, ddl, stage};

    /// Re-exported so cells can drive the describe-once decision
    /// without a service.
    pub use super::super::catalog::Catalog;

    /// The duplicate-key code, named from the constant the merge path
    /// keys on rather than repeated as a magic number.
    pub const DUPLICATE_ROW_IN_DML: &str = client::DUPLICATE_ROW_IN_DML;

    /// Connect and run one statement; a SELECT returns its first scalar
    /// as text.
    pub async fn connect_and_run(config: &Config, sql: &str) -> Result<String, DestinationError> {
        let executor = client::connect(config).await?;
        if sql.trim_start().to_ascii_uppercase().starts_with("SELECT") {
            Ok(executor.scalar_u64(sql, &[]).await?.to_string())
        } else {
            executor.execute(sql).await.map(|()| String::new())
        }
    }

    /// Run a script on ONE session, returning the last scalar. A
    /// session per statement cannot observe anything transactional.
    pub async fn connect_and_run_script(
        config: &Config,
        script: &[&str],
    ) -> Result<String, DestinationError> {
        let executor = client::connect(config).await?;
        let mut last = String::new();
        for sql in script {
            if sql.trim_start().to_ascii_uppercase().starts_with("SELECT") {
                last = executor.scalar_u64(sql, &[]).await?.to_string();
            } else {
                executor.execute(sql).await?;
            }
        }
        Ok(last)
    }

    /// A query's first column as a set of strings — the shape the
    /// scalar hook (which parses counts) cannot serve.
    pub async fn read_column(
        config: &Config,
        sql: &str,
    ) -> Result<std::collections::BTreeSet<String>, DestinationError> {
        let executor = client::connect(config).await?;
        let rows = executor.rows(sql, &[], &["name"]).await?;
        Ok(rows.into_iter().map(|mut row| row.remove(0)).collect())
    }

    /// Named columns from every row of one statement.
    pub async fn rows(
        config: &Config,
        sql: &str,
        columns: &[&str],
    ) -> Result<Vec<Vec<String>>, DestinationError> {
        client::connect(config).await?.rows(sql, &[], columns).await
    }

    /// Named columns from every row, after a setup script on the SAME
    /// session.
    pub async fn script_rows(
        config: &Config,
        setup: &[&str],
        sql: &str,
        columns: &[&str],
    ) -> Result<Vec<Vec<String>>, DestinationError> {
        let executor = client::connect(config).await?;
        for statement in setup {
            executor.execute(statement).await?;
        }
        executor.rows(sql, &[], columns).await
    }

    /// One statement through the UNIT executor — the DDL-refusing one.
    pub async fn run_in_unit(config: &Config, sql: &str) -> Result<(), DestinationError> {
        let executor = client::connect(config).await?;
        DmlOnly(executor.as_ref()).execute(sql).await
    }

    /// The structured code inside a classified error, if any.
    pub fn classify_live_error(err: &DestinationError) -> Option<String> {
        client::code_in(err)
    }

    /// Reclaim staged parts older than a CHOSEN age. The shipped rule
    /// waits a day no test can sit out; parameterising the age lets a
    /// cell prove both outcomes — stale removed, fresh kept.
    pub async fn reclaim_staged_older_than(
        config: &Config,
        pipeline: &str,
        load: &str,
        qualified_stage: &str,
        stale_after_hours: u32,
    ) -> Result<(), DestinationError> {
        let executor = client::connect(config).await?;
        stage::Stage::new(pipeline, load)
            .reclaim_remote_older_than(&*executor, qualified_stage, stale_after_hours)
            .await;
        Ok(())
    }

    /// Where a load's parts wait locally — so a cell can assert the
    /// directory is EMPTY once the load settles, instead of trusting
    /// that the next run cleans it.
    pub fn local_part_dir(pipeline: &str, load: &str) -> std::path::PathBuf {
        stage::Stage::new(pipeline, load).local_dir().to_path_buf()
    }

    /// The phase-1 ensure statements for a schema against a catalog
    /// image — the pin seam: rendered DDL compares as data, because
    /// executing it needs an account and comparing it needs nothing.
    /// The structural widen flag is an execution-time concern (the
    /// phase-1 loop's own advice-wrapping); pins here compare text
    /// only, so it is dropped at this boundary.
    pub fn ensure_table_sql(
        pipeline: &str,
        schema: &TableSchema,
        mode: &WriteMode,
        table_type: TableType,
        previous: Option<&TableSchema>,
        catalog: &Catalog,
    ) -> Vec<String> {
        ddl::table_ddl_stmts(pipeline, schema, mode, table_type, previous, catalog)
            .into_iter()
            .map(|(sql, _kind)| sql)
            .collect()
    }

    /// The phase-2 ensure statements (scd2 validity; never an index).
    pub fn ensure_merge_sql(
        options: &rdlt_connector_sqlcore::DestinationOptions,
        schema: &TableSchema,
        mode: &WriteMode,
        catalog: &Catalog,
    ) -> Result<Vec<String>, rdlt_connector_sqlcore::plan::ValidateError> {
        ddl::merge_ensure_stmts(options, schema, mode, catalog)
    }

    /// One table's columns as the live catalog reports them — proving
    /// the read-then-emit-nothing loop against a real account.
    pub async fn read_catalog(
        config: &Config,
        table: &str,
    ) -> Result<std::collections::BTreeSet<String>, DestinationError> {
        let executor = client::connect(config).await?;
        catalog::observe_table(executor.as_ref(), &config.database, &config.schema, table).await
    }

    /// Apply one statement against the account.
    pub async fn apply(config: &Config, sql: &str) -> Result<(), DestinationError> {
        client::connect(config).await?.execute(sql).await
    }
}
