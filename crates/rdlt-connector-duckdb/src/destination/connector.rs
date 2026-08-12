//! The destination connector: config in, [`Load`] sessions out.

use async_trait::async_trait;
use rdlt_connector_sdk::destination::DestinationConnector;
use rdlt_connector_sdk::spi::core::naming::IdentRules;
use rdlt_connector_sdk::spi::{DestinationCapabilities, DestinationError, OpenContext};

use super::client::Db;
use super::config::{Config, ConfigError, config_schema};
use super::load::Load;

/// The crash points this destination arms — exported so the sweeps
/// iterate exactly this list. Coarse by design: DuckDB's own
/// transaction is one atomic step. These spellings are frozen.
pub const FAIL_POINTS: &[&str] = &["duck.append", "duck.tx.commit"];

/// The DuckDB destination.
#[derive(Debug, Clone)]
pub struct DuckDb {
    config: Config,
    db: Db,
}

#[async_trait]
impl DestinationConnector for DuckDb {
    // Reverse-DNS, not bare `duckdb` (042, the identity rule reaching
    // this crate): NAME is what the wire handshake reports and the
    // client strictly verifies (D-039-2), and D-039-1 derives the
    // binary name `rdlt-connector-duckdb` from its last segment.
    const NAME: &'static str = "io.rapidbyte.duckdb";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");

    type Config = Config;
    type Backend = Load;

    fn assemble(config: Config) -> Result<Self, ConfigError> {
        // The database opens at assembly: settings and extensions
        // apply EAGERLY so a bad value errors here, and every session
        // clones from this ONE instance (two opens on one file are
        // two databases).
        let db = Db::connect(
            &config.path,
            config
                .settings
                .iter()
                .flat_map(|s| s.iter().map(|(k, v)| (k.clone(), v.clone())))
                .chain(
                    config
                        .memory_limit
                        .iter()
                        .map(|v| ("memory_limit".to_owned(), v.clone())),
                ),
            config.extensions.iter().flatten().cloned(),
        )
        .map_err(|e| ConfigError::Invalid(e.to_string()))?;
        Ok(Self { config, db })
    }

    fn config_schema() -> Option<serde_json::Value> {
        Some(config_schema())
    }

    fn capabilities(&self) -> DestinationCapabilities {
        // All native: real STRUCT/LIST lowering, JSON as a declared
        // JSON column (probe-proven), DECIMAL(p,s), and the full
        // sqlcore merge vocabulary.
        DestinationCapabilities::default()
            .with_merge(true)
            .with_structs(true)
            .with_scalar_lists(true)
            .with_json_type(true)
            .with_decimal(true)
            .with_ident_rules(IdentRules::default())
    }

    async fn connect(&self, _context: &OpenContext) -> Result<Load, DestinationError> {
        Load::open(&self.db, self.config.options())
    }
}

/// Seams the tests and cross-crate oracles need. Not a public API.
#[doc(hidden)]
pub mod testhook {
    use rdlt_connector_sdk::spi::DestinationError;
    use rdlt_connector_sdk::spi::core::{TableSchema, WriteMode};

    use super::super::config::Config;
    use super::super::schema;

    /// A READ-ONLY connection for the oracles. Read-only is
    /// load-bearing, not politeness: a second READ-WRITE instance
    /// opened beside a live one replays AND TRUNCATES the live
    /// instance's WAL, silently swallowing every commit it makes
    /// afterwards (031 review — measured: a mid-test count made the
    /// next session's committed row invisible). A read-only open
    /// replays the WAL without touching it, so the oracles are safe
    /// to interleave with a live shell.
    fn read_only(config: &Config) -> Result<duckdb::Connection, DestinationError> {
        let flags = duckdb::Config::default()
            .access_mode(duckdb::AccessMode::ReadOnly)
            .map_err(|e| DestinationError::fatal(e.to_string()))?;
        duckdb::Connection::open_with_flags(&config.path, flags)
            .map_err(|e| DestinationError::fatal(e.to_string()))
    }

    /// Rows under the (quoted) table name — the conformance and sweep
    /// oracle. Counts EVERYTHING including scd2 history.
    pub fn count_rows(config: &Config, table: &str) -> Result<u64, DestinationError> {
        read_only(config)?
            .query_row(
                &format!("SELECT count(*) FROM {}", schema::quote(table)),
                [],
                |row| row.get(0),
            )
            .map_err(|e| DestinationError::fatal(e.to_string()))
    }

    /// First column of the first row, as text — the probe seam.
    pub fn query_string(config: &Config, sql: &str) -> Result<String, DestinationError> {
        read_only(config)?
            .query_row(sql, [], |row| row.get(0))
            .map_err(|e| DestinationError::fatal(e.to_string()))
    }

    /// The phase-1 ensure DDL as data — the golden-pin seam.
    pub fn ensure_table_sql(schema: &TableSchema, previous: Option<&TableSchema>) -> Vec<String> {
        super::super::schema::table_ddl(schema, previous)
    }

    /// The phase-2 ensure DDL as data (statement + unique-index key
    /// columns).
    pub fn ensure_merge_sql(
        options: &rdlt_connector_sqlcore::DestinationOptions,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<
        Vec<super::super::schema::EnsureStatement>,
        rdlt_connector_sqlcore::plan::ValidateError,
    > {
        super::super::schema::merge_ddl(options, schema, mode)
    }
}
