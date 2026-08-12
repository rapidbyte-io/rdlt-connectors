//! Shared scaffolding for the case files: the raw-client idiom, scalar and
//! count readbacks, the bare YAML source builder, the leaked-tempdir DuckDB
//! destination, and the table probe the destination conformance suites read
//! back through.

use async_trait::async_trait;
use rdlt_connector_duckdb::destination::{Config, Shell, testhook};
use rdlt_connector_postgres::source;
use rdlt_testkit::{ProbeError, TableProbe};
use tokio_postgres::Client;

/// A raw client on `connection_string`, its connection task detached — the
/// one spelling of the connect idiom in the case files.
pub async fn connect(connection_string: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(connection_string, tokio_postgres::NoTls)
        .await
        .expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// One value from one row — the readback most assertions need.
pub async fn scalar<T>(connection_string: &str, sql: &str) -> T
where
    T: for<'a> tokio_postgres::types::FromSql<'a>,
{
    connect(connection_string)
        .await
        .query_one(sql, &[])
        .await
        .expect("scalar")
        .get(0)
}

/// Row count of `<dataset>.<table>`, distinguishing ABSENCE from failure
/// (042 round-2 fix wave — the old fold read every query error as an
/// empty table): a table or schema that does not exist yet answers 0
/// (SQLSTATE 42P01 / 3F000 — the recovery suites ask before the table
/// exists, and that zero is a fact); any other failure is an error
/// naming the cause, never a silent zero.
pub async fn try_count(connection_string: &str, dataset: &str, table: &str) -> Result<u64, String> {
    use tokio_postgres::error::SqlState;
    let sql = format!(
        "SELECT count(*) FROM \"{dataset}\".\"{}\"",
        table.replace('"', "")
    );
    match connect(connection_string).await.query_one(&sql, &[]).await {
        Ok(row) => Ok(row.get::<_, i64>(0) as u64),
        Err(e)
            if matches!(
                e.code(),
                Some(&SqlState::UNDEFINED_TABLE) | Some(&SqlState::INVALID_SCHEMA_NAME)
            ) =>
        {
            Ok(0)
        }
        Err(e) => Err(format!("count of \"{dataset}\".\"{table}\" failed: {e}")),
    }
}

/// [`try_count`] for assertion sites: a genuine failure panics loudly
/// instead of comparing as zero.
pub async fn count(connection_string: &str, dataset: &str, table: &str) -> u64 {
    try_count(connection_string, dataset, table)
        .await
        .expect("count query")
}

/// A source from the bare `conn:` line plus whatever YAML the suite appends.
pub fn source(connection_string: &str, extra_yaml: &str) -> source::Shell {
    source::Shell::from_yaml(&format!("conn: \"{connection_string}\"\n{extra_yaml}"))
        .expect("config")
}

/// A DuckDB destination in a leaked tempdir — leaked deliberately: the file
/// must outlive the test body so late engine writes never race teardown.
/// Second generation: the engine drives the [`Shell`]; the oracles go
/// through the crate's config-keyed READ-ONLY testhook, which is safe to
/// call while the live shell holds the file.
pub struct DuckDbDest {
    shell: Shell,
    config: Config,
}

impl DuckDbDest {
    /// The engine-facing destination (clones share one instance).
    pub fn shell(&self) -> Shell {
        self.shell.clone()
    }

    /// Rows under `table`, via the read-only oracle.
    pub fn count_rows(
        &self,
        table: &str,
    ) -> Result<u64, rdlt_connector_sdk::spi::DestinationError> {
        testhook::count_rows(&self.config, table)
    }

    /// First column of the first row as text, via the read-only oracle.
    pub fn query_string(
        &self,
        sql: &str,
    ) -> Result<String, rdlt_connector_sdk::spi::DestinationError> {
        testhook::query_string(&self.config, sql)
    }
}

pub fn duckdb_destination() -> DuckDbDest {
    let directory = tempfile::tempdir().expect("tempdir");
    let config = Config::new(directory.path().join("out.duckdb"));
    let shell = Shell::new(config.clone()).expect("open db");
    std::mem::forget(directory);
    DuckDbDest { shell, config }
}

/// The rowcount face the conformance harness reads a loaded table through:
/// one schema on one database, counted with the same missing-table-is-empty
/// contract [`count`] carries.
pub struct Probe {
    pub connection_string: String,
    pub schema: String,
}

#[async_trait]
impl TableProbe for Probe {
    async fn count(&self, table: &rdlt_connector_sdk::spi::TableName) -> Result<u64, ProbeError> {
        // A failure is the oracle's, surfaced as such — folding it into
        // 0 would certify invisibility clauses vacuously (042 round 2).
        try_count(&self.connection_string, &self.schema, table.as_str())
            .await
            .map_err(|message| ProbeError { message })
    }
}
