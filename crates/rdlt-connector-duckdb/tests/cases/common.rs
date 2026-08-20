//! Shared helpers for the duckdb-v2 suite.

#![allow(dead_code)] // shared across binaries; not every file uses every helper

use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Int64Array, StringArray};
use arrow_schema::{Field, Schema};
use async_trait::async_trait;
use rdlt_connector_duckdb::destination::{self, Config, Shell};
use rdlt_connector_sdk::spi::core::error::Error as RdltError;
use rdlt_connector_sdk::spi::core::report::Run as RunReport;
use rdlt_connector_sdk::spi::{
    arrow::RecordBatch, core::commit::WriteMode, core::cursor::Cursor, core::id::TableName,
    error::SourceError, source::ReadRequest, source::Source, source::StreamSpec,
    spec::ConnectorSpec,
};
use rdlt_engine::{config::Config as EngineConfig, engine::Engine};

/// A destination over a fresh database file in `dir`.
pub fn dest_in(dir: &std::path::Path) -> (Config, Shell) {
    let config = Config::new(dir.join("out.duckdb"));
    let shell = Shell::new(config.clone()).expect("valid");
    (config, shell)
}

// ---------------------------------------------------------------- probing

/// THE one absence-vs-failure count (round-3 fix — both probes call
/// here, so the rule cannot fork): open `path` read-only and count
/// `table`'s rows. Only ABSENCE reads as zero, keyed on the measured
/// `Catalog Error` class (031: duckdb's structured channel is
/// degenerate — classification is message-keyed); an unopenable store
/// or any other query failure is the oracle failing, never an empty
/// table. The kit's table names pass through quoting to stay one rule.
pub fn count_at(
    path: &std::path::Path,
    table: &str,
) -> Result<u64, rdlt_testkit::conformance::destination::ProbeError> {
    let oracle_failure =
        |message: String| rdlt_testkit::conformance::destination::ProbeError { message };
    let read_only = duckdb::Config::default()
        .access_mode(duckdb::AccessMode::ReadOnly)
        .map_err(|e| oracle_failure(format!("read-only config failed: {e}")))?;
    let conn = duckdb::Connection::open_with_flags(path, read_only)
        .map_err(|e| oracle_failure(format!("read-only open of the database file failed: {e}")))?;
    let ident = format!("\"{}\"", table.replace('"', "\"\""));
    match conn.query_row(&format!("SELECT count(*) FROM {ident}"), [], |row| {
        row.get::<_, u64>(0)
    }) {
        Ok(count) => Ok(count),
        Err(e) if e.to_string().contains("Catalog Error") => Ok(0),
        Err(e) => Err(oracle_failure(format!("counting `{table}` failed: {e}"))),
    }
}

/// Plant the broken-read fixture both probe fail-open pins share: a
/// store with a two-row `present` table and a `broken` view over a
/// parquet file DELETED after planting — `CREATE VIEW` binds eagerly,
/// so a never-valid view cannot be planted, while a valid one whose
/// file later vanishes fails at QUERY time (`IO Error`, measured) —
/// exactly the query-arm failure an absence fold would silently
/// swallow. Returns the store file's path.
pub fn plant_broken_view_store(dir: &std::path::Path) -> std::path::PathBuf {
    let file = dir.join("store.duckdb");
    let parquet = dir.join("gone.parquet");
    {
        let conn = duckdb::Connection::open(&file).expect("open");
        conn.execute_batch(&format!(
            "CREATE TABLE present(v BIGINT); INSERT INTO present VALUES (1), (2); \
             COPY (SELECT 1 AS v) TO '{path}' (FORMAT PARQUET); \
             CREATE VIEW broken AS SELECT * FROM read_parquet('{path}');",
            path = parquet.display()
        ))
        .expect("plant");
    }
    std::fs::remove_file(&parquet).expect("the parquet file vanishes");
    file
}

/// THE one fail-open pin body (round-8 fix — both probe suites carried
/// this plant-and-assert sequence verbatim, and their error-message
/// asserts had already drifted apart): against the broken-view store
/// ([`plant_broken_view_store`]), `probe` counts the present table,
/// reads absence as zero, and surfaces the broken view's read failure
/// as a probe ERROR spelled the way [`count_at`] renders it — never an
/// empty table.
pub async fn assert_probe_counts_absence_but_fails_broken_reads(
    probe: &dyn rdlt_testkit::conformance::destination::TableProbe,
) {
    assert_eq!(
        probe
            .count(&TableName::new("present"))
            .await
            .expect("a present table counts"),
        2
    );
    assert_eq!(
        probe
            .count(&TableName::new("never_created"))
            .await
            .expect("absence is a fact, not a failure"),
        0
    );
    let err = probe
        .count(&TableName::new("broken"))
        .await
        .expect_err("a genuine read failure must never read as an empty table");
    assert!(
        err.message.contains("counting `broken` failed"),
        "the probe error names the failing count, `count_at`'s spelling: {}",
        err.message
    );
}

// ---------------------------------------------------------------- documents

/// A Shell over `file` built from an options document (everything but
/// `path` — this fn supplies that), through the full document gate.
pub fn dest_with(file: &std::path::Path, mut options: serde_json::Value) -> Shell {
    options
        .as_object_mut()
        .expect("options must be a JSON object")
        .insert("path".into(), serde_json::json!(file));
    Shell::from_value(options).expect("valid destination document")
}

/// The document gate's refusal for an options document, rendered. The
/// gate runs before assembly, so no database file is ever created.
pub fn refused_at_parse(mut options: serde_json::Value) -> String {
    options
        .as_object_mut()
        .expect("options must be a JSON object")
        .insert("path".into(), serde_json::json!("never-opened.duckdb"));
    Shell::from_value(options)
        .expect_err("the document gate must refuse this")
        .to_string()
}

// ------------------------------------------------------- read-back oracles

/// Total rows in `table`, scd2 history included — a fresh instance over
/// the same file (sequential-only: call after the run under test ended).
pub fn rows_in(file: &std::path::Path, table: &str) -> u64 {
    destination::testhook::count_rows(&Config::new(file), table).expect("count")
}

/// First column of the first row, as text. Cast non-VARCHAR scalars in
/// the SQL (`::VARCHAR`) — the hook reads a string.
pub fn scalar(file: &std::path::Path, sql: &str) -> String {
    destination::testhook::query_string(&Config::new(file), sql).expect("scalar query")
}

// --------------------------------------------- keyed structured Arrow feed

/// Column builders: arrays carry their own Arrow types and [`unit`] reads
/// them back off the array, so a test spells only names and values.
pub fn ints(values: &[Option<i64>]) -> ArrayRef {
    Arc::new(Int64Array::from(values.to_vec()))
}

pub fn texts(values: &[Option<&str>]) -> ArrayRef {
    Arc::new(StringArray::from(values.to_vec()))
}

pub fn flags(values: &[Option<bool>]) -> ArrayRef {
    Arc::new(BooleanArray::from(values.to_vec()))
}

/// One Arrow batch; every column nullable, types taken from the arrays.
pub fn unit(columns: &[(&str, ArrayRef)]) -> RecordBatch {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, array)| Field::new(*name, array.data_type().clone(), true))
        .collect();
    let arrays: Vec<ArrayRef> = columns.iter().map(|(_, array)| Arc::clone(array)).collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("unit batch")
}

/// A keyed STRUCTURED source — the stream shape the merge vocabulary
/// applies to (declared primary key, Arrow batches, no shredding).
///
/// Each entry in `units` is pushed as one batch closed by its own
/// checkpoint, so under the default commit policy each entry is one
/// commit unit. Checkpoints number `committed + 1 ..`; a SECOND engine
/// run under the same pipeline must position itself past the first run's
/// cursor with [`KeyedFeed::resuming_after`], exactly like a real
/// incremental source delivering fresh data. `read` honors `since`, so a
/// resumed run re-reads strictly after the committed checkpoint.
pub struct KeyedFeed {
    stream: &'static str,
    key: Vec<String>,
    units: Vec<RecordBatch>,
    committed: u64,
}

/// A one-unit keyed feed for `stream`, keyed on `key`.
pub fn keyed(stream: &'static str, key: &[&str], rows: RecordBatch) -> KeyedFeed {
    KeyedFeed {
        stream,
        key: key.iter().map(|k| (*k).to_owned()).collect(),
        units: vec![rows],
        committed: 0,
    }
}

impl KeyedFeed {
    /// Push `rows` as an additional commit unit (the split-feed shape).
    pub fn and_unit(mut self, rows: RecordBatch) -> Self {
        self.units.push(rows);
        self
    }

    /// Position this extraction after `checkpoints` already-committed
    /// units — the second-run driver.
    pub fn resuming_after(mut self, checkpoints: u64) -> Self {
        self.committed = checkpoints;
        self
    }
}

#[async_trait]
impl Source for KeyedFeed {
    /// In-memory: the rows are already here, so there is nothing to
    /// reach and nothing that could be misconfigured. Answering Ok is
    /// the honest answer for this double, not a stub — a probe that
    /// passes what the read then fails is what the clause forbids.
    async fn check(&self) -> Result<(), SourceError> {
        Ok(())
    }
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("keyed-feed", "0.0.0")
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(vec![
            StreamSpec::new(self.stream)
                .with_structured()
                .with_primary_key(self.key.iter().cloned()),
        ])
    }

    async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
        let after = req
            .since
            .as_ref()
            .and_then(|cursor| cursor.as_value().as_u64())
            .unwrap_or(0);
        for (position, rows) in self.units.iter().enumerate() {
            let checkpoint = self.committed + position as u64 + 1;
            if checkpoint <= after {
                continue; // already committed — a real source would not resend
            }
            if req.out.arrow(rows.clone()).await.is_err()
                || req.out.checkpoint(Cursor::new(checkpoint)).await.is_err()
            {
                return Ok(()); // closed channel = cancellation, not an error
            }
        }
        Ok(())
    }
}

// ------------------------------------------------------------ engine driving

/// An engine config in the merge write mode, keyed on `key`.
pub fn merge_on(pipeline: &str, key: &[&str]) -> EngineConfig {
    EngineConfig::new(pipeline).with_write_mode(WriteMode::Merge {
        key: key.iter().map(|k| (*k).to_owned()).collect(),
    })
}

/// One pipeline run: `source` into `shell` under `config`.
pub async fn drive(
    config: EngineConfig,
    source: impl Source + 'static,
    shell: Shell,
) -> Result<RunReport, RdltError> {
    Engine::new(config, source, shell).run().await
}
