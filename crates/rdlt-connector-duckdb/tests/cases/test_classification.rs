//! Classification through the public surface, plus the two library
//! probes that keep the message-prefix rulebook honest.
//!
//! DuckDB's C API reports no structured error category, so this crate
//! classifies on stable message PREFIXES — which only works if a
//! message that merely MENTIONS violation-adjacent words is never
//! misread, and if the prefixes themselves stay put. The first two
//! cells pin the former end to end; the last two pin the latter
//! directly on the library so a wording change fails loudly here
//! instead of silently reclassifying production errors.

use duckdb::Connection;
use rdlt_connector_duckdb::destination::{Config, ConfigError, MergeStrategy, Shell};
use rdlt_connector_sdk::spi::core::{
    ColumnDef, ColumnType, LoadId, LogicalType, PipelineId, Provenance, TableSchema, WriteMode,
};
use rdlt_connector_sdk::spi::{Destination, DestinationError, OpenContext};
use rdlt_testkit::schema_for;

async fn ensure_under_upsert(
    path: &std::path::Path,
    schema: &TableSchema,
    key: &str,
) -> Result<(), DestinationError> {
    let mut config = Config::new(path);
    config.merge_strategy = Some(MergeStrategy::Upsert);
    let shell = Shell::new(config).expect("valid document");
    let mut session = shell
        .open(OpenContext::new(
            PipelineId::new("classify"),
            LoadId::new("load-1"),
        ))
        .await
        .expect("open");
    let keyed = WriteMode::Merge {
        key: vec![key.to_owned()],
    };
    session.ensure_table(schema, &keyed).await
}

/// [`schema_for`]'s `id` column plus a STRUCT `payload` — the shape the
/// non-constraint index-failure cell keys on.
fn schema_with_struct(table: &str) -> TableSchema {
    let mut schema = schema_for(table);
    schema.columns.push(ColumnDef {
        name: "payload".into(),
        column_type: ColumnType::Struct {
            fields: vec![ColumnDef {
                name: "x".into(),
                column_type: ColumnType::scalar(LogicalType::Int64),
                nullable: true,
                provenance: Provenance::Inferred,
            }],
        },
        nullable: true,
        provenance: Provenance::Inferred,
    });
    schema
}

/// A GENUINE duplicate-key failure — the upsert arbiter index refused
/// over pre-existing duplicate rows — renders sqlcore's shared
/// diagnosis, naming the strategy, the table, and the colliding key.
#[tokio::test]
async fn preexisting_duplicate_keys_render_the_upsert_diagnosis() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dupes.duckdb");
    // Seed duplicates the way a pre-rdlt database would carry them.
    let seed = Connection::open(&path).expect("seed connection");
    seed.execute_batch(
        "CREATE TABLE \"events\" (\"id\" BIGINT); INSERT INTO \"events\" VALUES (7), (7);",
    )
    .expect("seed");
    drop(seed);

    let err = ensure_under_upsert(&path, &schema_for("events"), "id")
        .await
        .expect_err("the arbiter index cannot build over duplicates");
    let text = err.to_string();
    assert!(
        text.contains("cannot create the unique index the upsert strategy requires"),
        "the shared diagnosis wording: {text}"
    );
    assert!(text.contains("`events`"), "names the table: {text}");
    assert!(
        text.contains("(id)"),
        "names the colliding merge key: {text}"
    );
}

/// A fatal failure ON the unique-index statement that is NOT a
/// constraint violation must surface as ITSELF — never dressed in the
/// duplicate-key diagnosis, which keys on DuckDB's `Constraint Error`
/// prefix alone. The vector (chosen in 031 round 1 after the old one —
/// a merge key naming a MISSING column — moved to sqlcore validation,
/// and a pre-created colliding index proved unusable because
/// `CREATE INDEX IF NOT EXISTS` silently no-ops on a name collision):
/// a merge key on a STRUCT column passes validation (the column
/// exists), but DuckDB's ART rejects nested index-key types, so the
/// `CREATE UNIQUE INDEX` itself fails with an `Invalid type Error` —
/// exactly the unique_index=Some, non-constraint branch.
#[tokio::test]
async fn a_non_constraint_failure_on_the_index_statement_is_not_the_diagnosis() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("artstruct.duckdb");
    let err = ensure_under_upsert(&path, &schema_with_struct("events"), "payload")
        .await
        .expect_err("ART refuses a STRUCT index key");
    assert!(
        matches!(err, DestinationError::Fatal(_)),
        "an index-key-type failure is fatal, not retryable: {err}"
    );
    let text = err.to_string();
    assert!(
        text.contains("Invalid type for index key"),
        "precondition — the failure is the index statement's own: {text}"
    );
    assert!(
        !text.contains("cannot create the unique index"),
        "misdiagnosed as a duplicate-key violation: {text}"
    );
}

/// The old vector for the cell above, now refused EARLIER: a merge key
/// naming a column the schema does not carry is a sqlcore validation
/// error (031 review N1) — typed, shared wording, before any DDL runs.
#[tokio::test]
async fn a_ghost_merge_key_column_is_refused_at_validation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ghostkey.duckdb");
    let err = ensure_under_upsert(&path, &schema_for("events"), "violates_nothing")
        .await
        .expect_err("a ghost merge-key column is a config error");
    assert!(
        matches!(err, DestinationError::Fatal(_)),
        "a config error is fatal, not retryable: {err}"
    );
    let text = err.to_string();
    assert!(
        text.contains("merge key column `violates_nothing` is not a column of table `events`"),
        "the shared sqlcore wording: {text}"
    );
}

/// N2 (031 round 1, measured): a second live read-write instance on
/// one database file replays AND TRUNCATES the first instance's WAL,
/// silently swallowing its later commits. TWO Shells on one file in
/// one process is therefore refused typed-fatal; dropping the first
/// makes a sequential re-open legal again.
#[test]
fn a_second_shell_on_one_file_is_refused_until_the_first_drops() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("guarded.duckdb");
    let first = Shell::new(Config::new(&path)).expect("first open");
    let err = Shell::new(Config::new(&path))
        .expect_err("a second live instance is the WAL-truncation hazard")
        .to_string();
    assert!(err.contains("already open in this process"), "{err}");
    drop(first);
    Shell::new(Config::new(&path)).expect("a sequential re-open stays legal");
}

/// A missing parent directory is DETERMINISTIC, not environmental
/// (037 US6 / Task 19, S5): it never heals on retry, so the carve-out
/// in `classify` (probe-pinned in test_probes.rs against the "Cannot
/// open file" template PLUS its "No such file or directory" suffix —
/// the template alone also carries healable errnos and is
/// deliberately not enough) routes it fatal, not transient. Assembly
/// happens inside `Shell::new`, so that fatal verdict arrives wrapped
/// in the config error's Invalid arm with the SPI framing intact.
#[test]
fn an_unopenable_path_is_fatal_not_retried_forever() {
    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("never-created").join("db.duckdb");
    let err = Shell::new(Config::new(missing)).expect_err("no parent directory");
    assert!(
        matches!(err, ConfigError::Invalid(_)),
        "assemble failures land in the Invalid arm: {err}"
    );
    let text = err.to_string();
    assert!(
        text.contains("fatal destination error"),
        "a deterministic IO failure must not ride the retry budget: {text}"
    );
    assert!(text.contains("IO Error"), "the classifier's key: {text}");
}

/// One genuine constraint violation straight from the library, for the
/// two probes below.
fn library_constraint_violation() -> duckdb::Error {
    let c = Connection::open_in_memory().expect("in-memory duckdb");
    c.execute_batch("CREATE TABLE probe (k BIGINT); INSERT INTO probe VALUES (3), (3);")
        .expect("seed");
    c.execute_batch("CREATE UNIQUE INDEX probe_k ON probe (k)")
        .expect_err("duplicates must refuse the unique index")
}

/// Library probe 1 — the structured channel stays DEGENERATE: the ffi
/// error carries `ErrorCode::Unknown` because the C API reports no
/// category. If this fails, duckdb-rs began populating real codes —
/// move classification onto them and retire the prefix matching.
#[test]
fn the_structured_error_channel_is_still_degenerate() {
    match library_constraint_violation() {
        duckdb::Error::DuckDBFailure(ffi, _) => assert_eq!(
            ffi.code,
            duckdb::ffi::ErrorCode::Unknown,
            "duckdb-rs now reports a structured category — reclassify on it"
        ),
        other => panic!("unexpected error variant for a constraint violation: {other:?}"),
    }
}

/// Library probe 2 — the `Constraint Error` message prefix is stable.
/// If this fails, DuckDB reworded the message — update the prefix in
/// destination/client.rs in the same change.
#[test]
fn the_constraint_message_prefix_is_still_stable() {
    match library_constraint_violation() {
        duckdb::Error::DuckDBFailure(_, Some(message)) => assert!(
            message.starts_with("Constraint Error"),
            "DuckDB reworded its constraint message — update destination/client.rs: {message}"
        ),
        other => panic!("constraint violation carried no message: {other:?}"),
    }
}
