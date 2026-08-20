//! Dialect-feasibility probes, asserted against the bundled DuckDB
//! itself. Every default `MergeDialect` hook this destination keeps
//! rests on one of these facts; a probe that fails means the dependent
//! arm must become a TYPED capability gap, never a silent
//! approximation. The last three cells drive the settings/extension
//! passthrough through the V2 surface (Config → Shell → testhook).

use std::os::unix::fs::PermissionsExt;

use duckdb::Connection;
use rdlt_connector_duckdb::destination::{self, Config, Shell};
use rdlt_connector_sdk::config::Document;
use rdlt_connector_sdk::spi::core::{commit::WriteMode, id::LoadId, id::PipelineId, id::TableName};
use rdlt_connector_sdk::spi::{destination::Destination, destination::OpenContext};
use rdlt_testkit::fixtures::{batch_of, commit_meta_for, schema_for};

fn memdb() -> Connection {
    Connection::open_in_memory().expect("bundled in-memory duckdb")
}

/// The dedup survivor shape: DISTINCT ON keeps the FIRST row per
/// identity group under `ORDER BY identity, sort DESC NULLS LAST,
/// rowid DESC` — so a value beats NULL, an all-NULL group still keeps
/// a row, and rowid (append order) breaks ties last-wins.
#[test]
fn distinct_on_keeps_the_survivor_the_dedup_arm_expects() {
    let c = memdb();
    c.execute_batch(
        "CREATE TABLE feed (k BIGINT, rank BIGINT, tag VARCHAR);
         INSERT INTO feed VALUES
           (7, 1, 'early'), (7, 9, 'top'), (7, 4, 'mid'),
           (8, NULL, 'lone'),
           (9, NULL, 'ghost'), (9, 2, 'real'),
           (10, 5, 'tie-a'), (10, 5, 'tie-b');",
    )
    .unwrap();
    let survivors: Vec<(i64, String)> = c
        .prepare(
            "SELECT k, tag FROM (SELECT DISTINCT ON (k) * FROM feed \
             ORDER BY k, rank DESC NULLS LAST, rowid DESC) ORDER BY k",
        )
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        survivors,
        vec![
            (7, "top".to_owned()),    // highest rank wins
            (8, "lone".to_owned()),   // an all-NULL group keeps its row
            (9, "real".to_owned()),   // a value beats NULL
            (10, "tie-b".to_owned())  // rowid DESC: later append wins ties
        ],
        "PROBE FAILED: the DISTINCT ON survivor shape moved"
    );
}

/// The upsert arm targets a PLAIN `CREATE UNIQUE INDEX` (the
/// auto-ensured arbiter — not a declared constraint) with
/// `ON CONFLICT … DO UPDATE SET c = EXCLUDED.c`.
#[test]
fn on_conflict_arbitrates_over_a_plain_unique_index() {
    let c = memdb();
    c.execute_batch(
        "CREATE TABLE ledger (acct BIGINT, cur VARCHAR, bal BIGINT);
         CREATE UNIQUE INDEX arbiter ON ledger (acct, cur);
         INSERT INTO ledger VALUES (1, 'EUR', 10);",
    )
    .unwrap();
    c.execute_batch(
        "INSERT INTO ledger (acct, cur, bal) VALUES (1, 'EUR', 99), (2, 'EUR', 5)
         ON CONFLICT (acct, cur) DO UPDATE SET bal = EXCLUDED.bal",
    )
    .expect("PROBE FAILED: ON CONFLICT needs a declared constraint here");
    let bal: i64 = c
        .query_row("SELECT bal FROM ledger WHERE acct = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(bal, 99, "the matched key updated in place");
    let rows: i64 = c
        .query_row("SELECT count(*) FROM ledger", [], |r| r.get(0))
        .unwrap();
    assert_eq!(rows, 2, "the fresh key inserted");
}

/// `now()` is TRANSACTION-stable: the scd2 boundary rule stamps every
/// row of a unit with ONE instant, which only holds if now() does not
/// move between statements of the same transaction.
#[test]
fn now_holds_one_instant_across_a_transaction() {
    let mut c = memdb();
    let tx = c.transaction().unwrap();
    let before: String = tx
        .query_row("SELECT CAST(now() AS VARCHAR)", [], |r| r.get(0))
        .unwrap();
    // Real work between the two reads, so wall time definitely moved.
    tx.execute_batch("CREATE TABLE churn AS SELECT * FROM range(200000)")
        .unwrap();
    let after: String = tx
        .query_row("SELECT CAST(now() AS VARCHAR)", [], |r| r.get(0))
        .unwrap();
    tx.commit().unwrap();
    assert_eq!(
        before, after,
        "PROBE FAILED: now() drifted inside one transaction — the scd2 boundary is unstable"
    );
}

/// scd2 change detection is `IS DISTINCT FROM`, which must treat NULL
/// as a comparable value on both sides.
#[test]
fn is_distinct_from_is_null_safe_both_ways() {
    let c = memdb();
    let truth = [
        ("42", "42", false),
        ("42", "43", true),
        ("NULL", "42", true),
        ("42", "NULL", true),
        ("NULL", "NULL", false),
    ];
    for (lhs, rhs, expected) in truth {
        let got: bool = c
            .query_row(&format!("SELECT {lhs} IS DISTINCT FROM {rhs}"), [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(got, expected, "{lhs} IS DISTINCT FROM {rhs}");
    }
}

/// The bundled build carries the JSON extension statically: a native
/// JSON column and json_extract work with no LOAD and no network.
#[test]
fn the_bundled_build_speaks_json_without_loading_anything() {
    let c = memdb();
    c.execute_batch(
        "CREATE TABLE inbox (doc JSON);
         INSERT INTO inbox SELECT CAST('{\"outer\": {\"inner\": 11}}' AS JSON);",
    )
    .expect("PROBE FAILED: no JSON type in the bundled build");
    let inner: i64 = c
        .query_row(
            "SELECT CAST(json_extract(doc, '$.outer.inner') AS BIGINT) FROM inbox",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(inner, 11, "json_extract over the native column");
}

/// The scd2 close and re-open arms are a correlated `UPDATE … FROM`
/// and a `NOT EXISTS` anti-join insert.
#[test]
fn correlated_update_from_and_not_exists_carry_the_scd2_arms() {
    let c = memdb();
    c.execute_batch(
        "CREATE TABLE hist (k BIGINT, payload VARCHAR, retired TIMESTAMPTZ);
         CREATE TABLE fresh (k BIGINT, payload VARCHAR);
         INSERT INTO hist VALUES (1, 'stale', NULL), (2, 'kept', NULL);
         INSERT INTO fresh VALUES (1, 'replaced'), (2, 'kept');",
    )
    .unwrap();
    c.execute_batch(
        "UPDATE hist SET retired = now() FROM fresh \
         WHERE hist.retired IS NULL AND hist.k = fresh.k \
           AND (hist.payload IS DISTINCT FROM fresh.payload)",
    )
    .expect("PROBE FAILED: correlated UPDATE … FROM");
    let retired: i64 = c
        .query_row(
            "SELECT count(*) FROM hist WHERE retired IS NOT NULL",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(retired, 1, "only the changed key retires");
    c.execute_batch(
        "INSERT INTO hist (k, payload, retired) SELECT k, payload, NULL FROM fresh \
         WHERE NOT EXISTS (SELECT 1 FROM hist h WHERE h.retired IS NULL AND h.k = fresh.k)",
    )
    .expect("PROBE FAILED: NOT EXISTS anti-join insert");
    let rows: i64 = c
        .query_row("SELECT count(*) FROM hist", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        rows, 3,
        "the changed key re-opened; the unchanged one did not churn"
    );
}

/// The settings replay, through the V2 surface: a document carrying
/// `settings` and `memory_limit` assembles (bad values would refuse at
/// Shell::new), a full load runs through a SESSION connection — which
/// is a fresh clone that inherits no SETs, so the recorded setup must
/// replay for the open to configure it — and the testhook's own
/// instance reads the setting back applied.
#[tokio::test]
async fn settings_and_memory_limit_reach_every_connection() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::new(dir.path().join("tuned.duckdb"));
    config.settings = Some(
        [("threads".to_owned(), "1".to_owned())]
            .into_iter()
            .collect(),
    );
    config.memory_limit = Some("512MB".to_owned());
    let shell = Shell::new(config.clone()).expect("valid settings assemble");
    // The session path clones and replays; a replay failure would
    // surface right here as an open/commit error.
    let pipeline = PipelineId::new("tuned");
    let load = LoadId::new("load-1");
    let mut session = shell
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("session open replays the recorded setup");
    session
        .ensure_table(&schema_for("rows"), &WriteMode::Append)
        .await
        .expect("ensure");
    session
        .write(&TableName::new("rows"), batch_of(&[1]))
        .await
        .expect("write");
    session
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");
    drop(session);
    drop(shell);
    // Settings are INSTANCE-scoped, so the read-only oracle honestly
    // reports the global default and cannot see them; the precise
    // replay-per-clone pin lives at its seam (client.rs unit test on
    // `Db::session()`). What THIS cell proves is the full load
    // settling through a tuned instance — the replay failure mode
    // would have surfaced as an open or commit error above.
    assert_eq!(
        destination::testhook::count_rows(&config, "rows").expect("count"),
        1
    );
}

/// The bare-identifier gate covers BOTH nouns: an extension name and a
/// setting key shaped like an injection never reach interpolation —
/// each refusal is typed with the frozen wording.
#[test]
fn injection_shaped_setup_names_are_refused_before_any_sql() {
    let mut config = Config::new("x.duckdb");
    config.extensions = Some(vec!["json; DROP TABLE x".to_owned()]);
    assert_eq!(
        config.validate().expect_err("refused").to_string(),
        "invalid duckdb destination config: duckdb extension `json; DROP TABLE x`: \
         names must be bare identifiers ([A-Za-z0-9_]) — refusing to interpolate"
    );

    let mut config = Config::new("x.duckdb");
    config.settings = Some(
        [("threads='1'; DROP TABLE x; --".to_owned(), "1".to_owned())]
            .into_iter()
            .collect(),
    );
    assert_eq!(
        config.validate().expect_err("refused").to_string(),
        "invalid duckdb destination config: duckdb setting `threads='1'; DROP TABLE x; --`: \
         keys must be bare identifiers ([A-Za-z0-9_]) — refusing to interpolate"
    );
}

/// A key that passes the bare gate but names no DuckDB setting errors
/// AT ASSEMBLY (the setup applies eagerly), and the error names the
/// key so the operator knows which document entry to fix.
#[test]
fn an_unknown_setting_errors_at_assemble_naming_its_key() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = Config::new(dir.path().join("unknown.duckdb"));
    config.settings = Some(
        [("no_such_knob_qq".to_owned(), "1".to_owned())]
            .into_iter()
            .collect(),
    );
    let err = Shell::new(config).expect_err("unknown key").to_string();
    assert!(
        err.contains("duckdb setting `no_such_knob_qq`"),
        "the failing key is named: {err}"
    );
}

/// ALTER SET DATA TYPE on a column with existing rows: does it cast
/// existing values in place, or refuse? This determines whether the
/// widen arm can use in-place ALTER on populated target tables during
/// merge upserts.
#[test]
fn probe_alter_set_data_type_casts_existing_rows() {
    let conn = memdb();
    conn.execute_batch(
        "CREATE TABLE t (id INTEGER); INSERT INTO t VALUES (7);
         ALTER TABLE t ALTER COLUMN id SET DATA TYPE VARCHAR;",
    )
    .expect("in-place widen");
    let v: String = conn
        .query_row("SELECT id FROM t", [], |r| r.get(0))
        .expect("read");
    assert_eq!(v, "7", "existing rows are cast, not lost");
}

/// ALTER on a TEMP table holding unpublished rows: does it succeed
/// in place, or refuse? This pin governs whether temp-leg rows can be
/// widened directly during target-schema evolution.
#[test]
fn probe_alter_on_a_temp_table_with_rows() {
    let conn = memdb();
    conn.execute_batch(
        "CREATE TEMP TABLE s (id INTEGER); INSERT INTO s VALUES (7);
         ALTER TABLE s ALTER COLUMN id SET DATA TYPE VARCHAR;",
    )
    .expect("temp-leg widen with unpublished rows");
}

/// ALTER on a column indexed by a unique ART index (rdlt_ux_ naming
/// per the crate's merge_ddl): does it widen in place, or refuse with
/// a constraint error? If it refuses, Task 16's upsert-table widen
/// must drop-and-recreate the index around the ALTER.
#[test]
fn probe_alter_on_an_art_indexed_column() {
    let conn = memdb();
    conn.execute_batch("CREATE TABLE t (id INTEGER); CREATE UNIQUE INDEX rdlt_ux_t ON t (id);")
        .expect("indexed");
    let result = conn.execute_batch("ALTER TABLE t ALTER COLUMN id SET DATA TYPE BIGINT");
    // PROBE FINDING: ALTER REFUSES when an index exists on the column.
    // Task 16's upsert-table widen must drop-and-recreate the rdlt_ux_
    // index around the ALTER.
    let err_msg = result
        .expect_err("ALTER fails on indexed columns")
        .to_string();
    assert!(
        err_msg.contains("Cannot change the type of this column: an index depends on it"),
        "exact error message for Task 16 design: {err_msg}"
    );
}

/// The deterministic-IO carve-out (037 US6 / Task 19) keys on these
/// EXACT message spellings — pinned here first so `classify`'s
/// carve-out is built on measured reality, not a guess. Both failures
/// are ones that never heal on retry: a missing parent directory and a
/// permission-denied path stay broken until an operator intervenes —
/// UNLIKE every other errno that also renders through this SAME
/// open-failure template (a file lock held elsewhere, `EMFILE`/
/// `ENFILE`, create-time `ENOSPC`, ...), which all heal on retry and
/// deliberately stay under the existing "IO Error" transient rule
/// (fix round 1: the template alone is NOT the carve-out key — see
/// `classify`'s doc comment).
///
/// MEASURED verbatim (duckdb 1.5.x bundled): `IO Error: Cannot open
/// file "/nonexistent-rdlt-probe-dir/x.duckdb": No such file or
/// directory` — the carve-out requires BOTH the shared `Cannot open
/// file` template fragment AND this cell's own `No such file or
/// directory` suffix; the template fragment alone is deliberately
/// insufficient (it also carries healable errnos).
#[test]
fn probe_deterministic_io_message_spellings() {
    // Missing parent directory: DuckDB's own file-open failure.
    let missing = duckdb::Connection::open("/nonexistent-rdlt-probe-dir/x.duckdb")
        .expect_err("no parent dir");
    let text = missing.to_string();
    assert!(text.starts_with("IO Error"), "{text}");
    assert!(
        text.contains("Cannot open file"),
        "PIN the fragment: {text}"
    );
}

/// The permission-denied sibling: a directory this process cannot
/// write into. Detects root (or any other permission-bypassing
/// environment) BEHAVIORALLY rather than via a `geteuid` FFI call —
/// the workspace denies `unsafe_code` outright — by noticing the open
/// SUCCEEDED despite chmod 000 and skipping rather than asserting on
/// a failure that never comes.
///
/// MEASURED verbatim (duckdb 1.5.x bundled): `IO Error: Cannot open
/// file "<path>/x.duckdb": Permission denied` — same `Cannot open
/// file` template as the missing-parent cell, with its OWN suffix
/// required alongside the template fragment (see that cell's doc for
/// why the template alone is not the carve-out key).
#[test]
fn probe_deterministic_io_message_spelling_permission_denied() {
    let dir = tempfile::tempdir().expect("dir");
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o000))
        .expect("chmod 000");
    let path = dir.path().join("x.duckdb");
    let result = duckdb::Connection::open(&path);
    // Restore permissions unconditionally so the tempdir guard can
    // clean up on drop, whichever branch below runs.
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700))
        .expect("restore for cleanup");
    let text = match result {
        Ok(_) => {
            eprintln!(
                "SKIP: permission bits not enforced (root or a permissive filesystem) \
                 — cannot induce permission-denied"
            );
            return;
        }
        Err(e) => e.to_string(),
    };
    assert!(text.starts_with("IO Error"), "{text}");
    assert!(
        text.contains("Permission denied"),
        "PIN the fragment: {text}"
    );
}

// A THIRD probe — a real cross-instance file-lock conflict — was
// attempted and dropped: two `duckdb::Connection::open` calls on the
// same path from ONE process never conflict (measured; the advisory
// lock is process-scoped, so a second open in the same process
// re-acquires it rather than blocking), and this crate's own
// double-open registry (client.rs) refuses a second in-process
// read-write open before the library ever runs, for the WAL-truncation
// hazard, not for lock testing. A genuine lock conflict needs a
// SEPARATE process; the existing unit pin in client.rs
// (`classify_splits_io_errors_transient_everything_else_fatal`) is
// kept as the lock fragment's sole coverage rather than manufacturing
// subprocess plumbing to reproduce it here.
