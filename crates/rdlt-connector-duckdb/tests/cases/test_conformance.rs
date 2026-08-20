//! Certification and end-to-end: the sdk destination kit over the Shell
//! (certified = passes conformance), then the engine driving real JSON
//! feeds into a database file — shredding, resume, in-span dedup, and
//! cross-run column drift.

use async_trait::async_trait;
use rdlt_connector_duckdb::destination::{Config, Shell};
use rdlt_connector_sdk::spi::core::{commit::WriteMode, id::TableName};
use rdlt_connector_sdk::spi::source::StreamSpec;
use rdlt_engine::config::Config as EngineConfig;
use rdlt_engine::engine::Engine;
use rdlt_testkit::{
    conformance::assert_conformant, conformance::destination::ProbeError,
    conformance::destination::TableProbe, conformance::destination::verify as verify_destination,
    memory::Batch as MemoryBatch, memory::Source as MemorySource, memory::Stream as MemoryStream,
};
use serde_json::json;

use super::common::{rows_in, scalar};

/// Counts alongside the LIVE shell through a READ-ONLY instance per
/// probe. This cannot be `testhook::count_rows`: the kit probes while
/// the shell under test stays open, and a second READ-WRITE instance on
/// the same file replays-and-truncates the live instance's WAL (the
/// client.rs instance model), silently swallowing every commit the kit
/// makes afterwards — measured here as D4/D8 "found 0". A read-only
/// instance replays the WAL without touching it. A table the kit has
/// not created yet counts as 0.
struct FileCount(std::path::PathBuf);

#[async_trait]
impl TableProbe for FileCount {
    async fn count(&self, table: &TableName) -> Result<u64, ProbeError> {
        // The one absence-vs-failure rule, shared with the spawned-bin
        // SnapshotCount ([`super::common::count_at`]).
        super::common::count_at(&self.0, table.as_str())
    }
}

/// The in-process probe's fail-open fold closed (042 round-2 fix wave —
/// wave 1 closed the open() arm only): absence reads as zero, a genuine
/// query failure is a probe error. Plant and asserts are the shared pin
/// body ([`super::common::assert_probe_counts_absence_but_fails_broken_reads`]).
#[tokio::test]
async fn file_count_absence_is_zero_but_a_broken_read_is_a_probe_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let file = super::common::plant_broken_view_store(dir.path());

    super::common::assert_probe_counts_absence_but_fails_broken_reads(&FileCount(file)).await;
}

#[tokio::test]
async fn the_sdk_kit_certifies_the_shell() {
    let dir = tempfile::tempdir().expect("dir");
    let file = dir.path().join("kit.duckdb");
    let shell = Shell::new(Config::new(&file)).expect("valid");
    assert_conformant(
        verify_destination(&shell, &FileCount(file))
            .await
            .expecting_no_skips(),
    );
}

/// Nested JSON through the engine: structs stay struct-native (dot
/// syntax answers), arrays shred into a child table that joins its
/// parent on the lineage ids.
#[tokio::test(flavor = "multi_thread")]
async fn nested_documents_land_as_structs_and_child_tables() {
    let dir = tempfile::tempdir().expect("dir");
    let file = dir.path().join("nested.duckdb");
    let dest = Shell::new(Config::new(&file)).expect("valid");
    let source = MemorySource::single_stream(
        StreamSpec::new("users"),
        vec![
            json!({"id": 1, "name": "lin", "home": {"city": "Warsaw"},
                   "tags": [{"tag": "alpha"}, {"tag": "beta"}]}),
            json!({"id": 2, "name": "mo", "home": {"city": "Oslo"}, "tags": []}),
        ],
    );
    let config = EngineConfig::new("nested").with_workdir(dir.path().join("wal"));
    let report = Engine::new(config, source, dest).run().await.expect("run");

    assert_eq!(report.total_rows(), 4, "two parents + two children");
    assert_eq!(rows_in(&file, "users"), 2);
    assert_eq!(rows_in(&file, "users__tags"), 2);
    assert_eq!(
        scalar(&file, "SELECT home.city FROM users WHERE id = 1"),
        "Warsaw",
        "struct-native lowering answers dot syntax"
    );
    assert_eq!(
        scalar(
            &file,
            "SELECT c.tag FROM users__tags c \
             JOIN users p ON c._rdlt_parent_id = p._rdlt_id \
             WHERE p.id = 1 ORDER BY c._rdlt_pos LIMIT 1"
        ),
        "alpha",
        "children join their parent on the lineage ids"
    );
}

/// A second run of the same pipeline resumes from the persisted cursor:
/// zero new rows read, zero rows duplicated.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_incremental_run_reads_nothing_new() {
    let dir = tempfile::tempdir().expect("dir");
    let file = dir.path().join("resume.duckdb");
    let feed = || {
        MemorySource::new(vec![MemoryStream::new(
            StreamSpec::new("ticks"),
            vec![
                MemoryBatch::new(vec![json!({"n": 1})]).with_checkpoint(1),
                MemoryBatch::new(vec![json!({"n": 2})]).with_checkpoint(2),
            ],
        )])
    };

    // Each run gets its own Shell so the instance is closed before the
    // count reads the file (sequential re-open — the documented-safe
    // pattern; an instance held open across a count loses later writes).
    let dest = Shell::new(Config::new(&file)).expect("valid");
    Engine::new(EngineConfig::new("resume"), feed(), dest)
        .run()
        .await
        .expect("run 1");
    assert_eq!(rows_in(&file, "ticks"), 2);

    let dest = Shell::new(Config::new(&file)).expect("valid");
    let second = Engine::new(EngineConfig::new("resume"), feed(), dest)
        .run()
        .await
        .expect("run 2");
    assert_eq!(second.total_rows(), 0, "the cursor already covers the feed");
    assert_eq!(rows_in(&file, "ticks"), 2, "and nothing duplicated");
}

/// Two versions of one key inside a single commit span: the LAST version
/// wins, deterministically.
#[tokio::test(flavor = "multi_thread")]
async fn in_span_merge_dedup_keeps_the_last_version() {
    let dir = tempfile::tempdir().expect("dir");
    let file = dir.path().join("lastwins.duckdb");
    let dest = Shell::new(Config::new(&file)).expect("valid");
    let source = MemorySource::single_stream(
        StreamSpec::new("kv").with_primary_key(["k"]),
        vec![json!({"k": 5, "v": "stale"}), json!({"k": 5, "v": "fresh"})],
    );
    let config = EngineConfig::new("lastwins").with_write_mode(WriteMode::Merge {
        key: vec!["k".into()],
    });
    Engine::new(config, source, dest).run().await.expect("run");

    assert_eq!(rows_in(&file, "kv"), 1);
    assert_eq!(scalar(&file, "SELECT v FROM kv WHERE k = 5"), "fresh");
}

/// Cross-run column drift publishes BY NAME: a later run that discovers
/// an extra column must land every value in its named column — and the
/// pre-drift row backfills NULL for the newcomer.
#[tokio::test(flavor = "multi_thread")]
async fn cross_run_column_drift_publishes_by_name() {
    let dir = tempfile::tempdir().expect("dir");
    let file = dir.path().join("drift.duckdb");
    let dest = Shell::new(Config::new(&file)).expect("valid");

    // Run 1: columns (a, b).
    let source =
        MemorySource::single_stream(StreamSpec::new("t"), vec![json!({"a": "a1", "b": "b1"})]);
    Engine::new(EngineConfig::new("drift"), source, dest.clone())
        .run()
        .await
        .expect("run 1");

    // Run 2: column c appears.
    let source = MemorySource::single_stream(
        StreamSpec::new("t"),
        vec![json!({"c": "c2", "b": "b2", "a": "a2"})],
    );
    Engine::new(EngineConfig::new("drift"), source, dest)
        .run()
        .await
        .expect("run 2");

    assert_eq!(rows_in(&file, "t"), 2);
    assert_eq!(
        scalar(&file, "SELECT a FROM t WHERE b = 'b2'"),
        "a2",
        "values land in their named columns"
    );
    assert_eq!(
        scalar(&file, "SELECT c FROM t WHERE b = 'b2'"),
        "c2",
        "the drift column carries its value"
    );
    assert_eq!(
        scalar(
            &file,
            "SELECT count(*)::VARCHAR FROM t WHERE b = 'b1' AND c IS NULL"
        ),
        "1",
        "the pre-drift row backfills NULL, never a shifted value"
    );
}

/// The S3 record (031, re-derived here as 028's read-before-write
/// catalog image): run 1 commits `n` as an integer; run 2 is a FRESH
/// session (a new `Shell`/`Load`, this crate's cross-run unit) whose
/// batch carries only a text value, so the engine's own schema
/// evolution widens its tracked schema for `n` to text before ever
/// calling `ensure_table`. Before 037 the fresh session's `previous`
/// was empty, so no widen was planned and the target's `n` column
/// stayed BIGINT while the stage table landed VARCHAR — the mismatch
/// surfaced at PUBLISH (`INSERT … SELECT` casting `'two'` into
/// BIGINT), not at the appender (recorded here since the brief's
/// prediction named the appender as one candidate site — this is the
/// other). The catalog image now feeds the widen planner even on a
/// fresh session, so the target's `n` column casts along with it and
/// both runs' rows publish.
#[tokio::test(flavor = "multi_thread")]
async fn a_type_widened_since_the_last_run_plans_a_real_alter() {
    let dir = tempfile::tempdir().expect("dir");
    let file = dir.path().join("widen.duckdb");

    // Run 1: `n` is an integer.
    let dest = Shell::new(Config::new(&file)).expect("valid");
    let source = MemorySource::single_stream(StreamSpec::new("t"), vec![json!({"n": 1})]);
    Engine::new(EngineConfig::new("widen"), source, dest)
        .run()
        .await
        .expect("run 1");

    // Run 2: a FRESH session over the SAME file — `n` carries text now.
    let dest = Shell::new(Config::new(&file)).expect("valid");
    let source = MemorySource::single_stream(StreamSpec::new("t"), vec![json!({"n": "two"})]);
    Engine::new(EngineConfig::new("widen"), source, dest)
        .run()
        .await
        .expect("run 2");

    assert_eq!(rows_in(&file, "t"), 2, "both runs' rows are present, cast");
    assert_eq!(
        scalar(&file, "SELECT n FROM t WHERE n = '1'"),
        "1",
        "run 1's row survived the widen, cast to text"
    );
    assert_eq!(
        scalar(&file, "SELECT n FROM t WHERE n = 'two'"),
        "two",
        "run 2's row landed in the widened column"
    );
}

/// The probe-fact consequence (Task 15, not in the brief): a MERGE-mode
/// table's upsert arbiter is a UNIQUE ART index on the merge key, and
/// DuckDB refuses `SET DATA TYPE` on a column that index depends on.
/// Run 1 creates the table (and its unique index on `id`) under
/// `upsert`; run 2 is a fresh session whose merge key `id` carries text
/// — the widen must drop the `rdlt_ux_` index, ALTER, and recreate it
/// (schema.rs's OWN `CREATE UNIQUE INDEX IF NOT EXISTS` renderer, never
/// a second copy of that SQL). The index must still ENFORCE afterward:
/// a duplicate key inserted post-widen gets the duplicate-key diagnosis,
/// not a silently duplicated row.
#[tokio::test(flavor = "multi_thread")]
async fn a_cross_run_widen_on_the_merge_key_survives_its_unique_index() {
    use super::common::{dest_with, drive, ints, keyed, merge_on, texts, unit};

    let dir = tempfile::tempdir().expect("dir");
    let file = dir.path().join("widen_merge.duckdb");
    // upsert requires a KEYED STRUCTURED stream (no shredder identity
    // column) — `keyed()` builds exactly that, Arrow-typed so the merge
    // key's TYPE can differ run to run. Each run gets its OWN Shell (not
    // a clone) so its instance is fully closed before the next run opens
    // — and before the read-only probes below reopen the file
    // (`rows_in`/`scalar` are sequential-only: they replay the WAL,
    // which needs the writer closed first).

    // Run 1: `id` is an integer, upsert strategy — the arbiter unique
    // index lands on `id` (the declared key, no shredder identity).
    drive(
        merge_on("widen-merge", &["id"]),
        keyed(
            "items",
            &["id"],
            unit(&[("id", ints(&[Some(1)])), ("v", texts(&[Some("a")]))]),
        ),
        dest_with(&file, json!({"merge_strategy": "upsert"})),
    )
    .await
    .expect("run 1");

    // Run 2: a FRESH session — `id` carries text now.
    drive(
        merge_on("widen-merge", &["id"]),
        keyed(
            "items",
            &["id"],
            unit(&[("id", texts(&[Some("two")])), ("v", texts(&[Some("b")]))]),
        )
        .resuming_after(1),
        dest_with(&file, json!({"merge_strategy": "upsert"})),
    )
    .await
    .expect("run 2 — the index must not block the widen");

    assert_eq!(rows_in(&file, "items"), 2, "both keys survived, unmerged");
    assert_eq!(
        scalar(&file, "SELECT v FROM items WHERE id = 'two'"),
        "b",
        "run 2's row landed with its widened key"
    );

    // The index still enforces: a THIRD run redelivering `id = "two"`
    // must upsert in place, not duplicate the row.
    drive(
        merge_on("widen-merge", &["id"]),
        keyed(
            "items",
            &["id"],
            unit(&[
                ("id", texts(&[Some("two")])),
                ("v", texts(&[Some("b-edited")])),
            ]),
        )
        .resuming_after(2),
        dest_with(&file, json!({"merge_strategy": "upsert"})),
    )
    .await
    .expect("run 3");

    assert_eq!(
        rows_in(&file, "items"),
        2,
        "the recreated index still enforces the merge key — no duplicate"
    );
    assert_eq!(
        scalar(&file, "SELECT v FROM items WHERE id = 'two'"),
        "b-edited",
        "the redelivered key upserted in place"
    );
}

/// Fix round 1 (F1, CRITICAL — 2026-08 review): the raw image feeds a
/// types-DIFFER planner with no notion of DIRECTION. Run 1 mixes a
/// zip-code-shaped string with a plain int in ONE batch, so the
/// engine's own intra-run widening lands `n` as text; run 2 sees only
/// integers, and BEFORE the F1 fix the image (text) vs def (int)
/// mismatch planned `ALTER … SET DATA TYPE BIGINT` — DuckDB's cast
/// accepts `'01234'` (silently dropping the leading zero, becoming
/// `1234`) and `'55555'`, so the ALTER SUCCEEDS and the run REPORTS
/// SUCCESS while irreversibly rewriting already-committed data. After
/// the fix, an incoming type going text → int is never a WIDEN (the
/// engine's own evolution contract only ever widens; a destination
/// catalog going text → int across runs is drift, not growth), so the
/// column is left untouched and the narrower int values simply cast UP
/// to text at publish — the pre-037 behavior.
#[tokio::test(flavor = "multi_thread")]
async fn a_narrowing_type_change_since_the_last_run_never_alters_committed_data() {
    let dir = tempfile::tempdir().expect("dir");
    let file = dir.path().join("narrow.duckdb");

    // Run 1: `n` mixes a zip-shaped string and a plain int in ONE
    // batch — intra-run widening lands the column as text.
    let dest = Shell::new(Config::new(&file)).expect("valid");
    let source = MemorySource::single_stream(
        StreamSpec::new("t"),
        vec![json!({"n": "01234"}), json!({"n": 55555})],
    );
    Engine::new(EngineConfig::new("narrow"), source, dest)
        .run()
        .await
        .expect("run 1");
    assert_eq!(
        scalar(
            &file,
            "SELECT data_type FROM information_schema.columns \
             WHERE table_name = 't' AND column_name = 'n'"
        ),
        "VARCHAR",
        "intra-run widening lands text, same as before this feature"
    );

    // Run 2: a FRESH session — `n` carries only integers now.
    let dest = Shell::new(Config::new(&file)).expect("valid");
    let source = MemorySource::single_stream(StreamSpec::new("t"), vec![json!({"n": 42})]);
    Engine::new(EngineConfig::new("narrow"), source, dest)
        .run()
        .await
        .expect("run 2 must still succeed — this used to succeed before the image existed");

    assert_eq!(
        scalar(
            &file,
            "SELECT data_type FROM information_schema.columns \
             WHERE table_name = 't' AND column_name = 'n'"
        ),
        "VARCHAR",
        "the column is NEVER narrowed — an int def never widens a text image"
    );
    assert_eq!(
        scalar(&file, "SELECT n FROM t WHERE n = '01234'"),
        "01234",
        "run 1's committed value survives BYTE-IDENTICAL — no silent rewrite"
    );
    assert_eq!(rows_in(&file, "t"), 3, "all three rows landed");
}

/// F1's other face: a run whose data is UNCASTABLE against the image's
/// type used to FAIL where it once succeeded. Run 1 commits `n` as
/// text with a non-numeric value; run 2 sees only integers. BEFORE the
/// fix, the image (text) vs def (int) mismatch planned `ALTER … SET
/// DATA TYPE BIGINT`, and `'two'` cannot cast to BIGINT — DuckDB
/// refuses, and a run that published fine before this feature existed
/// now errors. After the fix, text → int is never a widen, so no ALTER
/// is even attempted; run 2 succeeds.
#[tokio::test(flavor = "multi_thread")]
async fn an_uncastable_type_change_since_the_last_run_still_succeeds() {
    let dir = tempfile::tempdir().expect("dir");
    let file = dir.path().join("uncastable.duckdb");

    let dest = Shell::new(Config::new(&file)).expect("valid");
    let source = MemorySource::single_stream(StreamSpec::new("t"), vec![json!({"n": "two"})]);
    Engine::new(EngineConfig::new("uncastable"), source, dest)
        .run()
        .await
        .expect("run 1");

    let dest = Shell::new(Config::new(&file)).expect("valid");
    let source = MemorySource::single_stream(StreamSpec::new("t"), vec![json!({"n": 1})]);
    Engine::new(EngineConfig::new("uncastable"), source, dest)
        .run()
        .await
        .expect("run 2 must succeed — this used to succeed before the image existed");

    assert_eq!(rows_in(&file, "t"), 2, "both runs' rows landed");
    assert_eq!(
        scalar(&file, "SELECT n FROM t WHERE n = '1'"),
        "1",
        "the int value cast UP to the image's text column, never the reverse"
    );
}

/// Fix round 2 (N1, CRITICAL — 2026-08 review round 2): `Int64 ->
/// Float64` is NOT a safe cross-run widen. rdlt-core's own lattice
/// only takes this edge after a per-VALUE check (`int64_fits_in_f64`,
/// exact only within ±2^53) that a destination widening already-
/// committed rows cannot perform. Run 1 commits `9007199254740993`
/// (2^53 + 1, exactly one past the exact-float boundary) as `BIGINT`;
/// run 2's own batch carries only a float, so its fresh inference types
/// `n` as `Float64`. Before the N1 fix, `is_widening(Int64, Float64)`
/// was `true`, so the image kept its OLD (Int64) type, `schema_steps`
/// saw the mismatch, and `ALTER … SET DATA TYPE DOUBLE` cast the
/// committed row — silently and irreversibly turning
/// `9007199254740993` into `9007199254740992` while the run still
/// reported success. After the fix, the edge is gone: the column
/// stays `BIGINT` and run 1's value survives byte-identical.
#[tokio::test(flavor = "multi_thread")]
async fn an_int64_beyond_the_exact_float_boundary_is_never_widened_to_float() {
    let dir = tempfile::tempdir().expect("dir");
    let file = dir.path().join("int64_boundary.duckdb");

    // Run 1: `n` is an integer beyond ±2^53 (9007199254740992) — the
    // largest value a f64 can no longer represent exactly.
    let dest = Shell::new(Config::new(&file)).expect("valid");
    let source = MemorySource::single_stream(
        StreamSpec::new("t"),
        vec![json!({"n": 9_007_199_254_740_993_i64})],
    );
    Engine::new(EngineConfig::new("int64-boundary"), source, dest)
        .run()
        .await
        .expect("run 1");
    assert_eq!(
        scalar(
            &file,
            "SELECT data_type FROM information_schema.columns \
             WHERE table_name = 't' AND column_name = 'n'"
        ),
        "BIGINT"
    );

    // Run 2: a FRESH session — this run's own batch carries only a
    // float, so ITS fresh inference types `n` as Float64.
    let dest = Shell::new(Config::new(&file)).expect("valid");
    let source = MemorySource::single_stream(StreamSpec::new("t"), vec![json!({"n": 1.5})]);
    Engine::new(EngineConfig::new("int64-boundary"), source, dest)
        .run()
        .await
        .expect("run 2");

    assert_eq!(
        scalar(
            &file,
            "SELECT data_type FROM information_schema.columns \
             WHERE table_name = 't' AND column_name = 'n'"
        ),
        "BIGINT",
        "never widened to DOUBLE — the value-checked edge is absent here"
    );
    assert_eq!(
        scalar(&file, "SELECT n::VARCHAR FROM t WHERE n = 9007199254740993"),
        "9007199254740993",
        "run 1's value survives EXACTLY — no silent cast to ...992"
    );
}
