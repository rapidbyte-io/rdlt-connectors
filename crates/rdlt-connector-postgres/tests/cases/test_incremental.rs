//! Cursor-incremental semantics driven end to end, postgres source into a
//! DuckDB destination: delta loads, the closed boundary and its exclusive
//! opt-out, NULL-cursor policies, a clock that regresses, configured
//! windows, row-hash keys where no primary key exists, the lag window, and
//! the structured-stream Merge boundary (engine clause B4).
//!
//! ABOVE THE SIZE CEILING, DELIBERATELY: nineteen cells over ONE shared rig
//! exercising one contract — the incremental state machine keeps its own
//! parts together for the same reason.

use crate::cases::common;
use rdlt_connector_duckdb::destination as duck;
use rdlt_connector_postgres::fixtures::PostgresContainer;
use rdlt_connector_postgres::source;
use rdlt_engine::{Engine, EngineConfig};

const BASE: &str = r#"
CREATE TABLE ev (id int8 PRIMARY KEY, v text, ts timestamptz);
INSERT INTO ev VALUES
  (1, 'a', '2026-01-01T00:00:00Z'),
  (2, 'b', '2026-01-02T00:00:00Z'),
  (3, 'c', '2026-01-02T00:00:00Z');
"#;

fn ev_source(connection_string: &str, cursor_extra: &str) -> source::Shell {
    source::Shell::from_yaml(&format!(
        "conn: \"{connection_string}\"\nbatch_max_rows: 2\ntables:\n  - name: ev\n    cursor:\n      column: ts\n{cursor_extra}"
    ))
    .expect("config")
}

struct Harness {
    destination: common::DuckDbDest,
}

impl Harness {
    fn new() -> Self {
        Self {
            destination: common::duckdb_destination(),
        }
    }

    async fn run(&self, postgres_source: source::Shell, pipeline: &str) -> u64 {
        let report = Engine::new(
            EngineConfig::new(pipeline),
            postgres_source,
            self.destination.shell(),
        )
        .run()
        .await
        .expect("run");
        report.total_rows()
    }

    fn count(&self) -> u64 {
        self.destination.count_rows("ev").expect("count")
    }

    fn distinct_ids(&self) -> String {
        self.destination
            .query_string("SELECT CAST(count(DISTINCT id) AS VARCHAR) FROM ev")
            .expect("distinct")
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn delta_loads_and_closed_boundary_dedup() {
    let Some(fixture) = PostgresContainer::start().await else {
        return;
    };
    fixture.seed(BASE).await;
    let harness = Harness::new();

    // Run 1: full load.
    assert_eq!(
        harness
            .run(ev_source(&fixture.connection_string, ""), "inc")
            .await,
        3
    );
    assert_eq!(harness.count(), 3);

    // New row AT the exact watermark (2026-01-02) plus one beyond it.
    fixture
        .seed(
            "INSERT INTO ev VALUES (4, 'd', '2026-01-02T00:00:00Z'), \
             (5, 'e', '2026-01-03T00:00:00Z');",
        )
        .await;
    // Run 2: closed boundary re-fetches watermark-equal rows; ids 2 and 3 are
    // deduped source-side via boundary keys — exactly the two new rows load.
    assert_eq!(
        harness
            .run(ev_source(&fixture.connection_string, ""), "inc")
            .await,
        2
    );
    assert_eq!(harness.count(), 5, "no duplicates");
    assert_eq!(harness.distinct_ids(), "5");

    // Run 3 with nothing new: zero rows move.
    assert_eq!(
        harness
            .run(ev_source(&fixture.connection_string, ""), "inc")
            .await,
        0
    );
    assert_eq!(harness.count(), 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn open_boundary_skips_watermark_equal_rows() {
    let Some(fixture) = PostgresContainer::start().await else {
        return;
    };
    fixture.seed(BASE).await;
    let harness = Harness::new();
    let cursor_extra = "      boundary: exclusive\n";

    assert_eq!(
        harness
            .run(
                ev_source(&fixture.connection_string, cursor_extra),
                "inc-open"
            )
            .await,
        3
    );
    // A late row at the exact watermark: the open boundary (strict >) never
    // re-fetches it — the documented monotonic-cursor trade-off.
    fixture
        .seed("INSERT INTO ev VALUES (4, 'd', '2026-01-02T00:00:00Z');")
        .await;
    assert_eq!(
        harness
            .run(
                ev_source(&fixture.connection_string, cursor_extra),
                "inc-open"
            )
            .await,
        0
    );
    assert_eq!(
        harness.count(),
        3,
        "watermark-equal late row skipped by design"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn null_cursor_rows_are_excluded_by_default_and_reload_every_run_under_include() {
    let Some(fixture) = PostgresContainer::start().await else {
        return;
    };
    fixture.seed(BASE).await;
    fixture
        .seed("INSERT INTO ev VALUES (90, 'n1', NULL), (91, 'n2', NULL);")
        .await;

    // Default exclude: NULL-cursor rows never load.
    let harness = Harness::new();
    assert_eq!(
        harness
            .run(ev_source(&fixture.connection_string, ""), "inc-nx")
            .await,
        3
    );
    assert_eq!(harness.count(), 3);

    // Include: NULL-cursor rows load on EVERY run (they carry no watermark) —
    // the documented Append-mode consequence.
    let harness = Harness::new();
    let cursor_extra = "      nulls: include\n";
    assert_eq!(
        harness
            .run(
                ev_source(&fixture.connection_string, cursor_extra),
                "inc-ni"
            )
            .await,
        5
    );
    assert_eq!(
        harness
            .run(
                ev_source(&fixture.connection_string, cursor_extra),
                "inc-ni"
            )
            .await,
        2
    );
    let null_copies = harness
        .destination
        .query_string("SELECT CAST(count(*) AS VARCHAR) FROM ev WHERE id = 90")
        .expect("null copies");
    assert_eq!(
        null_copies, "2",
        "null-cursor rows re-fetch every run under include"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn regressing_clock_never_moves_watermark_backward() {
    let Some(fixture) = PostgresContainer::start().await else {
        return;
    };
    fixture.seed(BASE).await;
    let harness = Harness::new();

    assert_eq!(
        harness
            .run(ev_source(&fixture.connection_string, ""), "inc-reg")
            .await,
        3
    );
    // Clock skew writes a row BELOW the committed watermark: invisible to
    // cursor incremental (the documented caveat), and the watermark must not
    // regress because of it.
    fixture
        .seed("INSERT INTO ev VALUES (6, 'skew', '2026-01-01T12:00:00Z');")
        .await;
    assert_eq!(
        harness
            .run(ev_source(&fixture.connection_string, ""), "inc-reg")
            .await,
        0
    );
    // A later row still loads exactly once — state stayed at the max.
    fixture
        .seed("INSERT INTO ev VALUES (7, 'later', '2026-01-04T00:00:00Z');")
        .await;
    assert_eq!(
        harness
            .run(ev_source(&fixture.connection_string, ""), "inc-reg")
            .await,
        1
    );
    assert_eq!(harness.count(), 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn initial_and_end_value_window() {
    let Some(fixture) = PostgresContainer::start().await else {
        return;
    };
    fixture.seed(BASE).await;
    fixture
        .seed("INSERT INTO ev VALUES (8, 'h', '2026-01-05T00:00:00Z'), (9, 'i', '2026-01-06T00:00:00Z');")
        .await;
    let harness = Harness::new();
    let cursor_extra = "      initial_value: \"2026-01-02T00:00:00Z\"\n      end_value: \"2026-01-06T00:00:00Z\"\n";
    // Closed start (>= 01-02), exclusive end (< 01-06): ids 2,3,8.
    assert_eq!(
        harness
            .run(
                ev_source(&fixture.connection_string, cursor_extra),
                "inc-win"
            )
            .await,
        3
    );
    let ids = harness
        .destination
        .query_string(
            "SELECT CAST(string_agg(CAST(id AS VARCHAR), ',' ORDER BY id) AS VARCHAR) FROM ev",
        )
        .expect("ids");
    assert_eq!(ids, "2,3,8");
}

#[tokio::test(flavor = "multi_thread")]
async fn pkless_table_dedups_via_row_hash() {
    let Some(fixture) = PostgresContainer::start().await else {
        return;
    };
    fixture
        .seed(
            "CREATE TABLE ev (id int8, v text, ts timestamptz); \
             INSERT INTO ev VALUES (1, 'a', '2026-01-02T00:00:00Z'), (2, 'b', '2026-01-02T00:00:00Z');",
        )
        .await;
    let harness = Harness::new();
    assert_eq!(
        harness
            .run(ev_source(&fixture.connection_string, ""), "inc-hash")
            .await,
        2
    );
    // New DISTINCT row at the watermark: row-hash dedup passes it, drops the
    // re-fetched identical ones.
    fixture
        .seed("INSERT INTO ev VALUES (3, 'c', '2026-01-02T00:00:00Z');")
        .await;
    assert_eq!(
        harness
            .run(ev_source(&fixture.connection_string, ""), "inc-hash")
            .await,
        1
    );
    assert_eq!(harness.count(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn uuid_cursor_end_to_end() {
    // Pre-fix, a uuid cursor generated `"col" >= '...'::text`, which has no
    // uuid>=text operator — a guaranteed runtime error on the second run.
    let Some(fixture) = PostgresContainer::start().await else {
        return;
    };
    fixture
        .seed(
            "CREATE TABLE ev (id uuid PRIMARY KEY, v text); \
             INSERT INTO ev VALUES \
               ('00000000-0000-0000-0000-000000000001', 'a'), \
               ('00000000-0000-0000-0000-000000000002', 'b');",
        )
        .await;
    let harness = Harness::new();
    let uuid_source = |connection_string: &str| {
        source::Shell::from_yaml(&format!(
            "conn: \"{connection_string}\"\ntables:\n  - name: ev\n    cursor:\n      column: id\n"
        ))
        .expect("config")
    };
    assert_eq!(
        harness
            .run(uuid_source(&fixture.connection_string), "inc-uuid")
            .await,
        2
    );
    fixture
        .seed("INSERT INTO ev VALUES ('00000000-0000-0000-0000-000000000003', 'c');")
        .await;
    assert_eq!(
        harness
            .run(uuid_source(&fixture.connection_string), "inc-uuid")
            .await,
        1
    );
    assert_eq!(harness.count(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn text_cursor_mixed_case_byte_order() {
    // COLLATE "C" pins SQL ordering/filtering to the tracker's Rust byte
    // order: 'B' < 'a' in bytes, though most locales sort 'a' < 'B'.
    let Some(fixture) = PostgresContainer::start().await else {
        return;
    };
    fixture
        .seed(
            "CREATE TABLE ev (id int8 PRIMARY KEY, name text); \
             INSERT INTO ev VALUES (1, 'Alpha'), (2, 'beta'), (3, 'Gamma');",
        )
        .await;
    let harness = Harness::new();
    let name_source = |connection_string: &str| {
        source::Shell::from_yaml(&format!(
            "conn: \"{connection_string}\"\ntables:\n  - name: ev\n    cursor:\n      column: name\n"
        ))
        .expect("config")
    };
    assert_eq!(
        harness
            .run(name_source(&fixture.connection_string), "inc-text")
            .await,
        3
    );
    // 'Delta' < 'beta' in byte order (uppercase D), so under a locale sort it
    // would land inside the already-seen range; byte-order watermark 'beta'
    // means 'Delta' is BELOW the watermark — documented cursor semantics: a
    // non-monotonic text insert is invisible (same as the regressing clock).
    // 'zeta' is above in byte order and must load.
    fixture
        .seed("INSERT INTO ev VALUES (4, 'Delta'), (5, 'zeta');")
        .await;
    assert_eq!(
        harness
            .run(name_source(&fixture.connection_string), "inc-text")
            .await,
        1,
        "only the byte-order-greater row loads; ordering is consistent, no panic, no dupes"
    );
    assert_eq!(harness.count(), 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_by_declared_key_converges_and_keyless_is_rejected() {
    // Engine clause B4 as amended (contract merge-structured.md):
    // structured streams with a declared primary_key merge BY that key —
    // update-heavy re-runs converge to one row per key. Keyless structured
    // streams keep the original plan-time rejection.
    let Some(fixture) = PostgresContainer::start().await else {
        return;
    };
    fixture.seed(BASE).await;
    let harness = Harness::new();

    let merge_config = |pipeline: &str| {
        let mut config = EngineConfig::new(pipeline);
        config = config.with_write_mode(rdlt_connector_sdk::spi::core::WriteMode::Merge {
            key: vec!["id".into()],
        });
        config
    };
    let run_merge = |postgres_source: source::Shell| {
        let destination = harness.destination.shell();
        let config = merge_config("inc-merge");
        async move {
            Engine::new(config, postgres_source, destination)
                .run()
                .await
                .expect("keyed merge accepted")
                .total_rows()
        }
    };

    assert_eq!(
        run_merge(ev_source(&fixture.connection_string, "")).await,
        3
    );
    assert_eq!(harness.count(), 3);

    // Update TWO existing rows past the watermark and add one new row: the
    // merge must overwrite in place, not append.
    fixture
        .seed(
            "UPDATE ev SET v = 'a2', ts = '2026-01-05T00:00:00Z' WHERE id = 1; \
             UPDATE ev SET v = 'b2', ts = '2026-01-05T00:00:00Z' WHERE id = 2; \
             INSERT INTO ev VALUES (4, 'd', '2026-01-04T00:00:00Z');",
        )
        .await;
    assert_eq!(
        run_merge(ev_source(&fixture.connection_string, "")).await,
        3
    );
    assert_eq!(harness.count(), 4, "one row per key after update-heavy run");
    assert_eq!(harness.distinct_ids(), "4");
    let updated_value = harness
        .destination
        .query_string("SELECT v FROM ev WHERE id = 1")
        .expect("v1");
    assert_eq!(updated_value, "a2", "merge took the updated value");

    // Idempotent re-run (nothing past the watermark): still one row per key.
    assert_eq!(
        run_merge(ev_source(&fixture.connection_string, "")).await,
        0
    );
    assert_eq!(harness.count(), 4);

    // Keyless structured stream: B4 rejection stands, at plan time.
    fixture
        .seed("CREATE TABLE nokey (x int8); INSERT INTO nokey VALUES (1);")
        .await;
    let keyless = source::Shell::from_yaml(&format!(
        "conn: \"{}\"\ntables:\n  - name: nokey\n",
        fixture.connection_string
    ))
    .expect("config");
    let directory = tempfile::tempdir().expect("tempdir");
    let destination = duck::Shell::new(duck::Config::new(directory.path().join("nokey.duckdb")))
        .expect("open db");
    let mut config = merge_config("inc-merge-nokey");
    config = config.with_write_mode(rdlt_connector_sdk::spi::core::WriteMode::Merge {
        key: vec!["x".into()],
    });
    let error = Engine::new(config, keyless, destination)
        .run()
        .await
        .expect_err("keyless structured merge must be rejected");
    let message = error.to_string().to_lowercase();
    assert!(
        message.contains("merge") && message.contains("primary_key"),
        "{message}"
    );
}

// ---- cursor lag (contract cursor-lag.md) ----

#[tokio::test(flavor = "multi_thread")]
async fn lag_captures_late_arrivals_with_exact_totals_under_merge() {
    let Some(fixture) = PostgresContainer::start().await else {
        return;
    };
    fixture.seed(BASE).await;
    let harness = Harness::new();

    let lag_source = |connection_string: &str| {
        source::Shell::from_yaml(&format!(
            "conn: \"{connection_string}\"\ntables:\n  - name: ev\n    cursor:\n      column: ts\n      lag: \"5m\"\n"
        ))
        .expect("config")
    };
    let run_merge = |postgres_source: source::Shell| {
        let destination = harness.destination.shell();
        async move {
            let mut config = EngineConfig::new("inc-lag");
            config = config.with_write_mode(rdlt_connector_sdk::spi::core::WriteMode::Merge {
                key: vec!["id".into()],
            });
            Engine::new(config, postgres_source, destination)
                .run()
                .await
                .expect("lag run")
                .total_rows()
        }
    };

    assert_eq!(run_merge(lag_source(&fixture.connection_string)).await, 3);

    // A LATE commit: cursor value 3 minutes BEHIND the watermark (inside the
    // 5m window) — invisible without lag. Plus one far beyond the window.
    fixture
        .seed(
            "INSERT INTO ev VALUES (4, 'late', '2026-01-01T23:57:00Z'), \
             (5, 'too-old', '2026-01-01T12:00:00Z');",
        )
        .await;
    run_merge(lag_source(&fixture.connection_string)).await;
    assert_eq!(
        harness.count(),
        4,
        "late row captured, beyond-window row not"
    );
    assert_eq!(harness.distinct_ids(), "4");
    let missing = harness
        .destination
        .query_string("SELECT CAST(count(*) AS VARCHAR) FROM ev WHERE id = 5")
        .expect("count");
    assert_eq!(missing, "0", "L5: the window bounds the guarantee");

    // Idempotent window re-merge: three further runs, totals NEVER move —
    // window rows re-deliver and merge by key.
    for _ in 0..3 {
        run_merge(lag_source(&fixture.connection_string)).await;
        assert_eq!(harness.count(), 4, "destination totals stay exact");
        assert_eq!(harness.distinct_ids(), "4");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn lag_rejections_are_typed_and_early() {
    use rdlt_connector_sdk::spi::Source as _;
    let Some(fixture) = PostgresContainer::start().await else {
        return;
    };
    fixture
        .seed(
            "CREATE TABLE ev (id int8 PRIMARY KEY, v text, ts timestamptz); \
             CREATE TABLE nokey (x int8, ts timestamptz); \
             CREATE TABLE dated (id int8 PRIMARY KEY, d date);",
        )
        .await;
    let configured = |extra: &str| {
        source::Shell::from_yaml(&format!("conn: \"{}\"\n{extra}", fixture.connection_string))
            .expect("config")
    };

    // Text cursor: no defined subtraction — names column and type.
    let error =
        configured("tables:\n  - name: ev\n    cursor:\n      column: v\n      lag: \"5m\"\n")
            .streams()
            .await
            .expect_err("lag on text cursor");
    let message = error.to_string();
    assert!(
        message.contains('v') && message.contains("lag"),
        "{message}"
    );

    // Keyless stream: the merge dedup path must exist.
    let error =
        configured("tables:\n  - name: nokey\n    cursor:\n      column: ts\n      lag: \"5m\"\n")
            .streams()
            .await
            .expect_err("lag without a primary key");
    assert!(error.to_string().contains("primary key"), "{error}");

    // Sub-day lag on a date cursor.
    let error =
        configured("tables:\n  - name: dated\n    cursor:\n      column: d\n      lag: \"5m\"\n")
            .streams()
            .await
            .expect_err("sub-day lag on date");
    assert!(error.to_string().contains("whole-day"), "{error}");

    // Whole-day lag on date: accepted.
    configured("tables:\n  - name: dated\n    cursor:\n      column: d\n      lag: \"2d\"\n")
        .streams()
        .await
        .expect("whole-day lag on date is fine");
}

// ---- cursor edge policies (contract cursor-lag.md) ----

#[tokio::test(flavor = "multi_thread")]
async fn null_cursor_error_policy_fails_typed_and_old_policies_unchanged() {
    let Some(fixture) = PostgresContainer::start().await else {
        return;
    };
    fixture
        .seed(
            "CREATE TABLE ev (id int8 PRIMARY KEY, v text, ts timestamptz); \
             INSERT INTO ev VALUES (1, 'a', '2026-01-01T00:00:00Z'), \
             (2, 'null-cursor', NULL), (3, 'c', '2026-01-02T00:00:00Z');",
        )
        .await;
    let with_nulls = |nulls: &str| {
        source::Shell::from_yaml(&format!(
            "conn: \"{}\"\ntables:\n  - name: ev\n    cursor:\n      column: ts\n      nulls: {nulls}\n",
            fixture.connection_string
        ))
        .expect("config")
    };

    // `error`: typed FATAL naming stream and column; nothing publishes.
    let harness = Harness::new();
    let error = Engine::new(
        EngineConfig::new("inc-nulls-err"),
        with_nulls("error"),
        harness.destination.shell(),
    )
    .run()
    .await
    .expect_err("NULL cursor under `error` must fail");
    let message = error.to_string();
    assert!(
        message.contains("ev") && message.contains("ts") && message.contains("NULL"),
        "{message}"
    );
    assert_eq!(
        harness.destination.count_rows("ev").unwrap_or(0),
        0,
        "N2: nothing published from the failed run"
    );

    // Fix the data, re-run same pipeline: clean load, no duplicates (N2).
    fixture
        .seed("UPDATE ev SET ts = '2026-01-03T00:00:00Z' WHERE id = 2;")
        .await;
    assert_eq!(
        Engine::new(
            EngineConfig::new("inc-nulls-err"),
            with_nulls("error"),
            harness.destination.shell()
        )
        .run()
        .await
        .expect("clean after fix")
        .total_rows(),
        3
    );
    assert_eq!(harness.count(), 3);
    assert_eq!(harness.distinct_ids(), "3");

    // N3: exclude (default) and include pins — byte-identical behavior.
    let Some(second_fixture) = PostgresContainer::start().await else {
        return;
    };
    second_fixture
        .seed(
            "CREATE TABLE ev (id int8 PRIMARY KEY, v text, ts timestamptz); \
             INSERT INTO ev VALUES (1, 'a', '2026-01-01T00:00:00Z'), (2, 'n', NULL);",
        )
        .await;
    let second_with_nulls = |nulls: &str| {
        source::Shell::from_yaml(&format!(
            "conn: \"{}\"\ntables:\n  - name: ev\n    cursor:\n      column: ts\n      nulls: {nulls}\n",
            second_fixture.connection_string
        ))
        .expect("config")
    };
    let exclude_harness = Harness::new();
    assert_eq!(
        Engine::new(
            EngineConfig::new("inc-nulls-ex"),
            second_with_nulls("exclude"),
            exclude_harness.destination.shell()
        )
        .run()
        .await
        .expect("exclude")
        .total_rows(),
        1,
        "exclude filters the NULL row"
    );
    let include_harness = Harness::new();
    assert_eq!(
        Engine::new(
            EngineConfig::new("inc-nulls-in"),
            second_with_nulls("include"),
            include_harness.destination.shell()
        )
        .run()
        .await
        .expect("include")
        .total_rows(),
        2,
        "include loads the NULL row"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn inclusive_end_bound_loads_boundary_rows_exactly_once() {
    let Some(fixture) = PostgresContainer::start().await else {
        return;
    };
    fixture.seed(BASE).await; // ids 1..3, ts 01-01 .. 01-02
    let with_end_bound = |end_bound: &str| {
        source::Shell::from_yaml(&format!(
            "conn: \"{}\"\ntables:\n  - name: ev\n    cursor:\n      column: ts\n      end_value: \"2026-01-02T00:00:00Z\"\n      end_bound: {end_bound}\n",
            fixture.connection_string
        ))
        .expect("config")
    };

    // Exclusive (default semantics): boundary rows (ids 2,3 at 01-02) do NOT load.
    let harness = Harness::new();
    assert_eq!(
        Engine::new(
            EngineConfig::new("inc-endx"),
            with_end_bound("exclusive"),
            harness.destination.shell()
        )
        .run()
        .await
        .expect("exclusive")
        .total_rows(),
        1,
        "E1: exclusive stops before the bound"
    );

    // Inclusive: boundary rows load; re-run stays stable (E2).
    let harness = Harness::new();
    let run = || {
        let destination = harness.destination.shell();
        let postgres_source = with_end_bound("inclusive");
        async move {
            Engine::new(EngineConfig::new("inc-endi"), postgres_source, destination)
                .run()
                .await
                .expect("inclusive")
                .total_rows()
        }
    };
    assert_eq!(run().await, 3, "E1: rows exactly AT the bound load");
    assert_eq!(run().await, 0, "E2: re-run moves nothing");
    assert_eq!(harness.count(), 3);
    assert_eq!(harness.distinct_ids(), "3");
}

// ---- parameter-matrix gap cells ----

/// `direction: min` — descending cursors: the watermark is the MINIMUM
/// seen, and later runs load rows BELOW it.
#[tokio::test(flavor = "multi_thread")]
async fn direction_min_descends_and_resumes() {
    let Some(fixture) = PostgresContainer::start().await else {
        return;
    };
    fixture
        .seed(
            "CREATE TABLE ev (id int8 PRIMARY KEY, v text, ts timestamptz);\
             INSERT INTO ev VALUES (5, 'e', now()), (6, 'f', now());",
        )
        .await;
    let harness = Harness::new();
    let descending_source = |connection_string: &str| {
        source::Shell::from_yaml(&format!(
            "conn: \"{connection_string}\"\ntables:\n  - name: ev\n    cursor:\n      column: id\n      direction: min\n"
        ))
        .expect("config")
    };
    assert_eq!(
        harness
            .run(descending_source(&fixture.connection_string), "inc-min")
            .await,
        2
    );

    // Rows BELOW the min watermark arrive later (a descending feed).
    fixture
        .seed("INSERT INTO ev VALUES (3, 'c', now()), (4, 'd', now());")
        .await;
    assert_eq!(
        harness
            .run(descending_source(&fixture.connection_string), "inc-min")
            .await,
        2,
        "descending resume loads exactly the rows below the watermark"
    );
    assert_eq!(harness.count(), 4);
}

/// `lag` magnitude family — integer cursors take plain magnitudes: a
/// resumed run re-scans `watermark - N` and captures late rows, with
/// exact totals under merge.
#[tokio::test(flavor = "multi_thread")]
async fn magnitude_lag_for_integer_cursors() {
    let Some(fixture) = PostgresContainer::start().await else {
        return;
    };
    fixture
        .seed(
            "CREATE TABLE ev (id int8 PRIMARY KEY, v text, ts timestamptz);\
             INSERT INTO ev VALUES (1,'a',now()),(2,'b',now()),(3,'c',now()),(7,'g',now());",
        )
        .await;
    let harness = Harness::new();
    let magnitude_source = |connection_string: &str| {
        source::Shell::from_yaml(&format!(
            "conn: \"{connection_string}\"\ntables:\n  - name: ev\n    cursor:\n      column: id\n      lag: \"2\"\n"
        ))
        .expect("config")
    };
    let run = |postgres_source| {
        let mut config = EngineConfig::new("int-lag");
        config = config.with_write_mode(rdlt_connector_sdk::spi::WriteMode::Merge {
            key: vec!["id".into()],
        });
        Engine::new(config, postgres_source, harness.destination.shell())
    };
    run(magnitude_source(&fixture.connection_string))
        .run()
        .await
        .expect("run 1");
    // A LATE row lands inside the magnitude window (id 6 ≥ 7 - 2).
    fixture
        .seed("INSERT INTO ev VALUES (6, 'late', now());")
        .await;
    let report = run(magnitude_source(&fixture.connection_string))
        .run()
        .await
        .expect("run 2");
    assert_eq!(
        report.total_rows(),
        1,
        "the window re-reads [5,7], but boundary-key dedup drops the replayed \
         watermark row SOURCE-side — exactly the late row moves"
    );
    assert_eq!(
        harness.count(),
        5,
        "exact totals under merge (no dupes, late row present)"
    );
    assert_eq!(harness.distinct_ids(), "5");
}

/// `cursor.column` must survive the column selection — excluding it is a
/// typed error naming the column, before any data moves.
#[tokio::test(flavor = "multi_thread")]
async fn cursor_column_must_survive_selection() {
    let Some(fixture) = PostgresContainer::start().await else {
        return;
    };
    fixture
        .seed("CREATE TABLE ev (id int8 PRIMARY KEY, v text, ts timestamptz);")
        .await;
    let postgres_source = source::Shell::from_yaml(&format!(
        "conn: \"{}\"\ntables:\n  - name: ev\n    excluded_columns: [ts]\n    cursor:\n      column: ts\n",
        fixture.connection_string
    ))
    .expect("config parses — the check needs reflection");
    let harness = Harness::new();
    let config = EngineConfig::new("inc-cursor-sel");
    let error = Engine::new(config, postgres_source, harness.destination.shell())
        .run()
        .await
        .expect_err("excluded cursor column must fail typed")
        .to_string();
    assert!(error.contains("`ts`"), "{error}");
    assert!(error.contains("selected"), "{error}");
}

/// `batch_target_bytes` / `batch_max_rows` — the knobs OBSERVABLY cut
/// batches: tiny knobs produce many commit units, huge knobs one.
#[tokio::test(flavor = "multi_thread")]
async fn batch_knobs_cut_batches_observably() {
    let Some(fixture) = PostgresContainer::start().await else {
        return;
    };
    fixture
        .seed(
            "CREATE TABLE ev (id int8 PRIMARY KEY, v text, ts timestamptz);\
             INSERT INTO ev SELECT i, repeat('x', 500), now() FROM generate_series(1, 40) i;",
        )
        .await;
    let commits_with = |extra: &str| {
        let connection_string = fixture.connection_string.clone();
        let extra = extra.to_string();
        async move {
            let harness = Harness::new();
            let postgres_source = source::Shell::from_yaml(&format!(
                "conn: \"{connection_string}\"\n{extra}tables:\n  - name: ev\n    cursor:\n      column: id\n"
            ))
            .expect("config");
            let config = EngineConfig::new("inc-knobs");
            let report = Engine::new(config, postgres_source, harness.destination.shell())
                .run()
                .await
                .expect("run");
            report.commits
        }
    };
    let one = commits_with("").await;
    let by_rows = commits_with("batch_max_rows: 5\n").await;
    let by_bytes = commits_with("batch_target_bytes: 2048\n").await;
    // Incremental streams commit per checkpoint: one batch = one mid-stream
    // checkpoint + the final-state checkpoint = 2 commits at the default
    // knobs; the knobs multiply that observably.
    assert_eq!(
        one, 2,
        "default knobs: 40 small rows = one batch (+ final state)"
    );
    assert!(
        by_rows >= 8,
        "batch_max_rows=5 over 40 rows cuts many batches: {by_rows}"
    );
    assert!(
        by_bytes >= 4,
        "a 2 KiB byte target over ~20 KiB cuts many batches: {by_bytes}"
    );
}
