//! CDC crash sweep — every registered CDC fail point × three actions ×
//! both occurrence passes with armed-fire pins, post-recovery
//! source-equality checks, redelivery convergence, and a real container-kill
//! mid-catch-up.
//!
//! Needs a container runtime; run with `--features failpoints` (wired into
//! `make test TARGET=sweep`).

#![cfg(feature = "failpoints")]

use rdlt_connector_postgres::destination::{
    DestinationOptions, MergeStrategy, Postgres, TableOptions,
};
use rdlt_connector_postgres::fixtures::PostgresContainer;
use rdlt_connector_postgres::source;
use rdlt_connector_sdk::spi::core::failpoint::fail;
use rdlt_engine::config::Config as EngineConfig;
use rdlt_engine::engine::Engine;

/// Fail points are PROCESS-GLOBAL; serialize every arming test.
static FAIL_POINT_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const SEED: &str = "CREATE TABLE public.ev (id int8 PRIMARY KEY, v text); \
    INSERT INTO public.ev SELECT i, 'row-'||i FROM generate_series(1, 100) i;";

/// The recommended composition, small batches so checkpoints and commit
/// units exist for the crash paths to bite on.
struct Rig {
    workdir: std::path::PathBuf,
}

impl Rig {
    fn new() -> Self {
        let directory = tempfile::tempdir().expect("workdir");
        let workdir = directory.path().to_path_buf();
        std::mem::forget(directory);
        Self { workdir }
    }

    fn source(connection_string: &str) -> source::Shell {
        source::Shell::from_yaml(&format!(
            "conn: \"{connection_string}\"\nbatch_max_rows: 10\n\
             cdc:\n  slot: s1\n  publication: p1\n  create_if_missing: true\n\
             tables:\n  - name: ev\n"
        ))
        .expect("cdc source config")
    }

    fn destination(connection_string: &str) -> rdlt_connector_postgres::destination::Shell {
        Postgres::new(connection_string)
            .schema("mirror")
            .options(DestinationOptions {
                merge_strategy: Some(MergeStrategy::Upsert),
                tables: [(
                    "ev".to_string(),
                    TableOptions {
                        hard_delete: Some("_rdlt_deleted".into()),
                        ..TableOptions::default()
                    },
                )]
                .into_iter()
                .collect(),
            })
            .expect("valid destination options")
            .into_shell()
    }

    async fn attempt(
        &self,
        connection_string: &str,
    ) -> Result<rdlt_connector_sdk::spi::core::report::Run, String> {
        let config = EngineConfig::new("cdc-sweep")
            .with_workdir(self.workdir.clone())
            .with_write_mode(rdlt_connector_sdk::spi::core::commit::WriteMode::Merge {
                key: vec!["id".into()],
            });
        let engine = Engine::new(
            config,
            Self::source(connection_string),
            Self::destination(connection_string),
        );
        match tokio::spawn(engine.run()).await {
            Ok(Ok(report)) => Ok(report),
            Ok(Err(error)) => Err(error.to_string()),
            Err(join) => Err(format!("panicked: {join}")),
        }
    }
}

async fn assert_mirror_equals_source(container: &PostgresContainer, context: &str) {
    let client = container.client().await;
    for (left, right) in [("public", "mirror"), ("mirror", "public")] {
        let difference: i64 = client
            .query_one(
                &format!(
                    "SELECT count(*) FROM (SELECT id, v FROM {left}.ev \
                     EXCEPT SELECT id, v FROM {right}.ev) d"
                ),
                &[],
            )
            .await
            .expect("equality query")
            .get(0);
        assert_eq!(
            difference, 0,
            "[{context}] {left} \\ {right} should be empty"
        );
    }
}

/// Registry discipline: the crate's exported CDC list is pinned here, and
/// the sweep iterates exactly it.
#[test]
fn cdc_registry_is_pinned() {
    let mut registry: Vec<&str> = source::CDC_FAIL_POINTS.to_vec();
    registry.sort_unstable();
    let mut expected = vec![
        "cdc.slot.create",
        "cdc.snapshot.copy",
        "cdc.stream.peek",
        "cdc.ack.advance",
    ];
    expected.sort_unstable();
    assert_eq!(registry, expected, "update BOTH the const and this list");
}

#[tokio::test(flavor = "multi_thread")]
async fn every_cdc_fail_point_recovers_to_source_mirror_equality() {
    let _guard = FAIL_POINT_LOCK.lock().await;

    for &point in source::CDC_FAIL_POINTS {
        // `cdc.stream.peek` only fires on a CHANGE pass — those cells run a
        // clean snapshot first, then mutate, then arm. Every other point
        // fires on the first (snapshot) run.
        let needs_change_pass = point == "cdc.stream.peek";
        for action in ["return", "panic", "1*off->return"] {
            let context = format!("{point} / {action}");
            let Some(container) = PostgresContainer::start_for_cdc().await else {
                return;
            };
            container.seed(SEED).await;
            let connection_string = container.connection_string.clone();
            let rig = Rig::new();

            if needs_change_pass {
                rig.attempt(&connection_string)
                    .await
                    .expect("clean snapshot run");
                container
                    .seed(
                        "UPDATE public.ev SET v = 'changed' WHERE id <= 50; \
                         DELETE FROM public.ev WHERE id > 90; \
                         INSERT INTO public.ev VALUES (101, 'new');",
                    )
                    .await;
            }

            fail::cfg(point, action).expect("configure fail point");
            let armed1 = rig.attempt(&connection_string).await;
            // Second attempt still armed: failure during recovery itself.
            let armed2 = rig.attempt(&connection_string).await;
            fail::remove(point);
            // The instrument must FIRE: a deleted or unreachable crash_point!
            // site would leave armed attempts green and the sweep vacuous.
            match action {
                "1*off->return" => assert!(
                    armed1.is_err() || armed2.is_err(),
                    "[{context}] armed attempts never failed — point not firing"
                ),
                _ => assert!(
                    armed1.is_err(),
                    "[{context}] first armed attempt succeeded — point not firing"
                ),
            }

            // Recovery: the next clean run converges to source-equal state.
            let recovered = rig.attempt(&connection_string).await;
            assert!(
                recovered.is_ok(),
                "[{context}] recovery failed: {recovered:?}"
            );
            assert_mirror_equals_source(&container, &context).await;

            // Convergence: one more clean run moves nothing and stays equal.
            let stable = rig.attempt(&connection_string).await.expect("stable run");
            assert_eq!(stable.total_rows(), 0, "[{context}] not convergent");
            assert_mirror_equals_source(&container, &context).await;
        }
    }
}

/// Redelivery convergence: a run that dies AFTER applying changes
/// but BEFORE the ack redelivers the same transactions on the next run —
/// the redelivered update converges and the redelivered delete no-ops.
#[tokio::test(flavor = "multi_thread")]
async fn redelivered_changes_converge() {
    let _guard = FAIL_POINT_LOCK.lock().await;
    let Some(container) = PostgresContainer::start_for_cdc().await else {
        return;
    };
    container.seed(SEED).await;
    let connection_string = container.connection_string.clone();
    let rig = Rig::new();

    rig.attempt(&connection_string).await.expect("snapshot run");
    container
        .seed("UPDATE public.ev SET v = 'v2' WHERE id = 1; DELETE FROM public.ev WHERE id = 2;")
        .await;

    // The armed run applies the update and the delete, then dies at the ack.
    fail::cfg("cdc.ack.advance", "return").expect("configure");
    let armed = rig.attempt(&connection_string).await;
    fail::remove("cdc.ack.advance");
    assert!(armed.is_err(), "ack point fired");

    // Recovery re-peeks the same feed range: the update re-applies to the
    // same final value, the delete deletes nothing — exactly-once OUTCOMES.
    rig.attempt(&connection_string).await.expect("recovery");
    assert_mirror_equals_source(&container, "redelivery").await;
    let client = container.client().await;
    let updated: i64 = client
        .query_one("SELECT count(*) FROM mirror.ev WHERE id = 1", &[])
        .await
        .unwrap()
        .get(0);
    let deleted: i64 = client
        .query_one("SELECT count(*) FROM mirror.ev WHERE id = 2", &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!((updated, deleted), (1, 0), "update once, delete gone");
    let stable = rig.attempt(&connection_string).await.expect("stable");
    assert_eq!(stable.total_rows(), 0);
}

/// A REAL death mid-catch-up: the container dies while the change pass is
/// applying a large multi-transaction backlog. The error is typed, committed
/// work survives, and nothing double-applies (the destination here is
/// DuckDB — killing the source container must not take the destination down
/// with it).
#[tokio::test(flavor = "multi_thread")]
async fn container_kill_mid_catch_up_is_typed_and_preserves_commits() {
    // Arms nothing, but fail points are PROCESS-GLOBAL: without the lock,
    // the sweep's armed points fire inside THIS test's runs.
    let _guard = FAIL_POINT_LOCK.lock().await;
    let Some(container) = PostgresContainer::start_for_cdc().await else {
        return;
    };
    container
        .seed("CREATE TABLE public.ev (id int8 PRIMARY KEY, v text);")
        .await;
    let connection_string = container.connection_string.clone();

    let directory = tempfile::tempdir().expect("tempdir");
    let dest_config =
        rdlt_connector_duckdb::destination::Config::new(directory.path().join("out.duckdb"));
    let destination =
        rdlt_connector_duckdb::destination::Shell::new(dest_config.clone()).expect("open db");
    let workdir = directory.path().join("wal");
    std::mem::forget(directory);
    let run = |source_connection: String,
               destination: rdlt_connector_duckdb::destination::Shell,
               workdir: std::path::PathBuf| async move {
        let config = EngineConfig::new("cdc-kill")
            .with_workdir(workdir)
            .with_write_mode(rdlt_connector_sdk::spi::core::commit::WriteMode::Merge {
                key: vec!["id".into()],
            });
        let engine = Engine::new(config, Rig::source(&source_connection), destination);
        engine.run().await.map_err(|error| error.to_string())
    };

    // Snapshot first (tiny), then a large backlog across MANY transactions
    // so the catch-up has many commit boundaries to die between. The
    // backlog must dwarf container-teardown time: a smaller 400k/120-byte
    // version FINISHED (3.5 s) before the kill landed under parallel load —
    // expect_err saw a successful run.
    run(
        connection_string.clone(),
        destination.clone(),
        workdir.clone(),
    )
    .await
    .expect("snapshot run");
    let client = container.client().await;
    for chunk in 0..100i64 {
        client
            .execute(
                &format!(
                    "INSERT INTO public.ev \
                     SELECT g, repeat('x', 300) FROM \
                     generate_series({}, {}) g",
                    chunk * 10_000 + 1,
                    (chunk + 1) * 10_000
                ),
                &[],
            )
            .await
            .expect("backlog chunk");
    }

    // Deterministic kill: wait until at least one catch-up commit landed.
    // The oracle is CONFIG-keyed and read-only — safe to poll while the
    // live shell (held by the running engine) owns the file.
    let watched = dest_config.clone();
    let baseline =
        rdlt_connector_duckdb::destination::testhook::count_rows(&watched, "ev").unwrap_or(0);
    let killer = tokio::spawn(async move {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
        while rdlt_connector_duckdb::destination::testhook::count_rows(&watched, "ev").unwrap_or(0)
            <= baseline
        {
            assert!(
                std::time::Instant::now() < deadline,
                "no catch-up commit observed within 120s — cannot kill deterministically"
            );
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
        drop(container); // container stops; sockets die mid-pass
    });
    let outcome = run(connection_string, destination.clone(), workdir).await;
    killer.await.expect("killer task");

    let error = outcome.expect_err("mid-catch-up container death must fail the run");
    assert!(
        error.contains("postgres source"),
        "typed error names the source: {error}"
    );
    // Committed work survives: a prefix of whole transactions, no dupes.
    let count =
        rdlt_connector_duckdb::destination::testhook::count_rows(&dest_config, "ev").unwrap_or(0);
    assert!(
        count > baseline,
        "kill protocol guarantees a committed prefix"
    );
    let distinct = rdlt_connector_duckdb::destination::testhook::query_string(
        &dest_config,
        "SELECT CAST(count(DISTINCT id) AS VARCHAR) FROM ev",
    )
    .expect("distinct");
    assert_eq!(distinct, count.to_string(), "no double-applied rows");
}

/// A TRANSIENT mid-snapshot failure is retried by the ENGINE within one run
/// — the retry must get FRESH connections (a cached dead snapshot/control
/// client would fail every attempt after the fault cleared) and converge
/// exactly-once.
#[tokio::test(flavor = "multi_thread")]
async fn transient_mid_snapshot_resumes_within_one_run() {
    let _guard = FAIL_POINT_LOCK.lock().await;
    let Some(container) = PostgresContainer::start_for_cdc().await else {
        return;
    };
    container.seed(SEED).await;
    let rig = Rig::new();

    // Fail the first two snapshot chunk polls, then heal: one run() call
    // recovers by itself.
    fail::cfg("cdc.snapshot.copy", "2*return->off").expect("configure");
    let report = rig
        .attempt(&container.connection_string)
        .await
        .expect("run recovers in-run");
    fail::remove("cdc.snapshot.copy");

    assert!(report.retries > 0, "the engine's retry counter surfaces");
    assert_mirror_equals_source(&container, "in-run retry").await;
    let stable = rig
        .attempt(&container.connection_string)
        .await
        .expect("stable");
    assert_eq!(stable.total_rows(), 0, "convergent after in-run retries");
}
