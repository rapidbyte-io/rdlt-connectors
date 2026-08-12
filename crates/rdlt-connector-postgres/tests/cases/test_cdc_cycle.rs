//! The streaming cycle end to end: snapshot -> mutate -> catch-up equality,
//! the boundary-overlap guarantee, ack floors, burst tails with clean
//! cancellation, ack-off, the flag column, and lag reporting.

use rdlt_connector_postgres::destination::{
    DestinationOptions, MergeStrategy, Postgres, TableOptions,
};
use rdlt_engine::{Engine, EngineConfig};

use crate::cases::cdc_rig::Rig;
use crate::cases::common::source;

#[tokio::test(flavor = "multi_thread")]
async fn equality_cycle_snapshot_mutate_catch_up() {
    let Some(rig) = Rig::start("cdc-us1").await else {
        return;
    };
    rig.container
        .seed(
            "CREATE TABLE public.orders (id int8 PRIMARY KEY, total int4, note text);\
             INSERT INTO public.orders VALUES (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'c');",
        )
        .await;

    // Run 1: snapshot.
    let rows = rig.run(&["orders"], "id").await;
    assert_eq!(rows, 3, "snapshot loads every row");
    rig.assert_mirror_equals_source("orders", "id, total, note")
        .await;

    // Mutations: inserts, an update, a DELETE, a PK-changing update, a
    // multi-row transaction, and a net no-op transaction.
    let client = rig.container.client().await;
    client
        .batch_execute(
            "BEGIN;\
             INSERT INTO public.orders VALUES (4, 40, 'd'), (5, 50, 'e');\
             UPDATE public.orders SET total = 11 WHERE id = 1;\
             DELETE FROM public.orders WHERE id = 2;\
             COMMIT;\
             BEGIN; UPDATE public.orders SET id = 33 WHERE id = 3; COMMIT;\
             BEGIN; INSERT INTO public.orders VALUES (99, 0, 'x'); \
             DELETE FROM public.orders WHERE id = 99; COMMIT;\
             BEGIN; UPDATE public.orders SET total = 12 WHERE id = 1; COMMIT;",
        )
        .await
        .unwrap();

    // Run 2: catch-up. Destination equals source row-for-row; id 2 GONE,
    // id 3 relocated to 33, id 1 shows the LATER of the two updates
    // (commit order), 99 never materializes (net no-op transaction).
    rig.run(&["orders"], "id").await;
    rig.assert_mirror_equals_source("orders", "id, total, note")
        .await;
    assert_eq!(
        rig.scalar("SELECT count(*) FROM mirror.orders WHERE id IN (2, 3, 99)")
            .await,
        0,
        "deleted, relocated, and no-op keys are gone"
    );
    assert_eq!(
        rig.scalar("SELECT total::int8 FROM mirror.orders WHERE id = 1")
            .await,
        12,
        "sequential commits applied in order"
    );

    // Run 3: nothing changed — nothing moves, equality holds.
    let rows = rig.run(&["orders"], "id").await;
    assert_eq!(rows, 0, "a quiet feed moves nothing");
    rig.assert_mirror_equals_source("orders", "id, total, note")
        .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn boundary_overlap_row_appears_exactly_once_with_final_state() {
    // The NON-OPTIONAL proof of the boundary refinement: a row mutated
    // between slot creation and snapshot end lands in BOTH the snapshot and
    // the feed — it must appear exactly once, with its final state.
    let Some(rig) = Rig::start("cdc-overlap").await else {
        return;
    };
    rig.container
        .seed(
            "CREATE TABLE public.orders (id int8 PRIMARY KEY, total int4);\
             INSERT INTO public.orders VALUES (1, 10);\
             CREATE PUBLICATION p1 FOR TABLE public.orders;",
        )
        .await;
    let client = rig.container.client().await;
    // Slot FIRST (as run 1 would), then mutate INSIDE the window before the
    // snapshot begins — run 1 sees the slot pre-existing, snapshots the
    // final state, and initializes its cursor at the slot's confirmed
    // position, BEHIND the mutation.
    client
        .query_one(
            "SELECT 1 FROM pg_create_logical_replication_slot('s1', 'pgoutput')",
            &[],
        )
        .await
        .unwrap();
    client
        .batch_execute(
            "UPDATE public.orders SET total = 77 WHERE id = 1;\
             INSERT INTO public.orders VALUES (2, 20);",
        )
        .await
        .unwrap();

    // Run 1 (snapshot): final states, once each.
    rig.run(&["orders"], "id").await;
    rig.assert_mirror_equals_source("orders", "id, total").await;
    assert_eq!(rig.scalar("SELECT count(*) FROM mirror.orders").await, 2);

    // Run 2 replays the window through the feed (the overlap) — upsert
    // convergence: still exactly once, still the final state.
    rig.run(&["orders"], "id").await;
    assert_eq!(
        rig.scalar("SELECT count(*) FROM mirror.orders WHERE id = 1")
            .await,
        1,
        "the overlap row appears exactly once"
    );
    assert_eq!(
        rig.scalar("SELECT total::int8 FROM mirror.orders WHERE id = 1")
            .await,
        77
    );
    rig.assert_mirror_equals_source("orders", "id, total").await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ack_never_exceeds_the_least_committed_cursor() {
    // The weld: the slot's confirmed position may only reflect
    // positions the DESTINATION durably committed — across full runs (the
    // ack trails one run behind) and across a partial run (no ack at all).
    let Some(rig) = Rig::start("cdc-ack").await else {
        return;
    };
    rig.container
        .seed(
            "CREATE TABLE public.a (id int8 PRIMARY KEY, v int4);\
             CREATE TABLE public.b (id int8 PRIMARY KEY, v int4);\
             INSERT INTO public.a VALUES (1, 1); INSERT INTO public.b VALUES (1, 1);",
        )
        .await;
    let client = rig.container.client().await;
    let confirmed = || async {
        rig.container
            .client()
            .await
            .query_one(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots \
                 WHERE slot_name = 's1'",
                &[],
            )
            .await
            .unwrap()
            .get::<_, String>(0)
    };

    // Run 1 (snapshot): both streams checkpoint their start point; the ack
    // lands at min(start points).
    rig.run(&["a", "b"], "id").await;
    let after_snapshot = confirmed().await;

    client
        .batch_execute("UPDATE public.a SET v = 2; UPDATE public.b SET v = 2;")
        .await
        .unwrap();

    // Run 2 applies the changes, but its own checkpoints are not yet
    // KNOWN-committed at ack time — the ack may only use run-1 floors: the
    // confirmed position must not pass the update transactions.
    rig.run(&["a", "b"], "id").await;
    assert_eq!(
        rig.scalar("SELECT v::int8 FROM mirror.a WHERE id = 1")
            .await,
        2,
        "changes applied"
    );
    let after_catch_up = confirmed().await;
    let changes_still_peekable: i64 = rig
        .scalar(
            "SELECT count(*) FROM pg_logical_slot_peek_binary_changes(\
             's1', NULL, NULL, 'proto_version', '1', 'publication_names', 'p1')",
        )
        .await;
    assert!(
        changes_still_peekable > 0,
        "run 2's changes stay in the feed until a later run proves their \
         commit (confirmed {after_snapshot} -> {after_catch_up})"
    );

    // Partial run: break table b's publication membership — the run fails
    // before completing every stream, so it must ack NOTHING.
    client
        .batch_execute("ALTER PUBLICATION p1 DROP TABLE public.b; UPDATE public.a SET v = 3;")
        .await
        .unwrap();
    let error = rig.run_expecting_error(&["a", "b"], "id").await;
    assert!(error.contains("p1"), "{error}");
    assert_eq!(
        confirmed().await,
        after_catch_up,
        "a partial run acks nothing"
    );

    // Restore membership: the next full run converges and (one run later)
    // the ack finally passes the update transactions.
    client
        .batch_execute("ALTER PUBLICATION p1 ADD TABLE public.b")
        .await
        .unwrap();
    rig.run(&["a", "b"], "id").await;
    assert_eq!(
        rig.scalar("SELECT v::int8 FROM mirror.a WHERE id = 1")
            .await,
        3
    );
    rig.run(&["a", "b"], "id").await;
    let final_confirmed = confirmed().await;
    assert_ne!(
        final_confirmed, after_catch_up,
        "acks advance once commits are proven"
    );
}

// ───────────────────── continuous tail ─────────────────────

#[tokio::test(flavor = "multi_thread")]
async fn tail_applies_bursts_cancels_cleanly_and_resumes() {
    let Some(rig) = Rig::start("cdc-tail").await else {
        return;
    };
    rig.container
        .seed(
            "CREATE TABLE public.orders (id int8 PRIMARY KEY, total int4);\
             INSERT INTO public.orders VALUES (1, 10);",
        )
        .await;

    // A tail-mode source (idle_wait 1s), run in the background.
    let tail_source = source(
        &rig.container.connection_string,
        "cdc:\n  slot: s1\n  publication: p1\n  create_if_missing: true\n\
         \x20 mode: tail\n  idle_wait: \"1s\"\ntables:\n  - name: orders\n",
    );
    let config = EngineConfig::new("cdc-tail")
        .with_workdir(rig.workdir.clone())
        .with_write_mode(rdlt_connector_sdk::spi::WriteMode::Merge {
            key: vec!["id".into()],
        });
    let engine = Engine::new(config, tail_source, rig.destination(&["orders"]));
    let cancel = engine.cancellation_token();
    let tail = tokio::spawn(engine.run());

    let wait_for = |sql: String, expect: i64| {
        let rig = &rig;
        async move {
            let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
            loop {
                let client = rig.container.client().await;
                if let Ok(row) = client.query_one(&sql, &[]).await
                    && row.get::<_, i64>(0) == expect
                {
                    return;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "timed out waiting for `{sql}` == {expect}"
                );
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    };

    // Snapshot lands, then a first burst applies WITHOUT restart.
    wait_for("SELECT count(*) FROM mirror.orders".into(), 1).await;
    rig.container
        .seed("INSERT INTO public.orders VALUES (2, 20), (3, 30); UPDATE public.orders SET total = 11 WHERE id = 1;")
        .await;
    wait_for("SELECT count(*) FROM mirror.orders".into(), 3).await;
    wait_for(
        "SELECT total::int8 FROM mirror.orders WHERE id = 1".into(),
        11,
    )
    .await;

    // Quiet idle (≥ two idle_waits), then a SECOND burst still applies —
    // the loop wakes from idle rather than busy-spinning or wedging.
    tokio::time::sleep(std::time::Duration::from_millis(2500)).await;
    rig.container
        .seed("DELETE FROM public.orders WHERE id = 2;")
        .await;
    wait_for("SELECT count(*) FROM mirror.orders".into(), 2).await;

    // Clean cancellation at a commit boundary.
    cancel.cancel();
    let outcome = tail.await.expect("tail task");
    let error = outcome.expect_err("cancelled run reports cancellation");
    assert!(
        error.to_string().to_lowercase().contains("cancel"),
        "{error}"
    );
    rig.assert_mirror_equals_source("orders", "id, total").await;

    // A subsequent run (catch-up mode) resumes exactly: only the new delta
    // moves, nothing is lost or duplicated.
    rig.container
        .seed("INSERT INTO public.orders VALUES (4, 40);")
        .await;
    rig.run(&["orders"], "id").await;
    rig.assert_mirror_equals_source("orders", "id, total").await;
    let stable = rig.run(&["orders"], "id").await;
    assert_eq!(stable, 0, "quiet catch-up after the tail moves nothing");
}

// ---- parameter-matrix gap cells ----

#[tokio::test(flavor = "multi_thread")]
async fn ack_off_never_advances_the_slot() {
    // `ack: off` — data flows, but the slot's confirmed position never
    // moves (debugging / fan-in staging; WAL retention documented).
    let Some(rig) = Rig::start("cdc-ack-off").await else {
        return;
    };
    rig.container
        .seed(
            "CREATE TABLE public.orders (id int8 PRIMARY KEY, total int4);\
             INSERT INTO public.orders VALUES (1, 10);",
        )
        .await;
    let run = || async {
        let config = EngineConfig::new("cdc-ack-off")
            .with_workdir(rig.workdir.clone())
            .with_write_mode(rdlt_connector_sdk::spi::WriteMode::Merge {
                key: vec!["id".into()],
            });
        Engine::new(
            config,
            source(
                &rig.container.connection_string,
                "cdc:\n  slot: s1\n  publication: p1\n  create_if_missing: true\n\
                 \x20 ack: off\ntables:\n  - name: orders\n",
            ),
            rig.destination(&["orders"]),
        )
        .run()
        .await
        .expect("run")
    };
    run().await;
    let confirmed_after_snapshot = rig
        .scalar_text(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = 's1'",
        )
        .await;
    rig.container
        .seed("INSERT INTO public.orders VALUES (2, 20);")
        .await;
    run().await;
    run().await;
    assert_eq!(
        rig.scalar("SELECT count(*) FROM mirror.orders").await,
        2,
        "data flows normally under ack: off"
    );
    assert_eq!(
        rig.scalar_text(
            "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = 's1'",
        )
        .await,
        confirmed_after_snapshot,
        "the slot's acknowledged position never advances under ack: off"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn custom_flag_column_flows_end_to_end() {
    // `flag_column` — a custom name rides the whole composition: the CDC
    // stream emits it, the destination hard-deletes by it.
    let Some(rig) = Rig::start("cdc-flag-name").await else {
        return;
    };
    rig.container
        .seed(
            "CREATE TABLE public.orders (id int8 PRIMARY KEY, total int4);\
             INSERT INTO public.orders VALUES (1, 10), (2, 20);",
        )
        .await;
    let destination = || {
        Postgres::new(rig.container.connection_string.clone())
            .schema("mirror")
            .options(DestinationOptions {
                merge_strategy: Some(MergeStrategy::Upsert),
                tables: [(
                    "orders".to_string(),
                    TableOptions {
                        hard_delete: Some("gone".into()),
                        ..TableOptions::default()
                    },
                )]
                .into_iter()
                .collect(),
            })
            .expect("options")
            .into_shell()
    };
    let run = || async {
        let config = EngineConfig::new("cdc-flag-name")
            .with_workdir(rig.workdir.clone())
            .with_write_mode(rdlt_connector_sdk::spi::WriteMode::Merge {
                key: vec!["id".into()],
            });
        Engine::new(
            config,
            source(
                &rig.container.connection_string,
                "cdc:\n  slot: s1\n  publication: p1\n  create_if_missing: true\n\
                 \x20 flag_column: gone\ntables:\n  - name: orders\n",
            ),
            destination(),
        )
        .run()
        .await
        .expect("run")
    };
    run().await;
    rig.container
        .seed("DELETE FROM public.orders WHERE id = 2;")
        .await;
    run().await;
    assert_eq!(
        rig.scalar("SELECT count(*) FROM mirror.orders").await,
        1,
        "the delete hard-applied via the CUSTOM flag column"
    );
    assert_eq!(
        rig.scalar(
            "SELECT count(*) FROM information_schema.columns \
             WHERE table_schema = 'mirror' AND table_name = 'orders' AND column_name = 'gone'"
        )
        .await,
        1,
        "the custom flag column exists at the destination"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn replication_lag_lands_on_the_dedicated_target() {
    use std::sync::{Mutex, OnceLock};

    /// Minimal collector for `rdlt::cdc` lag events.
    struct LagCollector;
    static EVENTS: OnceLock<Mutex<Vec<String>>> = OnceLock::new();

    impl tracing::Subscriber for LagCollector {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            metadata.target() == "rdlt::cdc"
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct Fields(Option<u64>);
            impl tracing::field::Visit for Fields {
                fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
                    if field.name() == "lag_bytes" {
                        self.0 = Some(value);
                    }
                }
                fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
            }
            let mut fields = Fields(None);
            event.record(&mut fields);
            if let Some(bytes) = fields.0 {
                EVENTS
                    .get_or_init(Mutex::default)
                    .lock()
                    .expect("collector lock")
                    .push(format!("lag_bytes={bytes}"));
            }
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    let _ = tracing::subscriber::set_global_default(LagCollector);

    let Some(rig) = Rig::start("cdc-lag").await else {
        return;
    };
    rig.container
        .seed("CREATE TABLE public.ev (id int8 PRIMARY KEY, v int4); INSERT INTO public.ev VALUES (1, 1);")
        .await;
    rig.run(&["ev"], "id").await;
    rig.container.seed("UPDATE public.ev SET v = 2;").await;
    rig.run(&["ev"], "id").await;

    let events = EVENTS.get_or_init(Mutex::default).lock().expect("lock");
    assert!(
        events.len() >= 2,
        "one lag event per completed run: {events:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn session_gucs_do_not_break_change_decoding() {
    // The peek session pins DateStyle/bytea_output — a database with legacy
    // settings must decode identically.
    let Some(rig) = Rig::start("cdc-gucs").await else {
        return;
    };
    let client = rig.container.client().await;
    client
        .batch_execute("ALTER DATABASE postgres SET datestyle = 'SQL, DMY'")
        .await
        .unwrap();
    client
        .batch_execute("ALTER DATABASE postgres SET bytea_output = 'escape'")
        .await
        .unwrap();
    rig.container
        .seed(
            "CREATE TABLE public.ev (id int8 PRIMARY KEY, at timestamptz, d date, raw bytea);\
             INSERT INTO public.ev VALUES \
             (1, '2026-07-21T10:11:12.123456Z', '2026-07-21', '\\x0aff10');",
        )
        .await;
    rig.run(&["ev"], "id").await;
    rig.container
        .seed(
            "UPDATE public.ev SET at = '2026-07-22T01:02:03Z', d = '2026-07-22', \
             raw = '\\xdeadbeef' WHERE id = 1;\
             INSERT INTO public.ev VALUES (2, '2025-01-31T23:59:59Z', '2025-01-31', '\\x00');",
        )
        .await;
    rig.run(&["ev"], "id").await;
    rig.assert_mirror_equals_source("ev", "id, at, d, raw")
        .await;
}
