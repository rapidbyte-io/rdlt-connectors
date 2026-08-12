//! Slot lifecycle: distinguished creation errors, idempotent
//! create-if-missing with its consistent point, non-consuming peek +
//! explicit advance, and the concurrent-consumer refusal.

use futures::TryStreamExt;
use rdlt_connector_postgres::fixtures::PostgresContainer;
use rdlt_connector_postgres::testsupport::source::cdc_slot;
use rdlt_connector_postgres::testsupport::source::cdc_slot::Change;

use crate::cases::cdc_rig::{ORDERS_SEED, cdc_config};

#[tokio::test(flavor = "multi_thread")]
async fn create_if_missing_is_idempotent_and_reports_the_consistent_point() {
    let Some(container) = PostgresContainer::start_for_cdc().await else {
        return;
    };
    container.seed(ORDERS_SEED).await;
    let client = container.client().await;
    let tables = vec!["orders".to_string()];

    let first = cdc_slot::ensure(&client, &cdc_config("s1", "p1", true), "public", &tables)
        .await
        .expect("first ensure creates");
    assert!(first.created_slot);
    assert!(first.consistent_point.is_some());

    let second = cdc_slot::ensure(&client, &cdc_config("s1", "p1", true), "public", &tables)
        .await
        .expect("second ensure is a no-op");
    assert!(!second.created_slot);
    assert!(second.consistent_point.is_none());

    // Both resources exist exactly once; rdlt never dropped or duplicated.
    let slots: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 's1'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    let publications: i64 = client
        .query_one(
            "SELECT count(*) FROM pg_publication WHERE pubname = 'p1'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!((slots, publications), (1, 1));
}

#[tokio::test(flavor = "multi_thread")]
async fn missing_resources_without_create_are_typed_with_the_hint() {
    let Some(container) = PostgresContainer::start_for_cdc().await else {
        return;
    };
    container.seed(ORDERS_SEED).await;
    let client = container.client().await;
    let tables = vec!["orders".to_string()];

    // Nothing exists: the publication check fires first.
    let error = cdc_slot::ensure(&client, &cdc_config("s1", "p1", false), "public", &tables)
        .await
        .expect_err("missing publication");
    let message = error.to_string();
    assert!(message.contains("publication `p1`"), "{message}");
    assert!(message.contains("create_if_missing"), "{message}");

    // Publication present, slot missing: the slot error, with the hint.
    client
        .batch_execute("CREATE PUBLICATION p1 FOR TABLE public.orders")
        .await
        .unwrap();
    let error = cdc_slot::ensure(&client, &cdc_config("s1", "p1", false), "public", &tables)
        .await
        .expect_err("missing slot");
    let message = error.to_string();
    assert!(message.contains("slot `s1`"), "{message}");
    assert!(message.contains("create_if_missing"), "{message}");
}

#[tokio::test(flavor = "multi_thread")]
async fn publication_gap_names_publication_and_missing_tables() {
    let Some(container) = PostgresContainer::start_for_cdc().await else {
        return;
    };
    container
        .seed(
            "CREATE TABLE public.a (id int8 PRIMARY KEY);\
             CREATE TABLE public.b (id int8 PRIMARY KEY);\
             CREATE PUBLICATION partial_pub FOR TABLE public.a;",
        )
        .await;
    let client = container.client().await;
    let tables = vec!["a".to_string(), "b".to_string()];

    // A gap is an error even under create_if_missing — the publication is
    // user-owned; rdlt creates, never alters.
    let error = cdc_slot::ensure(
        &client,
        &cdc_config("s1", "partial_pub", true),
        "public",
        &tables,
    )
    .await
    .expect_err("publication gap");
    let message = error.to_string();
    assert!(message.contains("partial_pub"), "{message}");
    assert!(message.contains("`b`"), "{message}");
    assert!(
        !message.contains("`a`,"),
        "covered table not listed: {message}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn foreign_plugin_slot_is_typed_naming_both_plugins() {
    let Some(container) = PostgresContainer::start_for_cdc().await else {
        return;
    };
    container.seed(ORDERS_SEED).await;
    let client = container.client().await;
    client
        .query_one(
            "SELECT 1 FROM pg_create_logical_replication_slot('their_slot', 'test_decoding')",
            &[],
        )
        .await
        .expect("foreign slot");

    let error = cdc_slot::ensure(
        &client,
        &cdc_config("their_slot", "p1", true),
        "public",
        &["orders".to_string()],
    )
    .await
    .expect_err("wrong plugin");
    let message = error.to_string();
    assert!(message.contains("test_decoding"), "{message}");
    assert!(message.contains("pgoutput"), "{message}");
}

#[tokio::test(flavor = "multi_thread")]
async fn peek_is_nonconsuming_and_advance_acknowledges() {
    let Some(container) = PostgresContainer::start_for_cdc().await else {
        return;
    };
    container.seed(ORDERS_SEED).await;
    let client = container.client().await;
    let config = cdc_config("s1", "p1", true);
    cdc_slot::ensure(&client, &config, "public", &["orders".to_string()])
        .await
        .expect("ensure");

    client
        .batch_execute("INSERT INTO public.orders VALUES (3, 30), (4, 40); DELETE FROM public.orders WHERE id = 1;")
        .await
        .unwrap();
    let target = cdc_slot::current_wal_lsn(&client).await.expect("target");

    let first: Vec<Change> = cdc_slot::peek(&client, &config, target)
        .await
        .expect("peek")
        .try_collect()
        .await
        .expect("collect peek");
    assert!(!first.is_empty(), "changes visible");
    let second: Vec<Change> = cdc_slot::peek(&client, &config, target)
        .await
        .expect("re-peek")
        .try_collect()
        .await
        .expect("collect re-peek");
    assert_eq!(
        first.len(),
        second.len(),
        "peek consumed nothing — the per-table pass design depends on this"
    );
    assert!(
        first
            .iter()
            .zip(&second)
            .all(|(left, right)| left.lsn == right.lsn),
        "identical positions across passes"
    );

    let max_lsn = first.iter().map(|change| change.lsn).max().unwrap();
    cdc_slot::advance(&client, "s1", max_lsn)
        .await
        .expect("advance");
    let confirmed = cdc_slot::confirmed_flush_lsn(&client, "s1")
        .await
        .expect("confirmed");
    assert!(confirmed >= max_lsn, "ack recorded");
    let after: Vec<Change> = cdc_slot::peek(&client, &config, target)
        .await
        .expect("peek after ack")
        .try_collect()
        .await
        .expect("collect peek after ack");
    assert!(
        after.iter().all(|change| change.lsn > max_lsn),
        "acknowledged changes are gone from the feed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn concurrent_consumer_is_typed_naming_the_pid() {
    let Some(container) = PostgresContainer::start_for_cdc().await else {
        return;
    };
    container.seed(ORDERS_SEED).await;
    let client = container.client().await;
    let config = cdc_config("s1", "p1", true);
    cdc_slot::ensure(&client, &config, "public", &["orders".to_string()])
        .await
        .expect("ensure");

    // A big backlog makes the competing peek hold the slot long enough to
    // observe `active_pid` deterministically (poll below).
    client
        .batch_execute(
            "INSERT INTO public.orders \
             SELECT g, g::int4 FROM generate_series(100, 300000) g;",
        )
        .await
        .unwrap();

    let competing = container.client().await;
    let hold = tokio::spawn(async move {
        let _ = competing
            .query(
                "SELECT count(*) FROM pg_logical_slot_peek_binary_changes(\
                 's1', NULL, NULL, 'proto_version', '1', 'publication_names', 'p1')",
                &[],
            )
            .await;
    });
    let mut held = false;
    for _ in 0..200 {
        let active: Option<i32> = client
            .query_one(
                "SELECT active_pid FROM pg_replication_slots WHERE slot_name = 's1'",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        if active.is_some() {
            held = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(held, "competing consumer never observed holding the slot");

    let error = cdc_slot::ensure(&client, &config, "public", &["orders".to_string()])
        .await
        .expect_err("slot held");
    let message = error.to_string();
    assert!(message.contains("slot `s1`"), "{message}");
    assert!(message.contains("pid"), "{message}");
    assert!(message.contains("one consumer"), "{message}");
    hold.await.unwrap();
}
