//! CDC OVER THE WIRE, SETTLED (041 Task 1): the live proving cell for
//! the investigation's finding — CDC is wire-transparent. The CDC read
//! path IS `SourceConnector::read_stream` (`source/connector.rs`
//! dispatches into `cdc::read_stream`, which pushes Arrow batches and
//! checkpoint cursors through the SAME `Feed` every stream uses), its
//! resume state is ONE per-stream cursor document (`{"cdc_lsn": …}`,
//! frozen in `cdc/runtime.rs`) that rides the wire's
//! `checkpoint_cursor_json` / `since_cursor_json` fields, and nothing
//! CDC-shaped bypasses the sdk surface — no side channel, no wire
//! vocabulary v0 lacks.
//!
//! The cell: spawn the REAL `rdlt-connector-postgres` bin `--role
//! source` against a logical-replication postgres, drive the SPI
//! `Source` seam through the provider's `ManagedSource` (spawn →
//! handshake → dial; the exact object an engine would hold), and prove
//! snapshot rows, a resumed change pass across TWO separate processes,
//! and the JSON round-trip of the cursor between them.
//!
//! THE SLOT LIFECYCLE PIN is parity, not invention: rdlt NEVER drops
//! the slot or the publication (`cdc/slot.rs` module doc), and the
//! in-process suite pins survival (`test_cdc_slot.rs`,
//! `create_if_missing_is_idempotent_and_reports_the_consistent_point`
//! asserts `(slots, publications) == (1, 1)` after ensure). Clean
//! shutdown over the wire is the guard drop (`LifecycleGuard::drop`
//! `start_kill`s the child and unlinks its socket); the slot PERSISTS
//! past it — deliberately, because the resumed change pass NEEDS the
//! slot's retained history — and no consumer holds it afterwards
//! (CDC rides ordinary SQL peeks, never a held replication session,
//! so `active_pid` is NULL the moment no peek is in flight).

use rdlt_connector_postgres::fixtures::PostgresContainer;
use rdlt_connector_sdk::spi::{
    Cursor, PushPayload, ReadRequest, RecordBatch, RecordsIn, Source, StreamSpec, records_channel,
};
use rdlt_runtime::{ConnectorProvider, ConnectorRequirement, LocalBinaryConnectorProvider};

use super::support::spawn::built_bin;

/// One remote read, drained to completion: the read RPC and the SPI
/// channel drain run concurrently on this task (`join!`, no spawn — the
/// managed source is borrowed), returning every Arrow batch and every
/// checkpoint cursor in push order.
async fn read_all(
    source: &rdlt_runtime::ManagedSource,
    stream: &StreamSpec,
    since: Option<Cursor>,
) -> (Vec<RecordBatch>, Vec<Cursor>) {
    let (out, records_in) = records_channel(8 << 20);
    let request = ReadRequest::new(stream.clone(), since, out);
    let (read_result, drained) = tokio::join!(source.read(request), drain(records_in));
    read_result.expect("the remote read completes");
    drained
}

async fn drain(mut records: RecordsIn) -> (Vec<RecordBatch>, Vec<Cursor>) {
    let mut batches = Vec::new();
    let mut cursors = Vec::new();
    while let Some(push) = records.recv().await {
        match push.payload {
            PushPayload::Arrow(batch) => batches.push(batch),
            PushPayload::Checkpoint(cursor) => cursors.push(cursor),
            PushPayload::RawJson(_) => panic!("the postgres source pushes Arrow, never raw JSON"),
        }
    }
    (batches, cursors)
}

/// Every `id` across `batches`, sorted — the content oracle.
fn ids_of(batches: &[RecordBatch]) -> Vec<i64> {
    let mut ids = Vec::new();
    for batch in batches {
        let column = batch
            .column_by_name("id")
            .expect("the id column crossed the wire");
        let column = column
            .as_any()
            .downcast_ref::<arrow_array::Int64Array>()
            .expect("int8 arrives as Int64");
        for index in 0..column.len() {
            ids.push(column.value(index));
        }
    }
    ids.sort_unstable();
    ids
}

/// Poll until the guard-killed child is genuinely dead — `start_kill`
/// only SENDS the signal, so a bounded wait is the honest shape (the
/// `/proc` idiom from rdlt-runtime's own e2e suite; `Z` = a zombie
/// awaiting tokio's background reaper, no longer running anything).
async fn assert_process_dead(pid: u32) {
    for _ in 0..200 {
        match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Err(_) => return,
            Ok(stat) => {
                let state = stat
                    .rsplit_once(')')
                    .map(|(_, rest)| rest.trim_start().chars().next().unwrap_or('?'));
                if state == Some('Z') {
                    return;
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    panic!("spawned source process {pid} is still alive 5s after its guard dropped");
}

/// THE CELL: snapshot over the wire from process 1, clean shutdown,
/// a post-snapshot INSERT, then a resumed change pass over the wire
/// from a FRESH process 2 using process 1's checkpoint cursor —
/// re-hydrated through its JSON text form, exactly as persisted state
/// would carry it. Slot lifecycle pinned at every boundary.
#[tokio::test(flavor = "multi_thread")]
async fn cdc_crosses_the_wire_and_state_rides_the_checkpoint_cursor() {
    let Some(container) = PostgresContainer::start_for_cdc().await else {
        return;
    };
    container
        .seed(
            "CREATE TABLE public.orders (id int8 PRIMARY KEY, total int4);\
             INSERT INTO public.orders VALUES (1, 10), (2, 20);",
        )
        .await;
    let oracle = container.client().await;
    let slot_count = || async {
        oracle
            .query_one(
                "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 's1'",
                &[],
            )
            .await
            .expect("slot count query")
            .get::<_, i64>(0)
    };

    let bin = built_bin();
    let provider = LocalBinaryConnectorProvider::new();
    let requirement = ConnectorRequirement::new("io.rapidbyte.postgres").with_path(&bin);
    // The minimal `cdc:` document, the in-process rig's exact shape
    // (`cdc_rig.rs`: slot s1, publication p1, create_if_missing) as the
    // JSON the handshake carries.
    let config = serde_json::json!({
        "conn": container.connection_string,
        "cdc": {"slot": "s1", "publication": "p1", "create_if_missing": true},
        "tables": [{"name": "orders"}],
    });

    // ---- process 1: streams + the snapshot pass ----
    let source = provider
        .source(&requirement, &config)
        .await
        .expect("spawn + handshake against the built bin");
    let pid_one = source
        .guard()
        .expect("the local provider always attaches a guard")
        .pid()
        .expect("a just-spawned child has a pid");

    let streams = source.streams().await.expect("the streams RPC");
    assert_eq!(streams.len(), 1, "one configured CDC table, one stream");
    let stream = streams[0].clone();
    assert_eq!(stream.name.as_str(), "orders");
    // The replica-identity preflight's key crossed the wire in the
    // stream spec — CDC's keyed-stream declaration is wire-transparent.
    assert_eq!(
        stream.primary_key,
        Some(vec!["id".to_string()]),
        "the replica-identity key rides the StreamSpec across the wire"
    );

    let (batches, cursors) = read_all(&source, &stream, None).await;
    assert_eq!(
        ids_of(&batches),
        vec![1, 2],
        "the snapshot delivers every seeded row over the wire"
    );
    assert!(
        batches[0].schema().field_with_name("_rdlt_deleted").is_ok(),
        "the CDC flag column crossed the wire in the batch schema"
    );
    let cursor = cursors.last().expect("the snapshot read checkpoints");

    // The cursor is the FROZEN one-key document — and it round-trips
    // through JSON TEXT losslessly, which is exactly what the wire's
    // checkpoint_cursor_json / since_cursor_json carriage is.
    let value = cursor.as_value();
    assert!(
        value.get("cdc_lsn").is_some_and(serde_json::Value::is_u64),
        "the CDC cursor is the frozen {{\"cdc_lsn\": …}} document: {value}"
    );
    let text = serde_json::to_string(value).expect("the cursor renders to JSON text");
    let rehydrated: serde_json::Value =
        serde_json::from_str(&text).expect("the cursor's JSON text parses back");
    assert_eq!(&rehydrated, value, "the cursor round-trips as JSON");

    // While the connector lives: the slot exists exactly once.
    assert_eq!(
        slot_count().await,
        1,
        "the slot exists while the connector runs"
    );

    // ---- clean shutdown over the wire: the guard drop ----
    drop(source);
    assert_process_dead(pid_one).await;
    // PARITY, not invention (red-proven: asserting 0 here FAILED against
    // the live server): the slot PERSISTS past clean shutdown — rdlt
    // never drops it (`cdc/slot.rs`), in-process and over the wire
    // alike, because the resumed read below NEEDS its retained history.
    // And no consumer holds it: CDC peeks over ordinary SQL, so nothing
    // lingers once the process is gone.
    //
    // `active_pid` is read ONCE, with no bounded retry, and that is sound
    // only because of the sequence above it: `assert_process_dead` has
    // already established the connector process is gone, so no peek can
    // still be in flight and the server has nothing left to reap. Were
    // this assertion ever hoisted above the drain-then-drop pair — or the
    // drop made asynchronous — it would need a bounded retry, because a
    // peek that IS in flight legitimately holds the slot mid-run (the
    // in-process suite depends on exactly that: a concurrent consumer is
    // refused with a typed error naming the holding pid).
    assert_eq!(
        slot_count().await,
        1,
        "the slot persists past clean shutdown — rdlt never drops it"
    );
    let held: bool = oracle
        .query_one(
            "SELECT active_pid IS NOT NULL FROM pg_replication_slots WHERE slot_name = 's1'",
            &[],
        )
        .await
        .expect("active_pid query")
        .get(0);
    assert!(!held, "no consumer holds the slot after shutdown");

    // ---- the post-snapshot change, then process 2 resumes ----
    container
        .seed("INSERT INTO public.orders VALUES (3, 30);")
        .await;

    let source = provider
        .source(&requirement, &config)
        .await
        .expect("a fresh spawn for the resumed read");
    let pid_two = source
        .guard()
        .expect("the local provider always attaches a guard")
        .pid()
        .expect("a just-spawned child has a pid");
    let streams = source.streams().await.expect("the second streams RPC");
    let stream = streams[0].clone();

    let (batches, cursors) = read_all(&source, &stream, Some(Cursor::new(rehydrated))).await;
    assert_eq!(
        ids_of(&batches),
        vec![3],
        "exactly the post-snapshot INSERT arrives on the resumed read — \
         no re-snapshot, no loss, no duplication"
    );
    let resumed = cursors
        .last()
        .expect("the change pass checkpoints at commit boundaries");
    let advanced = resumed.as_value()["cdc_lsn"]
        .as_u64()
        .expect("the resumed cursor keeps the frozen shape");
    let started = value["cdc_lsn"].as_u64().expect("checked above");
    assert!(
        advanced >= started,
        "the resumed read's cursor never regresses ({advanced} < {started})"
    );

    drop(source);
    assert_process_dead(pid_two).await;
    assert_eq!(
        slot_count().await,
        1,
        "the slot persists after the second shutdown too — rdlt never drops it"
    );
}
