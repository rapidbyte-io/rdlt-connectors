//! Redelivery and Replace-clear guarantees on the DIRECT publish path — the
//! path a non-merge load takes when it writes straight into the target with no
//! staging table.
//!
//! Two invariants are pinned here, and both are load-bearing for exactly-once:
//! a redelivered unit must not duplicate rows, and a Replace target must be
//! cleared exactly once per load, durably, per target. Neither is currently
//! reachable through the crash sweep — the sweep cannot produce the state where
//! the server committed and the client never learned — so these direct pins are
//! the standing coverage for that state.
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use rdlt_connector_postgres::fixtures::PostgresContainer;
use rdlt_connector_sdk::spi::core::{
    commit::Counters as CommitCounters, id::LoadId, id::PipelineId, id::TableName, schema::Column,
    schema::ColumnType, schema::Provenance, schema::TableSchema, state::StateDoc,
    types::LogicalType,
};
use rdlt_connector_sdk::spi::{
    core::commit::CommitMeta, core::commit::WriteMode, destination::Destination,
    destination::OpenContext,
};
use std::sync::Arc;

fn schema(table: &str) -> TableSchema {
    TableSchema {
        table: TableName::new(table),
        parent: None,
        columns: vec![Column {
            name: "id".into(),
            column_type: ColumnType::Scalar {
                scalar: LogicalType::Int64,
            },
            nullable: false,
            provenance: Provenance::Hinted,
        }],
    }
}
fn batch(ids: &[i64]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)])),
        vec![Arc::new(Int64Array::from(ids.to_vec()))],
    )
    .expect("batch")
}
async fn count(connection_string: &str, table: &str) -> i64 {
    let (client, connection) = tokio_postgres::connect(connection_string, tokio_postgres::NoTls)
        .await
        .expect("conn");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
        .query_one(&format!("SELECT count(*) FROM {table}"), &[])
        .await
        .expect("count")
        .get(0)
}

/// A redelivered commit unit must not double its rows.
///
/// Writing straight into the target means a replayed unit has ALREADY put its
/// rows there by the time `commit` discovers the receipt exists. Committing
/// would land them twice; the unit is rolled back instead.
///
/// The staged path got this structurally — redelivered rows sat in a stage the
/// replay branch truncated without publishing — so nothing tested it, and the
/// crash sweep passed 23/23 while this was broken.
#[tokio::test(flavor = "multi_thread")]
async fn a_redelivered_unit_never_duplicates_its_rows() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let destination = rdlt_connector_postgres::destination::Postgres::new(&connection_string)
        .schema("rp1")
        .into_shell();
    let pipeline = PipelineId::new("rp1");
    let meta = |commit_seq: u64| CommitMeta {
        load_id: LoadId::new("rp1-load"),
        commit_seq,
        state: StateDoc::new(pipeline.clone(), env!("CARGO_PKG_VERSION")),
        counters: CommitCounters::default(),
    };
    let mut session = destination
        .open(OpenContext::new(pipeline.clone(), LoadId::new("rp1-load")))
        .await
        .expect("open");
    session
        .ensure_table(&schema("t"), &WriteMode::Append)
        .await
        .expect("ensure");
    session
        .write(&TableName::new("t"), batch(&[1, 2, 3]))
        .await
        .expect("write");
    session.commit(meta(0)).await.expect("commit");
    assert_eq!(
        count(&connection_string, "rp1.t").await,
        3,
        "first delivery"
    );

    // The redelivery window: the client died without learning the outcome, so
    // the same (load_id, commit_seq) is delivered again.
    session
        .write(&TableName::new("t"), batch(&[1, 2, 3]))
        .await
        .expect("re-write");
    session.commit(meta(0)).await.expect("replay commit");
    assert_eq!(
        count(&connection_string, "rp1.t").await,
        3,
        "REDELIVERY MUST NOT DUPLICATE"
    );
}

/// A Replace target whose first rows arrive after the load's first commit
/// unit must still be cleared.
///
/// `prepare_target` clears at the FIRST WRITE of a target, guarded by
/// `load_committed_before` so a crash-recovery session cannot re-truncate rows
/// an earlier unit published. That guard is per-LOAD, but the question it
/// answers has to be per-(LOAD, TARGET): a table registered by `ensure_table`
/// in unit 1 and first written in unit 2 finds the guard already set, skips
/// its TRUNCATE, and appends to the previous load's rows.
///
/// The staged path did not have this hole — `plan_commit` emitted
/// `ClearTarget` for every Replace table at unit 1's publish regardless of
/// whether it had staged anything.
///
/// Fixed by making the guard per-(load, target) and durable: the executor
/// seeds it from `_rdlt_cleared` before planning, and records the clear in the
/// same transaction as the TRUNCATE, so a rolled-back clear leaves no record
/// claiming it happened.
#[tokio::test(flavor = "multi_thread")]
async fn a_replace_target_first_written_in_unit_two_is_still_cleared() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let destination = rdlt_connector_postgres::destination::Postgres::new(&connection_string)
        .schema("rp2")
        .into_shell();
    let pipeline = PipelineId::new("rp2");
    let meta = |load: &str, commit_seq: u64| CommitMeta {
        load_id: LoadId::new(load),
        commit_seq,
        state: StateDoc::new(pipeline.clone(), env!("CARGO_PKG_VERSION")),
        counters: CommitCounters::default(),
    };
    // Load 1 leaves rows behind.
    let mut session = destination
        .open(OpenContext::new(pipeline.clone(), LoadId::new("L1")))
        .await
        .expect("open");
    session
        .ensure_table(&schema("t"), &WriteMode::Replace)
        .await
        .expect("ensure");
    session
        .write(&TableName::new("t"), batch(&[1, 2, 3]))
        .await
        .expect("write");
    session.commit(meta("L1", 0)).await.expect("commit");
    drop(session);
    assert_eq!(count(&connection_string, "rp2.t").await, 3, "load 1 landed");

    // Load 2: table T gets NO write in unit 0, which still commits; its first
    // rows arrive in unit 1.
    let mut session = destination
        .open(OpenContext::new(pipeline.clone(), LoadId::new("L2")))
        .await
        .expect("open");
    session
        .ensure_table(&schema("t"), &WriteMode::Replace)
        .await
        .expect("ensure");
    session
        .commit(meta("L2", 0))
        .await
        .expect("empty unit commits");
    session
        .write(&TableName::new("t"), batch(&[9]))
        .await
        .expect("write in unit 1");
    session.commit(meta("L2", 1)).await.expect("commit unit 1");
    assert_eq!(
        count(&connection_string, "rp2.t").await,
        1,
        "REPLACE MUST CLEAR load 1's rows"
    );
}
