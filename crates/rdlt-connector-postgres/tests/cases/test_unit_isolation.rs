//! What the direct-to-target unit transaction guarantees: a reload is
//! never observed empty, and a crashed unit leaves the target untouched.

use std::sync::Arc;

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

fn schema() -> TableSchema {
    TableSchema {
        table: TableName::new("iso"),
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

async fn count(client: &tokio_postgres::Client, table: &str) -> i64 {
    client
        .query_one(&format!("SELECT count(*) FROM {table}"), &[])
        .await
        .expect("count")
        .get(0)
}

/// A Replace load clears its target and refills it in ONE transaction, so
/// no reader ever observes the gap between the two. Pinned here because
/// the MECHANISM is not the one an isolation-level argument would predict,
/// and the difference is what operators feel:
///
/// `clear_table` is `TRUNCATE`, which takes ACCESS EXCLUSIVE. A concurrent
/// `SELECT` needs ACCESS SHARE, which conflicts — so a reader arriving
/// mid-unit does not see the old rows under MVCC, it **blocks** until the
/// unit commits and then sees the new ones. (A `DELETE`-based clear would
/// take only ROW EXCLUSIVE and readers would proceed against the old
/// snapshot; `TRUNCATE` is chosen for speed, and this is its price.)
///
/// The guarantee "never observed empty" therefore holds by locking, not by
/// snapshots — and it holds at every isolation level for the same reason.
/// What this story changed is the WINDOW: the lock used to be held for the
/// publish alone, and is now held from the first batch to the commit. See
/// the module doc on what a unit transaction costs.
#[tokio::test(flavor = "multi_thread")]
async fn a_replace_reload_is_never_observed_empty() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let destination = rdlt_connector_postgres::destination::Postgres::new(&connection_string)
        .schema("iso")
        .into_shell();
    let pipeline = PipelineId::new("iso");

    let meta = |load: &str, commit_seq: u64| CommitMeta {
        load_id: LoadId::new(load),
        commit_seq,
        state: StateDoc::new(pipeline.clone(), env!("CARGO_PKG_VERSION")),
        counters: CommitCounters::default(),
    };

    // Load 1 establishes the "previous contents".
    let mut first = destination
        .open(OpenContext::new(pipeline.clone(), LoadId::new("iso-1")))
        .await
        .expect("open");
    first
        .ensure_table(&schema(), &WriteMode::Replace)
        .await
        .expect("ensure");
    first
        .write(&TableName::new("iso"), batch(&[1, 2, 3]))
        .await
        .expect("write");
    first.commit(meta("iso-1", 0)).await.expect("commit");
    drop(first);

    let observer = crate::cases::common::connect(&connection_string).await;
    assert_eq!(count(&observer, "iso.iso").await, 3, "load 1 landed");

    // Load 2 clears and refills. Mid-unit — after the TRUNCATE and the
    // COPY, before the commit — the reader must still see load 1.
    let mut second = destination
        .open(OpenContext::new(pipeline.clone(), LoadId::new("iso-2")))
        .await
        .expect("open");
    second
        .ensure_table(&schema(), &WriteMode::Replace)
        .await
        .expect("ensure");
    second
        .write(&TableName::new("iso"), batch(&[4, 5]))
        .await
        .expect("write");
    // The reader is now behind the TRUNCATE's ACCESS EXCLUSIVE lock. It
    // must NOT complete — and must not come back with 0 — while the unit
    // is open.
    let mut blocked = tokio::spawn(async move { count(&observer, "iso.iso").await });
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(750), &mut blocked)
            .await
            .is_err(),
        "a reader must block on a cleared-but-uncommitted target, never observe it empty"
    );

    second.commit(meta("iso-2", 0)).await.expect("commit");
    assert_eq!(
        blocked.await.expect("reader task"),
        2,
        "the blocked reader resumes and sees load 2's contents, never an empty table"
    );
}

/// A Replace load must not cost the target anything that lives on
/// the table rather than in it. The clear is `TRUNCATE`, which keeps the
/// table's identity — so indexes, constraints, grants and dependent views
/// all survive.
///
/// This is the property that ruled out the faster-looking alternative. A
/// swap (`CREATE new; ... ; ALTER TABLE RENAME`) clears in ~21 ms against
/// TRUNCATE-plus-refill, but it produces a table with a NEW oid, which
/// means rebuilding every index, re-granting every privilege and breaking
/// every view bound to the old one. Pinned so a future "optimization"
/// cannot take that trade silently.
#[tokio::test(flavor = "multi_thread")]
async fn a_replace_load_preserves_indexes_grants_and_dependents() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let admin = crate::cases::common::connect(&connection_string).await;
    admin
        .batch_execute(
            "CREATE SCHEMA IF NOT EXISTS iso3;
             DROP VIEW IF EXISTS iso3.iso_v;
             DROP TABLE IF EXISTS iso3.iso;
             CREATE TABLE iso3.iso (id BIGINT CONSTRAINT iso_id_positive CHECK (id > 0));
             CREATE INDEX iso_id_idx ON iso3.iso (id);
             CREATE VIEW iso3.iso_v AS SELECT id FROM iso3.iso;
             CREATE ROLE iso_reader;
             GRANT SELECT ON iso3.iso TO iso_reader;",
        )
        .await
        .expect("fixture objects");
    let oid_before: u32 = admin
        .query_one("SELECT 'iso3.iso'::regclass::oid", &[])
        .await
        .expect("oid")
        .get(0);

    let destination = rdlt_connector_postgres::destination::Postgres::new(&connection_string)
        .schema("iso3")
        .into_shell();
    let pipeline = PipelineId::new("iso3");
    let mut session = destination
        .open(OpenContext::new(pipeline.clone(), LoadId::new("iso3-load")))
        .await
        .expect("open");
    session
        .ensure_table(&schema(), &WriteMode::Replace)
        .await
        .expect("ensure");
    session
        .write(&TableName::new("iso"), batch(&[7, 8]))
        .await
        .expect("write");
    session
        .commit(CommitMeta {
            load_id: LoadId::new("iso3-load"),
            commit_seq: 0,
            state: StateDoc::new(pipeline.clone(), env!("CARGO_PKG_VERSION")),
            counters: CommitCounters::default(),
        })
        .await
        .expect("commit");

    let oid_after: u32 = admin
        .query_one("SELECT 'iso3.iso'::regclass::oid", &[])
        .await
        .expect("oid")
        .get(0);
    assert_eq!(oid_before, oid_after, "the target keeps its identity");

    let survivors = |what: &'static str, sql: &'static str| {
        let admin = &admin;
        async move {
            let found: i64 = admin.query_one(sql, &[]).await.expect(what).get(0);
            assert_eq!(found, 1, "{what} did not survive the Replace load");
        }
    };
    survivors(
        "the index",
        "SELECT count(*) FROM pg_indexes WHERE schemaname='iso3' AND indexname='iso_id_idx'",
    )
    .await;
    survivors(
        "the check constraint",
        "SELECT count(*) FROM pg_constraint WHERE conname='iso_id_positive'",
    )
    .await;
    survivors(
        "the grant",
        "SELECT count(*) FROM information_schema.table_privileges \
         WHERE table_schema='iso3' AND table_name='iso' AND grantee='iso_reader' \
           AND privilege_type='SELECT'",
    )
    .await;
    // The dependent view still resolves AND sees the new contents.
    let through_view: i64 = admin
        .query_one("SELECT count(*) FROM iso3.iso_v", &[])
        .await
        .expect("the dependent view")
        .get(0);
    assert_eq!(through_view, 2, "the view reads the reloaded table");

    admin
        .batch_execute("DROP OWNED BY iso_reader; DROP ROLE IF EXISTS iso_reader;")
        .await
        .expect("cleanup");
}

/// The clear happens once per LOAD, not once per commit unit: unit 2 of a
/// Replace load appends to what unit 1 published rather than wiping it.
#[tokio::test(flavor = "multi_thread")]
async fn a_multi_unit_replace_load_clears_exactly_once() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let destination = rdlt_connector_postgres::destination::Postgres::new(&connection_string)
        .schema("iso2")
        .into_shell();
    let pipeline = PipelineId::new("iso2");
    let mut session = destination
        .open(OpenContext::new(pipeline.clone(), LoadId::new("iso2-load")))
        .await
        .expect("open");
    session
        .ensure_table(&schema(), &WriteMode::Replace)
        .await
        .expect("ensure");

    let meta = |commit_seq: u64| CommitMeta {
        load_id: LoadId::new("iso2-load"),
        commit_seq,
        state: StateDoc::new(pipeline.clone(), env!("CARGO_PKG_VERSION")),
        counters: CommitCounters::default(),
    };

    for (commit_seq, ids) in [(0u64, [1i64, 2].as_slice()), (1, [3].as_slice())] {
        session
            .write(&TableName::new("iso"), batch(ids))
            .await
            .expect("write");
        session.commit(meta(commit_seq)).await.expect("commit");
    }

    let client = crate::cases::common::connect(&connection_string).await;
    assert_eq!(
        count(&client, "iso2.iso").await,
        3,
        "unit 2 must append to the load's own rows, not re-clear them"
    );
}
