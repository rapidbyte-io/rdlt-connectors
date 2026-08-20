//! Crash-recovery regression: the Replace-mode
//! truncate-once guard must be DURABLE across sessions — the parquet twin was
//! the feature-002 review's confirmed data-loss finding; Postgres carried the
//! same latent in-memory pattern (never crash-swept until now).

use crate::cases::common::count;
use rdlt_connector_postgres::destination::Postgres;
use rdlt_connector_postgres::fixtures::PostgresContainer;
use rdlt_connector_sdk::spi::core::{commit::WriteMode, id::LoadId, id::PipelineId, id::TableName};
use rdlt_connector_sdk::spi::{destination::Destination, destination::OpenContext};
use rdlt_testkit::fixtures::{batch_of, commit_meta_for, schema_for};

#[tokio::test(flavor = "multi_thread")]
async fn replace_recovery_session_keeps_prior_commits_of_same_load() {
    let Some(container) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = container.connection_string.clone();
    let destination = Postgres::new(&connection_string).schema("rec").into_shell();
    let pipeline = PipelineId::new("p1");
    let load = LoadId::new("load-a");
    let table_schema = schema_for("events");
    let first_batch = batch_of(&[1, 2, 3]);
    let second_batch = batch_of(&[4, 5]);
    let table = TableName::new("events");

    let mut first_session = destination
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open first session");
    first_session
        .ensure_table(&table_schema, &WriteMode::Replace)
        .await
        .expect("ensure");
    first_session
        .write(&table, first_batch)
        .await
        .expect("write");
    first_session
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit 1");
    assert_eq!(count(&connection_string, "rec", "events").await, 3);

    // Crash before commit #2's receipt: fresh session, same load.
    let mut recovery_session = destination
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open recovery session");
    recovery_session
        .ensure_table(&table_schema, &WriteMode::Replace)
        .await
        .expect("ensure again");
    recovery_session
        .write(&table, second_batch)
        .await
        .expect("write tail");
    recovery_session
        .commit(commit_meta_for(&pipeline, &load, 2))
        .await
        .expect("commit 2");

    assert_eq!(
        count(&connection_string, "rec", "events").await,
        5,
        "commit #1's published rows must survive recovery (durable Replace guard)"
    );
}
