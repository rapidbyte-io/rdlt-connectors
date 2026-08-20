//! Native type fidelity on the write side (contract dest-types.md):
//! NUMERIC(p,s), JSONB, UUID and NOT NULL land as native target columns
//! with zero user configuration, an extreme decimal round-trips through a
//! real server, and a value the server refuses fails typed with its column
//! named and its message and SQLSTATE intact.

use std::sync::Arc;

use arrow_array::{Decimal128Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use rdlt_connector_sdk::spi::core::{
    id::LoadId, id::PipelineId, id::TableName, schema::Column, schema::ColumnType,
    schema::Provenance, schema::TableSchema, types::LogicalType,
};
use rdlt_connector_sdk::spi::{
    core::commit::WriteMode, destination::Destination as _, destination::OpenContext,
};
use rdlt_testkit::conformance::destination::TableProbe as _;
use rdlt_testkit::fixtures::commit_meta_for;

use crate::cases::common;
use rdlt_connector_postgres::destination;
use rdlt_connector_postgres::fixtures::PostgresContainer;

fn column(name: &str, scalar: LogicalType, nullable: bool) -> Column {
    Column {
        name: name.into(),
        column_type: ColumnType::Scalar { scalar },
        nullable,
        provenance: Provenance::Hinted,
    }
}

fn fidelity_schema() -> TableSchema {
    TableSchema {
        table: TableName::new("fidelity"),
        parent: None,
        columns: vec![
            column("id", LogicalType::Int64, false),
            column(
                "amount",
                LogicalType::Decimal {
                    precision: 12,
                    scale: 4,
                },
                true,
            ),
            column("doc", LogicalType::Json, true),
            column("uid", LogicalType::Uuid, true),
        ],
    }
}

type FidelityRow<'a> = (i64, Option<i128>, Option<&'a str>, Option<&'a str>);

fn fidelity_batch(rows: &[FidelityRow<'_>]) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Decimal128(12, 4), true),
            Field::new("doc", DataType::Utf8, true),
            Field::new("uid", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|row| row.0).collect::<Vec<_>>(),
            )),
            Arc::new(
                Decimal128Array::from(rows.iter().map(|row| row.1).collect::<Vec<_>>())
                    .with_precision_and_scale(12, 4)
                    .expect("decimal shape"),
            ),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.2).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                rows.iter().map(|row| row.3).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("batch")
}

#[tokio::test(flavor = "multi_thread")]
async fn native_types_land_with_exact_values() {
    let Some(fixture) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = fixture.connection_string.clone();
    let postgres_destination = destination::Postgres::new(&connection_string)
        .schema("fid")
        .into_shell();
    let pipeline = PipelineId::new("fid");
    const LOAD: &str = "fid-load";
    let mut session = postgres_destination
        .open(OpenContext::new(pipeline.clone(), LoadId::new(LOAD)))
        .await
        .expect("open");

    let schema = fidelity_schema();
    session
        .ensure_table(&schema, &WriteMode::Append)
        .await
        .expect("ensure");
    session
        .write(
            &schema.table,
            fidelity_batch(&[
                (
                    1,
                    Some(123_456_781_234), // 12345678.1234
                    Some(r#"{"city": "NYC", "zip": 10001}"#),
                    Some("550e8400-e29b-41d4-a716-446655440000"),
                ),
                (
                    2,
                    Some(-5), // -0.0005
                    Some(r#"{"city": "LA"}"#),
                    Some("00000000-0000-0000-0000-000000000001"),
                ),
                (3, None, None, None), // NULLs survive in every native type
            ]),
        )
        .await
        .expect("write");
    session
        .commit(commit_meta_for(&pipeline, &LoadId::new(LOAD), 0))
        .await
        .expect("commit");

    let probe = common::Probe {
        connection_string: connection_string.clone(),
        schema: "fid".into(),
    };
    assert_eq!(
        probe
            .count(&TableName::new("fidelity"))
            .await
            .expect("probe"),
        3
    );

    let client = common::connect(&connection_string).await;

    // T1/T2/T3 catalog assertions: the COLUMN TYPES are native.
    let type_of = |name: &'static str| {
        let client = &client;
        async move {
            let declared: String = client
                .query_one(
                    "SELECT format_type(atttypid, atttypmod) FROM pg_attribute
                     WHERE attrelid = 'fid.fidelity'::regclass AND attname = $1",
                    &[&name],
                )
                .await
                .expect("catalog")
                .get(0);
            declared
        }
    };
    assert_eq!(type_of("amount").await, "numeric(12,4)", "T1");
    assert_eq!(type_of("doc").await, "jsonb", "T2");
    assert_eq!(type_of("uid").await, "uuid", "T3");

    // T4: NOT NULL honored on the target.
    let not_null: bool = client
        .query_one(
            "SELECT attnotnull FROM pg_attribute
             WHERE attrelid = 'fid.fidelity'::regclass AND attname = 'id'",
            &[],
        )
        .await
        .expect("nullability")
        .get(0);
    assert!(not_null, "T4: id declared non-nullable");

    // T1: exact decimal math, zero float involvement.
    let sum: String = client
        .query_one("SELECT SUM(amount)::text FROM fid.fidelity", &[])
        .await
        .expect("sum")
        .get(0);
    assert_eq!(sum, "12345678.1229", "exact NUMERIC sum");

    // T2: native JSON path query.
    let city: String = client
        .query_one("SELECT doc->>'city' FROM fid.fidelity WHERE id = 1", &[])
        .await
        .expect("json path")
        .get(0);
    assert_eq!(city, "NYC");

    // T3: uuid-literal equality join.
    let id: i64 = client
        .query_one(
            "SELECT id FROM fid.fidelity
             WHERE uid = '550e8400-e29b-41d4-a716-446655440000'::uuid",
            &[],
        )
        .await
        .expect("uuid join")
        .get(0);
    assert_eq!(id, 1);

    // NULL row survived in every native type.
    let nulls: i64 = client
        .query_one(
            "SELECT count(*) FROM fid.fidelity
             WHERE id = 3 AND amount IS NULL AND doc IS NULL AND uid IS NULL",
            &[],
        )
        .await
        .expect("nulls")
        .get(0);
    assert_eq!(nulls, 1);
}

/// Review F1 live proof: a 38-digit NUMERIC at a pad-requiring scale —
/// the exact shape whose encoding overflowed pre-review — round-trips
/// exactly through a real server.
#[tokio::test(flavor = "multi_thread")]
async fn extreme_decimal_round_trips_through_the_server() {
    let Some(fixture) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = fixture.connection_string.clone();
    let postgres_destination = destination::Postgres::new(&connection_string)
        .schema("wide")
        .into_shell();
    let pipeline = PipelineId::new("wide");
    const LOAD: &str = "w-load";
    let mut session = postgres_destination
        .open(OpenContext::new(pipeline.clone(), LoadId::new(LOAD)))
        .await
        .expect("open");
    let schema = TableSchema {
        table: TableName::new("wide"),
        parent: None,
        columns: vec![
            column("id", LogicalType::Int64, false),
            column(
                "amount",
                LogicalType::Decimal {
                    precision: 38,
                    scale: 3,
                },
                true,
            ),
        ],
    };
    session
        .ensure_table(&schema, &WriteMode::Append)
        .await
        .expect("ensure");
    // 38 nines at scale 3 (pad = 1 in base-10000 alignment).
    let value: i128 = 10i128.pow(38) - 1;
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("amount", DataType::Decimal128(38, 3), true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![1i64])),
            Arc::new(
                Decimal128Array::from(vec![Some(value)])
                    .with_precision_and_scale(38, 3)
                    .expect("decimal shape"),
            ),
        ],
    )
    .expect("batch");
    session.write(&schema.table, batch).await.expect("write");
    session
        .commit(commit_meta_for(&pipeline, &LoadId::new(LOAD), 0))
        .await
        .expect("commit");

    let client = common::connect(&connection_string).await;
    let text: String = client
        .query_one("SELECT amount::text FROM wide.wide WHERE id = 1", &[])
        .await
        .expect("value")
        .get(0);
    assert_eq!(
        text, "99999999999999999999999999999999999.999",
        "38-digit value at scale 3 lands exactly"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rejected_documents_and_uuids_fail_typed_naming_the_column() {
    let Some(fixture) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = fixture.connection_string.clone();
    let postgres_destination = destination::Postgres::new(&connection_string)
        .schema("fidbad")
        .into_shell();
    let pipeline = PipelineId::new("fidbad");
    const LOAD: &str = "fb-load";
    let mut session = postgres_destination
        .open(OpenContext::new(pipeline.clone(), LoadId::new(LOAD)))
        .await
        .expect("open");
    let schema = fidelity_schema();
    session
        .ensure_table(&schema, &WriteMode::Append)
        .await
        .expect("ensure");

    // Non-canonical uuid: OUR typed error, names the column, before COPY.
    let error = session
        .write(
            &schema.table,
            fidelity_batch(&[(1, None, None, Some("not-a-uuid"))]),
        )
        .await
        .expect_err("bad uuid must fail");
    let message = error.to_string();
    assert!(
        message.contains("uid") && message.contains("not-a-uuid"),
        "{message}"
    );

    // JSONB-rejected document (NUL escape): the SERVER refuses it and the
    // surfaced error carries its message + SQLSTATE.
    let nul_document = "{\"k\": \"\\u0000\"}".to_string();
    let error = session
        .write(
            &schema.table,
            fidelity_batch(&[(1, None, Some(&nul_document), None)]),
        )
        .await
        .expect_err("NUL escape must be rejected by jsonb");
    // Review F5: a poisoned document is PERMANENT (never retried) and
    // the server's CONTEXT line names the column.
    let debug = format!("{error:?}");
    assert!(
        debug.starts_with("Fatal"),
        "data error must be fatal: {debug}"
    );
    let message = error.to_string();
    assert!(
        message.contains("Unicode escape")
            && message.contains("SQLSTATE")
            && message.contains("doc"),
        "server message + SQLSTATE + column context: {message}"
    );
}

/// ANY forced db failure carries the server message + SQLSTATE.
#[tokio::test(flavor = "multi_thread")]
async fn forced_db_failure_surfaces_server_message_and_sqlstate() {
    let Some(fixture) = PostgresContainer::start().await else {
        return;
    };
    let connection_string = fixture.connection_string.clone();
    let postgres_destination = destination::Postgres::new(&connection_string)
        .schema("f6")
        .into_shell();
    let pipeline = PipelineId::new("f6");
    const LOAD: &str = "f6-load";
    let mut session = postgres_destination
        .open(OpenContext::new(pipeline.clone(), LoadId::new(LOAD)))
        .await
        .expect("open");
    let schema = fidelity_schema();
    session
        .ensure_table(&schema, &WriteMode::Append)
        .await
        .expect("ensure");
    // NOT NULL violation. Append rows go STRAIGHT into the target, so the
    // constraint is enforced by the COPY itself and the failure surfaces
    // at `write` — at the offending row, which the server names — rather
    // than a whole batch later at publish. It used to surface at publish
    // because the row first landed in a nullable stage table. What the rule
    // requires is unchanged either way: the server's message and SQLSTATE
    // reach the caller, never a bare "db error".
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)])),
        vec![Arc::new(Int64Array::from(vec![None::<i64>]))],
    )
    .expect("batch");
    let error = session
        .write(&schema.table, batch)
        .await
        .expect_err("NOT NULL violation on the direct write");
    let message = error.to_string();
    assert!(
        message.contains("null value") && message.contains("SQLSTATE 23502"),
        "F6: server message + SQLSTATE, never bare db error: {message}"
    );
    // The failed unit rolled back, so the session is still usable — the
    // engine may retry a transient failure on this same session.
    assert!(
        session.read_state(&pipeline).await.is_ok(),
        "a failed unit must leave the connection out of its aborted state"
    );
}
