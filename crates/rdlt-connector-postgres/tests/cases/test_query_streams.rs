//! Query streams (contract query-streams.md): a SQL statement becomes a
//! stream whose schema is DESCRIBED by the server, carries the same
//! incremental semantics as a table stream, and is refused — typed, before
//! any data moves — when it mutates, hides its cursor column, or collides
//! with a reflected table.

use crate::cases::common;
use rdlt_connector_postgres::fixtures::PostgresContainer;
use rdlt_connector_postgres::source;
use rdlt_connector_sdk::config::Document;
use rdlt_engine::{Engine, EngineConfig};

const SEED: &str = r#"
CREATE TABLE orders (id int8 PRIMARY KEY, updated_at timestamptz NOT NULL);
CREATE TABLE order_items (order_id int8, amount numeric(10,2));
INSERT INTO orders VALUES (1, '2026-01-01T00:00:00Z'), (2, '2026-01-02T00:00:00Z');
INSERT INTO order_items VALUES (1, 10.50), (1, 2.25), (2, 7.00);
"#;

const TOTALS_SQL: &str = "SELECT o.id, o.updated_at, sum(i.amount) AS total FROM orders o JOIN order_items i ON i.order_id = o.id GROUP BY o.id, o.updated_at";

fn totals_yaml(cursor: bool) -> String {
    let cursor_part = if cursor {
        "    cursor:\n      column: updated_at\n"
    } else {
        ""
    };
    format!(
        "tables:\n  - name: orders\n    cursor:\n      column: updated_at\nqueries:\n  - name: order_totals\n    sql: \"{TOTALS_SQL}\"\n{cursor_part}    primary_key: [id]\n    type_hints:\n      total: decimal(14,2)\n"
    )
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
        Engine::new(
            EngineConfig::new(pipeline),
            postgres_source,
            self.destination.shell(),
        )
        .run()
        .await
        .expect("run")
        .total_rows()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn join_query_lands_with_described_schema_and_incremental_works() {
    let Some(fixture) = PostgresContainer::start().await else {
        return;
    };
    fixture.seed(SEED).await;
    let harness = Harness::new();

    // Run 1: table stream (2 rows) + query stream (2 aggregated rows).
    assert_eq!(
        harness
            .run(
                common::source(&fixture.connection_string, &totals_yaml(true)),
                "qs"
            )
            .await,
        4
    );
    let total = harness
        .destination
        .query_string("SELECT CAST(total AS VARCHAR) FROM order_totals WHERE id = 1")
        .expect("total");
    assert_eq!(total, "12.75", "aggregated + hinted decimal(14,2)");
    let total_type = harness
        .destination
        .query_string("SELECT CAST(typeof(total) AS VARCHAR) FROM order_totals WHERE id = 1")
        .expect("typeof");
    assert!(
        total_type.contains("DECIMAL"),
        "hint restored decimality: {total_type}"
    );

    // Delta: a new order past the watermark → exactly the aggregate row
    // (and the table row) move.
    fixture
        .seed(
            "INSERT INTO orders VALUES (3, '2026-01-03T00:00:00Z'); \
             INSERT INTO order_items VALUES (3, 1.00);",
        )
        .await;
    assert_eq!(
        harness
            .run(
                common::source(&fixture.connection_string, &totals_yaml(true)),
                "qs"
            )
            .await,
        2,
        "one table row + one query row past the watermark"
    );
    let count = harness
        .destination
        .query_string("SELECT CAST(count(*) AS VARCHAR) FROM order_totals")
        .expect("count");
    assert_eq!(count, "3", "no duplicates across incremental runs");
}

/// `streams()` opens a fresh connection per call; the source NEVER retries
/// (clause S3 — retries are the caller's). Under full-gate load a container
/// port-proxy can transiently refuse a connect, which would surface here as a
/// connect-phase error instead of the typed rejection under test — so this
/// harness plays the engine's role and retries ONLY transient connect errors.
async fn rejection_of(postgres_source: &source::Shell) -> String {
    use rdlt_connector_sdk::spi::Source as _;
    for _ in 0..3 {
        let error = postgres_source
            .streams()
            .await
            .expect_err("stream validation must fail");
        let text = error.to_string();
        if text.contains("connect phase") {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            continue;
        }
        return text;
    }
    panic!("connect phase stayed transient across retries");
}

#[tokio::test(flavor = "multi_thread")]
async fn rejections_are_typed_and_early() {
    let Some(fixture) = PostgresContainer::start().await else {
        return;
    };
    fixture.seed(SEED).await;

    // Mutating SQL: rejected at describe time (subquery rules), before data.
    let mutating = common::source(
        &fixture.connection_string,
        "queries:\n  - name: nope\n    sql: \"DELETE FROM orders RETURNING id\"\n",
    );
    let error = rejection_of(&mutating).await;
    assert!(error.contains("read-only"), "{error}");

    // Data-modifying CTE: same rejection through the same wrapper.
    let mutating_cte = common::source(
        &fixture.connection_string,
        "queries:\n  - name: nope\n    sql: \"WITH d AS (DELETE FROM orders RETURNING id) SELECT * FROM d\"\n",
    );
    rejection_of(&mutating_cte).await; // any typed rejection — message pinned above

    // Cursor column absent from the described output.
    let missing_cursor = common::source(
        &fixture.connection_string,
        "queries:\n  - name: q\n    sql: \"SELECT id FROM orders\"\n    cursor:\n      column: updated_at\n",
    );
    let error = rejection_of(&missing_cursor).await;
    assert!(error.contains("updated_at"), "{error}");

    // Name collision with a reflected table.
    let colliding = common::source(
        &fixture.connection_string,
        "queries:\n  - name: orders\n    sql: \"SELECT 1 AS x\"\n",
    );
    let error = rejection_of(&colliding).await;
    assert!(error.contains("collides"), "{error}");

    // Duplicate query names die at config parse (validation).
    assert!(
        source::Config::from_yaml(&format!(
            "conn: \"{}\"\nqueries:\n  - name: a\n    sql: \"SELECT 1 AS x\"\n  - name: a\n    sql: \"SELECT 2 AS y\"\n",
            fixture.connection_string
        ))
        .is_err()
    );
}
