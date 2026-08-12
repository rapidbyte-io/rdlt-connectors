//! Catalog reflection against a live server: which relations come into
//! scope, the primary keys and NOT NULL flags that come back, how quoted
//! mixed-case identifiers survive, and which declared types resolve to a
//! native Arrow type versus the text policy.

use rdlt_connector_postgres::fixtures::PostgresContainer;
use rdlt_connector_postgres::source::Config;
use rdlt_connector_postgres::testsupport::source::reflect_for_tests;
use rdlt_connector_sdk::config::Document;

const SEED: &str = r#"
CREATE SCHEMA sales;
CREATE TYPE sales.mood AS ENUM ('happy', 'grumpy');
CREATE DOMAIN sales.price AS numeric(12,4);
CREATE TABLE sales."Order Items" (
    "Id"        int8 NOT NULL,
    "qty"       int4,
    PRIMARY KEY ("Id")
);
CREATE TABLE sales.orders (
    id          int8 NOT NULL,
    created_at  timestamptz NOT NULL,
    total       numeric(10,2),
    unit_price  sales.price,
    tags        text[],
    mood        sales.mood,
    payload     jsonb,
    PRIMARY KEY (id, created_at)
);
CREATE VIEW sales.orders_view AS SELECT id, total FROM sales.orders;
CREATE TABLE public.not_in_scope (x int4);
"#;

#[tokio::test(flavor = "multi_thread")]
async fn reflects_schema_shape_pks_views_and_type_policies() {
    let Some(fixture) = PostgresContainer::start().await else {
        return;
    };
    fixture.seed(SEED).await;

    // Tables only (no views).
    let config = Config::from_yaml(&format!(
        "conn: \"{}\"\nschema: sales\n",
        fixture.connection_string
    ))
    .expect("config");
    let tables = reflect_for_tests(&config).await.expect("reflect");
    assert_eq!(
        tables.keys().collect::<Vec<_>>(),
        ["Order Items", "orders"],
        "views excluded by default; public schema out of scope"
    );

    let orders = &tables["orders"];
    assert_eq!(orders.primary_key(), vec!["id", "created_at"]);
    let column = |name: &str| {
        orders
            .column(name)
            .unwrap_or_else(|| panic!("column {name}"))
    };
    // Declared-type facts, per the type-mapping contract.
    assert_eq!(
        column("total").arrow_type().to_string(),
        "Decimal128(10, 2)"
    );
    // Domain resolves one level to numeric(12,4).
    assert_eq!(
        column("unit_price").arrow_type().to_string(),
        "Decimal128(12, 4)"
    );
    // Array + enum + jsonb → Utf8 policy rows.
    for policy_column in ["tags", "mood", "payload"] {
        assert_eq!(
            column(policy_column).arrow_type().to_string(),
            "Utf8",
            "{policy_column}"
        );
    }
    assert!(column("id").is_not_null(), "NOT NULL reflected");

    // Quoted mixed-case identifiers reflect verbatim.
    let items = &tables["Order Items"];
    assert_eq!(items.primary_key(), vec!["Id"]);

    // include_views picks up the view.
    let config_with_views = Config::from_yaml(&format!(
        "conn: \"{}\"\nschema: sales\ninclude_views: true\n",
        fixture.connection_string
    ))
    .expect("config");
    let with_views = reflect_for_tests(&config_with_views)
        .await
        .expect("reflect views");
    assert!(with_views.contains_key("orders_view"));

    // Unknown listed table is a typed reflect error.
    let config_missing_table = Config::from_yaml(&format!(
        "conn: \"{}\"\nschema: sales\ntables:\n  - name: ghost\n",
        fixture.connection_string
    ))
    .expect("config");
    let error = reflect_for_tests(&config_missing_table).await.unwrap_err();
    assert!(error.to_string().contains("fatal"), "{error}");
}
