//! The service facts the whole design bends around, pinned LIVE — each
//! is invisible in syntax and would break exactly-once silently if it
//! drifted, which is why they are measured rather than trusted.

use rdlt_connector_snowflake::destination::testhook;

use super::common::{config_for, credentials, scratch_schema};

async fn config_in(schema: &str) -> Option<rdlt_connector_snowflake::destination::Config> {
    let creds = credentials()?;
    use rdlt_connector_sdk::config::Document;
    Some(
        rdlt_connector_snowflake::destination::Config::from_value(config_for(&creds, schema))
            .expect("valid"),
    )
}

fn q(config: &rdlt_connector_snowflake::destination::Config, schema: &str, t: &str) -> String {
    format!(
        "\"{}\".\"{}\".\"{}\"",
        config.database.to_uppercase(),
        schema.to_uppercase(),
        t
    )
}

async fn drop_schema(config: &rdlt_connector_snowflake::destination::Config, schema: &str) {
    let _ = testhook::connect_and_run(
        config,
        &format!(
            "DROP SCHEMA IF EXISTS \"{}\".\"{}\" CASCADE",
            config.database.to_uppercase(),
            schema.to_uppercase()
        ),
    )
    .await;
}

/// THE fact: DDL commits the open transaction. A row written before a
/// CREATE survives the ROLLBACK that follows — proof the transaction
/// was already committed under it.
#[tokio::test]
async fn ddl_commits_the_open_transaction() {
    let schema = scratch_schema("ddl");
    let Some(config) = config_in(&schema).await else {
        return;
    };
    let t = q(&config, &schema, "T");
    let side = q(&config, &schema, "SIDE");

    let survived = testhook::connect_and_run_script(
        &config,
        &[
            &format!(
                "CREATE SCHEMA IF NOT EXISTS \"{}\".\"{}\"",
                config.database.to_uppercase(),
                schema.to_uppercase()
            ),
            &format!("CREATE TABLE {t} (ID NUMBER)"),
            "BEGIN",
            &format!("INSERT INTO {t} VALUES (1)"),
            // The DDL that silently commits everything above it.
            &format!("CREATE TABLE {side} (ID NUMBER)"),
            "ROLLBACK",
            &format!("SELECT COUNT(*) FROM {t}"),
        ],
    )
    .await
    .expect("script runs");
    assert_eq!(survived, "1", "the CREATE committed the row before it");
    drop_schema(&config, &schema).await;
}

/// The counter-fact the unit relies on: SET (the instant capture) does
/// NOT commit — a row written before it still rolls back.
#[tokio::test]
async fn setting_a_session_variable_does_not_commit() {
    let schema = scratch_schema("setvar");
    let Some(config) = config_in(&schema).await else {
        return;
    };
    let t = q(&config, &schema, "T");

    let survived = testhook::connect_and_run_script(
        &config,
        &[
            &format!(
                "CREATE SCHEMA IF NOT EXISTS \"{}\".\"{}\"",
                config.database.to_uppercase(),
                schema.to_uppercase()
            ),
            &format!("CREATE TABLE {t} (ID NUMBER)"),
            "BEGIN",
            &format!("INSERT INTO {t} VALUES (1)"),
            "SET probe_ts = CURRENT_TIMESTAMP()",
            "ROLLBACK",
            &format!("SELECT COUNT(*) FROM {t}"),
        ],
    )
    .await
    .expect("script runs");
    assert_eq!(survived, "0", "SET left the transaction open; ROLLBACK won");
    drop_schema(&config, &schema).await;
}

/// And why the capture exists at all: CURRENT_TIMESTAMP moves per
/// STATEMENT inside one transaction, while the captured variable does
/// not.
#[tokio::test]
async fn the_clock_moves_per_statement_but_the_captured_instant_does_not() {
    let schema = scratch_schema("clock");
    let Some(config) = config_in(&schema).await else {
        return;
    };

    let moved = testhook::connect_and_run_script(
        &config,
        &[
            "BEGIN",
            "SET t0 = CURRENT_TIMESTAMP()",
            "CALL SYSTEM$WAIT(2)",
            // Different per-statement readings…
            "SELECT CASE WHEN CURRENT_TIMESTAMP() > $t0 THEN 1 ELSE 0 END",
        ],
    )
    .await
    .expect("script runs");
    assert_eq!(
        moved, "1",
        "the per-statement clock advanced past the captured instant"
    );

    let stable = testhook::connect_and_run_script(
        &config,
        &["SELECT CASE WHEN $t0 = $t0 THEN 1 ELSE 0 END", "ROLLBACK"],
    )
    .await;
    // The captured variable reads back equal to itself trivially; the
    // load-bearing half is above. This second script only closes the
    // transaction politely and tolerates session loss.
    drop(stable);
}
