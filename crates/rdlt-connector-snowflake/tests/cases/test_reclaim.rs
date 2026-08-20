//! The aged remote reclaim, proven BOTH ways against the service: a
//! stale part goes, a fresh part stays. The shipped threshold is a day
//! no test can sit out — the testhook exposes the age precisely so both
//! outcomes are checkable instead of only one.

use rdlt_connector_sdk::spi::core::{commit::WriteMode, id::LoadId, id::PipelineId, id::TableName};
use rdlt_connector_sdk::spi::{destination::Destination, destination::OpenContext};
use rdlt_connector_snowflake::destination::{Shell, testhook};
use rdlt_testkit::fixtures::{batch_of, schema_for};

use super::common::{config_for, credentials, scratch_schema};

/// Stage a part WITHOUT committing (open a session, ensure, write, drop
/// the session), leaving remote debris exactly as a crashed load would.
///
/// `parts` is forced to close on every write, because since 034 a
/// write ACCUMULATES into an open part and only uploads it when the
/// part rolls or a commit closes it. Under the shipping 128 MiB
/// default three rows leave nothing remote at all — which is a real
/// improvement (a crashed load below the target orphans nothing), and
/// exactly why this test has to ask for the upload it wants to reclaim.
async fn stage_orphan(doc: &serde_json::Value, pipeline: &str, load: &str) {
    let mut doc = doc.clone();
    doc["parts"] = serde_json::json!({"target_bytes": 1});
    let shell = Shell::from_value(doc).expect("valid");
    let mut session = shell
        .open(OpenContext::new(
            PipelineId::from(pipeline),
            LoadId::from(load),
        ))
        .await
        .expect("open");
    session
        .ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    session
        .write(&TableName::new("events"), batch_of(&[1, 2, 3]))
        .await
        .expect("write stages a part");
    drop(session); // no commit: the part is now remote debris
}

#[tokio::test]
async fn stale_parts_are_reclaimed_and_fresh_parts_survive() {
    let Some(creds) = credentials() else { return };
    let schema = scratch_schema("rcl");
    let doc = config_for(&creds, &schema);
    let config = {
        use rdlt_connector_sdk::config::Document;
        rdlt_connector_snowflake::destination::Config::from_value(doc.clone()).expect("valid")
    };
    let pipeline = "sf-rcl";

    stage_orphan(&doc, pipeline, "orphan-load").await;

    // The stage object's qualified name mirrors the connector's rule.
    let stage_names = testhook::rows(
        &config,
        &format!(
            "SHOW STAGES IN SCHEMA \"{}\".\"{}\"",
            config.database.to_uppercase(),
            schema.to_uppercase()
        ),
        &["name"],
    )
    .await
    .expect("stages listed");
    assert_eq!(stage_names.len(), 1, "one staging object: {stage_names:?}");
    let qualified_stage = format!(
        "\"{}\".\"{}\".\"{}\"",
        config.database.to_uppercase(),
        schema.to_uppercase(),
        stage_names[0][0]
    );

    let listed = |cfg: rdlt_connector_snowflake::destination::Config, stage: String| async move {
        testhook::rows(&cfg, &format!("LIST @{stage}"), &["name"])
            .await
            .map(|rows| rows.len())
            .unwrap_or(0)
    };

    assert!(
        listed(config.clone(), qualified_stage.clone()).await >= 1,
        "the orphaned part is really there"
    );

    // Fresh-kept half: a 24h threshold must NOT touch a part staged
    // seconds ago.
    testhook::reclaim_staged_older_than(&config, pipeline, "orphan-load", &qualified_stage, 24)
        .await
        .expect("reclaim runs");
    assert!(
        listed(config.clone(), qualified_stage.clone()).await >= 1,
        "a fresh part survives the shipped window"
    );

    // Stale-removed half: at age zero everything qualifies.
    testhook::reclaim_staged_older_than(&config, pipeline, "orphan-load", &qualified_stage, 0)
        .await
        .expect("reclaim runs");
    assert_eq!(
        listed(config.clone(), qualified_stage.clone()).await,
        0,
        "the stale part is gone"
    );

    let _ = testhook::connect_and_run(
        &config,
        &format!(
            "DROP SCHEMA IF EXISTS \"{}\".\"{}\" CASCADE",
            config.database.to_uppercase(),
            schema.to_uppercase()
        ),
    )
    .await;
}
