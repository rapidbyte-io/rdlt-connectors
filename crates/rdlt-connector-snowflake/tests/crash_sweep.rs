//! The crash-point sweep — its own binary, because the by-hand sweep
//! selects it by NAME (023's recorded convention; this suite costs live
//! account time and is never part of `make check`):
//!
//! ```text
//! cargo nextest run -p rdlt-connector-snowflake --features failpoints \
//!     -E 'binary(crash_sweep)'
//! ```
//!
//! Each cell arms one registered point with one action, drives a load,
//! and proves the protocol settles exactly-once afterwards: the same
//! rows land despite the crash, no duplicates, no local residue.
#![cfg(feature = "failpoints")]

mod cases {
    pub mod common;
}

use cases::common::{config_for, credentials, scratch_schema};
use rdlt_connector_sdk::spi::StreamSpec;
use rdlt_connector_sdk::spi::core::failpoint::fail;
use rdlt_connector_snowflake::destination::{FAIL_POINTS, Shell, testhook};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};
use serde_json::json;

/// The registry agrees with the sources, in both provable directions —
/// and the sweep below iterates the registry itself, so a point missing
/// from either side fails here before an account minute is spent.
#[test]
fn the_registry_matches_the_sources() {
    rdlt_testkit::assert_registry_matches_sources(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .as_path(),
        &[FAIL_POINTS],
    );
}

fn source_of(rows: Vec<serde_json::Value>) -> MemorySource {
    MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![MemoryBatch::new(rows).with_checkpoint(1)],
    )])
}

/// Arm each point, crash a load on it, then run the SAME load again
/// unarmed and prove exactly-once recovery.
#[tokio::test(flavor = "multi_thread")]
async fn every_registered_point_recovers_exactly_once() {
    let Some(creds) = credentials() else { return };

    for point in FAIL_POINTS {
        let schema = scratch_schema(&point.replace('.', "_"));
        let doc = config_for(&creds, &schema);
        let config = {
            use rdlt_connector_sdk::config::Document;
            rdlt_connector_snowflake::destination::Config::from_value(doc.clone()).expect("valid")
        };
        let rows = vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})];
        let workdir = tempfile::tempdir().expect("workdir");
        let engine_config = || {
            EngineConfig::new(format!("sweep-{}", point.replace('.', "-")))
                .with_workdir(workdir.path().join("wal"))
        };

        // Armed attempt: the injected crash fails the run (or the run
        // survives it by design — either way nothing may double-land).
        fail::cfg(*point, "return").expect("arm");
        let armed = Engine::new(
            engine_config(),
            source_of(rows.clone()),
            Shell::from_value(doc.clone()).expect("valid"),
        )
        .run()
        .await;
        fail::remove(*point);
        assert!(
            armed.is_err(),
            "[{point}] the armed run must report the injected crash"
        );

        // Recovery: the same pipeline, unarmed, settles exactly-once.
        let report = Engine::new(
            engine_config(),
            source_of(rows.clone()),
            Shell::from_value(doc.clone()).expect("valid"),
        )
        .run()
        .await
        .unwrap_or_else(|e| panic!("[{point}] recovery must settle: {e:?}"));

        // The settled load's local part directory must be gone or
        // empty — the "no local residue" half of the doc's claim,
        // asserted rather than trusted (the sf.stage.write cell leaves
        // exactly the file local reclaim exists for).
        let local = testhook::local_part_dir(
            &format!("sweep-{}", point.replace('.', "-")),
            report.load_id.as_str(),
        );
        let leftover = std::fs::read_dir(&local)
            .map(|entries| entries.count())
            .unwrap_or(0);
        assert_eq!(
            leftover,
            0,
            "[{point}] local residue at {}",
            local.display()
        );

        let landed = testhook::rows(
            &config,
            &format!(
                "SELECT COUNT(*) AS N, COUNT(DISTINCT \"ID\") AS K \
                 FROM \"{}\".\"{}\".\"EVENTS\"",
                config.database.to_uppercase(),
                schema.to_uppercase()
            ),
            &["n", "k"],
        )
        .await
        .expect("read back");
        assert_eq!(landed[0][0], "3", "[{point}] exactly the fed rows");
        assert_eq!(landed[0][1], "3", "[{point}] no duplicates");

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
}
