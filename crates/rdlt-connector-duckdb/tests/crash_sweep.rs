#![cfg(feature = "failpoints")]
//! The strategy-arm crash sweep: every fail point × action × strategy,
//! with an armed-fire pin and crash/rerun convergence proven per cell.
//!
//! The armed-fire pin is the anti-vacuousness instrument — a sweep that
//! tolerates a point which never fires proves nothing, so each
//! (strategy, point) must demonstrably fail an armed attempt before its
//! recovery is credited.
//!
//! ONE test function for the whole matrix, deliberately: fail points
//! are process-global, so splitting cells across tests lets one test's
//! armed point fire inside another running concurrently.
//!
//! Helpers live IN this binary rather than in `cases/common.rs` — the
//! sweep is its own compilation root and keeps its feed local.
//!
//! Run with `--features failpoints` (the `make test TARGET=sweep` gate).

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use rdlt_connector_duckdb::destination::{self, Config, FAIL_POINTS};
use rdlt_connector_sdk::config::Document as _;
use rdlt_connector_sdk::spi::core::failpoint::fail;
use rdlt_connector_sdk::spi::{
    ConnectorSpec, Cursor, ReadRequest, Source, SourceError, StreamSpec, WriteMode,
};
use rdlt_engine::{Engine, EngineConfig};

const ACTIONS: [&str; 3] = ["return", "panic", "1*off->return"];

// ---- the keyed structured feed ----------------------------------------------

/// One unit, one checkpoint, resume honored (clause E6): a crash-
/// recovered run re-reads AFTER the last committed checkpoint, so a
/// fully-committed unit is never redelivered — re-emitting it would
/// (correctly) trip the single-unit rules for the scoped strategies.
struct KvFeed {
    unit: RecordBatch,
}

#[async_trait]
impl Source for KvFeed {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("sweep-feed", "0.0.0")
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(vec![
            StreamSpec::new("kv")
                .with_structured()
                .with_primary_key(["k"]),
        ])
    }

    async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
        let resume = req
            .since
            .as_ref()
            .and_then(|c| c.as_value().as_u64())
            .unwrap_or(0);
        if resume < 1 {
            let _ = req.out.arrow(self.unit.clone()).await;
            let _ = req.out.checkpoint(Cursor::new(1)).await;
        }
        Ok(())
    }
}

/// The one sweep unit: k=1 delivered twice (old then new — arrival AND
/// `seq` desc both pick "k1-new"), one row per remaining key, everything
/// in day 1 except k=3, no hard-delete flags set. One unit keeps the
/// scoped/scd2 tables inside their single commit unit — the sweep
/// exercises crash edges, not the split-feed rejection.
fn feed() -> KvFeed {
    let columns: Vec<(Field, ArrayRef)> = vec![
        (
            Field::new("k", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2), Some(3)])),
        ),
        (
            Field::new("seq", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![Some(1), Some(9), Some(5), None])),
        ),
        (
            Field::new("day", DataType::Int64, true),
            Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(1), Some(2)])),
        ),
        (
            Field::new("v", DataType::Utf8, true),
            Arc::new(StringArray::from(vec![
                Some("k1-old"),
                Some("k1-new"),
                Some("k2"),
                Some("k3"),
            ])),
        ),
        (
            Field::new("gone", DataType::Boolean, true),
            Arc::new(BooleanArray::from(vec![
                Some(false),
                Some(false),
                Some(false),
                Some(false),
            ])),
        ),
    ];
    let (fields, arrays): (Vec<_>, Vec<_>) = columns.into_iter().unzip();
    KvFeed {
        unit: RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("sweep unit"),
    }
}

// ---- the harness ------------------------------------------------------------

/// The strategy's document over a fresh database path.
fn config_for(dir: &Path, options: &serde_json::Value) -> Config {
    let mut doc = serde_json::json!({ "path": dir.join("sweep.duckdb") });
    let fields = doc.as_object_mut().expect("object");
    for (key, value) in options.as_object().into_iter().flatten() {
        fields.insert(key.clone(), value.clone());
    }
    Config::from_value(doc).expect("valid sweep document")
}

/// One engine run: a fresh Shell over the same file (sequential re-open
/// is the documented-safe pattern; each attempt's instance drops before
/// the next opens). Spawned so the "panic" action lands as a JoinError
/// instead of aborting the test.
async fn attempt(config: &Config, workdir: &Path) -> Result<(), String> {
    let dest = destination::Shell::new(config.clone()).expect("valid config");
    let engine_config = EngineConfig::new("sweep")
        .with_write_mode(WriteMode::Merge {
            key: vec!["k".into()],
        })
        .with_workdir(workdir.to_path_buf());
    match tokio::spawn(Engine::new(engine_config, feed(), dest).run()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(join) => Err(format!("panicked: {join}")),
    }
}

/// Canonical converged rows: `k:v` pairs sorted by key — every strategy
/// resolves k=1 to "k1-new" (scd2 reads the ACTIVE view only).
fn canon(config: &Config, scd2: bool) -> String {
    let filter = if scd2 {
        " WHERE _rdlt_valid_to IS NULL"
    } else {
        ""
    };
    destination::testhook::query_string(
        config,
        &format!("SELECT string_agg(k::VARCHAR || ':' || v, ',' ORDER BY k) FROM kv{filter}"),
    )
    .expect("canon query")
}

#[tokio::test(flavor = "multi_thread")]
async fn strategy_arms_survive_crash_sweep() {
    let strategies: Vec<(&str, serde_json::Value, bool)> = vec![
        ("delete_insert", serde_json::json!({}), false),
        (
            "upsert",
            serde_json::json!({
                "merge_strategy": "upsert",
                "tables": {"kv": {"hard_delete": "gone",
                                   "dedup_sort": {"column": "seq", "order": "desc"}}}
            }),
            false,
        ),
        (
            "scd2_retire",
            serde_json::json!({
                "merge_strategy": "scd2",
                "tables": {"kv": {"scd2": {"absent": "retire"}}}
            }),
            true,
        ),
        (
            "merge_scope",
            serde_json::json!({
                "tables": {"kv": {"merge_scope": ["day"],
                                   "dedup_sort": {"column": "seq", "order": "desc"}}}
            }),
            false,
        ),
    ];
    let expected = "1:k1-new,2:k2,3:k3";

    let mut fired: BTreeSet<(&str, &str)> = BTreeSet::new();
    for (label, options, scd2) in &strategies {
        for &point in FAIL_POINTS {
            for action in ACTIONS {
                let dir = tempfile::tempdir().expect("tempdir");
                let workdir = dir.path().join("wal");
                let config = config_for(dir.path(), options);

                fail::cfg(point, action).expect("configure fail point");
                let armed1 = attempt(&config, &workdir).await;
                // Second run still armed: a crash during recovery too.
                let armed2 = attempt(&config, &workdir).await;
                fail::remove(point);
                if armed1.is_err() || armed2.is_err() {
                    fired.insert((label, point));
                }

                let recovered = attempt(&config, &workdir).await;
                assert!(
                    recovered.is_ok(),
                    "[{label} / {point} / {action}] recovery failed: {recovered:?}"
                );
                assert_eq!(
                    canon(&config, *scd2),
                    expected,
                    "[{label} / {point} / {action}] crash/rerun did not converge"
                );
                if *scd2 {
                    // History convergence: a replayed load must not
                    // double-close or duplicate versions — unchanged
                    // keys carry no closed rows at all.
                    let closed = destination::testhook::query_string(
                        &config,
                        "SELECT coalesce(count(*), 0)::VARCHAR FROM kv \
                         WHERE _rdlt_valid_to IS NOT NULL AND k <> 1",
                    )
                    .expect("closed-churn query");
                    assert_eq!(
                        closed, "0",
                        "[{label} / {point} / {action}] unchanged keys churned history"
                    );
                }
            }
        }
    }

    // The armed-fire pin over the full (strategy × point) matrix: a
    // missing entry means a crash_point! site went dead (vacuous sweep).
    let expected_fired: BTreeSet<(&str, &str)> = strategies
        .iter()
        .flat_map(|(label, _, _)| FAIL_POINTS.iter().map(move |&point| (*label, point)))
        .collect();
    assert_eq!(fired, expected_fired, "armed-fire pin diverged");
}

/// The registry names exactly the crash points armed in this crate's
/// sources. The sweep's own fired-vs-registry check cannot establish
/// this — deleting a point from BOTH the code and the list keeps it
/// true while the matrix quietly shrinks — so this reads the sources.
/// One registry here, so the union is itself. The ungated twin lives in
/// `cases/test_gating.rs`.
#[test]
fn the_registry_matches_the_sources() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    rdlt_testkit::assert_registry_matches_sources(&src, &[FAIL_POINTS]);
}
