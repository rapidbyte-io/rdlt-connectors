//! Exactly-once, live: replays publish nothing, resume reads catalog
//! state, a narrowed stream null-fills, and Replace refuses against a
//! real catalog.

use rdlt_connector_iceberg::destination::Shell;
use rdlt_connector_sdk::spi::core::commit::WriteMode;
use rdlt_connector_sdk::spi::source::StreamSpec;
use rdlt_engine::config::Config as EngineConfig;
use rdlt_engine::engine::Engine;
use rdlt_testkit::memory::{Batch as MemoryBatch, Source as MemorySource, Stream as MemoryStream};
use serde_json::json;

use super::common::{CatalogFixture, minimal_doc};

fn one_batch(rows: Vec<serde_json::Value>) -> MemorySource {
    MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        vec![MemoryBatch::new(rows).with_checkpoint(1)],
    )])
}

/// A RESUMED pipeline redelivers nothing: run 2 reads run 1's state
/// from the CATALOG (the marker table) and the source resumes past
/// its checkpoint — totals stay exact.
#[tokio::test]
async fn a_resumed_pipeline_republishes_nothing() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "resume_v2";
    let doc = fixture.doc(namespace);
    let workdir = tempfile::tempdir().expect("workdir");
    let wal = workdir.path().join("wal");

    for _ in 0..2 {
        Engine::new(
            EngineConfig::new("ice-resume-v2").with_workdir(wal.clone()),
            one_batch(vec![json!({"id": 1}), json!({"id": 2})]),
            Shell::from_value(doc.clone()).expect("valid"),
        )
        .run()
        .await
        .expect("the load settles");
    }

    let summaries = fixture.snapshot_summaries(namespace, "events").await;
    let total: u64 = summaries
        .iter()
        .filter_map(|s| s.get("added-records").and_then(|v| v.parse::<u64>().ok()))
        .sum();
    assert_eq!(
        total, 2,
        "run 2 resumed past the checkpoint and republished nothing: {summaries:?}"
    );

    // The raw state protocol behind the resume: the marker table holds
    // this pipeline's scope key.
    use rdlt_connector_iceberg::destination::{Config, testhook};
    use rdlt_connector_sdk::config::Document;
    let config = Config::from_value(doc).expect("valid");
    let raw = testhook::read_raw_state(
        &config,
        &[namespace.to_owned()],
        &testhook::scope_of("ice-resume-v2"),
    )
    .await
    .expect("state readable")
    .expect("state present after a committed load");
    assert!(
        raw.contains("ice-resume-v2"),
        "the doc names its pipeline: {raw}"
    );
}

/// 037 D1, live: state stranded under the pre-037 12-hex scope key
/// REFUSES typed rather than being silently treated as a fresh
/// pipeline. Run 1 writes state at the current 32-hex key the normal
/// way; the state property is then relocated to the legacy key
/// (`testhook::move_state_to_legacy_key`, simulating a warehouse never
/// touched since the widen); run 2 loads two DIFFERENT rows (3, 4) —
/// it must refuse with the frozen spelling before writing anything,
/// and the snapshot oracle below proves run 2 published NOTHING: the
/// total stays at run 1's 2 rows, not 4.
///
/// Red-proved against the unfixed code: with the legacy-key probe
/// absent, `read_state` sees nothing at the 32-hex key, agrees this is
/// a first run, and the engine runs run 2 to completion as an ordinary
/// Append — its two rows (3, 4) land on top of run 1's already-
/// committed two (4 total across two snapshots), and no refusal ever
/// surfaces.
#[tokio::test]
async fn state_stranded_under_the_legacy_scope_key_refuses_typed() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "legacy_scope_v2";
    let doc = fixture.doc(namespace);
    let pipeline = "ice-legacy-scope";
    let workdir = tempfile::tempdir().expect("workdir");

    Engine::new(
        EngineConfig::new(pipeline).with_workdir(workdir.path().join("wal1")),
        one_batch(vec![json!({"id": 1}), json!({"id": 2})]),
        Shell::from_value(doc.clone()).expect("valid"),
    )
    .run()
    .await
    .expect("run 1 settles and writes state at the current 32-hex key");

    use rdlt_connector_iceberg::destination::{Config, testhook};
    use rdlt_connector_sdk::config::Document;
    let config = Config::from_value(doc.clone()).expect("valid");
    testhook::move_state_to_legacy_key(&config, &[namespace.to_owned()], pipeline)
        .await
        .expect("state relocated to the legacy 12-hex key");

    let err = Engine::new(
        EngineConfig::new(pipeline).with_workdir(workdir.path().join("wal2")),
        one_batch(vec![json!({"id": 3}), json!({"id": 4})]),
        Shell::from_value(doc).expect("valid"),
    )
    .run()
    .await
    .expect_err("run 2 must refuse rather than silently re-loading from zero");
    let text = format!("{err}");
    // A distinctive substring, not the full frozen string — the offline
    // suite (load.rs's `frozen_legacy_refusal`) pins the exact wording;
    // this live cell only needs to prove the RIGHT refusal fired.
    assert!(
        text.contains(&format!(
            "state for pipeline `{pipeline}` predates this build"
        )) && text.contains("the pipeline scope key widened (12-hex to 32-hex)")
            && text.contains("remove the stale `rdlt.state."),
        "{text}"
    );

    let summaries = fixture.snapshot_summaries(namespace, "events").await;
    let total: u64 = summaries
        .iter()
        .filter_map(|s| s.get("added-records").and_then(|v| v.parse::<u64>().ok()))
        .sum();
    assert_eq!(
        total, 2,
        "run 2 refused before writing anything — no snapshot from run 2 exists: {summaries:?}"
    );
}

/// A REPLAYED commit publishes nothing: the same (load, seq)
/// redelivered through the SPI — a fresh session staging the same
/// batch — converges on the snapshot history instead of appending a
/// second copy.
#[tokio::test]
async fn a_replayed_commit_publishes_nothing() {
    use std::sync::Arc;

    use rdlt_connector_sdk::spi::core::{
        commit::WriteMode, id::LoadId, id::PipelineId, id::TableName, schema::Column,
        schema::ColumnType, schema::Provenance, schema::TableSchema, state::StateDoc,
        types::LogicalType,
    };
    use rdlt_connector_sdk::spi::{
        core::commit::CommitMeta, destination::Destination, destination::OpenContext,
    };

    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "spi_replay_v2";
    let shell = Shell::from_value(fixture.doc(namespace)).expect("valid");

    let schema = TableSchema {
        table: TableName::from("events"),
        parent: None,
        columns: vec![Column {
            name: "id".into(),
            column_type: ColumnType::scalar(LogicalType::Int64),
            nullable: false,
            provenance: Provenance::Inferred,
        }],
    };
    let batch = arrow_array::RecordBatch::try_new(
        Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "id",
            arrow_schema::DataType::Int64,
            false,
        )])),
        vec![Arc::new(arrow_array::Int64Array::from(vec![1, 2]))],
    )
    .expect("batch");
    let pipeline = PipelineId::from("ice-spi-replay");
    let load = LoadId::from("load-replay-1");
    let meta = || CommitMeta {
        load_id: load.clone(),
        commit_seq: 1,
        state: StateDoc::new(pipeline.clone(), "test"),
        counters: Default::default(),
    };

    // The same (load, seq) staged and published by TWO sessions — the
    // second is the redelivery.
    for _ in 0..2 {
        let mut session = shell
            .open(OpenContext::new(pipeline.clone(), load.clone()))
            .await
            .expect("open");
        session
            .ensure_table(&schema, &WriteMode::Append)
            .await
            .expect("ensure");
        session
            .write(&TableName::from("events"), batch.clone())
            .await
            .expect("write");
        session.commit(meta()).await.expect("commit");
    }

    let summaries = fixture.snapshot_summaries(namespace, "events").await;
    assert_eq!(
        summaries.len(),
        1,
        "the replay published NOTHING: {summaries:?}"
    );
    let total: u64 = summaries
        .iter()
        .filter_map(|s| s.get("added-records").and_then(|v| v.parse::<u64>().ok()))
        .sum();
    assert_eq!(total, 2, "one copy of the rows: {summaries:?}");
}

/// A stream that stops providing a nullable column keeps loading —
/// the absent column null-fills against the live table.
#[tokio::test]
async fn a_narrowed_stream_null_fills_the_missing_column() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "narrow_v2";
    let doc = fixture.doc(namespace);
    let workdir = tempfile::tempdir().expect("workdir");

    Engine::new(
        EngineConfig::new("ice-narrow-1").with_workdir(workdir.path().join("wal1")),
        one_batch(vec![json!({"id": 1, "note": "present"})]),
        Shell::from_value(doc.clone()).expect("valid"),
    )
    .run()
    .await
    .expect("run 1 settles");

    Engine::new(
        EngineConfig::new("ice-narrow-2").with_workdir(workdir.path().join("wal2")),
        one_batch(vec![json!({"id": 2})]),
        Shell::from_value(doc).expect("valid"),
    )
    .run()
    .await
    .expect("run 2 settles despite the narrowed stream");

    let summaries = fixture.snapshot_summaries(namespace, "events").await;
    assert_eq!(summaries.len(), 2, "{summaries:?}");
}

/// Replace refuses LIVE with the frozen spelling — before any table
/// is touched.
#[tokio::test]
async fn replace_is_refused_against_the_live_catalog() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let doc = fixture.doc("replace_v2");
    let workdir = tempfile::tempdir().expect("workdir");
    let err = Engine::new(
        EngineConfig::new("ice-replace-v2")
            .with_workdir(workdir.path().join("wal"))
            .with_write_mode(WriteMode::Replace),
        one_batch(vec![json!({"id": 1})]),
        Shell::from_value(doc).expect("valid"),
    )
    .run()
    .await
    .expect_err("Replace must refuse");
    let text = format!("{err}");
    assert!(
        text.contains("Replace is not supported")
            && text.contains("use Append, or a SQL destination for replace semantics"),
        "{text}"
    );
}

/// OFFLINE (no fixture, no catalog reachable): the engine refuses a
/// no-workdir run against this destination before it ever connects.
/// `capabilities()` is a synchronous, pre-connect call on the
/// connector (029 N2 / 037 US3); `validate_streams` reads it and
/// returns before `Destination::open` — so this pin exercises the real
/// `Iceberg` connector's declared `requires_durable_identity` and the
/// engine's refusal with no live catalog at all.
#[tokio::test]
async fn a_no_workdir_run_is_refused_before_any_catalog_connection() {
    let err = Engine::new(
        EngineConfig::new("ice-no-workdir"),
        one_batch(vec![json!({"id": 1})]),
        Shell::from_value(minimal_doc()).expect("valid"),
    )
    .run()
    .await
    .expect_err("N2: no workdir means duplication on mid-publish retry");
    let text = format!("{err}");
    assert!(text.contains("requires a workdir"), "{text}");
}

/// THE LOAD-LEVEL RECEIPT, live (042 — 029 D7 reversed by owner
/// ruling; the receipt's home moved in the round-2 fix wave): a
/// committed load's receipt is found by a FRESH session through
/// `existing_receipt`, read off the `_rdlt_state` marker table's
/// `rdlt.receipt.<load_id>` property — stamped by publish in the same
/// property commit as the state doc, one table read, no namespace
/// enumeration. Three postures in one fixture boot: the same pipeline
/// (crash-recovery replay), a SIBLING pipeline with a different scope
/// hash (the kill matrix's K-D5 convergence re-run rides
/// `{pipeline}-r` — the receipt is LOAD-keyed, so the sibling finds it
/// too), and a never-committed load staying an honest `None`.
#[tokio::test]
async fn a_committed_loads_receipt_is_found_by_a_fresh_session() {
    use rdlt_connector_iceberg::destination::{Config, Iceberg};
    use rdlt_connector_sdk::config::Document;
    use rdlt_connector_sdk::destination::{Backend, DestinationConnector};
    use rdlt_connector_sdk::spi::core::{id::LoadId, id::PipelineId};
    use rdlt_connector_sdk::spi::destination::OpenContext;
    use rdlt_testkit::fixtures::{batch_of, commit_meta_for, schema_for};

    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "receipt_v2";
    let config = Config::from_value(fixture.doc(namespace)).expect("valid");
    let dest = Iceberg::assemble(config).expect("assembles");
    let pipeline = PipelineId::new("ice-receipt-v2");
    let load = LoadId::new("load-r1");

    // Session 1 commits the load.
    let mut first = dest
        .connect(&OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("connects");
    first
        .ensure_table(&schema_for("receipt_evts"), &WriteMode::Append)
        .await
        .expect("ensures");
    first
        .write(&"receipt_evts".into(), batch_of(&[1, 2, 3]))
        .await
        .expect("writes");
    first
        .publish(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("publishes");

    // A FRESH session on the SAME pipeline finds the receipt.
    let mut fresh = dest
        .connect(&OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("connects");
    let receipt = fresh
        .existing_receipt(&load, 1)
        .await
        .expect("the receipt scan answers")
        .expect("the committed load's receipt is found across sessions");
    assert_eq!(receipt.load_id, load);
    assert_eq!(receipt.commit_seq, 1);

    // The SIBLING pipeline (a different scope hash) finds it too — the
    // receipt is keyed by load identity, never by pipeline scope.
    let mut sibling = dest
        .connect(&OpenContext::new(
            PipelineId::new("ice-receipt-v2-r"),
            load.clone(),
        ))
        .await
        .expect("connects");
    let from_sibling = sibling
        .existing_receipt(&load, 1)
        .await
        .expect("the receipt scan answers")
        .expect("the receipt is load-keyed: a sibling pipeline scope still finds it");
    assert_eq!(from_sibling.load_id, load);

    // A load nothing ever committed stays an honest None.
    let absent = fresh
        .existing_receipt(&LoadId::new("load-never-committed"), 1)
        .await
        .expect("the receipt scan answers");
    assert!(absent.is_none(), "an uncommitted load has no receipt");
}

/// THE REPLAY DISCARD, live (042 — reachable for the first time now
/// that `existing_receipt` answers): a fresh session redelivers an
/// already-committed window's rows, `replay` is called, and the
/// discarded rows must NOT ride the session's NEXT genuine publish —
/// the staged-window hazard the sdk `Backend::replay` doc warns about.
#[tokio::test]
async fn a_replayed_windows_staged_rows_never_reach_the_next_publish() {
    use rdlt_connector_iceberg::destination::{Config, Iceberg};
    use rdlt_connector_sdk::config::Document;
    use rdlt_connector_sdk::destination::{Backend, DestinationConnector};
    use rdlt_connector_sdk::spi::core::{id::LoadId, id::PipelineId};
    use rdlt_connector_sdk::spi::destination::OpenContext;
    use rdlt_testkit::fixtures::{batch_of, commit_meta_for, schema_for};

    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "replay_discard_v2";
    let config = Config::from_value(fixture.doc(namespace)).expect("valid");
    let dest = Iceberg::assemble(config).expect("assembles");
    let pipeline = PipelineId::new("ice-replay-discard");
    let load = LoadId::new("load-rd1");

    // Session 1 commits window 1.
    let mut first = dest
        .connect(&OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("connects");
    first
        .ensure_table(&schema_for("discard_evts"), &WriteMode::Append)
        .await
        .expect("ensures");
    first
        .write(&"discard_evts".into(), batch_of(&[1, 2, 3]))
        .await
        .expect("writes");
    let receipt = first
        .publish(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("publishes");

    // Session 2 redelivers window 1's rows, replays, then publishes a
    // GENUINE window 2 — only window 2's rows may land.
    let mut second = dest
        .connect(&OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("connects");
    second
        .ensure_table(&schema_for("discard_evts"), &WriteMode::Append)
        .await
        .expect("ensures");
    second
        .write(&"discard_evts".into(), batch_of(&[1, 2, 3]))
        .await
        .expect("stages the redelivery");
    let meta = commit_meta_for(&pipeline, &load, 1);
    second
        .replay(&meta, &receipt)
        .await
        .expect("replay housekeeping");
    second
        .write(&"discard_evts".into(), batch_of(&[4, 5, 6]))
        .await
        .expect("stages window 2");
    second
        .publish(commit_meta_for(&pipeline, &load, 2))
        .await
        .expect("publishes window 2");

    let summaries = fixture.snapshot_summaries(namespace, "discard_evts").await;
    let total: u64 = summaries
        .last()
        .and_then(|s| s.get("total-records"))
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    assert_eq!(
        total, 6,
        "the replayed window's redelivered rows were discarded, not re-published: {summaries:?}"
    );
}

/// FIX ROUND 1, CRITICAL 1 (042 review): a publish that committed only
/// SOME of a redelivery's tables must converge through the replay path
/// without losing the unfinished ones. Session 1 publishes `(load, 1)`
/// with table A alone in its set — so the receipt exists while table B
/// has no data. The recovery redelivery ensures BOTH tables, stages
/// both windows, finds the receipt through `existing_receipt`, and
/// `replay` must then land table B's window (A's discarded) — a replay
/// that discards wholesale loses B forever, because the framework
/// returns the receipt instead of publishing once `existing_receipt`
/// answers. (The receipt-less residue of a crash BETWEEN per-table
/// commits converges through the re-driven publish instead — its
/// per-table history scan discards what the dead attempt committed.)
#[tokio::test]
async fn a_partial_publish_converges_through_replay_without_losing_tables() {
    use rdlt_connector_iceberg::destination::{Config, Iceberg};
    use rdlt_connector_sdk::config::Document;
    use rdlt_connector_sdk::destination::{Backend, DestinationConnector};
    use rdlt_connector_sdk::spi::core::{id::LoadId, id::PipelineId};
    use rdlt_connector_sdk::spi::destination::OpenContext;
    use rdlt_testkit::fixtures::{batch_of, commit_meta_for, schema_for};

    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "partial_publish_v2";
    let config = Config::from_value(fixture.doc(namespace)).expect("valid");
    let dest = Iceberg::assemble(config).expect("assembles");
    let pipeline = PipelineId::new("ice-partial-publish");
    let load = LoadId::new("load-pp1");

    // Session 1: table A's commit landed, table B's never did.
    let mut first = dest
        .connect(&OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("connects");
    first
        .ensure_table(&schema_for("pp_table_a"), &WriteMode::Append)
        .await
        .expect("ensures A");
    first
        .write(&"pp_table_a".into(), batch_of(&[1, 2, 3]))
        .await
        .expect("writes A");
    let receipt = first
        .publish(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("publishes A's half");

    // The recovery redelivery: both tables ensured and staged, the
    // receipt found, replay must CONVERGE — not discard.
    let mut recovery = dest
        .connect(&OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("connects");
    for table in ["pp_table_a", "pp_table_b"] {
        recovery
            .ensure_table(&schema_for(table), &WriteMode::Append)
            .await
            .expect("ensures");
        recovery
            .write(&table.into(), batch_of(&[1, 2, 3]))
            .await
            .expect("stages the redelivery");
    }
    let found = recovery
        .existing_receipt(&load, 1)
        .await
        .expect("the receipt scan answers")
        .expect("A's stamp is found");
    assert_eq!(found.load_id, load);
    recovery
        .replay(&commit_meta_for(&pipeline, &load, 1), &receipt)
        .await
        .expect("replay converges");

    let count = |table: &'static str| {
        let fixture = &fixture;
        async move {
            fixture
                .snapshot_summaries(namespace, table)
                .await
                .last()
                .and_then(|s| s.get("total-records"))
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(0)
        }
    };
    assert_eq!(
        count("pp_table_a").await,
        3,
        "table A stays exact — its redelivered window is discarded"
    );
    assert_eq!(
        count("pp_table_b").await,
        3,
        "table B's window LANDS — a wholesale-discard replay loses it forever"
    );
}

/// ROUND-10 FIX (the sibling mid-publish window): a crash BETWEEN a
/// table's `append_commit` and the receipt stamp leaves the snapshot
/// committed under the DYING attempt's pipeline scope with NO receipt.
/// A re-drive under a SIBLING scope (the kill matrix's `-r` re-run, or
/// any orchestrator re-scope) honestly finds no receipt and re-drives
/// publish — whose settled check was SCOPE-matched, so it missed the
/// dead scope's snapshot and appended the redelivered window a second
/// time: duplicate rows in the warehouse. The check is LOAD-keyed now:
/// a 042+ load id is entropy-unique, so a load-id match IS the same
/// load whatever scope stamped it.
#[tokio::test]
async fn a_sibling_scope_re_drive_converges_a_mid_publish_crash_without_duplicating() {
    use rdlt_connector_iceberg::destination::{Config, Iceberg, testhook};
    use rdlt_connector_sdk::config::Document;
    use rdlt_connector_sdk::destination::{Backend, DestinationConnector};
    use rdlt_connector_sdk::spi::core::{id::LoadId, id::PipelineId};
    use rdlt_connector_sdk::spi::destination::OpenContext;
    use rdlt_testkit::fixtures::{batch_of, commit_meta_for, schema_for};

    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "sibling_window_v2";
    let config = Config::from_value(fixture.doc(namespace)).expect("valid");
    let dest = Iceberg::assemble(config).expect("assembles");
    let pipeline = PipelineId::new("ice-sibling-window");
    let load = LoadId::new("load-sw1");

    // Session 1 (scope X): the table's snapshot commits; the testhooks
    // then strip receipt AND state — the exact residue of a crash
    // between `append_commit` and the receipt stamp.
    let mut first = dest
        .connect(&OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("connects");
    first
        .ensure_table(&schema_for("sw_events"), &WriteMode::Append)
        .await
        .expect("ensures");
    first
        .write(&"sw_events".into(), batch_of(&[1, 2, 3]))
        .await
        .expect("writes");
    first
        .publish(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("publishes");
    let cfg = Config::from_value(fixture.doc(namespace)).expect("valid");
    testhook::remove_state(&cfg, &[namespace.to_owned()], pipeline.as_str())
        .await
        .expect("the state half of the residue is staged");
    testhook::remove_receipt(&cfg, &[namespace.to_owned()], load.as_str())
        .await
        .expect("the receipt half of the residue is staged");

    // The sibling-scope re-drive: same load, no receipt to find, so
    // the framework re-drives publish.
    let sibling = PipelineId::new("ice-sibling-window-r");
    let mut redrive = dest
        .connect(&OpenContext::new(sibling.clone(), load.clone()))
        .await
        .expect("connects");
    redrive
        .ensure_table(&schema_for("sw_events"), &WriteMode::Append)
        .await
        .expect("ensures");
    redrive
        .write(&"sw_events".into(), batch_of(&[1, 2, 3]))
        .await
        .expect("stages the redelivery");
    assert!(
        redrive
            .existing_receipt(&load, 1)
            .await
            .expect("the receipt scan answers")
            .is_none(),
        "the crash window left no receipt — publish is re-driven, not replayed"
    );
    redrive
        .publish(commit_meta_for(&sibling, &load, 1))
        .await
        .expect("the re-driven publish settles");

    let total = fixture
        .snapshot_summaries(namespace, "sw_events")
        .await
        .last()
        .and_then(|s| s.get("total-records"))
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    assert_eq!(
        total, 3,
        "the dead attempt's snapshot is recognized by LOAD id across scopes — the \
         redelivered window is discarded, never appended a second time"
    );
}

/// FIX ROUND 1, CRITICAL 2 (042 review): the replay path must persist
/// the state doc. Publish writes state LAST, so a crash at
/// `ice.receipt.visible` leaves every table's data committed with NO
/// state — healed, pre-receipt, by the redelivery's re-publish
/// (Settled + write_state). The receipt fast path bypasses publish, so
/// `replay` itself must write `meta.state` or the pipeline finishes
/// with a stale/absent cursor and the NEXT run re-ingests under a
/// fresh load id — cross-run duplication.
#[tokio::test]
async fn the_replay_path_persists_the_state_doc() {
    use rdlt_connector_iceberg::destination::{Config, Iceberg, testhook};
    use rdlt_connector_sdk::config::Document;
    use rdlt_connector_sdk::destination::{Backend, DestinationConnector};
    use rdlt_connector_sdk::spi::core::{id::LoadId, id::PipelineId};
    use rdlt_connector_sdk::spi::destination::OpenContext;
    use rdlt_testkit::fixtures::{batch_of, commit_meta_for, schema_for};

    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "replay_state_v2";
    let config = Config::from_value(fixture.doc(namespace)).expect("valid");
    let dest = Iceberg::assemble(config).expect("assembles");
    let pipeline = PipelineId::new("ice-replay-state");
    let load = LoadId::new("load-rs1");

    // Session 1 commits data AND state; the testhook then removes the
    // state property — the exact residue of a crash at
    // `ice.receipt.visible` (data committed, state write never ran).
    let mut first = dest
        .connect(&OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("connects");
    first
        .ensure_table(&schema_for("rs_events"), &WriteMode::Append)
        .await
        .expect("ensures");
    first
        .write(&"rs_events".into(), batch_of(&[1, 2, 3]))
        .await
        .expect("writes");
    let receipt = first
        .publish(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("publishes");
    testhook::remove_state(
        &Config::from_value(fixture.doc(namespace)).expect("valid"),
        &[namespace.to_owned()],
        pipeline.as_str(),
    )
    .await
    .expect("the crash residue is staged");

    // Recovery: the receipt fast path runs — and must re-persist state.
    let mut recovery = dest
        .connect(&OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("connects");
    recovery
        .ensure_table(&schema_for("rs_events"), &WriteMode::Append)
        .await
        .expect("ensures");
    recovery
        .write(&"rs_events".into(), batch_of(&[1, 2, 3]))
        .await
        .expect("stages the redelivery");
    recovery
        .existing_receipt(&load, 1)
        .await
        .expect("the receipt scan answers")
        .expect("the receipt is found");
    recovery
        .replay(&commit_meta_for(&pipeline, &load, 1), &receipt)
        .await
        .expect("replay converges");

    let state = recovery
        .read_state(&pipeline)
        .await
        .expect("state readable")
        .expect("the replay path persisted the state doc — a bypassed write leaves the next run a stale cursor");
    assert_eq!(state.pipeline, pipeline);
    let total = fixture
        .snapshot_summaries(namespace, "rs_events")
        .await
        .last()
        .and_then(|s| s.get("total-records"))
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);
    assert_eq!(
        total, 3,
        "the redelivered window was discarded, not re-landed"
    );
}
