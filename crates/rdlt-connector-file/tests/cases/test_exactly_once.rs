//! The commit protocol under redelivery and interleaving: receipts
//! forever, once-per-load truncation guarded durably, state landing
//! with the receipt LAST.

use rdlt_connector_file::destination::{self, DestFormat};
use rdlt_connector_file::source;
use rdlt_connector_sdk::spi::core::{LoadId, PipelineId, TableName, WriteMode};
use rdlt_connector_sdk::spi::{Destination, OpenContext};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{
    MemoryBatch, MemorySource, MemoryStream, batch_of, commit_meta_for, schema_for,
};

use super::common::{jsonl_source, local_dest, plant};

/// The lease's TTL (`lease.rs`'s `TTL_SECS`, `pub(super)` and so
/// unreachable from this external test). 300s, mirrored here rather
/// than imported — a drift between the two would only ever make this
/// test too lenient (a "stale" plant that is not actually past the
/// real TTL), never silently wrong the other way, since a full engine
/// run either takes the lease over or it does not.
const LEASE_TTL_SECS: u64 = 300;

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_millis() as u64
}

/// Plant a lease document directly on disk — the shape a dead or a
/// merely-crashed session's lease would have, bypassing
/// `Lease::acquire` entirely (mirrors `lease.rs`'s own `plant_lease`
/// test helper, which is `pub(super)` and so not reachable from here).
fn plant_lease_doc(dir: &std::path::Path, pipeline: &str, owner: &str, renewed_at_ms: u64) {
    let scope = rdlt_connector_sdk::spi::core::naming::ident_hash(pipeline, 12);
    plant(
        dir,
        &format!("_rdlt_lease.{scope}.json"),
        serde_json::json!({
            "format_version": 1,
            "pipeline": pipeline,
            "owner": owner,
            "acquired_at_ms": renewed_at_ms,
            "renewed_at_ms": renewed_at_ms,
        })
        .to_string()
        .as_bytes(),
    );
}

fn lease_doc_path(dir: &std::path::Path, pipeline: &str) -> std::path::PathBuf {
    let scope = rdlt_connector_sdk::spi::core::naming::ident_hash(pipeline, 12);
    dir.join(format!("_rdlt_lease.{scope}.json"))
}

/// Open one session against a FRESH `File` connector — a distinct
/// instance every call, so this mints a distinct owner token each time
/// (037 US2 T7's mandatory rule: the owner is process-unique, never
/// derived from config/pipeline/path). This is the shape two separate
/// `rdlt run` invocations of the SAME pipeline take, as opposed to
/// `run_load`'s single shared `dest`, whose repeated `.open()` calls
/// all share one owner.
async fn open_session(
    dir: &std::path::Path,
    pipeline: &str,
    load: &str,
) -> Result<Box<dyn rdlt_connector_sdk::spi::LoadSession>, rdlt_connector_sdk::spi::DestinationError>
{
    let config = local_dest(dir);
    let dest = destination::Shell::new(config).expect("valid");
    dest.open(OpenContext::new(
        PipelineId::new(pipeline),
        LoadId::new(load),
    ))
    .await
}

/// S6 (030): scope-wide reclaim meant two live sessions of ONE
/// pipeline silently destroyed each other's staging. Now the second
/// open refuses typed while the first holds the lease.
#[tokio::test]
async fn a_second_session_of_the_same_pipeline_is_refused_not_destroyed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let first = open_session(dir.path(), "one-pipeline", "load-a")
        .await
        .expect("first opens");
    let second = open_session(dir.path(), "one-pipeline", "load-b").await;
    let err = match second {
        Ok(_) => panic!("the lease refuses a concurrent same-pipeline session"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("holds the destination lease"),
        "{err}"
    );
    drop(first);
}

/// 037 US2 T7 fix round 1: the hole the coordinator's own instruction
/// created and this fix closes. A session that publishes TWO commits
/// must hold the lease BETWEEN them, not just up to the first — a
/// second connector's open is refused after commit 1 and before commit
/// 2. Red-proved against the code this fix replaces (release at the end
/// of `publish`): there, the assertion below fails because the second
/// open SUCCEEDS — `first`'s lease was already released after its
/// first commit, so `second` reacquires as a fresh session and would
/// have gone on to destroy `first`'s in-flight staging for commit 2 via
/// `prepare_staging`'s scope-wide wipe.
#[tokio::test]
async fn a_second_session_is_refused_between_two_commits_of_one_session() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pipeline = PipelineId::new("multi-commit-lease");

    let config = local_dest(dir.path());
    let dest = destination::Shell::new(config).expect("valid");
    let load = LoadId::new("load-a");
    let mut first = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("first opens");
    first
        .ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    first
        .write(&TableName::new("events"), batch_of(&[1]))
        .await
        .expect("write");
    first
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("first commit");

    // BETWEEN commit 1 and commit 2: a second connector's open must
    // still be refused — the lease is not released until `close`.
    let second = open_session(dir.path(), "multi-commit-lease", "load-b").await;
    let err = match second {
        Ok(_) => panic!(
            "a second session must be refused BETWEEN this session's commits, not just \
             before its first"
        ),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("holds the destination lease"),
        "{err}"
    );

    first
        .write(&TableName::new("events"), batch_of(&[2]))
        .await
        .expect("write");
    first
        .commit(commit_meta_for(&pipeline, &load, 2))
        .await
        .expect("second commit");
    first.close().await.expect("close");

    // AFTER close: a new session is welcome.
    open_session(dir.path(), "multi-commit-lease", "load-c")
        .await
        .expect("a session opens freely once the prior one has closed");
}

/// 037 US2 fix round 2, M6: a write after `close` is refused typed,
/// not silently allowed. The engine itself never reaches this path
/// (the SDK choreography never calls `write`/`commit` again after
/// `close`), so this is specifically the embedder-misuse guard — the
/// frozen spelling, pinned exactly.
#[tokio::test]
async fn a_write_after_close_is_refused_not_silently_allowed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pipeline = PipelineId::new("write-after-close");
    let config = local_dest(dir.path());
    let dest = destination::Shell::new(config).expect("valid");
    let load = LoadId::new("load-a");
    let mut session = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    session
        .ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    session
        .write(&TableName::new("events"), batch_of(&[1]))
        .await
        .expect("write");
    session
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");
    session.close().await.expect("close");

    let err = session
        .write(&TableName::new("events"), batch_of(&[2]))
        .await
        .expect_err("a write after close must be refused, not silently accepted");
    assert_eq!(
        err.to_string(),
        "fatal destination error: the destination session is closed; writes after close are a \
         host defect"
    );
}

/// 037 US2 fix round 2, I1: the DECISION this round made — the lease
/// protects CONCURRENT sessions, not dead ones, so a FAILED run must
/// release it too (best-effort, from the engine's abandonment path),
/// not just a clean one. Red-proved against fix round 1's code (which
/// only closed on the strict success path): there, this test's final
/// open is refused for the lease's full TTL, since the failed run's
/// session was simply dropped, never closed.
#[tokio::test]
async fn a_run_that_fails_mid_way_still_releases_the_lease() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dest_config = local_dest(dir.path());
    let dest = destination::Shell::new(dest_config).expect("valid");
    // One batch reaches the destination (proving the session did real
    // work, not just an open-then-immediately-fail), then the source
    // fails FATALLY — the engine never retries a fatal source error,
    // so the run reports Err promptly rather than exhausting a retry
    // budget first.
    let source = MemorySource::new(vec![
        MemoryStream::new(
            rdlt_connector_sdk::spi::StreamSpec::new("events"),
            vec![MemoryBatch::new(vec![serde_json::json!({"id": 1})])],
        )
        .fatal_after(1),
    ]);
    let pipeline = "run-fails-mid-way";
    let workdir = tempfile::tempdir().expect("workdir");
    let result = Engine::new(
        EngineConfig::new(pipeline).with_workdir(workdir.path().join("wal")),
        source,
        dest,
    )
    .run()
    .await;
    assert!(
        result.is_err(),
        "the injected fatal source error must fail the run"
    );

    // A NEW connector instance (new owner) must be welcome IMMEDIATELY
    // — the failed run's session was best-effort closed on the way out,
    // not left holding the lease for a different process to wait out.
    let opened = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        open_session(dir.path(), pipeline, "load-recovery"),
    )
    .await
    .expect("must not need to wait out any part of the 300s TTL");
    opened.expect("a failed run's session releases the lease, best-effort, on the way out");
}

/// 037 US2 T7 fix round 1: the UX half of the same fix. A clean run
/// (open, write, commit, close) must not lock a NEW process's
/// immediate next run out for any part of the lease's TTL — `close`
/// releases promptly, so a fresh connector instance (a new owner) opens
/// right away.
#[tokio::test]
async fn consecutive_runs_are_never_ttl_locked_out_after_a_clean_close() {
    let dir = tempfile::tempdir().expect("tempdir");
    run_load(
        &local_dest(dir.path()),
        &PipelineId::new("consecutive-runs"),
        "load-a",
        1,
        &WriteMode::Append,
        &[1],
    )
    .await;

    // A brand-new connector instance (a new owner — the shape a new
    // `rdlt run` process takes) opens IMMEDIATELY, no TTL wait.
    let opened_immediately = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        open_session(dir.path(), "consecutive-runs", "load-b"),
    )
    .await
    .expect("must not need to wait out any part of the 300s TTL");
    opened_immediately.expect("a clean close leaves nothing for the next run to wait on");
}

/// 037 US2 T8: a stale lease composes with the WHOLE engine path, not
/// just `Lease::acquire` in isolation — `lease.rs`'s own unit tests
/// already cover the primitive. Plant a dead owner's lease doc
/// directly (older than the TTL), then drive a COMPLETE engine
/// pipeline through the file destination: it must take the lease
/// over, load every row exactly once (the row-count oracle), and
/// leave the lease doc RELEASED — absent, exactly as a clean run that
/// never needed a takeover leaves it — because `close` runs at the end
/// of this session same as any other.
#[tokio::test]
async fn a_full_engine_run_takes_over_a_stale_lease_and_publishes_exactly_once() {
    let out = tempfile::tempdir().expect("out");
    let input = tempfile::tempdir().expect("input");
    let workdir = tempfile::tempdir().expect("workdir");
    let pipeline = "takeover-full-pipeline";

    plant_lease_doc(
        out.path(),
        pipeline,
        "dead-owner-from-a-crashed-process",
        now_ms() - (LEASE_TTL_SECS + 1) * 1000,
    );
    plant(
        input.path(),
        "data/events.jsonl",
        b"{\"id\": 1}\n{\"id\": 2}\n{\"id\": 3}\n",
    );

    let src = source::Shell::new(jsonl_source(input.path(), "data/*.jsonl")).expect("valid");
    let dest_config = local_dest(out.path());
    let dest = destination::Shell::new(dest_config.clone()).expect("valid");

    Engine::new(
        EngineConfig::new(pipeline).with_workdir(workdir.path().join("wal")),
        src,
        dest,
    )
    .run()
    .await
    .expect("the run takes the stale lease over and completes");

    assert_eq!(
        destination::testhook::count_rows(&dest_config, "events").expect("count"),
        3,
        "the takeover run published every row exactly once"
    );
    assert!(
        !lease_doc_path(out.path(), pipeline).exists(),
        "close releases the taken-over lease; nothing is left holding the scope behind it"
    );
}

/// 037 US2 review-mandated pin: the documented hard-crash residual
/// (lease.rs's module doc, "037 LEASE-ABANDON RESIDUAL"). A FRESH
/// lease — stamped `now`, so indistinguishable from a live session by
/// age alone — is what a process leaves behind on a SIGKILL or a panic
/// that unwinds past even `Drop`: nothing released it, and nothing
/// proves the holder is dead rather than merely slow. A full engine
/// run against that scope must refuse, typed, naming the holder — not
/// silently proceed and corrupt the crashed process's staging, and not
/// panic either. This is the composed, whole-pipeline half of what
/// `the_fresh_foreign_refusal_message_is_pinned_exactly` already pins
/// at the `Lease::acquire` level.
#[tokio::test]
async fn a_fresh_abandoned_lease_refuses_a_full_engine_run() {
    let out = tempfile::tempdir().expect("out");
    let input = tempfile::tempdir().expect("input");
    let workdir = tempfile::tempdir().expect("workdir");
    let pipeline = "abandoned-fresh-lease";

    plant_lease_doc(out.path(), pipeline, "crashed-just-now", now_ms());
    plant(input.path(), "data/events.jsonl", b"{\"id\": 1}\n");

    let src = source::Shell::new(jsonl_source(input.path(), "data/*.jsonl")).expect("valid");
    let dest = destination::Shell::new(local_dest(out.path())).expect("valid");

    let err = Engine::new(
        EngineConfig::new(pipeline).with_workdir(workdir.path().join("wal")),
        src,
        dest,
    )
    .run()
    .await
    .expect_err("a fresh foreign lease refuses the whole run rather than proceeding");
    let text = err.to_string();
    assert!(
        text.contains("destination error"),
        "the failure surfaces as the destination error, not some other classification: {text}"
    );
    assert!(
        text.contains("holds the destination lease"),
        "the frozen lease refusal must reach the caller through the whole engine path: {text}"
    );
    assert!(
        text.contains("crashed-just-now"),
        "the refusal names the holder: {text}"
    );
}

async fn run_load(
    config: &destination::Config,
    pipeline: &PipelineId,
    load: &str,
    seq: u64,
    mode: &WriteMode,
    rows: &[i64],
) {
    let dest = destination::Shell::new(config.clone()).expect("valid");
    let load_id = LoadId::new(load);
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load_id.clone()))
        .await
        .expect("open");
    s.ensure_table(&schema_for("events"), mode)
        .await
        .expect("ensure");
    s.write(&TableName::new("events"), batch_of(rows))
        .await
        .expect("write");
    s.commit(commit_meta_for(pipeline, &load_id, seq))
        .await
        .expect("commit");
    // 037 US2 T7 fix round 1: production usage (the engine) always
    // closes a session that completed its last commit — a NEW `dest`
    // per call here mints a NEW owner, so a helper that skipped this
    // would deadlock every subsequent call against this same pipeline
    // for the lease's full TTL.
    s.close().await.expect("close");
}

/// A redelivered (load, seq) is recognised even after LATER loads have
/// run — receipts are retained for the life of the destination.
#[tokio::test]
async fn a_redelivered_commit_is_recognised_after_later_loads_have_run() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = local_dest(dir.path());
    let pipeline = PipelineId::new("later-loads");
    run_load(&config, &pipeline, "load-1", 1, &WriteMode::Append, &[1, 2]).await;
    run_load(&config, &pipeline, "load-2", 1, &WriteMode::Append, &[3]).await;
    // Redeliver load-1 seq 1: recognised, nothing republished.
    run_load(&config, &pipeline, "load-1", 1, &WriteMode::Append, &[1, 2]).await;
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        3,
        "the redelivered load republished nothing"
    );
}

/// Replace truncation runs ONCE per load, read from the receipt log: a
/// second commit of the SAME load must not delete what the first
/// commit of that load published.
#[tokio::test]
async fn replace_truncates_once_per_load_guarded_by_the_receipt_log() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = local_dest(dir.path());
    let pipeline = PipelineId::new("replace-guard");
    // A predecessor load's data.
    run_load(
        &config,
        &pipeline,
        "load-old",
        1,
        &WriteMode::Replace,
        &[9, 9, 9],
    )
    .await;

    // A new Replace load, TWO commits through one session.
    let dest = destination::Shell::new(config.clone()).expect("valid");
    let load = LoadId::new("load-new");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    s.ensure_table(&schema_for("events"), &WriteMode::Replace)
        .await
        .expect("ensure");
    s.write(&TableName::new("events"), batch_of(&[1]))
        .await
        .expect("write");
    s.commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("first commit");
    s.write(&TableName::new("events"), batch_of(&[2]))
        .await
        .expect("write");
    s.commit(commit_meta_for(&pipeline, &load, 2))
        .await
        .expect("second commit");

    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        2,
        "the predecessor is gone; BOTH of this load's commits survive"
    );
}

/// The state document lands with the receipt and reads back for its
/// OWN pipeline only — a sibling sharing the output stays invisible.
#[tokio::test]
async fn state_reads_back_scoped_to_its_own_pipeline() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = local_dest(dir.path());
    let pipeline = PipelineId::new("state-owner");
    let sibling = PipelineId::new("state-sibling");
    run_load(&config, &pipeline, "load-1", 1, &WriteMode::Append, &[1]).await;

    let dest = destination::Shell::new(config.clone()).expect("valid");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), LoadId::new("load-2")))
        .await
        .expect("open");
    let state = s.read_state(&pipeline).await.expect("read");
    assert_eq!(state.expect("the owner sees its state").pipeline, pipeline);
    let none = s.read_state(&sibling).await.expect("read");
    assert!(none.is_none(), "a sibling pipeline sees nothing");
}

/// Opening a session reclaims only ITS OWN pipeline's staging: a
/// sibling's staged parts and state survive.
#[tokio::test]
async fn open_does_not_destroy_another_pipelines_staging_or_state() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = local_dest(dir.path());
    let ours = PipelineId::new("pipeline-a");
    let theirs = PipelineId::new("pipeline-b");

    // The sibling stages a part but never commits (a live session).
    let dest = destination::Shell::new(config.clone()).expect("valid");
    let their_load = LoadId::new("their-load");
    let mut their_session = dest
        .open(OpenContext::new(theirs.clone(), their_load.clone()))
        .await
        .expect("open");
    their_session
        .ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    their_session
        .write(&TableName::new("events"), batch_of(&[7]))
        .await
        .expect("write");

    // Our session opens: the sibling's staged file must survive.
    let _our_session = dest
        .open(OpenContext::new(ours.clone(), LoadId::new("our-load")))
        .await
        .expect("open");
    their_session
        .commit(commit_meta_for(&theirs, &their_load, 1))
        .await
        .expect("the sibling's staged part is still there to publish");
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        1
    );
}

/// A future MANIFEST version refuses the same way — planted beside
/// the commit-log pin because the two gates must never drift. Review
/// round 3 caught this path unpinned, hiding a garbled spelling (a
/// run of literal spaces baked into the message).
#[tokio::test]
async fn a_future_manifest_version_refuses_upgrade_not_reset() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = local_dest(dir.path());
    let pipeline = PipelineId::new("future-manifest");
    let scope = rdlt_connector_sdk::spi::core::naming::ident_hash(pipeline.as_str(), 12);
    let file = format!("_rdlt_manifest.{scope}.json");
    std::fs::write(
        dir.path().join(&file),
        r#"{"format_version": 99, "load_id": "x", "commit_seq": 1, "names": []}"#,
    )
    .expect("plant");

    let dest = destination::Shell::new(config).expect("valid");
    let load = LoadId::new("load-1");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    s.ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    let err = s
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect_err("refused")
        .to_string();
    assert!(
        err.contains(&format!(
            "manifest `{file}` format v99 is newer than this build supports \
             (v2); upgrade rdlt instead of resetting"
        )),
        "{err}"
    );
}

/// A v1 MANIFEST — the retired partition-directory encoding — refuses
/// as PREDATING v2, never a silent reinterpretation (037 US4 D1:
/// greenfield format break, no migration). Planted beside the v99
/// (newer) and commit-log siblings so all four gates are pinned
/// together.
#[tokio::test]
async fn a_v1_manifest_version_refuses_as_predating_v2() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = local_dest(dir.path());
    let pipeline = PipelineId::new("stale-manifest");
    let scope = rdlt_connector_sdk::spi::core::naming::ident_hash(pipeline.as_str(), 12);
    let file = format!("_rdlt_manifest.{scope}.json");
    std::fs::write(
        dir.path().join(&file),
        r#"{"format_version": 1, "load_id": "x", "commit_seq": 1, "names": []}"#,
    )
    .expect("plant");

    let dest = destination::Shell::new(config).expect("valid");
    let load = LoadId::new("load-1");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    s.ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    let err = s
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect_err("refused")
        .to_string();
    assert!(
        err.contains(&format!(
            "manifest `{file}` format v1 predates this build (v2): the partition-directory \
             encoding changed; point the destination at a fresh path or delete the old output"
        )),
        "{err}"
    );
}

/// A future commit-log version refuses as an upgrade prompt with the
/// frozen spelling — never a reset.
#[tokio::test]
async fn a_future_commit_log_version_refuses_upgrade_not_reset() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = local_dest(dir.path());
    let pipeline = PipelineId::new("future-log");
    let scope = rdlt_connector_sdk::spi::core::naming::ident_hash(pipeline.as_str(), 12);
    let file = format!("_rdlt_commits.{scope}.json");
    std::fs::write(
        dir.path().join(&file),
        r#"{"format_version": 99, "receipts": []}"#,
    )
    .expect("plant");

    let dest = destination::Shell::new(config).expect("valid");
    let load = LoadId::new("load-1");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    s.ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    let err = s
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect_err("refused")
        .to_string();
    assert!(
        err.contains(&format!(
            "commit log `{file}` format v99 is newer than this build supports \
             (v2); upgrade rdlt instead of resetting"
        )),
        "{err}"
    );
}

/// A v1 commit log — the retired partition-directory encoding —
/// refuses as PREDATING v2, never a silent reinterpretation (037 US4
/// D1: greenfield format break, no migration; absent format_version
/// = v0 = also predates, per `layout::version_gate`'s own unit pin).
#[tokio::test]
async fn a_v1_commit_log_version_refuses_as_predating_v2() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = local_dest(dir.path());
    let pipeline = PipelineId::new("stale-log");
    let scope = rdlt_connector_sdk::spi::core::naming::ident_hash(pipeline.as_str(), 12);
    let file = format!("_rdlt_commits.{scope}.json");
    std::fs::write(
        dir.path().join(&file),
        r#"{"format_version": 1, "receipts": []}"#,
    )
    .expect("plant");

    let dest = destination::Shell::new(config).expect("valid");
    let load = LoadId::new("load-1");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    s.ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    let err = s
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect_err("refused")
        .to_string();
    assert!(
        err.contains(&format!(
            "commit log `{file}` format v1 predates this build (v2): the partition-directory \
             encoding changed; point the destination at a fresh path or delete the old output"
        )),
        "{err}"
    );
}

/// Merge is refused at ensure with the frozen spelling.
#[tokio::test]
async fn merge_is_refused_at_ensure() {
    let dir = tempfile::tempdir().expect("tempdir");
    let dest = destination::Shell::new(local_dest(dir.path())).expect("valid");
    let mut s = dest
        .open(OpenContext::new(
            PipelineId::new("merge-refusal"),
            LoadId::new("load-1"),
        ))
        .await
        .expect("open");
    let err = s
        .ensure_table(
            &schema_for("events"),
            &WriteMode::Merge {
                key: vec!["id".into()],
            },
        )
        .await
        .expect_err("refused")
        .to_string();
    assert!(
        err.contains("file destination does not support Merge (capabilities.merge = false)"),
        "{err}"
    );
}

/// Jsonl parts publish under the same protocol (the format matters to
/// the encoder, never to the commit).
#[tokio::test]
async fn jsonl_output_moves_through_the_same_protocol() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = local_dest(dir.path()).with_format(DestFormat::Jsonl);
    let pipeline = PipelineId::new("jsonl-out");
    run_load(
        &config,
        &pipeline,
        "load-1",
        1,
        &WriteMode::Append,
        &[1, 2, 3],
    )
    .await;
    assert!(
        dir.path().join("events/part-load-1-1-0.jsonl").is_file(),
        "the jsonl part carries the same frozen name shape"
    );
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        3
    );
}
