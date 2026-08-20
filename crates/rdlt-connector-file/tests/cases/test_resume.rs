//! The cursor rulebook LIVE through the engine: append-growth resumes
//! reading only the delta, and the same-size rewrite tripwire refuses
//! rather than trusting a stale offset.

use rdlt_connector_file::{destination, source};
use rdlt_engine::config::Config as EngineConfig;
use rdlt_engine::engine::Engine;

use super::common::{jsonl_source, local_dest, plant};

async fn run(
    input: &std::path::Path,
    config: &destination::Config,
    workdir: &std::path::Path,
) -> Result<(), String> {
    let src = source::Shell::new(jsonl_source(input, "data/*.jsonl")).expect("valid");
    let dest = destination::Shell::new(config.clone()).expect("valid");
    Engine::new(
        EngineConfig::new("file-resume").with_workdir(workdir.join("wal")),
        src,
        dest,
    )
    .run()
    .await
    .map(|_| ())
    .map_err(|e| e.to_string())
}

/// Appended growth resumes from the recorded offset: the second run
/// reads ONLY the delta (proven by totals — a whole re-read would
/// double the early rows).
#[tokio::test]
async fn appended_growth_resumes_reading_only_the_delta() {
    let input = tempfile::tempdir().expect("input");
    let out = tempfile::tempdir().expect("out");
    let workdir = tempfile::tempdir().expect("workdir");
    let config = local_dest(out.path());

    plant(
        input.path(),
        "data/events.jsonl",
        b"{\"id\": 1}\n{\"id\": 2}\n",
    );
    run(input.path(), &config, workdir.path())
        .await
        .expect("first run");
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        2
    );

    // Append one record; a stale-offset bug would re-read all three.
    let mut existing = std::fs::read(input.path().join("data/events.jsonl")).expect("read");
    existing.extend_from_slice(b"{\"id\": 3}\n");
    plant(input.path(), "data/events.jsonl", &existing);
    run(input.path(), &config, workdir.path())
        .await
        .expect("second run");
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        3,
        "only the appended record landed"
    );
}

/// A same-size rewrite behind the recorded offset trips the tail-hash
/// verification: the run REFUSES with the frozen framing instead of
/// emitting stale-offset garbage.
#[tokio::test]
async fn a_same_size_rewrite_trips_the_tail_hash() {
    let input = tempfile::tempdir().expect("input");
    let out = tempfile::tempdir().expect("out");
    let workdir = tempfile::tempdir().expect("workdir");
    let config = local_dest(out.path());

    plant(
        input.path(),
        "data/events.jsonl",
        b"{\"id\": 1}\n{\"id\": 2}\n",
    );
    run(input.path(), &config, workdir.path())
        .await
        .expect("first run");

    // Same LENGTH, different content, then grown — the tail window no
    // longer hashes to what the cursor remembered. The mtime moves
    // explicitly so the tripwire cannot depend on clock granularity.
    let rewritten = b"{\"id\": 8}\n{\"id\": 9}\n{\"id\": 3}\n";
    plant(input.path(), "data/events.jsonl", rewritten);
    let file = std::fs::File::options()
        .write(true)
        .open(input.path().join("data/events.jsonl"))
        .expect("open");
    file.set_times(
        std::fs::FileTimes::new()
            .set_modified(std::time::SystemTime::now() + std::time::Duration::from_secs(30)),
    )
    .expect("set mtime");

    let err = run(input.path(), &config, workdir.path())
        .await
        .expect_err("refused");
    assert!(
        err.contains("was rewritten before the resume offset") && err.contains("data/events.jsonl"),
        "{err}"
    );
}

/// A shrunken file refuses with the frozen framing.
#[tokio::test]
async fn a_shrunken_file_refuses() {
    let input = tempfile::tempdir().expect("input");
    let out = tempfile::tempdir().expect("out");
    let workdir = tempfile::tempdir().expect("workdir");
    let config = local_dest(out.path());

    plant(
        input.path(),
        "data/events.jsonl",
        b"{\"id\": 1}\n{\"id\": 2}\n",
    );
    run(input.path(), &config, workdir.path())
        .await
        .expect("first run");

    plant(input.path(), "data/events.jsonl", b"{\"id\": 1}\n");
    let err = run(input.path(), &config, workdir.path())
        .await
        .expect_err("refused");
    assert!(
        err.contains("shrank or was rewritten") && err.contains("data/events.jsonl"),
        "{err}"
    );
}

/// A PRE-UPGRADE cursor (no tail_hash) resumes unverified — and the
/// tail it records must cover the full window, or the NEXT resume
/// falsely refuses a healthy append as "rewritten" (030 review: the
/// tail was seeded only on the verified path, so every unverified
/// resume under 4 KiB of growth poisoned the record).
#[tokio::test]
async fn an_unverified_resume_records_a_tail_the_next_resume_can_verify() {
    use rdlt_connector_sdk::spi::core::schema::ident_hash;

    let input = tempfile::tempdir().expect("input");
    let out = tempfile::tempdir().expect("out");
    let workdir = tempfile::tempdir().expect("workdir");
    let config = local_dest(out.path());

    plant(
        input.path(),
        "data/events.jsonl",
        b"{\"id\": 1}\n{\"id\": 2}\n",
    );
    run(input.path(), &config, workdir.path())
        .await
        .expect("first run");

    // Strip the integrity keys from the persisted state — the exact
    // shape a pre-tail-hash cursor leaves behind.
    let scope = ident_hash("file-resume", 12);
    let state_path = out.path().join(format!("_rdlt_state.{scope}.json"));
    let mut state: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&state_path).expect("state")).expect("json");
    let files = state["cursors"]["events"]["files"]
        .as_object_mut()
        .expect("files map");
    for (_, record) in files.iter_mut() {
        record.as_object_mut().expect("record").remove("tail_hash");
    }
    std::fs::write(
        &state_path,
        serde_json::to_vec_pretty(&state).expect("bytes"),
    )
    .expect("rewrite state");

    // Grow by less than the tail window: resumes unverified, records.
    let mut bytes = std::fs::read(input.path().join("data/events.jsonl")).expect("read");
    bytes.extend_from_slice(b"{\"id\": 3}\n");
    plant(input.path(), "data/events.jsonl", &bytes);
    run(input.path(), &config, workdir.path())
        .await
        .expect("unverified resume");

    // Grow again: THIS resume verifies against what the last recorded.
    let mut bytes = std::fs::read(input.path().join("data/events.jsonl")).expect("read");
    bytes.extend_from_slice(b"{\"id\": 4}\n");
    plant(input.path(), "data/events.jsonl", &bytes);
    run(input.path(), &config, workdir.path())
        .await
        .expect("a healthy append must verify, not refuse");
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        4,
        "each run read exactly its delta"
    );
}
