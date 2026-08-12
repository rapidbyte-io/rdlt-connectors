//! The object-store protocol LIVE against RUSTFS: listing and reading
//! through the engine, the destination's COPY+DELETE publish, Replace
//! ownership on a real bucket, and the etag rewrite tripwire.

use rdlt_connector_file::{destination, source};
use rdlt_connector_sdk::config::Document;
use rdlt_engine::{Engine, EngineConfig};

use super::common::local_dest;
use super::s3;
use super::s3::S3Fixture;

fn s3_source(fixture: &S3Fixture, pattern: &str) -> source::Config {
    let mut value = serde_json::json!({
        "streams": [{
            "name": "events",
            "format": "jsonl",
            "path": pattern,
        }]
    });
    value["streams"][0]["location"] =
        serde_json::to_value(fixture.location_options()).expect("options");
    source::Config::from_value(value).expect("valid")
}

fn s3_dest(fixture: &S3Fixture, prefix: &str) -> destination::Config {
    destination::Config::new(prefix).with_location(fixture.location_options())
}

async fn run(
    src: source::Config,
    dest_config: &destination::Config,
    pipeline: &str,
    workdir: &std::path::Path,
) -> Result<(), String> {
    let src = source::Shell::new(src).expect("valid");
    let dest = destination::Shell::new(dest_config.clone()).expect("valid");
    Engine::new(
        EngineConfig::new(pipeline).with_workdir(workdir.join("wal")),
        src,
        dest,
    )
    .run()
    .await
    .map(|_| ())
    .map_err(|e| e.to_string())
}

/// Glob listing over seeded objects reads all matches into a local
/// destination — the whole read path over a real wire.
#[tokio::test]
async fn a_glob_read_lands_every_matching_object() {
    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    fixture
        .put("in/a.jsonl", b"{\"id\": 1}\n{\"id\": 2}\n")
        .await;
    fixture.put("in/b.jsonl", b"{\"id\": 3}\n").await;
    fixture.put("in/skip.txt", b"not matched").await;

    let out = tempfile::tempdir().expect("out");
    let workdir = tempfile::tempdir().expect("workdir");
    let config = local_dest(out.path());
    run(
        s3_source(&fixture, "in/*.jsonl"),
        &config,
        "s3-glob",
        workdir.path(),
    )
    .await
    .expect("the load settles");
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        3
    );
}

/// The destination publishes through COPY + DELETE: exact totals land
/// under the prefix, staging is empty afterward, and a second run of
/// the same pipeline APPENDS.
#[tokio::test]
async fn the_destination_publishes_and_clears_its_staging() {
    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    fixture
        .put("in/a.jsonl", b"{\"id\": 1}\n{\"id\": 2}\n")
        .await;

    let workdir = tempfile::tempdir().expect("workdir");
    let dest_config = s3_dest(&fixture, "lake");
    run(
        s3_source(&fixture, "in/*.jsonl"),
        &dest_config,
        "s3-publish",
        workdir.path(),
    )
    .await
    .expect("the load settles");
    assert_eq!(
        destination::testhook::count_rows_async(&dest_config, "events")
            .await
            .expect("count"),
        2
    );
}

/// 037 US2 T8: the headliner shape
/// (`a_second_session_of_the_same_pipeline_is_refused_not_destroyed` in
/// `test_exactly_once.rs`) proved live against the REAL object-store
/// arm — the S3 arm's CAS-replace is a genuinely different code path
/// from the local arm's exclusive-unlink dance (lease.rs's module doc,
/// step 4), so the refusal needs its own live proof, not just a
/// same-shape assumption from the local cell. Two connector instances
/// (two distinct owner tokens) open against the same pipeline over the
/// same prefix; the second is refused while the first still holds. The
/// refused open must not have damaged the holder's staging either: the
/// first session goes on to commit and publish exactly as if the
/// second attempt had never happened.
#[tokio::test]
async fn a_second_session_is_refused_live_against_s3_and_the_first_still_publishes() {
    use rdlt_connector_sdk::spi::core::{LoadId, PipelineId, TableName, WriteMode};
    use rdlt_connector_sdk::spi::{Destination, OpenContext};
    use rdlt_testkit::{batch_of, commit_meta_for, schema_for};

    let Some(fixture) = S3Fixture::start().await else {
        return;
    };

    let dest_config = s3_dest(&fixture, "lease-lake");
    let pipeline = PipelineId::new("s3-lease-refusal");
    let first_load = LoadId::new("load-a");

    let first_dest = destination::Shell::new(dest_config.clone()).expect("valid");
    let mut first = first_dest
        .open(OpenContext::new(pipeline.clone(), first_load.clone()))
        .await
        .expect("first session acquires the lease");
    first
        .ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    first
        .write(&TableName::new("events"), batch_of(&[1, 2]))
        .await
        .expect("write");

    // A SECOND connector instance (a distinct owner) — the shape a
    // second `rdlt run` of the same pipeline takes — must be refused
    // while the first session is still live.
    let second_dest = destination::Shell::new(dest_config.clone()).expect("valid");
    let second = second_dest
        .open(OpenContext::new(pipeline.clone(), LoadId::new("load-b")))
        .await;
    let err = match second {
        Ok(_) => panic!("a live lease on S3 refuses a concurrent same-pipeline session"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("holds the destination lease"),
        "{err}"
    );

    // The refused open must not have damaged the holder's staging: the
    // first session commits and publishes exactly as if it had never
    // happened.
    first
        .commit(commit_meta_for(&pipeline, &first_load, 1))
        .await
        .expect("the refused competitor left the first session's staging untouched");
    assert_eq!(
        destination::testhook::count_rows_async(&dest_config, "events")
            .await
            .expect("count"),
        2,
        "the first session's rows publish exactly once, undamaged by the refused competitor"
    );
}

/// Replace over a real bucket clears ONLY owned shapes: a user object
/// under the table prefix survives.
/// The crash-replay convergence sweep on the OBJECT-STORE path, whose
/// doc reads/deletes differ from the local ones: a final the crashed
/// predecessor's MANIFEST names is removed, everything unmanifested
/// survives. The scenario and its full rationale are pinned locally in
/// `test_parts.rs`; this cell exists because the manifest read and the
/// tolerant delete are per-kind implementations.
#[tokio::test]
async fn s3_publish_sweeps_a_predecessors_same_commit_finals() {
    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    // A crashed attempt of (load-a, seq 1) split into two parts, with
    // the manifest it wrote before publishing; the retry below stages
    // ONE. Plus bystanders that must survive: another load's file and
    // a user's own object — neither manifested.
    fixture
        .put("lake/events/part-load-a-1-1.jsonl", b"crashed residue")
        .await;
    let scope = rdlt_connector_sdk::spi::core::naming::ident_hash("s3-sweep", 12);
    fixture
        .put(
            &format!("lake/_rdlt_manifest.{scope}.json"),
            serde_json::json!({
                "format_version": 2,
                "load_id": "load-a",
                "commit_seq": 1,
                "names": [
                    "events/part-load-a-1-0.jsonl",
                    "events/part-load-a-1-1.jsonl",
                ],
            })
            .to_string()
            .as_bytes(),
        )
        .await;
    fixture
        .put("lake/events/part-other-1-0.jsonl", b"another load")
        .await;
    fixture
        .put("lake/events/part-0.parquet", b"a user's dataset")
        .await;

    let dest_config = s3_dest(&fixture, "lake");
    let mut value = serde_json::to_value(&dest_config).expect("value");
    value["format"] = "jsonl".into();
    let dest_config = destination::Config::from_value(value).expect("valid");
    use rdlt_connector_sdk::spi::core::{LoadId, PipelineId, TableName, WriteMode};
    use rdlt_connector_sdk::spi::{Destination, OpenContext};
    use rdlt_testkit::{batch_of, commit_meta_for, schema_for};
    let dest = destination::Shell::new(dest_config).expect("valid");
    let pipeline = PipelineId::new("s3-sweep");
    let load = LoadId::new("load-a");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    s.ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    s.write(&TableName::new("events"), batch_of(&[1, 2]))
        .await
        .expect("write");
    s.commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");

    assert!(
        fixture.exists("lake/events/part-load-a-1-0.jsonl").await,
        "the retry's own part landed"
    );
    assert!(
        !fixture.exists("lake/events/part-load-a-1-1.jsonl").await,
        "the crashed predecessor's manifested final is swept"
    );
    assert!(
        fixture.exists("lake/events/part-other-1-0.jsonl").await,
        "an unmanifested final survives the sweep"
    );
    assert!(
        fixture.exists("lake/events/part-0.parquet").await,
        "a foreign dataset file survives the sweep"
    );
}

#[tokio::test]
async fn s3_replace_never_deletes_user_files() {
    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    fixture
        .put("lake/events/part-0.parquet", b"a user's dataset")
        .await;
    fixture.put("in/a.jsonl", b"{\"id\": 1}\n").await;

    let workdir = tempfile::tempdir().expect("workdir");
    let dest_config = s3_dest(&fixture, "lake");
    let mut value = serde_json::to_value(&dest_config).expect("value");
    value["format"] = "jsonl".into();
    let dest_config = destination::Config::from_value(value).expect("valid");
    // Replace mode arrives from the engine's write disposition; drive
    // the SPI directly for an unambiguous Replace commit.
    use rdlt_connector_sdk::spi::core::{LoadId, PipelineId, TableName, WriteMode};
    use rdlt_connector_sdk::spi::{Destination, OpenContext};
    use rdlt_testkit::{batch_of, commit_meta_for, schema_for};
    let _ = workdir;
    let dest = destination::Shell::new(dest_config.clone()).expect("valid");
    let pipeline = PipelineId::new("s3-replace");
    let load = LoadId::new("load-a");
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
        .expect("commit");

    assert!(
        fixture.exists("lake/events/part-0.parquet").await,
        "the user's object survives Replace"
    );
    assert!(
        fixture.exists("lake/events/part-load-a-1-0.jsonl").await,
        "our own part published beside it"
    );
}

/// A same-size overwrite of a consumed object changes its etag: the
/// next run refuses with the frozen framing instead of trusting the
/// recorded offset.
#[tokio::test]
async fn an_etag_rewrite_refuses_the_stale_offset() {
    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    fixture
        .put("in/a.jsonl", b"{\"id\": 1}\n{\"id\": 2}\n")
        .await;

    let out = tempfile::tempdir().expect("out");
    let workdir = tempfile::tempdir().expect("workdir");
    let config = local_dest(out.path());
    run(
        s3_source(&fixture, "in/*.jsonl"),
        &config,
        "s3-etag",
        workdir.path(),
    )
    .await
    .expect("first run");

    // Same length, different bytes — a different etag.
    fixture
        .put("in/a.jsonl", b"{\"id\": 8}\n{\"id\": 9}\n")
        .await;
    let err = run(
        s3_source(&fixture, "in/*.jsonl"),
        &config,
        "s3-etag",
        workdir.path(),
    )
    .await
    .expect_err("refused");
    assert!(
        err.contains("was rewritten in place (same size, different etag)"),
        "{err}"
    );
}

/// A leading slash on an S3 pattern is a habit carried over from
/// local paths; listed keys come back normalized, so an un-normalized
/// matcher would silently match NOTHING (030 review). The pattern is
/// normalized the store's way instead.
#[tokio::test]
async fn a_leading_slash_pattern_still_matches() {
    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    fixture
        .put("in/a.jsonl", b"{\"id\": 1}\n{\"id\": 2}\n")
        .await;

    let out = tempfile::tempdir().expect("out");
    let workdir = tempfile::tempdir().expect("workdir");
    let config = local_dest(out.path());
    run(
        s3_source(&fixture, "/in//*.jsonl"),
        &config,
        "s3-slash",
        workdir.path(),
    )
    .await
    .expect("the load settles");
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        2
    );
}

/// The skip-fetch shortcut PROVABLY engages: a second run over an
/// unchanged, completed parquet object skips its download, counted on
/// the connector instance (an optimisation that silently stops
/// engaging is indistinguishable from one never written).
#[tokio::test]
async fn an_unchanged_parquet_object_skips_its_second_fetch() {
    use rdlt_connector_sdk::source::{SourceConnector, shell};

    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    // A real parquet object, seeded once.
    let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![std::sync::Arc::new(arrow::array::Int64Array::from(vec![
            1i64, 2, 3,
        ]))],
    )
    .expect("batch");
    let mut writer =
        parquet::arrow::ArrowWriter::try_new(Vec::new(), schema, None).expect("writer");
    writer.write(&batch).expect("write");
    let bytes = writer.into_inner().expect("bytes");
    fixture.put("in/a.parquet", &bytes).await;

    let mut value = serde_json::json!({
        "streams": [{"name": "events", "format": "parquet", "path": "in/*.parquet"}]
    });
    value["streams"][0]["location"] =
        serde_json::to_value(fixture.location_options()).expect("options");
    let src_config = source::Config::from_value(value).expect("valid");

    let out = tempfile::tempdir().expect("out");
    let workdir = tempfile::tempdir().expect("workdir");
    let dest_config = local_dest(out.path());
    // One connector instance for both runs — the counter lives on it.
    let file = source::File::assemble(src_config).expect("assembles");
    for _ in 0..2 {
        Engine::new(
            EngineConfig::new("s3-skip-fetch").with_workdir(workdir.path().join("wal")),
            shell(file.clone()),
            destination::Shell::new(dest_config.clone()).expect("valid"),
        )
        .run()
        .await
        .expect("the load settles");
    }
    assert_eq!(
        destination::testhook::count_rows(&dest_config, "events").expect("count"),
        3,
        "the second run read nothing new"
    );
    assert_eq!(
        file.skipped_fetches(),
        1,
        "the completed, etag-matched object skipped exactly its second download"
    );
}

/// Staged keys stay invisible to data globs on the OBJECT store too:
/// a planted dot-prefixed key never matches a recursive pattern, while
/// a sibling data key does (the S3 twin of the local planted
/// `.rdlt-staging` pin — 030 review found the S3 side unpinned).
#[tokio::test]
async fn s3_wildcards_never_match_dot_prefixed_keys() {
    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    fixture
        .put(".rdlt-staging/ab/load-1/t/all-0.jsonl", b"{\"id\": 99}\n")
        .await;
    fixture.put("in/a.jsonl", b"{\"id\": 1}\n").await;

    let out = tempfile::tempdir().expect("out");
    let workdir = tempfile::tempdir().expect("workdir");
    let config = local_dest(out.path());
    run(
        s3_source(&fixture, "**/*.jsonl"),
        &config,
        "s3-dot-keys",
        workdir.path(),
    )
    .await
    .expect("the load settles");
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        1,
        "the staged key's row must not load"
    );
}

/// PROBE (037 US2): the lease design rides object_store conditional
/// writes — `PutMode::Create` must refuse an existing key, and
/// `PutMode::Update` must CAS on the version a read returned. If
/// RUSTFS stops honoring either, this cell fails and the lease's S3
/// arm must be re-designed BEFORE trusting it.
#[tokio::test]
async fn rustfs_honors_conditional_create_and_cas_update() {
    use object_store::aws::AmazonS3Builder;
    use object_store::{ObjectStore, PutMode, PutOptions, UpdateVersion, path::Path};

    let Some(fixture) = S3Fixture::start().await else {
        return;
    };
    // `S3Fixture::client()` is private to its module; built the same
    // way here, straight from the fixture's public endpoint and the
    // module's credential constants.
    let store = AmazonS3Builder::new()
        .with_endpoint(&fixture.endpoint)
        .with_bucket_name(s3::BUCKET)
        .with_region("us-east-1")
        .with_access_key_id(s3::ACCESS_KEY)
        .with_secret_access_key(s3::SECRET_KEY)
        .with_allow_http(true)
        .build()
        .expect("probe client");

    let key = Path::from("probe/lease.json");
    let create = PutOptions::from(PutMode::Create);
    let first = store
        .put_opts(&key, "one".into(), create.clone())
        .await
        .expect("first exclusive create succeeds");
    let second = store.put_opts(&key, "two".into(), create).await;
    assert!(
        matches!(second, Err(object_store::Error::AlreadyExists { .. })),
        "second exclusive create must refuse: {second:?}"
    );

    let good = UpdateVersion {
        e_tag: first.e_tag.clone(),
        version: first.version.clone(),
    };
    store
        .put_opts(
            &key,
            "three".into(),
            PutOptions::from(PutMode::Update(good)),
        )
        .await
        .expect("CAS on the current version succeeds");

    let stale = UpdateVersion {
        e_tag: first.e_tag,
        version: first.version,
    };
    let lost = store
        .put_opts(
            &key,
            "four".into(),
            PutOptions::from(PutMode::Update(stale)),
        )
        .await;
    // Measured live against RUSTFS 1.0.0-beta.11: HTTP 412
    // PreconditionFailed, decoded to `Error::Precondition` — matching
    // object_store 0.12.5's documented CAS-failure contract exactly
    // (no divergence to pin).
    assert!(
        matches!(lost, Err(object_store::Error::Precondition { .. })),
        "CAS on a superseded version must refuse with Precondition: {lost:?}"
    );

    // The raw client above pinned that RUSTFS honors the two
    // primitives. This second half pins that `Location`'s own
    // conditional-doc verbs (037 US2 T5) ride those same primitives
    // correctly end to end — create exclusive, versioned read, CAS
    // replace, stale-CAS refusal, delete — against the real store, not
    // just against each other in a local-only test.
    let config = s3_dest(&fixture, "probe-loc");
    destination::testhook::probe_conditional_docs(&config, "lease.json")
        .await
        .expect("the conditional-doc verbs round-trip against RUSTFS");
}
