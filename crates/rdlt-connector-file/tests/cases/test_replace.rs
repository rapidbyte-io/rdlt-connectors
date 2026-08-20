//! Replace truncation LIVE: ownership precision against foreign
//! datasets, and the frozen local plain-parquet addendum.

use rdlt_connector_file::destination::{self, DestFormat};
use rdlt_connector_sdk::spi::core::{commit::WriteMode, id::LoadId, id::PipelineId, id::TableName};
use rdlt_connector_sdk::spi::{destination::Destination, destination::OpenContext};
use rdlt_testkit::fixtures::{batch_of, commit_meta_for, schema_for};

use super::common::{local_dest, plant};

async fn replace_load(config: &destination::Config, pipeline: &str, load: &str, rows: &[i64]) {
    let dest = destination::Shell::new(config.clone()).expect("valid");
    let pipeline = PipelineId::new(pipeline);
    let load_id = LoadId::new(load);
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load_id.clone()))
        .await
        .expect("open");
    s.ensure_table(&schema_for("events"), &WriteMode::Replace)
        .await
        .expect("ensure");
    s.write(&TableName::new("events"), batch_of(rows))
        .await
        .expect("write");
    s.commit(commit_meta_for(&pipeline, &load_id, 1))
        .await
        .expect("commit");
    // 037 US2 T7 fix round 1: a NEW `dest` per call mints a NEW owner —
    // without closing, `replace_clears_a_predecessor_of_another_format`'s
    // second call against the SAME pipeline would be refused by this
    // call's still-held lease.
    s.close().await.expect("close");
}

/// A foreign dataset under the table root — pyarrow and Spark default
/// basenames, partitioned or not — is NEVER ours to delete. (The
/// jsonl-format run keeps the frozen plain-parquet addendum out of
/// play; this is the shape rule alone.)
#[tokio::test]
async fn a_foreign_dataset_is_never_ours_to_delete() {
    let dir = tempfile::tempdir().expect("tempdir");
    plant(dir.path(), "events/part-0.parquet", b"foreign");
    plant(
        dir.path(),
        "events/part-00000-1a2b-c000.snappy.parquet",
        b"foreign",
    );
    plant(dir.path(), "events/region=eu/part-0.parquet", b"foreign");
    plant(dir.path(), "events/notes.txt", b"foreign");

    let config = local_dest(dir.path()).with_format(DestFormat::Jsonl);
    replace_load(&config, "foreign-net", "load-a", &[1, 2]).await;

    for survivor in [
        "events/part-0.parquet",
        "events/part-00000-1a2b-c000.snappy.parquet",
        "events/region=eu/part-0.parquet",
        "events/notes.txt",
    ] {
        assert!(
            dir.path().join(survivor).is_file(),
            "{survivor} must survive"
        );
    }
    assert!(dir.path().join("events/part-load-a-1-0.jsonl").is_file());
}

/// Replace clears its own predecessors even when the earlier load
/// wrote a DIFFERENT format — the shape rule reads no configuration.
#[tokio::test]
async fn replace_clears_a_predecessor_of_another_format() {
    let dir = tempfile::tempdir().expect("tempdir");
    let parquet = local_dest(dir.path());
    replace_load(&parquet, "cross-format", "load-a", &[1, 2, 3]).await;
    assert!(dir.path().join("events/part-load-a-1-0.parquet").is_file());

    let jsonl = local_dest(dir.path()).with_format(DestFormat::Jsonl);
    replace_load(&jsonl, "cross-format", "load-b", &[4]).await;
    assert!(
        !dir.path().join("events/part-load-a-1-0.parquet").exists(),
        "the parquet predecessor is cleared by a jsonl Replace"
    );
    assert_eq!(
        destination::testhook::count_rows(&jsonl, "events").expect("count"),
        1
    );
}

/// The frozen pre-015 addendum: local + parquet + unpartitioned
/// Replace also clears TOP-LEVEL `*.parquet` of any name — and ONLY
/// top-level.
#[tokio::test]
async fn the_frozen_rule_clears_top_level_parquet_of_any_name() {
    let dir = tempfile::tempdir().expect("tempdir");
    plant(dir.path(), "events/legacy-export.parquet", b"pre-015");
    plant(
        dir.path(),
        "events/archive/keep.parquet",
        b"nested is foreign",
    );

    let config = local_dest(dir.path());
    replace_load(&config, "frozen-rule", "load-a", &[1]).await;

    assert!(
        !dir.path().join("events/legacy-export.parquet").exists(),
        "top-level *.parquet is the frozen pre-015 ownership"
    );
    assert!(
        dir.path().join("events/archive/keep.parquet").is_file(),
        "nested stays foreign"
    );
}

/// Append never truncates anything.
#[tokio::test]
async fn append_never_truncates() {
    let dir = tempfile::tempdir().expect("tempdir");
    plant(dir.path(), "events/anything.parquet", b"data");
    let config = local_dest(dir.path());
    let dest = destination::Shell::new(config).expect("valid");
    let pipeline = PipelineId::new("append-net");
    let load = LoadId::new("load-a");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    s.ensure_table(&schema_for("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    s.write(&TableName::new("events"), batch_of(&[1]))
        .await
        .expect("write");
    s.commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");
    assert!(dir.path().join("events/anything.parquet").is_file());
}

/// Row counting reports OWNED shapes only: a user's own files under
/// the table directory — including an unreadable foreign parquet —
/// neither inflate nor break the count (030 review, docket S12 pin).
#[tokio::test]
async fn count_rows_ignores_foreign_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    plant(dir.path(), "events/part-0.parquet", b"not real parquet");
    plant(dir.path(), "events/notes.jsonl", b"{}\n{}\n{}\n");

    let config = local_dest(dir.path()).with_format(DestFormat::Jsonl);
    replace_load(&config, "count-filter", "load-a", &[1, 2]).await;
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        2,
        "only the owned part's rows count; foreign files are invisible"
    );
}
