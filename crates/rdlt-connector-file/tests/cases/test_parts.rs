//! Output part sizing (034): how many writes land in one file, when a
//! part rolls, and the two things that close one regardless of size.
//!
//! The distinction this suite exists to hold: `batch_policy` bounds
//! the ARROW footprint the engine accumulates, and `parts` bounds the
//! ENCODED bytes the destination writes. Only the second answers "I
//! want ~128 MB files", which is why this is measured on the files
//! themselves rather than on row counts.

use rdlt_connector_file::destination::parts;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use rdlt_connector_file::destination;
use rdlt_connector_sdk::spi::core::types::LogicalType;
use rdlt_connector_sdk::spi::core::{
    commit::WriteMode, id::LoadId, id::PipelineId, id::TableName, schema::Column,
    schema::ColumnType, schema::Provenance, schema::TableSchema,
};
use rdlt_connector_sdk::spi::{
    arrow::RecordBatch, destination::Destination, destination::LoadSession,
    destination::OpenContext,
};
use rdlt_testkit::fixtures::commit_meta_for;

use super::common::local_dest;

fn schema_of(table: &str, columns: &[&str]) -> TableSchema {
    TableSchema {
        table: TableName::new(table),
        parent: None,
        columns: columns
            .iter()
            .map(|name| Column {
                name: (*name).to_owned(),
                column_type: ColumnType::scalar(if *name == "id" {
                    LogicalType::Int64
                } else {
                    LogicalType::Utf8
                }),
                nullable: true,
                provenance: Provenance::Inferred,
            })
            .collect(),
    }
}

/// A batch of `rows` rows whose payload is DISTINCT per row and per
/// call — repeated identical values compress to nothing and would make
/// a size threshold untestable.
fn batch_of(offset: i64, rows: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("payload", DataType::Utf8, true),
    ]));
    let ids: Vec<i64> = (offset..offset + rows).collect();
    let payload: Vec<String> = ids
        .iter()
        .map(|i| format!("row-{i}-{}", "x".repeat(64)))
        .collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(payload)),
        ],
    )
    .expect("batch builds")
}

/// The published parquet files for one table, with their sizes.
fn parts_in(dir: &Path, table: &str) -> Vec<(String, u64)> {
    let mut found: Vec<(String, u64)> = std::fs::read_dir(dir.join(table))
        .expect("table directory exists")
        .map(|entry| entry.expect("entry"))
        .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "parquet"))
        .map(|entry| {
            (
                entry.file_name().to_string_lossy().into_owned(),
                entry.metadata().expect("metadata").len(),
            )
        })
        .collect();
    found.sort();
    found
}

async fn session_over(
    config: destination::Config,
    pipeline: &PipelineId,
    load: &LoadId,
) -> Box<dyn LoadSession> {
    let dest = destination::Shell::new(config).expect("valid");
    let mut session = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    session
        .ensure_table(&schema_of("events", &["id", "payload"]), &WriteMode::Append)
        .await
        .expect("ensure");
    session
}

/// THE DEFAULT. Twenty small writes make ONE file, not twenty — which
/// is the whole point: a source that pages a few hundred rows should
/// not dictate the shape of the data lake it lands in.
#[tokio::test]
async fn the_default_coalesces_many_small_writes_into_one_part() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pipeline = PipelineId::new("coalesce");
    let load = LoadId::new("load-a");
    let mut session = session_over(local_dest(dir.path()), &pipeline, &load).await;

    for page in 0..20 {
        session
            .write(&TableName::new("events"), batch_of(page * 100, 100))
            .await
            .expect("write");
    }
    session
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");

    let parts = parts_in(dir.path(), "events");
    assert_eq!(
        parts.len(),
        1,
        "the 128 MiB default must not roll on 2,000 small rows: {parts:?}"
    );
}

/// A reached target rolls, and the parts land AT it — measured on the
/// files, since the encoded size is the thing being bounded.
///
/// The band is deliberate, not slack. `bytes_written` is exact for
/// flushed row groups, but `in_progress_size` is the library's
/// ANTICIPATED size for pages not yet flushed, so a part can close
/// slightly under its target. MEASURED at a 4 KiB target with nothing
/// flushed — the worst case, since every byte is anticipated: parts of
/// 3,616-3,845 bytes, i.e. 88-94% of target. The error is bounded by
/// one page of unflushed data per column, so it shrinks toward nothing
/// as the target grows; at 128 MiB it is under a percent.
#[tokio::test]
async fn a_reached_target_rolls_and_every_part_meets_it() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pipeline = PipelineId::new("rolling");
    let load = LoadId::new("load-a");
    let config = local_dest(dir.path()).with_parts(parts::Options {
        target_bytes: Some(4_096),
        ..parts::Options::default()
    });
    let mut session = session_over(config, &pipeline, &load).await;

    for page in 0..8 {
        session
            .write(&TableName::new("events"), batch_of(page * 200, 200))
            .await
            .expect("write");
    }
    session
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");

    let parts = parts_in(dir.path(), "events");
    assert!(
        parts.len() > 1,
        "a 4 KiB target must roll across 1,600 padded rows: {parts:?}"
    );
    // EVERY part sits in the band here — each 200-row write alone
    // crosses the 4 KiB target, so even the final part was closed by
    // the roll, not by the commit.
    for (name, size) in &parts {
        assert!(
            (3_200..8_192).contains(size),
            "{name} is {size} bytes against a 4,096-byte target — outside the band the \
             anticipated-size estimate can explain"
        );
    }
}

/// D4: no part spans a commit. Two commits over identical writes give
/// two files even though neither came near the target, because the
/// publish protocol moves whole files and an open part has none.
#[tokio::test]
async fn a_commit_closes_the_open_part() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pipeline = PipelineId::new("commit-closes");
    let load = LoadId::new("load-a");
    let mut session = session_over(local_dest(dir.path()), &pipeline, &load).await;

    for commit_seq in 1..=2 {
        session
            .write(&TableName::new("events"), batch_of(commit_seq * 100, 100))
            .await
            .expect("write");
        session
            .commit(commit_meta_for(&pipeline, &load, commit_seq as u64))
            .await
            .expect("commit");
    }

    let parts = parts_in(dir.path(), "events");
    assert_eq!(
        parts.len(),
        2,
        "each commit closes its own part regardless of size: {parts:?}"
    );
}

/// D4: a schema change rolls, because a parquet file carries ONE
/// schema. Without this the second write would be refused by the
/// writer rather than given its own file.
#[tokio::test]
async fn a_widened_schema_rolls_the_open_part() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pipeline = PipelineId::new("widen");
    let load = LoadId::new("load-a");
    let mut session = session_over(local_dest(dir.path()), &pipeline, &load).await;

    session
        .write(&TableName::new("events"), batch_of(0, 100))
        .await
        .expect("write");

    // The same rows plus a column: a different parquet schema.
    let widened = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int64Array::from(vec![1_i64])) as _),
        ("payload", Arc::new(StringArray::from(vec![Some("p")])) as _),
        ("extra", Arc::new(StringArray::from(vec![Some("e")])) as _),
    ])
    .expect("batch");
    session
        .ensure_table(
            &schema_of("events", &["id", "payload", "extra"]),
            &WriteMode::Append,
        )
        .await
        .expect("ensure");
    session
        .write(&TableName::new("events"), widened)
        .await
        .expect("write");
    session
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");

    let parts = parts_in(dir.path(), "events");
    assert_eq!(
        parts.len(),
        2,
        "a parquet file holds one schema, so a widened batch starts a part: {parts:?}"
    );
}

/// The SAFETY VALVE: a partitioned destination holds one open part
/// per partition, so without a ceiling the footprint is
/// `partitions × target_bytes`. Under a small ceiling the parts close
/// early — undersized, deliberately, because the alternative is an
/// unbounded heap.
#[tokio::test]
async fn the_memory_ceiling_closes_parts_before_their_target() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pipeline = PipelineId::new("budget");
    let load = LoadId::new("load-a");
    // A target no part will reach, under a ceiling every part will.
    let config = local_dest(dir.path())
        .with_partition_by("payload")
        .with_parts(parts::Options {
            target_bytes: Some(64 * 1024),
            roll_after_seconds: None,
            max_open_bytes: Some(64 * 1024),
        });
    let dest = destination::Shell::new(config).expect("valid");
    let mut session = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    session
        .ensure_table(&schema_of("events", &["id", "payload"]), &WriteMode::Append)
        .await
        .expect("ensure");

    // Every row its own partition: 400 parts open at once if nothing
    // stops them.
    for page in 0..4 {
        session
            .write(&TableName::new("events"), batch_of(page * 100, 100))
            .await
            .expect("write");
    }
    session
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");

    let partitions: Vec<_> = std::fs::read_dir(dir.path().join("events"))
        .expect("table directory")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.is_dir())
        .collect();
    assert_eq!(partitions.len(), 400, "one partition per distinct payload");
    // The ceiling did the closing, so the parts are far under target.
    for partition in &partitions {
        let written: u64 = std::fs::read_dir(partition)
            .expect("partition directory")
            .map(|e| e.expect("entry").metadata().expect("metadata").len())
            .sum();
        assert!(
            written > 0 && written < 64 * 1024,
            "{partition:?} holds {written} bytes — the ceiling closes parts early"
        );
    }
}

/// The crash-replay CONVERGENCE SWEEP, pinned with a planted fixture.
///
/// The scenario it reproduces: an attempt of this very commit crashed
/// AFTER publishing (no receipt landed), and — because
/// `roll_after_seconds` makes part boundaries a function of wall clock
/// — the retry splits the same rows into FEWER parts. Without the
/// sweep, the crashed attempt's higher-index finals survive beside the
/// retry's files and hold the same rows again: 030's part-index
/// defect (6 rows where 4 loaded), reopened through a new mechanism.
///
/// Planted rather than crash-injected: a crashed attempt's residue is
/// fully characterised by the files it left AND the manifest it wrote
/// before publishing them, so the fixture IS the crash, with nothing
/// probabilistic about it.
#[tokio::test]
async fn a_replayed_commit_sweeps_a_predecessors_differently_split_finals() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pipeline = PipelineId::new("sweep");
    let load = LoadId::new("load-a");

    // The crashed attempt: same load, same seq 1, split into three
    // parts — indices 0..3 — and the manifest it wrote FIRST.
    let events = dir.path().join("events");
    std::fs::create_dir_all(&events).expect("table dir");
    for index in 0..3 {
        std::fs::write(
            events.join(format!("part-load-a-1-{index}.parquet")),
            b"crashed attempt residue",
        )
        .expect("plant");
    }
    let scope = rdlt_connector_sdk::spi::core::schema::ident_hash(pipeline.as_str(), 12);
    std::fs::write(
        dir.path().join(format!("_rdlt_manifest.{scope}.json")),
        serde_json::json!({
            "format_version": 2,
            "load_id": "load-a",
            "commit_seq": 1,
            "names": [
                "events/part-load-a-1-0.parquet",
                "events/part-load-a-1-1.parquet",
                "events/part-load-a-1-2.parquet",
            ],
        })
        .to_string(),
    )
    .expect("plant manifest");
    // Bystanders that must SURVIVE: another load's final, another
    // seq's final, and a foreign dataset file.
    for name in [
        "part-other-load-1-0.parquet",
        "part-load-a-2-0.parquet",
        "part-00000-1a2b3c4d-c000.snappy.parquet",
    ] {
        std::fs::write(events.join(name), b"bystander").expect("plant");
    }

    // The retry: everything in ONE part (the default target), same
    // load and commit seq.
    let mut session = session_over(local_dest(dir.path()), &pipeline, &load).await;
    session
        .write(&TableName::new("events"), batch_of(0, 300))
        .await
        .expect("write");
    session
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");

    let names: Vec<String> = std::fs::read_dir(&events)
        .expect("table dir")
        .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        names.contains(&"part-load-a-1-0.parquet".to_owned()),
        "the retry's own part landed: {names:?}"
    );
    for stale in ["part-load-a-1-1.parquet", "part-load-a-1-2.parquet"] {
        assert!(
            !names.iter().any(|n| n == stale),
            "{stale}: a crashed predecessor's manifested final must be swept: {names:?}"
        );
    }
    for survivor in [
        "part-other-load-1-0.parquet",
        "part-load-a-2-0.parquet",
        "part-00000-1a2b3c4d-c000.snappy.parquet",
    ] {
        assert!(
            names.iter().any(|n| n == survivor),
            "{survivor}: the sweep deletes only what the manifest names: {names:?}"
        );
    }
    // Index 0 was OVERWRITTEN by the retry, not left as residue.
    let kept = std::fs::read(events.join("part-load-a-1-0.parquet")).expect("read");
    assert_ne!(
        kept, b"crashed attempt residue",
        "index 0 is the retry's file"
    );
}

/// ROUND 2's regression pin — the reason the sweep is manifest-based
/// and not listing-based. Final names carry no pipeline marker and
/// load ids are unique only WITHIN a pipeline, so another pipeline
/// sharing this output table can legitimately own files whose names
/// parse as OUR (load, seq). A listing sweep deleted them; the
/// manifest sweep cannot, because our manifest never named them.
#[tokio::test]
async fn anothers_colliding_names_survive_because_the_manifest_never_named_them() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pipeline = PipelineId::new("sweep-collide");
    let load = LoadId::new("load-a");

    // Another pipeline's files under the SAME table, whose load id and
    // seq COLLIDE with ours by coincidence. No manifest of ours names
    // them — that pipeline's manifest lives under its own scope.
    let events = dir.path().join("events");
    std::fs::create_dir_all(&events).expect("table dir");
    for index in 1..3 {
        std::fs::write(
            events.join(format!("part-load-a-1-{index}.parquet")),
            b"another pipeline's rows",
        )
        .expect("plant");
    }

    let mut session = session_over(local_dest(dir.path()), &pipeline, &load).await;
    session
        .write(&TableName::new("events"), batch_of(0, 100))
        .await
        .expect("write");
    session
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");

    for foreign in ["part-load-a-1-1.parquet", "part-load-a-1-2.parquet"] {
        let bytes = std::fs::read(events.join(foreign)).expect("survives");
        assert_eq!(
            bytes, b"another pipeline's rows",
            "{foreign}: a colliding name our manifest never claimed must survive"
        );
    }
}

/// The same sweep, partitioned: the residue lives under the partition
/// directory, named by the predecessor's manifest, and only what the
/// manifest names goes.
#[tokio::test]
async fn the_sweep_reaches_partition_directories() {
    let dir = tempfile::tempdir().expect("tempdir");
    let pipeline = PipelineId::new("sweep-part");
    let load = LoadId::new("load-a");

    let eu = dir.path().join("events").join("eu");
    std::fs::create_dir_all(&eu).expect("partition dir");
    std::fs::write(eu.join("part-load-a-1-1.parquet"), b"residue").expect("plant");
    std::fs::write(eu.join("part-other-1-0.parquet"), b"bystander").expect("plant");
    let scope = rdlt_connector_sdk::spi::core::schema::ident_hash(pipeline.as_str(), 12);
    std::fs::write(
        dir.path().join(format!("_rdlt_manifest.{scope}.json")),
        serde_json::json!({
            "format_version": 2,
            "load_id": "load-a",
            "commit_seq": 1,
            "names": [
                "events/eu/part-load-a-1-0.parquet",
                "events/eu/part-load-a-1-1.parquet",
            ],
        })
        .to_string(),
    )
    .expect("plant manifest");

    let config = local_dest(dir.path()).with_partition_by("payload");
    let dest = destination::Shell::new(config).expect("valid");
    let mut session = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    session
        .ensure_table(&schema_of("events", &["id", "payload"]), &WriteMode::Append)
        .await
        .expect("ensure");
    // One row whose partition value is exactly `eu`.
    let batch = RecordBatch::try_from_iter(vec![
        ("id", Arc::new(Int64Array::from(vec![1_i64])) as _),
        (
            "payload",
            Arc::new(StringArray::from(vec![Some("eu")])) as _,
        ),
    ])
    .expect("batch");
    session
        .write(&TableName::new("events"), batch)
        .await
        .expect("write");
    session
        .commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");

    let names: Vec<String> = std::fs::read_dir(&eu)
        .expect("partition dir")
        .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        !names.iter().any(|n| n == "part-load-a-1-1.parquet"),
        "manifested residue under the partition is swept: {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "part-other-1-0.parquet"),
        "an unmanifested file survives: {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "part-load-a-1-0.parquet"),
        "the retry's own part landed: {names:?}"
    );
}

/// The refusals, with the frozen spellings. Zero would silently mean
/// "one part per batch", which is what `parts` exists to prevent.
#[test]
fn a_zero_threshold_is_refused_at_the_config_gate() {
    use rdlt_connector_sdk::config::Document;

    for (json, want) in [
        (
            serde_json::json!({"path": "out", "parts": {"target_bytes": 0}}),
            "file destination: `target_bytes` is 0 — a part must hold something; remove the \
             setting to use the 128 MiB default, or give a positive size",
        ),
        (
            serde_json::json!({"path": "out", "parts": {"roll_after_seconds": 0}}),
            "file destination: `roll_after_seconds` is 0 — a part would close on every write; \
             remove the setting, or give a positive number of seconds",
        ),
        (
            serde_json::json!({"path": "out", "parts": {"max_open_bytes": 0}}),
            "file destination: `max_open_bytes` is 0 — open parts must be allowed some \
             memory; remove the setting to use the 512 MiB default, or give a positive size",
        ),
        // A ceiling below the target means the target is unreachable —
        // and would be missed silently, which is the defect class
        // `parts` exists to end.
        (
            serde_json::json!({
                "path": "out",
                "parts": {"target_bytes": 1024, "max_open_bytes": 512}
            }),
            "file destination: `max_open_bytes` (512) is below `target_bytes` (1024) — no \
             part could ever reach its target before the memory ceiling closed it; raise \
             the ceiling, or lower the target",
        ),
    ] {
        let err = destination::Config::from_value(json).expect_err("refused");
        assert_eq!(
            format!("{err}"),
            format!("invalid file destination config: {want}")
        );
    }
}

/// 036: closed parts REPORT themselves through the engine's event
/// feed, with exact encoded sizes and the reason that closed them —
/// the end-to-end pin for the whole telemetry seam (connector
/// callback -> engine forwarder -> PipelineEvent), asserted against
/// the bytes on disk rather than the connector's own bookkeeping.
#[tokio::test]
async fn closed_parts_report_their_size_and_reason_through_the_event_feed() {
    use rdlt_connector_sdk::config::Document;
    use rdlt_engine::config::Config as EngineConfig;
    use rdlt_engine::engine::Engine;
    use rdlt_testkit::memory::{
        Batch as MemoryBatch, Source as MemorySource, Stream as MemoryStream,
    };

    let dir = tempfile::tempdir().expect("tempdir");
    let config = destination::Config::from_value(serde_json::json!({
        "path": dir.path().to_str().expect("utf8"),
        "format": "jsonl",
        "parts": {"target_bytes": 4096},
    }))
    .expect("valid");

    // Three batches that each cross the 4 KiB target alone (Target
    // rolls), then a tiny tail that stays open until the commit
    // closes it (the Commit reason).
    let batch = |range: std::ops::Range<i64>| {
        MemoryBatch::new(
            range
                .map(|i| {
                    serde_json::json!({"id": i, "payload": format!("row-{i}-{}", "x".repeat(64))})
                })
                .collect(),
        )
    };
    let source = MemorySource::new(vec![MemoryStream::new(
        rdlt_connector_sdk::spi::source::StreamSpec::new("events"),
        vec![
            batch(0..200),
            batch(200..400),
            batch(400..600),
            batch(600..605),
        ],
    )]);

    let workdir = tempfile::tempdir().expect("workdir");
    let engine = Engine::new(
        EngineConfig::new("parts-events").with_workdir(workdir.path().join("wal")),
        source,
        destination::Shell::new(config).expect("valid"),
    );
    let mut events = engine.events();
    let report = engine.run().await.expect("run");

    let mut closed = Vec::new();
    while let Some(event) = events.recv().await {
        if let rdlt_connector_sdk::spi::core::event::PipelineEvent::PartClosed {
            table,
            encoded_bytes,
            reason,
        } = event
        {
            closed.push((table, encoded_bytes, reason));
        }
    }
    assert!(
        closed.len() > 1,
        "a 4 KiB target over ~45 KB of rows rolls several parts: {closed:?}"
    );
    // Reasons: every roll is Target here; the tail part is closed by
    // the commit.
    assert!(
        closed
            .iter()
            .filter(|(_, _, r)| *r == rdlt_connector_sdk::spi::core::event::PartCloseReason::Target)
            .count()
            >= 1,
        "size-rolled parts say Target: {closed:?}"
    );
    assert!(
        closed
            .iter()
            .any(|(_, _, r)| *r == rdlt_connector_sdk::spi::core::event::PartCloseReason::Commit),
        "the tail part says Commit: {closed:?}"
    );
    // The reported sizes are the FILES', exactly.
    let reported: u64 = closed.iter().map(|(_, b, _)| b).sum();
    let on_disk: u64 = std::fs::read_dir(dir.path().join("events"))
        .expect("table dir")
        .map(|e| e.expect("entry").metadata().expect("meta").len())
        .sum();
    assert_eq!(
        reported, on_disk,
        "reported part bytes are the on-disk bytes, not an estimate"
    );
    // 036 stage 5: the run report carries the same total — the
    // machine-readable copy of the output size, exact regardless of
    // event-feed lag.
    assert_eq!(
        report.tables[&closed[0].0].output_bytes, on_disk,
        "the report's output_bytes are the on-disk bytes"
    );
}
