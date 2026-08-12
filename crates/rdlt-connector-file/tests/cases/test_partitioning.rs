//! Partitioned output: bare-value directories, `__null__`, injectively
//! encoded slugs, and final names independent of cross-table arrival
//! order.

use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use rdlt_connector_file::destination;
use rdlt_connector_sdk::spi::core::types::LogicalType;
use rdlt_connector_sdk::spi::core::{
    ColumnDef, ColumnType, LoadId, PipelineId, Provenance, TableName, TableSchema, WriteMode,
};
use rdlt_connector_sdk::spi::{Destination, OpenContext, RecordBatch};
use rdlt_testkit::commit_meta_for;

use super::common::local_dest;

fn partitioned_schema(table: &str) -> TableSchema {
    let col = |name: &str, ty| ColumnDef {
        name: name.to_owned(),
        column_type: ColumnType::scalar(ty),
        nullable: true,
        provenance: Provenance::Inferred,
    };
    TableSchema {
        table: TableName::new(table),
        parent: None,
        columns: vec![
            col("id", LogicalType::Int64),
            col("region", LogicalType::Utf8),
        ],
    }
}

fn regional_batch(values: &[Option<&str>]) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("region", DataType::Utf8, true),
    ]));
    let ids: Vec<i64> = (0..values.len() as i64).collect();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(values.to_vec())),
        ],
    )
    .expect("batch")
}

/// Bare-value partition directories with `__null__` and injectively
/// encoded slugs — never Hive `col=value`.
#[tokio::test]
async fn partition_directories_are_bare_sanitized_values() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = local_dest(dir.path()).with_partition_by("region");
    let dest = destination::Shell::new(config.clone()).expect("valid");
    let pipeline = PipelineId::new("partitions");
    let load = LoadId::new("load-a");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    s.ensure_table(&partitioned_schema("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    s.write(
        &TableName::new("events"),
        regional_batch(&[Some("eu"), None, Some("us/east"), Some("eu")]),
    )
    .await
    .expect("write");
    s.commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");

    for part in [
        "events/eu/part-load-a-1-0.parquet",
        "events/__null__/part-load-a-1-0.parquet",
        "events/us%2Feast/part-load-a-1-0.parquet",
    ] {
        assert!(dir.path().join(part).is_file(), "{part} must exist");
    }
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        4,
        "partitioned rows all count"
    );
}

/// A missing partition column is refused at write time with the
/// frozen spelling.
#[tokio::test]
async fn a_missing_partition_column_refuses_at_write() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = local_dest(dir.path()).with_partition_by("zone");
    let dest = destination::Shell::new(config).expect("valid");
    let mut s = dest
        .open(OpenContext::new(
            PipelineId::new("missing-col"),
            LoadId::new("load-a"),
        ))
        .await
        .expect("open");
    s.ensure_table(&partitioned_schema("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    let err = s
        .write(&TableName::new("events"), regional_batch(&[Some("eu")]))
        .await
        .expect_err("refused")
        .to_string();
    assert!(
        err.contains("partition_by column `zone` does not exist in stream `events`"),
        "{err}"
    );
}

/// Final names count per TABLE+PARTITION: interleaving a second
/// table's writes cannot change the first table's final names.
///
/// Parts close per WRITE here, which is what makes there be two
/// `alpha` parts to index at all: the subject is the index
/// arithmetic, and under the shipping 128 MiB default both `alpha`
/// writes coalesce into one file and the interleaving has nothing
/// left to disturb.
#[tokio::test]
async fn final_names_independent_of_cross_table_arrival_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config =
        local_dest(dir.path()).with_parts(rdlt_connector_sdk::spi::PartOptions::per_write());
    let dest = destination::Shell::new(config).expect("valid");
    let pipeline = PipelineId::new("arrival-order");
    let load = LoadId::new("load-a");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    for table in ["alpha", "beta"] {
        s.ensure_table(&partitioned_schema(table), &WriteMode::Append)
            .await
            .expect("ensure");
    }
    // Interleaved arrival: alpha, beta, alpha.
    s.write(&TableName::new("alpha"), regional_batch(&[Some("x")]))
        .await
        .expect("write");
    s.write(&TableName::new("beta"), regional_batch(&[Some("x")]))
        .await
        .expect("write");
    s.write(&TableName::new("alpha"), regional_batch(&[Some("x")]))
        .await
        .expect("write");
    s.commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");

    for part in [
        "alpha/part-load-a-1-0.parquet",
        "alpha/part-load-a-1-1.parquet",
        "beta/part-load-a-1-0.parquet",
    ] {
        assert!(
            dir.path().join(part).is_file(),
            "{part}: the index counts per table, not per arrival"
        );
    }
}

/// The 030 staging-injectivity amendment, live: dashed table names and
/// dashed partition values that COLLIDED under the gen-1 flat staged
/// name (`events` + `us-east` vs `events-us` + `east`) publish their
/// own rows — no overwrite, no NotFound at promote.
#[tokio::test]
async fn dashed_tables_and_partitions_never_share_a_staged_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = local_dest(dir.path()).with_partition_by("region");
    let dest = destination::Shell::new(config.clone()).expect("valid");
    let pipeline = PipelineId::new("staging-injective");
    let load = LoadId::new("l");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    for table in ["events", "events-us"] {
        s.ensure_table(&partitioned_schema(table), &WriteMode::Append)
            .await
            .expect("ensure");
    }
    s.write(
        &TableName::new("events"),
        regional_batch(&[Some("us-east")]),
    )
    .await
    .expect("write");
    s.write(
        &TableName::new("events-us"),
        regional_batch(&[Some("east")]),
    )
    .await
    .expect("write");
    s.commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");

    for part in [
        "events/us-east/part-l-1-0.parquet",
        "events-us/east/part-l-1-0.parquet",
    ] {
        assert!(dir.path().join(part).is_file(), "{part} must exist");
    }
    for (table, want) in [("events", 1), ("events-us", 1)] {
        assert_eq!(
            destination::testhook::count_rows(&config, table).expect("count"),
            want,
            "{table} carries its own row"
        );
    }
}

/// A partition VALUE of `..` (or `.`) must never be interpreted by
/// path resolution: layout v2's encoding escapes a leading `.` to
/// `%2E`, so the part lands INSIDE the table directory under the
/// escaped name, where counting and Replace can reach it (030 review;
/// the retired `__dot__`/`__dotdot__` sentinels handled this by name
/// before the encoding itself made it structurally impossible).
#[tokio::test]
async fn dot_partition_values_cannot_escape_the_table_directory() {
    let dir = tempfile::tempdir().expect("tempdir");
    let config = local_dest(dir.path()).with_partition_by("region");
    let dest = destination::Shell::new(config.clone()).expect("valid");
    let pipeline = PipelineId::new("dot-escape");
    let load = LoadId::new("l");
    let mut s = dest
        .open(OpenContext::new(pipeline.clone(), load.clone()))
        .await
        .expect("open");
    s.ensure_table(&partitioned_schema("events"), &WriteMode::Append)
        .await
        .expect("ensure");
    s.write(
        &TableName::new("events"),
        regional_batch(&[Some(".."), Some(".")]),
    )
    .await
    .expect("write");
    s.commit(commit_meta_for(&pipeline, &load, 1))
        .await
        .expect("commit");

    assert!(dir.path().join("events/%2E./part-l-1-0.parquet").is_file());
    assert!(dir.path().join("events/%2E/part-l-1-0.parquet").is_file());
    assert!(
        !dir.path().join("part-l-1-0.parquet").exists(),
        "nothing escaped to the destination root"
    );
    assert_eq!(
        destination::testhook::count_rows(&config, "events").expect("count"),
        2,
        "both rows are countable inside the table"
    );
}
