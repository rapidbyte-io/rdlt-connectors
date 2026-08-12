//! Parquet: row groups pushed downstream as already-structured Arrow
//! batches (no shredding), with the cursor counting ROW GROUPS and
//! resume integrity carried by a CONTENT digest.
//!
//! Layout quantities alone cannot anchor a resume: group sizes and
//! page offsets are determined by position, so a group regenerated
//! with different values — or a rewrite that shifts uniformly-shaped
//! groups — reproduces every number in the footer. The digest
//! therefore reads the prefix's own bytes, and folds the schema in
//! too: renaming a column or changing a type at equal width alters
//! what the prefix MEANS while moving nothing.

use std::io::{Read as _, Seek as _, SeekFrom};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::FileReader as _;
use parquet::file::reader::SerializedFileReader;
use rdlt_connector_sdk::source::Feed;
use rdlt_connector_sdk::spi::SourceError;

use crate::source::cursor::{
    FileCursor, FileMeta, FileProgress, FileTask, ResumeCheck, TAIL_WINDOW,
};
use crate::source::list::local_listing;

/// The local listing, with each file's size restated in the parquet
/// unit: its ROW-GROUP count.
pub(crate) fn local_listing_with_row_groups(pattern: &str) -> Result<Vec<FileMeta>, SourceError> {
    let mut listing = local_listing(pattern)?;
    for meta in &mut listing {
        let file = std::fs::File::open(&meta.path)
            .map_err(|e| SourceError::fatal(format!("opening `{}`: {e}", meta.path)))?;
        let reader = SerializedFileReader::new(file)
            .map_err(|e| SourceError::fatal(format!("reading parquet `{}`: {e}", meta.path)))?;
        meta.size_units = reader.metadata().num_row_groups() as u64;
    }
    Ok(listing)
}

/// The exclusive byte bound of row group `last`: one past the end of
/// whichever of its column chunks reaches furthest. A chunk starts at
/// its dictionary page when it has one, otherwise at its first data
/// page, and `compressed_size` spans all of its pages. Every step is
/// CHECKED — the footer is untrusted input, which is also why the
/// library's `byte_range()` is avoided: it asserts on a negative
/// offset instead of reporting one.
fn prefix_end(path: &str, metadata: &ParquetMetaData, last: u64) -> Result<u64, SourceError> {
    let malformed = |what: &str| {
        SourceError::fatal(format!(
            "parquet `{path}`: row group {last} has {what}; refusing to verify a resume offset against a footer that does not describe itself"
        ))
    };
    let mut furthest = 0u64;
    for chunk in metadata.row_group(last as usize).columns() {
        let first_page = chunk
            .dictionary_page_offset()
            .unwrap_or_else(|| chunk.data_page_offset());
        let start = u64::try_from(first_page).map_err(|_| malformed("a negative page offset"))?;
        let size = u64::try_from(chunk.compressed_size())
            .map_err(|_| malformed("a negative chunk size"))?;
        let end = start
            .checked_add(size)
            .ok_or_else(|| malformed("an overflowing extent"))?;
        furthest = furthest.max(end);
    }
    Ok(furthest)
}

/// The resume integrity digest over the first `groups` row groups.
///
/// Its inputs, IN ORDER: each schema column's dotted path, physical
/// type, and logical type, every piece NUL-terminated; the group
/// count as little-endian bytes; then the last `min(TAIL_WINDOW,
/// prefix)` bytes of the prefix itself — one bounded read, the same
/// discipline as the record formats' tail window. That sequence is a
/// PERSISTED contract: digests already sitting in pipeline state were
/// computed from exactly this order.
fn resume_digest(
    path: &str,
    metadata: &ParquetMetaData,
    groups: u64,
) -> Result<String, SourceError> {
    let mut hasher = blake3::Hasher::new();
    for column in metadata.file_metadata().schema_descr().columns() {
        hasher.update(column.path().string().as_bytes());
        hasher.update(&[0]);
        hasher.update(format!("{:?}", column.physical_type()).as_bytes());
        hasher.update(&[0]);
        hasher.update(format!("{:?}", column.logical_type_ref()).as_bytes());
        hasher.update(&[0]);
    }
    hasher.update(&groups.to_le_bytes());

    let end = prefix_end(path, metadata, groups - 1)?;
    let window = end.min(TAIL_WINDOW);
    let mut file = std::fs::File::open(path)
        .map_err(|e| SourceError::fatal(format!("opening `{path}`: {e}")))?;
    file.seek(SeekFrom::Start(end - window))
        .map_err(|e| SourceError::fatal(format!("seeking `{path}`: {e}")))?;
    let mut buffer = vec![0u8; window as usize];
    file.read_exact(&mut buffer).map_err(|e| {
        SourceError::fatal(format!(
            "parquet `{path}`: the {window} bytes ending the consumed prefix could not be read ({e}); refusing to trust a resume offset the file no longer covers"
        ))
    })?;
    hasher.update(&buffer);
    Ok(hasher.finalize().to_hex().to_string())
}

/// Open the file and parse its footer into a batch-reader builder.
fn footer_reader(
    read_path: &str,
    shown_path: &str,
) -> Result<ParquetRecordBatchReaderBuilder<std::fs::File>, SourceError> {
    let file = std::fs::File::open(read_path)
        .map_err(|e| SourceError::fatal(format!("opening `{shown_path}`: {e}")))?;
    ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| SourceError::fatal(format!("reading parquet `{shown_path}`: {e}")))
}

/// Read from `task.start` (a row-group index) to the end, one fresh
/// reader and one checkpoint per group — resume is group-scoped, so
/// each group has to stand alone anyway, and re-parsing the footer
/// costs microseconds.
pub(crate) async fn read_task(
    task: &FileTask,
    cursor: &mut FileCursor,
    feed: &mut Feed,
) -> Result<bool, SourceError> {
    // The mirror of the jsonl reader's wrong-kind rule: a byte-window
    // hash written by a record reader says nothing about row groups,
    // so it is refused rather than dropped (030 review — this used to
    // fall through and silently disarm verification). Checked before
    // ANY file IO, because the refusal needs nothing from the file.
    if matches!(task.resume_check, Some(ResumeCheck::TailBytes { .. })) {
        return Err(SourceError::fatal(format!(
            "file `{}` recorded a byte-window integrity value but is being read as row groups; refusing to resume without a check this reader can evaluate — clear it from the pipeline state",
            task.path
        )));
    }

    let read_path = task.read_path.as_deref().unwrap_or(&task.path);
    let metadata = footer_reader(read_path, &task.path)?.metadata().clone();
    let group_count = metadata.num_row_groups() as u64;

    // Operators can edit the cursor document, so a position past the
    // end refuses typed, never wraps. EQUAL to the count is just a
    // file with nothing new — the loop below runs zero times, and
    // that is fine.
    if task.start > group_count {
        return Err(SourceError::fatal(format!(
            "file `{}` records progress through row group {} but now holds only {group_count} — refusing to resume past the end; clear it from the pipeline state or restore the file",
            task.path, task.start
        )));
    }

    if let Some(ResumeCheck::RowGroupPrefix { hash }) = task.resume_check.as_ref()
        && resume_digest(read_path, &metadata, task.start)? != *hash
    {
        return Err(SourceError::fatal(format!(
            "file `{}` was rewritten before the resume offset (the content of the {} row groups preceding it changed since the last run); refusing to read from a stale offset — clear it from the pipeline state or restore the file",
            task.path, task.start
        )));
    }

    for group in task.start..group_count {
        let batches = footer_reader(read_path, &task.path)?
            .with_row_groups(vec![group as usize])
            .build()
            .map_err(|e| SourceError::fatal(format!("reading parquet `{}`: {e}", task.path)))?;
        for batch in batches {
            let batch = batch.map_err(|e| {
                SourceError::fatal(format!(
                    "corrupt parquet `{}` (row group {group}): {e}",
                    task.path
                ))
            })?;
            if feed.arrow(batch).await.is_break() {
                return Ok(false);
            }
        }
        cursor.record(
            &task.path,
            FileProgress {
                done_units: group + 1,
                size_units: group_count,
                ended_at_record_boundary: true, // a group boundary cannot split a record
                mtime_ms: task.mtime_ms,
                etag: task.etag.clone(),
                tail_hash: None, // the digest is the group-unit counterpart
                row_groups_hash: Some(resume_digest(read_path, &metadata, group + 1)?),
            },
        );
        if feed.checkpoint(cursor.encode()).await.is_break() {
            return Ok(false);
        }
    }
    Ok(true)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    fn write_parquet(path: &std::path::Path, values: &[i64]) {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(arrow::array::Int64Array::from(values.to_vec()))],
        )
        .expect("batch");
        let file = std::fs::File::create(path).expect("create");
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None).expect("writer");
        writer.write(&batch).expect("write");
        writer.close().expect("close");
    }

    /// Content, not layout, decides the digest: two files of the same
    /// shape but different values must disagree, and one file must
    /// digest to the same value twice.
    #[test]
    fn the_prefix_digest_reads_content_not_layout() {
        let dir = tempfile::tempdir().expect("dir");
        let a = dir.path().join("a.parquet");
        let b = dir.path().join("b.parquet");
        write_parquet(&a, &[1, 2, 3]);
        write_parquet(&b, &[4, 5, 6]);
        let digest_of = |p: &std::path::Path| {
            let file = std::fs::File::open(p).expect("open");
            let reader = SerializedFileReader::new(file).expect("reader");
            resume_digest(p.to_str().expect("utf8"), reader.metadata(), 1).expect("digest")
        };
        assert_eq!(digest_of(&a), digest_of(&a), "stable");
        assert_ne!(
            digest_of(&a),
            digest_of(&b),
            "same shape, different values, different digest"
        );
    }

    /// Listing sizes are ROW-GROUP counts, not bytes.
    #[test]
    fn the_listing_counts_row_groups() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("g.parquet");
        write_parquet(&path, &[1, 2, 3, 4]);
        let pattern = path.to_str().expect("utf8");
        let listing = local_listing_with_row_groups(pattern).expect("lists");
        assert_eq!(listing[0].size_units, 1, "one batch, one group");
    }
}

#[cfg(test)]
mod wrong_kind_tests {
    use rdlt_connector_sdk::source::Feed;
    use rdlt_connector_sdk::spi::records_channel;

    use crate::source::cursor::{FileCursor, FileTask, ResumeCheck};

    /// A byte-window check reaching a row-group reader refuses before
    /// any IO — the twin of the jsonl reader's rule (030 review: gen
    /// 1 fell through here and skipped groups without a word).
    #[tokio::test]
    async fn a_tail_bytes_check_is_refused_not_ignored() {
        let (out, _keep) = records_channel(1 << 20);
        let mut feed = Feed::new(out);
        let mut cursor = FileCursor::default();
        let task = FileTask {
            path: "data/a.parquet".into(),
            read_path: None,
            start: 1,
            size_units: 3,
            mtime_ms: None,
            etag: None,
            resume_check: Some(ResumeCheck::TailBytes {
                window: 10,
                hash: "beef".into(),
            }),
        };
        let err = super::read_task(&task, &mut cursor, &mut feed)
            .await
            .expect_err("refused")
            .to_string();
        assert!(
            err.contains("recorded a byte-window integrity value but is being read as row groups"),
            "{err}"
        );
    }
}
