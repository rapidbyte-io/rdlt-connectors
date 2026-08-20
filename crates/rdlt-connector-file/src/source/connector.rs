//! Where the sdk drives the file source. [`File`] answers the
//! `SourceConnector` questions; `read_stream` walks one stream end to
//! end — resolve the inputs, plan against recorded progress, put local
//! copies behind any remote reads, then hand each task to its format's
//! reader.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use rdlt_connector_sdk::source::{Feed, SourceConnector};
use rdlt_connector_sdk::spi::core::{crash_point, cursor::Cursor, id::StreamName};
use rdlt_connector_sdk::spi::{error::SourceError, source::StreamSpec};

use super::config::{self, Config, Stream};
use super::cursor::{FileCursor, FileMeta, FileTask};
use super::{list, read};
use crate::format::{Format, codec_of};
use crate::location::Location;

/// The crash points this source arms — exported so the sweep iterates
/// exactly this list. These spellings are frozen.
pub const FAIL_POINTS: &[&str] = &["file.list", "file.read"];

/// The file source: a validated document in, streams of files out.
#[derive(Debug, Clone)]
pub struct File {
    config: Config,
    /// How many object fetches this instance declined because the
    /// cursor already proved the object done. Held per instance: one
    /// process may run several pipelines at once, and a shared tally
    /// would blur them together (docket S14). The counter exists so a
    /// test can PROVE the shortcut fired — a shortcut nobody can
    /// observe is free to quietly stop firing.
    skipped_fetches: Arc<AtomicU64>,
}

impl File {
    fn declared_stream(&self, name: &StreamName) -> Option<&Stream> {
        self.config.streams.iter().find(|s| s.name == name.as_str())
    }

    /// This instance's skip tally, exposed for tests.
    #[doc(hidden)]
    pub fn skipped_fetches(&self) -> u64 {
        self.skipped_fetches.load(Ordering::Relaxed)
    }
}

#[async_trait]
impl SourceConnector for File {
    // Reverse-DNS, not bare `file` (039 T6): NAME is the connector id
    // the wire handshake reports and the client verifies by STRICT
    // equality against a `ConnectorRequirement.id` — and D-039-1 keys
    // discovery on the id's last segment (`io.rapidbyte.file` → binary
    // `rdlt-connector-file` on PATH), so the id, the reported identity
    // and the binary name all derive from this one const.
    const NAME: &'static str = "io.rapidbyte.file";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");

    type Config = Config;

    fn assemble(config: Config) -> Result<Self, config::ConfigError> {
        Ok(Self {
            config,
            skipped_fetches: Default::default(),
        })
    }

    fn config_schema() -> Option<serde_json::Value> {
        Some(config::config_schema())
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        let mut specs = Vec::with_capacity(self.config.streams.len());
        for declared in &self.config.streams {
            let mut spec = StreamSpec::new(declared.name.as_str());
            match declared.format {
                // Parquet arrives as Arrow batches: a structured
                // stream, provenance at run level only.
                Format::Parquet => spec = spec.with_structured(),
                // The record formats carry keys and hints.
                Format::Jsonl | Format::Csv => {
                    if let Some(key) = &declared.primary_key {
                        spec = spec.with_primary_key(key.iter().cloned());
                    }
                    for (column, hint) in &declared.type_hints {
                        spec = spec.with_type_hint(column.clone(), (*hint).into());
                    }
                }
            }
            specs.push(spec);
        }
        Ok(specs)
    }

    async fn read_stream(
        &self,
        stream: &StreamSpec,
        since: Option<Cursor>,
        feed: &mut Feed,
    ) -> Result<(), SourceError> {
        let declared = self
            .declared_stream(&stream.name)
            .ok_or_else(|| SourceError::fatal(format!("unknown stream {}", stream.name)))?;

        let mut cursor = FileCursor::decode(since.as_ref())?;
        let location = Location::from_options(declared.location.as_ref())?;

        let (listing, mut copies) =
            snapshot(&location, declared, &cursor, &self.skipped_fetches).await?;
        crash_point!(
            "file.list",
            Err(SourceError::fatal("injected crash at file.list"))
        );
        let mut work = plan_work(&cursor, declared, listing)?;
        localize_reads(&location, declared, &mut work, &mut copies).await?;

        let csv_options = declared.csv.clone().unwrap_or_default();
        for task in &work {
            crash_point!(
                "file.read",
                Err(SourceError::fatal("injected crash at file.read"))
            );
            let keep_going = match declared.format {
                Format::Csv => {
                    read::csv::read_task(
                        task,
                        &csv_options,
                        &declared.type_hints,
                        &mut cursor,
                        feed,
                    )
                    .await?
                }
                Format::Parquet => read::parquet::read_task(task, &mut cursor, feed).await?,
                Format::Jsonl if codec_of(&task.path).is_plain() => {
                    read::jsonl::read_task(
                        &location,
                        task,
                        declared.validate.unwrap_or(true),
                        &mut cursor,
                        feed,
                    )
                    .await?
                }
                Format::Jsonl => {
                    read::jsonl::read_task_whole(
                        task,
                        declared.validate.unwrap_or(true),
                        &mut cursor,
                        feed,
                    )
                    .await?
                }
            };
            if !keep_going {
                // The host asked to stop; oblige without ceremony.
                break;
            }
        }
        // `copies` — scratch directory included — lives to this closing
        // brace, so success, error, and cancellation all release the
        // fetched files the same way.
        Ok(())
    }
}

/// Local files standing in for remote objects during one read, plus
/// the scratch directory that owns them.
struct LocalCopies {
    by_object: BTreeMap<String, String>,
    scratch: Option<ScratchDir>,
}

impl LocalCopies {
    fn none() -> Self {
        Self {
            by_object: BTreeMap::new(),
            scratch: None,
        }
    }

    /// The scratch directory's root, created on first demand.
    fn scratch_root(&mut self, stream: &str) -> Result<PathBuf, SourceError> {
        if self.scratch.is_none() {
            self.scratch = Some(ScratchDir::create(stream)?);
        }
        let dir = self.scratch.as_ref().expect("filled just above");
        Ok(dir.path().to_path_buf())
    }
}

/// One listing per run: whatever appears later waits for the next run.
/// Object-store parquet additionally comes down to temp files here,
/// before anything is planned — correctness beats streaming, and the
/// cursor keys on the OBJECT either way. The one exception: an object
/// the cursor can PROVE both unchanged and fully ingested keeps its
/// recorded unit count and is not fetched at all.
async fn snapshot(
    location: &Location,
    stream: &Stream,
    cursor: &FileCursor,
    skip_tally: &AtomicU64,
) -> Result<(Vec<FileMeta>, LocalCopies), SourceError> {
    if matches!(location, Location::Local { .. }) {
        let listing = match stream.format {
            Format::Parquet => read::parquet::local_listing_with_row_groups(&stream.path)?,
            Format::Jsonl | Format::Csv => list::local_listing(&stream.path)?,
        };
        return Ok((listing, LocalCopies::none()));
    }

    let listed = location.list(&stream.path).await?;
    if stream.format != Format::Parquet {
        return Ok((listed, LocalCopies::none()));
    }

    let scratch = ScratchDir::create(&stream.name)?;
    let mut described = Vec::with_capacity(listed.len());
    let mut by_object = BTreeMap::new();
    for (slot, meta) in listed.into_iter().enumerate() {
        if let Some(units) = settled_unit_count(cursor, &meta) {
            skip_tally.fetch_add(1, Ordering::Relaxed);
            described.push(FileMeta {
                size_units: units,
                ..meta
            });
            continue;
        }
        let local = download_object(location, &meta.path, scratch.path(), slot).await?;
        let local = local.to_string_lossy().into_owned();
        let row_groups = read::parquet::local_listing_with_row_groups(&local)?
            .first()
            .map_or(0, |m| m.size_units);
        by_object.insert(meta.path.clone(), local);
        described.push(FileMeta {
            size_units: row_groups,
            ..meta
        });
    }
    Ok((
        described,
        LocalCopies {
            by_object,
            scratch: Some(scratch),
        },
    ))
}

/// The recorded unit count for an object whose fetch would prove
/// nothing new — else None. PROVE carries the weight: progress must be
/// complete AND an etag must exist on both sides AND the two must be
/// equal. Every other combination falls through to the fetch and the
/// usual tripwires, so the worst this can do is download needlessly;
/// it can never vouch for something `plan` would have rejected.
fn settled_unit_count(cursor: &FileCursor, meta: &FileMeta) -> Option<u64> {
    let seen = cursor.files.get(&meta.path)?;
    let complete = seen.done_units == seen.size_units;
    match (complete, seen.etag.as_deref(), meta.etag.as_deref()) {
        (true, Some(then), Some(now)) if then == now => Some(seen.size_units),
        _ => None,
    }
}

/// Turn the listing into tasks under each format's incremental unit.
/// Jsonl needs care: one glob can cover plain files (byte-resumable)
/// and compressed ones (whole-file units) at once, so the two halves
/// plan under their own rules and the result re-sorts by path.
fn plan_work(
    cursor: &FileCursor,
    stream: &Stream,
    listing: Vec<FileMeta>,
) -> Result<Vec<FileTask>, SourceError> {
    match stream.format {
        Format::Parquet => cursor.plan(&listing),
        Format::Csv => cursor.plan_whole(&listing),
        Format::Jsonl => {
            let mut resumable = Vec::new();
            let mut opaque = Vec::new();
            for meta in listing {
                match codec_of(&meta.path).is_plain() {
                    true => resumable.push(meta),
                    false => opaque.push(meta),
                }
            }
            let mut work = cursor.plan(&resumable)?;
            work.append(&mut cursor.plan_whole(&opaque)?);
            work.sort_by(|a, b| a.path.cmp(&b.path));
            Ok(work)
        }
    }
}

/// Give every task that must decode locally a local path. Object-store
/// parquet tasks point at the copies `snapshot` already fetched;
/// object-store csv and compressed jsonl fetch here, AFTER planning,
/// so only planned (never skipped) tasks cost a download.
async fn localize_reads(
    location: &Location,
    stream: &Stream,
    work: &mut [FileTask],
    copies: &mut LocalCopies,
) -> Result<(), SourceError> {
    let remote = matches!(location, Location::S3(_));
    for (slot, task) in work.iter_mut().enumerate() {
        if let Some(local) = copies.by_object.get(&task.path) {
            task.read_path = Some(local.clone());
            continue;
        }
        if !remote || stream.format == Format::Parquet {
            continue;
        }
        let must_decode_locally = stream.format == Format::Csv || !codec_of(&task.path).is_plain();
        if !must_decode_locally || task.read_path.is_some() {
            continue;
        }
        let dir = copies.scratch_root(&stream.name)?;
        let local = download_object(location, &task.path, &dir, slot).await?;
        task.read_path = Some(local.to_string_lossy().into_owned());
    }
    Ok(())
}

/// A scratch directory that is its own janitor. Deleting on Drop is a
/// deliberate ownership choice: the read loop exits by success, error,
/// AND cancellation, and cleanup tied to only one of those paths would
/// leak a listing's worth of fetches on the other two.
#[derive(Debug)]
struct ScratchDir {
    root: PathBuf,
}

impl ScratchDir {
    /// Create a directory no concurrent pipeline can collide with:
    /// the name folds in the pid and a process-wide counter.
    fn create(stream: &str) -> Result<Self, SourceError> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let nonce = NEXT.fetch_add(1, Ordering::Relaxed);
        let root =
            std::env::temp_dir().join(format!("rdlt-file-{}-{nonce}-{stream}", std::process::id()));
        std::fs::create_dir_all(&root)
            .map_err(|e| SourceError::fatal(format!("temp dir for object fetch: {e}")))?;
        Ok(Self { root })
    }

    fn path(&self) -> &Path {
        &self.root
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        // Drop offers no error channel, and if we are unwinding, a
        // removal failure must not eclipse the error that got us here
        // — so this is best-effort by design.
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// Stream one object into a temp file, one bounded slab at a time.
async fn download_object(
    location: &Location,
    key: &str,
    dir: &Path,
    slot: usize,
) -> Result<PathBuf, SourceError> {
    use std::io::Write as _;
    let mut reader = location.open_from(key, 0).await?;
    let target = dir.join(format!("obj-{slot}"));
    let mut sink = std::fs::File::create(&target)
        .map_err(|e| SourceError::fatal(format!("temp file for `{key}`: {e}")))?;
    let mut slab = vec![0u8; crate::format::SLAB_BYTES];
    loop {
        let got = reader
            .read_full(&mut slab)
            .await
            .map_err(|e| crate::location::classify_read_error(&format!("fetching `{key}`"), e))?;
        if got == 0 {
            break;
        }
        sink.write_all(&slab[..got])
            .map_err(|e| SourceError::fatal(format!("writing temp for `{key}`: {e}")))?;
    }
    Ok(target)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::cursor::FileProgress;

    fn listed(path: &str, etag: Option<&str>) -> FileMeta {
        FileMeta {
            path: path.into(),
            size_units: 3,
            mtime_ms: None,
            etag: etag.map(str::to_owned),
        }
    }

    fn remembered(path: &str, done: u64, size: u64, etag: Option<&str>) -> FileCursor {
        let mut cursor = FileCursor::default();
        cursor.record(
            path,
            FileProgress {
                done_units: done,
                size_units: size,
                ended_at_record_boundary: true,
                mtime_ms: None,
                etag: etag.map(str::to_owned),
                tail_hash: None,
                row_groups_hash: Some("g".into()),
            },
        );
        cursor
    }

    /// A skip needs ALL of: complete recorded progress, an etag on
    /// both sides, and the two equal. Anything less downloads.
    #[test]
    fn a_fetch_is_skipped_only_on_complete_progress_and_matching_etags() {
        let object = listed("k", Some("e1"));
        assert_eq!(
            settled_unit_count(&remembered("k", 3, 3, Some("e1")), &object),
            Some(3),
            "provably unchanged and complete"
        );
        assert!(settled_unit_count(&remembered("k", 2, 3, Some("e1")), &object).is_none());
        assert!(settled_unit_count(&remembered("k", 3, 3, None), &object).is_none());
        assert!(settled_unit_count(&remembered("k", 3, 3, Some("e2")), &object).is_none());
        assert!(
            settled_unit_count(&remembered("k", 3, 3, Some("e1")), &listed("k", None)).is_none()
        );
        assert!(settled_unit_count(&FileCursor::default(), &object).is_none());
    }

    /// Two scratch dirs from one process never share a path, and each
    /// deletes itself when dropped.
    #[test]
    fn scratch_dirs_are_distinct_and_delete_themselves() {
        let first = ScratchDir::create("events").expect("first");
        let second = ScratchDir::create("events").expect("second");
        assert_ne!(first.path(), second.path());
        let kept = first.path().to_path_buf();
        assert!(kept.exists());
        drop(first);
        assert!(!kept.exists(), "gone once dropped");
        drop(second);
    }
}
