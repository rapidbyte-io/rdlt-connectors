//! Storage dispatch. A [`Location`] is the single value through which
//! every disk-versus-bucket decision in the crate flows: the source's
//! listing and sequential reads on one side, the destination's
//! staging, atomic publish, durable documents, and table-ownership
//! sweeps on the other. Each method matches once and hands the local
//! arm to the disk helpers at the bottom of this file; the S3 arm
//! delegates to [`S3Location`].

use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use rdlt_connector_sdk::spi::core::crash_point;
use rdlt_connector_sdk::spi::{error::DestinationError, error::SourceError};
use serde::Serialize;

use super::options::LocationOptions;
use super::s3::S3Location;
use crate::destination::STAGING_DIR;
use crate::source::cursor::FileMeta;
use crate::source::list::local_listing;

/// Render any displayable cause as a fatal destination error.
fn to_fatal<E: std::fmt::Display>(cause: E) -> DestinationError {
    DestinationError::fatal(cause.to_string())
}

/// One connected storage target. The destination form carries `root`,
/// the directory everything hangs under; the source form is rootless
/// and addresses whole paths (or keys) directly.
#[derive(Debug, Clone)]
pub(crate) enum Location {
    Local { root: PathBuf },
    S3(S3Location),
}

/// The compare-and-swap handle a conditional document write needs. A
/// local write has nothing to compare against — the exclusivity a
/// lease needs comes entirely from `create_doc_exclusive`'s O_EXCL, so
/// the local variant carries no data; the S3 variant carries the
/// store's own version identity, which `replace_doc_if` sends back
/// unmodified.
#[derive(Debug, Clone)]
pub(crate) enum DocVersion {
    Local,
    S3(object_store::UpdateVersion),
}

/// The outcome of an exclusive create. `AlreadyExists` is a normal
/// result a losing racer expects to see — not an error — so the lease
/// acquire step can branch on it without an error path standing in
/// for a business outcome.
#[derive(Debug)]
pub(crate) enum CreateDoc {
    Created(DocVersion),
    AlreadyExists,
}

/// The CAS replace lost: the document's version moved under us
/// (someone else's write landed between the read and this write). A
/// TYPED marker, carried as `replace_doc_if`'s `DestinationError`
/// SOURCE — matched by [`is_stale_version`] downcasting through
/// `source()`, never by rendered text (the constitution forbids
/// substring-matching a rendered error, and a future reword of the
/// message must not silently break the lease's takeover detection).
/// S3-arm only: see `replace_doc_if`'s doc for why the local arm has
/// no equivalent signal.
#[derive(Debug)]
pub(crate) struct StaleDocVersion {
    /// The document whose version was stale, for an operator-readable
    /// message; not part of the typed check.
    pub(crate) name: String,
}

impl std::fmt::Display for StaleDocVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "document version is stale (CAS replace lost) for `{}`",
            self.name
        )
    }
}

impl std::error::Error for StaleDocVersion {}

/// Was this `replace_doc_if` failure a lost CAS — the document moved
/// under us — as opposed to a genuine store/transport failure? Walks
/// exactly the one `source()` frame `replace_doc_if` attaches the
/// marker at; a `DestinationError::context` applied in between would
/// re-box the cause as rendered text and end the chain there (by
/// design — see `DestinationError::context`'s own doc), so this must
/// see the error `replace_doc_if` returned directly, not a
/// context-wrapped copy of it.
///
/// Re-exported through `location/mod.rs` (`pub(crate) use
/// kind::{CreateDoc, DocVersion, is_stale_version};`) since 037 US2 T6:
/// the lease module lives outside `location` and is the first real
/// caller beyond `probe_conditional_docs` below, which stays in this
/// module.
pub(crate) fn is_stale_version(error: &DestinationError) -> bool {
    std::error::Error::source(error)
        .is_some_and(|cause| cause.downcast_ref::<StaleDocVersion>().is_some())
}

impl Location {
    /// Source-side constructor — rootless, since streams name their
    /// files in full.
    pub(crate) fn from_options(options: Option<&LocationOptions>) -> Result<Self, SourceError> {
        let Some(s3) = options.and_then(|o| o.s3.as_ref()) else {
            return Ok(Self::Local {
                root: PathBuf::new(),
            });
        };
        Ok(Self::S3(S3Location::connect(s3)?))
    }

    /// Destination-side constructor. On disk `path` is the output
    /// directory (created here); on S3 it becomes the key prefix.
    pub(crate) fn for_dest(
        path: &str,
        options: Option<&LocationOptions>,
    ) -> Result<Self, DestinationError> {
        if let Some(s3) = options.and_then(|o| o.s3.as_ref()) {
            let prefix = path.trim_matches('/').to_owned();
            return Ok(Self::S3(S3Location::connect_for_dest(s3, prefix)?));
        }
        Self::local_dir(PathBuf::from(path))
    }

    /// A local output directory, created if absent. The `PathBuf` is
    /// taken verbatim — a non-UTF-8 path keeps its exact bytes.
    pub(crate) fn local_dir(root: PathBuf) -> Result<Self, DestinationError> {
        std::fs::create_dir_all(&root).map_err(to_fatal)?;
        Ok(Self::Local { root })
    }

    /// True on disk. The durability steps that only exist locally —
    /// and the crash points guarding them — branch on this.
    pub(crate) fn is_local(&self) -> bool {
        matches!(self, Self::Local { .. })
    }

    /// The output directory, when there is one; the synchronous local
    /// row counter needs the concrete path.
    pub(crate) fn local_root(&self) -> Option<&Path> {
        if let Self::Local { root } = self {
            Some(root)
        } else {
            None
        }
    }

    /// List everything a pattern names — complete, or a typed error.
    pub(crate) async fn list(&self, pattern: &str) -> Result<Vec<FileMeta>, SourceError> {
        match self {
            Self::Local { .. } => local_listing(pattern),
            Self::S3(s3) => s3.list(pattern).await,
        }
    }

    /// A byte reader positioned `start` bytes in.
    pub(crate) async fn open_from(
        &self,
        name: &str,
        start: u64,
    ) -> Result<ByteReader, SourceError> {
        match self {
            Self::S3(s3) => s3.open_from(name, start).await,
            Self::Local { .. } => {
                use std::io::{Seek as _, SeekFrom};
                let mut file = std::fs::File::open(name)
                    .map_err(|e| SourceError::fatal(format!("opening `{name}`: {e}")))?;
                if start > 0 {
                    file.seek(SeekFrom::Start(start))
                        .map_err(|e| SourceError::fatal(format!("seek `{name}`: {e}")))?;
                }
                Ok(ByteReader::Local(file))
            }
        }
    }

    /// Clear whatever a dead session left staged under this scope —
    /// staged-but-never-committed bytes must not leak into a new load —
    /// then make room for the fresh one. The scope key keeps a sibling
    /// pipeline's live staging untouched.
    pub(crate) async fn prepare_staging(
        &self,
        scope: &str,
        load: &str,
    ) -> Result<(), DestinationError> {
        match self {
            Self::S3(s3) => {
                let scope_prefix = format!("{STAGING_DIR}/{scope}");
                for stale in s3.list_keys(&scope_prefix).await? {
                    s3.delete_key(&stale).await?;
                }
                Ok(())
            }
            Self::Local { root } => {
                let scope_dir = root.join(STAGING_DIR).join(scope);
                if scope_dir.exists() {
                    std::fs::remove_dir_all(&scope_dir).map_err(to_fatal)?;
                }
                std::fs::create_dir_all(scope_dir.join(load)).map_err(to_fatal)
            }
        }
    }

    /// Write one part into staging. The S3 staging crash point sits
    /// immediately ahead of the PUT.
    pub(crate) async fn stage_put(
        &self,
        staging_tail: &str,
        bytes: Vec<u8>,
    ) -> Result<(), DestinationError> {
        match self {
            Self::Local { root } => {
                let path = root.join(staging_tail);
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).map_err(to_fatal)?;
                }
                std::fs::write(path, &bytes).map_err(to_fatal)
            }
            Self::S3(s3) => {
                crash_point!(
                    "file.stage.put",
                    Err(DestinationError::fatal("injected crash at file.stage.put"))
                );
                s3.put(staging_tail, bytes).await
            }
        }
    }

    /// Drop one staged part without caring whether it succeeds — a
    /// replayed commit clears its staging before handing back the
    /// receipt it already earned.
    pub(crate) async fn stage_remove(&self, staging_tail: &str) {
        match self {
            Self::S3(s3) => s3.delete_best_effort(staging_tail).await,
            Self::Local { root } => {
                let _ = std::fs::remove_file(root.join(staging_tail));
            }
        }
    }

    /// Make one staged part visible under its deterministic final
    /// name. Disk gets fsync-then-rename (atomic per file); S3 gets
    /// COPY followed by an idempotent DELETE, so a replayed finalize
    /// that finds the staged object already gone still succeeds. Each
    /// arm's crash points separate its own sub-steps.
    pub(crate) async fn publish_part(
        &self,
        staging_tail: &str,
        final_tail: &str,
    ) -> Result<(), DestinationError> {
        match self {
            Self::Local { root } => promote_on_disk(root, staging_tail, final_tail),
            Self::S3(s3) => {
                crash_point!(
                    "file.finalize.copy",
                    Err(DestinationError::fatal(
                        "injected crash at file.finalize.copy"
                    ))
                );
                s3.copy(staging_tail, final_tail).await?;
                crash_point!(
                    "file.finalize.delete",
                    Err(DestinationError::fatal(
                        "injected crash at file.finalize.delete"
                    ))
                );
                s3.delete_idempotent(staging_tail).await
            }
        }
    }

    /// Fsync a directory so a rename it contains survives power loss.
    /// Object stores have no such step — nothing to do there.
    pub(crate) fn sync_dir(&self, dir_tail: &str) -> Result<(), DestinationError> {
        match self {
            Self::S3(_) => Ok(()),
            Self::Local { root } => sync_directory(&root.join(dir_tail)),
        }
    }

    /// One durable document's exact bytes, or `None` if it does not
    /// exist.
    pub(crate) async fn read_doc(&self, name: &str) -> Result<Option<Vec<u8>>, DestinationError> {
        match self {
            Self::Local { root } => read_optional(&root.join(name)),
            Self::S3(s3) => s3.read_doc(name).await,
        }
    }

    /// Persist one document. Metadata may never be flimsier than the
    /// data parts it describes, so the disk arm goes through the full
    /// atomic sequence (temp file, fsync, rename, parent-dir fsync);
    /// on S3 a lone PUT already is the atomic step.
    pub(crate) async fn write_doc<T: Serialize>(
        &self,
        name: &str,
        value: &T,
    ) -> Result<(), DestinationError> {
        match self {
            Self::Local { root } => write_doc_durably(&root.join(name), value),
            Self::S3(s3) => {
                let body = serde_json::to_vec_pretty(value).map_err(to_fatal)?;
                s3.put(name, body).await
            }
        }
    }

    /// Exclusive create: local O_EXCL (`create_new`), S3
    /// `PutMode::Create`. Used for the lease's acquire step, where two
    /// processes racing to create the SAME document must have exactly
    /// one of them win — the loser gets `AlreadyExists`, not an error.
    pub(crate) async fn create_doc_exclusive(
        &self,
        name: &str,
        bytes: Vec<u8>,
    ) -> Result<CreateDoc, DestinationError> {
        match self {
            Self::Local { root } => {
                let path = root.join(name);
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).map_err(to_fatal)?;
                }
                // Deliberately not the tmp+rename shape the other doc
                // writers use: a rename replaces unconditionally, so it
                // cannot express "only if nothing is there yet". O_EXCL
                // is the one primitive the OS gives us for that, and
                // it's also what makes the local arm race-safe without
                // any lock file of its own.
                use std::io::Write as _;
                match std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)
                {
                    Ok(mut file) => {
                        file.write_all(&bytes).map_err(to_fatal)?;
                        Ok(CreateDoc::Created(DocVersion::Local))
                    }
                    Err(e) if e.kind() == ErrorKind::AlreadyExists => Ok(CreateDoc::AlreadyExists),
                    Err(e) => Err(to_fatal(e)),
                }
            }
            Self::S3(s3) => s3.create_doc_exclusive(name, bytes).await,
        }
    }

    /// Read with the CAS handle a later `replace_doc_if` needs.
    /// `None` means the document does not exist.
    pub(crate) async fn read_doc_versioned(
        &self,
        name: &str,
    ) -> Result<Option<(Vec<u8>, DocVersion)>, DestinationError> {
        match self {
            Self::Local { root } => {
                Ok(read_optional(&root.join(name))?.map(|bytes| (bytes, DocVersion::Local)))
            }
            Self::S3(s3) => s3.read_doc_versioned(name).await,
        }
    }

    /// CAS replace: local plain tmp+rename, S3 `PutMode::Update`.
    ///
    /// The STALENESS SIGNAL IS S3-ARM ONLY. The local arm performs no
    /// version check at all — `version` is accepted but never
    /// inspected, and a stale one still writes successfully.
    ///
    /// (037 US2 review round 1, C2) An EARLIER version of this doc
    /// claimed that was safe unconditionally, on the theory that
    /// `create_doc_exclusive`'s O_EXCL already serializes same-machine
    /// takeover so nothing could ever present a stale version here.
    /// That claim was FALSE for a takeover race specifically: two
    /// local processes racing to take over the SAME stale lease both
    /// pass `create_doc_exclusive` → `AlreadyExists` → read the same
    /// stale doc → and would BOTH have their `replace_doc_if` "succeed"
    /// here, since this arm never checks the version — two winners,
    /// last-writer-wins. The lease's takeover step therefore does not
    /// call this method at all on the local arm; it uses
    /// [`Location::delete_doc_exclusive`] instead, whose atomic unlink
    /// genuinely picks one winner, and loops back to
    /// `create_doc_exclusive` (see `lease.rs`'s module doc).
    ///
    /// What remains true, and is what actually makes the local arm's
    /// lack of CAS safe for the calls that DO still reach it: every
    /// surviving local caller (the lease's own-owner reacquire, and
    /// its heartbeat's renewal) only ever replaces a document that
    /// caller's own identity already owns — never a foreign takeover.
    /// Two heartbeats of the SAME owner do not run concurrently
    /// (`Load` holds one `Lease`), and an own-owner reacquire race is
    /// the engine retrying its own failed attempt, not a competing
    /// session. Nothing outside those callers on one filesystem calls
    /// `replace_doc_if` against a lease document at all.
    ///
    /// S3 has no single-writer guarantee even for those calls (a
    /// second machine can run either one), which is why only that arm
    /// needs, and gets, a real compare-and-swap; S3's takeover path
    /// keeps using this method precisely because its CAS IS sound
    /// there — `Error::Precondition` genuinely serializes it.
    ///
    /// On S3, a lost CAS returns `Err` carrying [`StaleDocVersion`] as
    /// the cause — check with [`is_stale_version`], never by matching
    /// this method's rendered error text, to tell "the lease was taken
    /// over" apart from "the store failed".
    pub(crate) async fn replace_doc_if(
        &self,
        name: &str,
        bytes: Vec<u8>,
        version: DocVersion,
    ) -> Result<DocVersion, DestinationError> {
        match self {
            Self::Local { root } => {
                write_bytes_durably(&root.join(name), &bytes)?;
                Ok(DocVersion::Local)
            }
            Self::S3(s3) => {
                let DocVersion::S3(update) = version else {
                    return Err(DestinationError::fatal(
                        "replace_doc_if: an S3 location needs an S3 version handle, not a \
                         local one",
                    ));
                };
                s3.replace_doc_if(name, bytes, update).await
            }
        }
    }

    /// Delete a document where absence already counts as done — the
    /// lease's best-effort release must not fail just because a
    /// takeover, or an earlier crashed release, already removed it.
    pub(crate) async fn delete_doc(&self, name: &str) -> Result<(), DestinationError> {
        match self {
            Self::Local { root } => match std::fs::remove_file(root.join(name)) {
                Ok(()) => Ok(()),
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
                Err(e) => Err(to_fatal(e)),
            },
            Self::S3(s3) => s3.delete_doc(name).await,
        }
    }

    /// Exclusive delete: "did *I* remove this document?", as opposed
    /// to `delete_doc`'s "is it now gone". `Ok(true)` means THIS call
    /// is the one that removed it; `Ok(false)` means it was already
    /// gone (someone else's remove — or an earlier release — got
    /// there first).
    ///
    /// LOCAL ARM ONLY, in practice (037 US2 review round 1, C2). POSIX
    /// `unlink` is atomic across processes on one filesystem: of two
    /// concurrent removers of the same path, the kernel hands exactly
    /// one of them success and the other `NotFound` — which is the
    /// primitive the lease's local takeover race decides its ONE
    /// winner with (see `lease.rs`'s module doc and
    /// `replace_doc_if`'s amended safety paragraph, which explains why
    /// a plain CAS-replace takeover was unsound there).
    ///
    /// The S3 arm below exists only so this method's `match` stays
    /// exhaustive over both storage kinds — object_store 0.12.5's
    /// `ObjectStore::delete` has no conditional/precondition form (only
    /// `put_opts` and similar writes do; checked directly against the
    /// vendored 0.12.5 source, not assumed), so there is no primitive
    /// to build a genuinely exclusive delete on there. Nothing in this
    /// crate calls this method on the S3 arm — the lease's S3 takeover
    /// keeps using `replace_doc_if`'s real CAS instead, which the S3
    /// store DOES support. This arm's `Ok(true)` is therefore not a
    /// meaningful exclusivity claim, only a type-compatible stand-in.
    pub(crate) async fn delete_doc_exclusive(&self, name: &str) -> Result<bool, DestinationError> {
        match self {
            Self::Local { root } => match std::fs::remove_file(root.join(name)) {
                Ok(()) => Ok(true),
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
                Err(e) => Err(to_fatal(e)),
            },
            Self::S3(s3) => {
                s3.delete_doc(name).await?;
                Ok(true)
            }
        }
    }

    /// One CAS round-trip through the abstraction itself: create,
    /// versioned read, CAS replace, stale-CAS refusal, delete — the
    /// stale-CAS step asserts the refusal carries the TYPED
    /// [`StaleDocVersion`] marker via [`is_stale_version`], not merely
    /// that it errored. Flattened to `Result<(), DestinationError>` —
    /// no step needs the caller to name [`CreateDoc`] or [`DocVersion`],
    /// which is what lets the live S3 probe in `tests/cases/test_s3.rs`
    /// drive it through `destination::testhook` without either type
    /// crossing out of this module (037 US2). Real CAS refusal only
    /// exists on the S3 arm — the local arm's `replace_doc_if` never
    /// refuses (see its own doc), so this exercise is meaningful there
    /// and not against `Local`; the local sibling tests below cover the
    /// local shape directly instead.
    pub(crate) async fn probe_conditional_docs(&self, name: &str) -> Result<(), DestinationError> {
        let first = self.create_doc_exclusive(name, b"one".to_vec()).await?;
        let CreateDoc::Created(created_version) = first else {
            return Err(DestinationError::fatal(
                "probe_conditional_docs: first exclusive create must succeed on a fresh key",
            ));
        };

        let second = self.create_doc_exclusive(name, b"two".to_vec()).await?;
        if !matches!(second, CreateDoc::AlreadyExists) {
            return Err(DestinationError::fatal(
                "probe_conditional_docs: second exclusive create must refuse an existing key",
            ));
        }

        let (bytes, version) = self.read_doc_versioned(name).await?.ok_or_else(|| {
            DestinationError::fatal("probe_conditional_docs: doc must read back after create")
        })?;
        if bytes != b"one" {
            return Err(DestinationError::fatal(
                "probe_conditional_docs: doc bytes must match what was created",
            ));
        }

        self.replace_doc_if(name, b"three".to_vec(), version)
            .await?;

        match self
            .replace_doc_if(name, b"four".to_vec(), created_version)
            .await
        {
            Ok(_) => {
                return Err(DestinationError::fatal(
                    "probe_conditional_docs: CAS on the superseded version must be refused",
                ));
            }
            Err(e) if is_stale_version(&e) => {}
            Err(e) => {
                return Err(DestinationError::fatal(format!(
                    "probe_conditional_docs: CAS refusal must carry the typed StaleDocVersion \
                     marker, not just any error: {e}"
                )));
            }
        }

        self.delete_doc(name).await?;
        if self.read_doc(name).await?.is_some() {
            return Err(DestinationError::fatal(
                "probe_conditional_docs: doc must be gone after delete",
            ));
        }
        Ok(())
    }

    /// The ownership sweep: every file under `{table}/`, expressed as
    /// tails relative to the table root. Both consumers — row counting
    /// and Replace truncation — read this one method, so what "owned"
    /// means is decided in exactly one place.
    pub(crate) async fn keys_of_table(&self, table: &str) -> Result<Vec<String>, DestinationError> {
        match self {
            Self::Local { root } => files_under(&root.join(table)),
            Self::S3(s3) => {
                let table_root = format!("{}/", s3.key_of_table_root(table));
                let mut owned = Vec::new();
                for key in s3.list_keys(table).await? {
                    let full = key.to_string();
                    if let Some(rest) = tail_under_root(&full, &table_root)? {
                        owned.push(rest.to_owned());
                    }
                }
                Ok(owned)
            }
        }
    }

    /// Remove one published final by tail from the output root,
    /// TOLERATING absence: the manifest sweep deletes what a crashed
    /// predecessor MAY have published, and intent always covers more
    /// than what landed before the crash.
    pub(crate) async fn remove_final_if_present(&self, tail: &str) -> Result<(), DestinationError> {
        match self {
            Self::Local { root } => match std::fs::remove_file(root.join(tail)) {
                Err(e) if e.kind() == ErrorKind::NotFound => Ok(()),
                other => other.map_err(to_fatal),
            },
            Self::S3(s3) => s3.delete_tail_if_present(tail).await,
        }
    }

    /// Fetch one owned file, addressed by tail (row counting).
    pub(crate) async fn read_table_file(
        &self,
        table: &str,
        tail: &str,
    ) -> Result<Vec<u8>, DestinationError> {
        match self {
            Self::Local { root } => std::fs::read(root.join(table).join(tail)).map_err(to_fatal),
            Self::S3(s3) => s3.get_key(&s3.key_of_table(table, tail)).await,
        }
    }

    /// Remove one owned file, addressed by tail (Replace truncation).
    pub(crate) async fn delete_table_file(
        &self,
        table: &str,
        tail: &str,
    ) -> Result<(), DestinationError> {
        match self {
            Self::Local { root } => {
                std::fs::remove_file(root.join(table).join(tail)).map_err(to_fatal)
            }
            Self::S3(s3) => s3.delete_key(&s3.key_of_table(table, tail)).await,
        }
    }
}

/// A byte stream over either storage kind.
pub(crate) enum ByteReader {
    Local(std::fs::File),
    S3(super::s3::S3Reader),
}

impl std::fmt::Debug for ByteReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local(_) => f.write_str("ByteReader::Local"),
            Self::S3(reader) => reader.fmt(f),
        }
    }
}

impl ByteReader {
    /// Read until `buf` is full; a shorter count means the stream
    /// ended.
    pub(crate) async fn read_full(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::S3(reader) => reader.read_full(buf).await,
            Self::Local(file) => {
                use std::io::Read as _;
                let mut done = 0;
                while done < buf.len() {
                    let n = match file.read(&mut buf[done..]) {
                        Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                        Err(e) => return Err(e),
                        Ok(n) => n,
                    };
                    if n == 0 {
                        break;
                    }
                    done += n;
                }
                Ok(done)
            }
        }
    }
}

/// Turn a `read_full` failure into a source error: the one retryable
/// io kind becomes transient (the engine budget absorbs it); anything
/// else is fatal with the subject named.
pub(crate) fn classify_read_error(context: &str, e: std::io::Error) -> SourceError {
    let rendered = format!("{context}: {e}");
    match e.kind() {
        ErrorKind::ConnectionReset => SourceError::transient(rendered),
        _ => SourceError::fatal(rendered),
    }
}

// ---- disk helpers ----------------------------------------------------------

/// Local promotion of one staged part: ensure the target's directory,
/// force the staged bytes to stable storage, then rename them into
/// place. The two crash points bracket the fsync and the rename so
/// the sweep can interrupt either side.
fn promote_on_disk(
    root: &Path,
    staging_tail: &str,
    final_tail: &str,
) -> Result<(), DestinationError> {
    let staged = root.join(staging_tail);
    let target = root.join(final_tail);
    if let Some(dir) = target.parent() {
        std::fs::create_dir_all(dir).map_err(to_fatal)?;
    }
    crash_point!(
        "pq.staged.sync",
        Err(DestinationError::fatal("injected crash at pq.staged.sync"))
    );
    std::fs::File::open(&staged)
        .and_then(|f| f.sync_all())
        .map_err(to_fatal)?;
    crash_point!(
        "pq.part.rename",
        Err(DestinationError::fatal("injected crash at pq.part.rename"))
    );
    std::fs::rename(&staged, &target).map_err(to_fatal)
}

/// Fsync one directory through an open handle.
fn sync_directory(dir: &Path) -> Result<(), DestinationError> {
    std::fs::File::open(dir)
        .and_then(|f| f.sync_all())
        .map_err(to_fatal)
}

/// Durable JSON replacement: serialize, then hand off to
/// [`write_bytes_durably`] for the atomic tmp+fsync+rename shape.
fn write_doc_durably<T: Serialize>(path: &Path, value: &T) -> Result<(), DestinationError> {
    let body = serde_json::to_vec_pretty(value).map_err(to_fatal)?;
    write_bytes_durably(path, &body)
}

/// Durable byte replacement: write a `.tmp` sibling next to `path`,
/// fsync it, rename over the destination, fsync the parent directory.
/// Appending `.tmp` to the whole file name (rather than swapping the
/// extension) keeps this correct for names that already carry one, and
/// for names that carry none — both shapes this crate's documents use.
fn write_bytes_durably(path: &Path, bytes: &[u8]) -> Result<(), DestinationError> {
    use std::io::Write as _;
    let mut tmp_name = path.file_name().unwrap_or_default().to_os_string();
    tmp_name.push(".tmp");
    let scratch = path.with_file_name(tmp_name);
    {
        let mut out = std::fs::File::create(&scratch).map_err(to_fatal)?;
        out.write_all(bytes).map_err(to_fatal)?;
        out.sync_all().map_err(to_fatal)?;
    }
    std::fs::rename(&scratch, path).map_err(to_fatal)?;
    match path.parent() {
        Some(dir) => sync_directory(dir),
        None => Ok(()),
    }
}

/// A file's bytes, with absence mapped to `None` rather than an error.
fn read_optional(path: &Path) -> Result<Option<Vec<u8>>, DestinationError> {
    match std::fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(e) if e.kind() == ErrorKind::NotFound => Ok(None),
        Err(e) => Err(to_fatal(e)),
    }
}

/// Every regular file below `base`, depth-first, as slash-joined
/// tails. An absent `base` simply means nothing has been written yet.
/// Symlinks are never followed: this crate never stages one, and
/// walking through one would let truncation delete files the root
/// does not own. A name that is not valid UTF-8 is refused with a
/// typed error — inventing a tail for it would misplace its children
/// and give truncation a path the listing never produced.
fn files_under(base: &Path) -> Result<Vec<String>, DestinationError> {
    let mut found = Vec::new();
    descend(base, None, &mut found)?;
    Ok(found)
}

fn descend(dir: &Path, rel: Option<&str>, found: &mut Vec<String>) -> Result<(), DestinationError> {
    let listing = match std::fs::read_dir(dir) {
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(()),
        other => other.map_err(to_fatal)?,
    };
    for item in listing {
        let item = item.map_err(to_fatal)?;
        let file_type = item.file_type().map_err(to_fatal)?;
        if file_type.is_symlink() {
            continue;
        }
        let path = item.path();
        let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
            return Err(to_fatal(format!(
                "output directory contains a non-UTF-8 name under `{}`; rename it or move \
                 it outside the destination",
                dir.display()
            )));
        };
        let tail = match rel {
            Some(above) => format!("{above}/{name}"),
            None => name.to_owned(),
        };
        if file_type.is_dir() {
            descend(&path, Some(&tail), found)?;
        } else {
            found.push(tail);
        }
    }
    Ok(())
}

/// What remains of a listed key once the table root is stripped off
/// the FRONT — stripping, never searching: nothing stops a partition
/// directory from being named like the table, and a substring search
/// would cut at the wrong occurrence. A key the prefix does not
/// cover is a typed listing violation: silently dropping it would
/// shrink ownership and let Replace strand data. The single tolerated
/// exception is the zero-byte directory marker whose key equals the
/// table root itself (consoles and folder-path `put-object` create
/// these); it carries no data and yields no tail.
fn tail_under_root<'k>(key: &'k str, root: &str) -> Result<Option<&'k str>, DestinationError> {
    if let Some(tail) = key.strip_prefix(root) {
        return Ok(Some(tail));
    }
    if key == root.trim_end_matches('/') {
        return Ok(None);
    }
    Err(to_fatal(format!(
        "listing returned key `{key}`, which is not under the prefix `{root}` it was \
         listed by"
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Nested files come back as tails, symlinks are invisible, and a
    /// base that does not exist lists as empty.
    #[test]
    fn ownership_walk_covers_depth_skips_symlinks_and_tolerates_absence() {
        let dir = tempfile::tempdir().expect("dir");
        let table = dir.path().join("t");
        std::fs::create_dir_all(table.join("us")).expect("dirs");
        std::fs::write(table.join("a.parquet"), b"x").expect("write");
        std::fs::write(table.join("us/b.parquet"), b"x").expect("write");
        #[cfg(unix)]
        std::os::unix::fs::symlink("/etc", table.join("link")).expect("symlink");

        let mut tails = files_under(&table).expect("walk");
        tails.sort();
        assert_eq!(tails, vec!["a.parquet", "us/b.parquet"]);

        let nothing = files_under(&dir.path().join("missing")).expect("empty");
        assert!(nothing.is_empty());
    }

    /// The root is stripped rather than searched for, the directory
    /// marker yields no tail, and an out-of-prefix key is refused.
    #[test]
    fn tails_are_stripped_never_searched() {
        assert_eq!(
            tail_under_root("out/t/t/part.parquet", "out/t/").expect("ok"),
            Some("t/part.parquet"),
            "a same-named partition directory must not confuse the strip"
        );
        assert_eq!(tail_under_root("out/t", "out/t/").expect("marker"), None);

        let err = tail_under_root("elsewhere/k", "out/t/").expect_err("violation");
        assert!(
            format!("{err}").contains(
                "listing returned key `elsewhere/k`, which is not under the prefix `out/t/`"
            ),
            "{err}"
        );
    }

    /// A written document reads back byte-faithfully, the temp sibling
    /// is gone, and an absent document is `None`.
    #[test]
    fn doc_writes_are_atomic_and_clean() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("doc.json");
        write_doc_durably(&path, &serde_json::json!({"v": 1})).expect("writes");

        let bytes = read_optional(&path).expect("read").expect("present");
        let value: serde_json::Value = serde_json::from_slice(&bytes).expect("parses");
        assert_eq!(value, serde_json::json!({"v": 1}));

        assert!(!path.with_extension("json.tmp").exists(), "no temp residue");
        assert!(
            read_optional(&dir.path().join("absent.json"))
                .expect("ok")
                .is_none()
        );
    }

    /// (037 US2 T5, red step) The failing shape the lease's acquire
    /// step rides: two exclusive creates racing for the same document
    /// name, the second finding the first already there. Before
    /// `create_doc_exclusive`/`CreateDoc` existed this did not compile
    /// — that non-compile IS the red step this task's TDD cycle
    /// records, since the method and type were simply absent.
    #[tokio::test]
    async fn exclusive_create_refuses_the_second_writer() {
        let dir = tempfile::tempdir().expect("tempdir");
        let location = Location::local_dir(dir.path().to_path_buf()).expect("local");
        let first = location
            .create_doc_exclusive("x/lease.json", b"a".to_vec())
            .await
            .expect("io");
        assert!(matches!(first, CreateDoc::Created(_)));
        let second = location
            .create_doc_exclusive("x/lease.json", b"b".to_vec())
            .await
            .expect("io");
        assert!(matches!(second, CreateDoc::AlreadyExists));

        // The loser's bytes never landed — the winner's are still there.
        let (bytes, _version) = location
            .read_doc_versioned("x/lease.json")
            .await
            .expect("io")
            .expect("present");
        assert_eq!(bytes, b"a");
    }

    /// `replace_doc_if`'s local arm carries no real CAS — only the
    /// durable-write shape — so any version value round-trips the
    /// bytes; this pins that the write itself lands and the temp
    /// sibling is cleaned up, matching `write_doc_durably`'s own
    /// atomicity test above.
    #[tokio::test]
    async fn replace_doc_if_round_trips_locally() {
        let dir = tempfile::tempdir().expect("tempdir");
        let location = Location::local_dir(dir.path().to_path_buf()).expect("local");
        let created = location
            .create_doc_exclusive("lease.json", b"one".to_vec())
            .await
            .expect("io");
        let CreateDoc::Created(version) = created else {
            panic!("fresh key must create");
        };

        let replaced = location
            .replace_doc_if("lease.json", b"two".to_vec(), version)
            .await
            .expect("replace succeeds locally");
        assert!(matches!(replaced, DocVersion::Local));

        let (bytes, _) = location
            .read_doc_versioned("lease.json")
            .await
            .expect("io")
            .expect("present");
        assert_eq!(bytes, b"two");
        assert!(
            !dir.path().join("lease.json.tmp").exists(),
            "no temp residue after replace"
        );
    }

    /// The lease's release step calls `delete_doc` best-effort — a
    /// takeover, or an earlier crashed release, may have already
    /// removed the document, and that must not be an error: deleting
    /// twice (or deleting what was never there) is `Ok(())` both times.
    #[tokio::test]
    async fn delete_doc_is_idempotent() {
        let dir = tempfile::tempdir().expect("tempdir");
        let location = Location::local_dir(dir.path().to_path_buf()).expect("local");

        // Never existed at all.
        location.delete_doc("never-was.json").await.expect("ok");

        location
            .create_doc_exclusive("lease.json", b"one".to_vec())
            .await
            .expect("io");
        location
            .delete_doc("lease.json")
            .await
            .expect("first delete");
        assert!(
            location
                .read_doc_versioned("lease.json")
                .await
                .expect("io")
                .is_none()
        );
        // Already gone — still Ok, not NotFound surfaced as an error.
        location
            .delete_doc("lease.json")
            .await
            .expect("second delete");
    }

    /// (Fix round 1, 037 US2) Pin the local arm's documented gap
    /// itself, not just assert it in prose: an out-of-band rewrite
    /// (standing in for a second process, since a single machine can
    /// have only one) makes the version `replace_doc_if` is handed
    /// genuinely stale, and the local arm still writes — no staleness
    /// signal exists there. That is the flip side of the doc comment's
    /// safety argument: it is sound only because `create_doc_exclusive`
    /// already keeps two LOCAL processes from both holding the lease at
    /// once, which this test does not (and cannot) exercise.
    #[tokio::test]
    async fn local_replace_has_no_staleness_signal() {
        let dir = tempfile::tempdir().expect("tempdir");
        let location = Location::local_dir(dir.path().to_path_buf()).expect("local");
        let created = location
            .create_doc_exclusive("lease.json", b"one".to_vec())
            .await
            .expect("io");
        let CreateDoc::Created(stale_version) = created else {
            panic!("fresh key must create");
        };

        // Out-of-band rewrite: bypasses `Location` entirely, the way a
        // takeover on a DIFFERENT machine would land on S3 — but here,
        // on one filesystem, nothing detects it.
        std::fs::write(dir.path().join("lease.json"), b"rewritten-out-of-band")
            .expect("simulated external rewrite");

        let outcome = location
            .replace_doc_if(
                "lease.json",
                b"replace-after-stale-read".to_vec(),
                stale_version,
            )
            .await;
        assert!(
            outcome.is_ok(),
            "documented gap: the local arm has no CAS, so a stale version still writes: \
             {outcome:?}"
        );
        let (bytes, _) = location
            .read_doc_versioned("lease.json")
            .await
            .expect("io")
            .expect("present");
        assert_eq!(bytes, b"replace-after-stale-read");
    }
}
