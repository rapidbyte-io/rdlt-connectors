//! The object-store side of [`Location`](super::Location): the one
//! place credentials are revealed into a client, a listing that drains
//! every continuation page or fails typed, a chunked range reader for
//! resumed tails, and the key-space primitives behind the
//! destination's staging/publish protocol.

use futures::StreamExt as _;
use object_store::ObjectStore as _;
use object_store::path::Path as Key;
use object_store::{PutMode, PutOptions, UpdateVersion};
use rdlt_connector_sdk::spi::{DestinationError, SourceError};

use super::kind::{CreateDoc, DocVersion, StaleDocVersion};
use super::options::S3Options;
use crate::source::cursor::FileMeta;

/// The glob metacharacters this crate recognises in a pattern.
const GLOB_META: [char; 3] = ['*', '?', '['];

/// Construct the raw client. This function is the crate's entire
/// `Secret::reveal` surface — a credential audit reads it and nothing
/// else.
pub(crate) fn build_store(options: &S3Options) -> Result<object_store::aws::AmazonS3, String> {
    let region = options.region.as_deref().unwrap_or("us-east-1");
    object_store::aws::AmazonS3Builder::new()
        .with_endpoint(&options.endpoint)
        .with_bucket_name(&options.bucket)
        .with_region(region)
        .with_access_key_id(options.access_key.reveal())
        .with_secret_access_key(options.secret_key.reveal())
        .with_virtual_hosted_style_request(!options.path_style)
        .with_unsigned_payload(options.unsigned_payload)
        .with_allow_http(true)
        .build()
        .map_err(|e| {
            format!(
                "s3 location `{}` bucket `{}`: {e}",
                options.endpoint, options.bucket
            )
        })
}

/// Severity for a destination-side store failure comes from the one
/// shared recoverability rulebook — never re-derived locally.
fn dest_failure(cause: object_store::Error) -> DestinationError {
    if rdlt_connector_sdk::spi::store::is_recoverable(&cause) {
        DestinationError::transient(cause.to_string())
    } else {
        DestinationError::fatal(cause.to_string())
    }
}

/// One connected bucket. The source builds it with an empty `prefix`
/// and speaks full keys; the destination hangs every tail under its
/// prefix.
#[derive(Debug, Clone)]
pub(crate) struct S3Location {
    store: object_store::aws::AmazonS3,
    endpoint: String,
    bucket: String,
    /// Key prefix for the write half; the read half leaves it empty.
    prefix: String,
}

impl S3Location {
    /// Read-half constructor: no prefix, callers name keys in full.
    pub(crate) fn connect(options: &S3Options) -> Result<Self, SourceError> {
        Ok(Self {
            store: build_store(options).map_err(SourceError::fatal)?,
            endpoint: options.endpoint.clone(),
            bucket: options.bucket.clone(),
            prefix: String::new(),
        })
    }

    /// Write-half constructor: `prefix` becomes the root every tail
    /// hangs under.
    pub(crate) fn connect_for_dest(
        options: &S3Options,
        prefix: String,
    ) -> Result<Self, DestinationError> {
        match Self::connect(options) {
            Ok(mut location) => {
                location.prefix = prefix;
                Ok(location)
            }
            Err(e) => Err(DestinationError::fatal(e.to_string())),
        }
    }

    /// Source-side failure rendering. The transient/fatal call is made
    /// before any text is composed — by the SPI's shared recoverability
    /// predicate — which leaves the branches below with nothing to
    /// decide except phrasing.
    fn read_failure(&self, verb: &str, subject: &str, cause: object_store::Error) -> SourceError {
        let heading = format!(
            "{verb} `{subject}` (s3 `{}` bucket `{}`)",
            self.endpoint, self.bucket
        );
        if rdlt_connector_sdk::spi::store::is_recoverable(&cause) {
            return SourceError::transient(format!("{heading}: {cause}"));
        }
        match &cause {
            object_store::Error::NotFound { .. } => {
                SourceError::fatal(format!("{heading}: not found"))
            }
            object_store::Error::Unauthenticated { .. }
            | object_store::Error::PermissionDenied { .. } => SourceError::fatal(format!(
                "{heading}: unauthorized — check credentials/bucket"
            )),
            _ => SourceError::fatal(format!("{heading}: {cause}")),
        }
    }

    /// The literal key prefix a server-side listing can scope to:
    /// everything up to (and including) the last `/` before the first
    /// glob metacharacter. No metacharacter means the whole pattern is
    /// literal.
    fn listing_scope(pattern: &str) -> &str {
        let Some(meta_at) = pattern.find(GLOB_META) else {
            return pattern;
        };
        match pattern[..meta_at].rfind('/') {
            Some(slash) => &pattern[..=slash],
            None => "",
        }
    }

    /// The complete listing for one pattern: every continuation page
    /// drained, or a typed failure. The local ambiguity rule carries
    /// over unchanged — a single HEAD first decides whether a
    /// metacharacter-bearing pattern names a real object, and only
    /// when it does not is the pattern read as a glob, in which
    /// `*`/`?` never span a `/` (a staged key must never satisfy a
    /// data glob).
    pub(crate) async fn list(&self, pattern: &str) -> Result<Vec<FileMeta>, SourceError> {
        // The store hands keys back in normalized form — leading
        // slash gone, empty segments collapsed — but the matcher is
        // compiled from whatever the operator typed. Left raw, a
        // local-path habit like `path: /data/*.jsonl` compiles into a
        // pattern no normalized key can ever satisfy, and the run
        // reports zero files with no hint why (030 review). So the
        // pattern's segments get the same normalization first.
        let mut normalized = String::with_capacity(pattern.len());
        for segment in pattern.split('/').filter(|s| !s.is_empty()) {
            if !normalized.is_empty() {
                normalized.push('/');
            }
            normalized.push_str(segment);
        }
        let pattern = normalized.as_str();

        let wildcarded = pattern.contains(GLOB_META);
        match self.store.head(&Key::from(pattern)).await {
            Ok(head) => {
                return Ok(vec![FileMeta {
                    path: pattern.to_owned(),
                    size_units: head.size,
                    mtime_ms: epoch_millis(head.last_modified),
                    etag: head.e_tag,
                }]);
            }
            Err(object_store::Error::NotFound { .. }) => {
                if !wildcarded {
                    return Err(SourceError::fatal(format!(
                        "object `{pattern}` (s3 `{}` bucket `{}`): not found",
                        self.endpoint, self.bucket
                    )));
                }
            }
            Err(e) => return Err(self.read_failure("object", pattern, e)),
        }

        let matcher = glob::Pattern::new(pattern)
            .map_err(|e| SourceError::fatal(format!("invalid glob `{pattern}`: {e}")))?;
        // `require_literal_separator` confines `*`/`?` to a single
        // segment. `require_literal_leading_dot` is the load-bearing
        // one: it bars EVERY wildcard, `**` included, from matching a
        // dot-led name, and that — not the separator rule — is what
        // hides `.rdlt-staging` from data globs (030 review found a
        // recursive glob over a shared prefix reading uncommitted
        // staged parts without it).
        let rules = glob::MatchOptions {
            require_literal_separator: true,
            require_literal_leading_dot: true,
            ..Default::default()
        };

        let scope = Self::listing_scope(pattern);
        let scope_key = (!scope.is_empty()).then(|| Key::from(scope));
        let mut pages = self.store.list(scope_key.as_ref());
        let mut found = Vec::new();
        while let Some(next) = pages.next().await {
            let object = next.map_err(|e| self.read_failure("listing", pattern, e))?;
            let key = object.location.to_string();
            if !matcher.matches_with(&key, rules) {
                continue;
            }
            found.push(FileMeta {
                path: key,
                size_units: object.size,
                // The mtime is the rewrite tripwire's second leg: on an
                // S3-compatible store that serves no etags, a same-size
                // rewrite would otherwise go completely undetected
                // (docket S10).
                mtime_ms: epoch_millis(object.last_modified),
                etag: object.e_tag,
            });
        }
        found.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(found)
    }

    /// A streaming GET beginning at `start` — how a resumed tail picks
    /// up mid-object.
    pub(crate) async fn open_from(
        &self,
        name: &str,
        start: u64,
    ) -> Result<super::ByteReader, SourceError> {
        let options = object_store::GetOptions {
            range: (start > 0).then_some(object_store::GetRange::Offset(start)),
            ..Default::default()
        };
        let body = self
            .store
            .get_opts(&Key::from(name), options)
            .await
            .map_err(|e| self.read_failure("reading", name, e))?;
        Ok(super::ByteReader::S3(S3Reader {
            chunks: body.into_stream().boxed(),
            held: bytes::Bytes::new(),
            subject: format!("{name} (s3 `{}` bucket `{}`)", self.endpoint, self.bucket),
        }))
    }

    // ---- write-half key space and operations -------------------------------

    /// Expand a tail into its full object key under this location's
    /// prefix.
    fn object_key(&self, tail: &str) -> Key {
        if self.prefix.is_empty() {
            return Key::from(tail);
        }
        Key::from(format!("{}/{tail}", self.prefix.trim_end_matches('/')))
    }

    /// Full key of one file that lives under `{table}/`.
    pub(crate) fn key_of_table(&self, table: &str, tail: &str) -> Key {
        self.object_key(&format!("{table}/{tail}"))
    }

    /// The table's root key, without a trailing separator.
    pub(crate) fn key_of_table_root(&self, table: &str) -> String {
        self.object_key(table).to_string()
    }

    pub(crate) async fn put(&self, tail: &str, bytes: Vec<u8>) -> Result<(), DestinationError> {
        self.store
            .put(&self.object_key(tail), bytes::Bytes::from(bytes).into())
            .await
            .map_err(dest_failure)?;
        Ok(())
    }

    pub(crate) async fn copy(
        &self,
        from_tail: &str,
        to_tail: &str,
    ) -> Result<(), DestinationError> {
        self.store
            .copy(&self.object_key(from_tail), &self.object_key(to_tail))
            .await
            .map_err(dest_failure)
    }

    /// Delete where already-gone counts as done — the shape a replayed
    /// finalize needs.
    pub(crate) async fn delete_idempotent(&self, tail: &str) -> Result<(), DestinationError> {
        match self.store.delete(&self.object_key(tail)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(dest_failure(e)),
        }
    }

    /// Delete and ignore the outcome; the next open's reclaim sweep
    /// converges whatever this missed.
    pub(crate) async fn delete_best_effort(&self, tail: &str) {
        let _ = self.store.delete(&self.object_key(tail)).await;
    }

    /// Every full key below `tail`, gathered by a server-side
    /// segment-scoped listing.
    pub(crate) async fn list_keys(&self, tail: &str) -> Result<Vec<Key>, DestinationError> {
        let mut pages = self.store.list(Some(&self.object_key(tail)));
        let mut keys = Vec::new();
        while let Some(next) = pages.next().await {
            keys.push(next.map_err(dest_failure)?.location);
        }
        Ok(keys)
    }

    pub(crate) async fn delete_key(&self, key: &Key) -> Result<(), DestinationError> {
        self.store.delete(key).await.map_err(dest_failure)
    }

    /// Delete a tail where absence is success — the manifest sweep's
    /// shape (intent covers more than a crash let land).
    pub(crate) async fn delete_tail_if_present(&self, tail: &str) -> Result<(), DestinationError> {
        match self.store.delete(&self.object_key(tail)).await {
            Err(object_store::Error::NotFound { .. }) | Ok(()) => Ok(()),
            Err(e) => Err(dest_failure(e)),
        }
    }

    pub(crate) async fn get_key(&self, key: &Key) -> Result<Vec<u8>, DestinationError> {
        let body = self.store.get(key).await.map_err(dest_failure)?;
        let bytes = body.bytes().await.map_err(dest_failure)?;
        Ok(bytes.to_vec())
    }

    /// One document's bytes; absence is `None`, not an error.
    pub(crate) async fn read_doc(&self, name: &str) -> Result<Option<Vec<u8>>, DestinationError> {
        match self.store.get(&self.object_key(name)).await {
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(dest_failure(e)),
            Ok(body) => Ok(Some(body.bytes().await.map_err(dest_failure)?.to_vec())),
        }
    }

    /// Exclusive create: `PutMode::Create`. `Error::AlreadyExists` here
    /// is the DETERMINED outcome of a conditional put this call
    /// deliberately issues — not the ambiguous "conflicting operation
    /// in progress, try again" case `dest_failure`'s shared
    /// recoverability rule exists to retry (rulebook in `store.rs`,
    /// which never sees a conditional request from anywhere else in
    /// this crate). So it is matched BEFORE that shared classifier and
    /// handed back as data, never as an error.
    pub(crate) async fn create_doc_exclusive(
        &self,
        name: &str,
        bytes: Vec<u8>,
    ) -> Result<CreateDoc, DestinationError> {
        match self
            .store
            .put_opts(
                &self.object_key(name),
                bytes::Bytes::from(bytes).into(),
                PutOptions::from(PutMode::Create),
            )
            .await
        {
            Ok(put) => Ok(CreateDoc::Created(DocVersion::S3(UpdateVersion::from(put)))),
            Err(object_store::Error::AlreadyExists { .. }) => Ok(CreateDoc::AlreadyExists),
            Err(e) => Err(dest_failure(e)),
        }
    }

    /// One document's bytes plus the CAS handle a later
    /// `replace_doc_if` needs; absence is `None`.
    pub(crate) async fn read_doc_versioned(
        &self,
        name: &str,
    ) -> Result<Option<(Vec<u8>, DocVersion)>, DestinationError> {
        match self.store.get(&self.object_key(name)).await {
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(e) => Err(dest_failure(e)),
            Ok(result) => {
                let version = UpdateVersion {
                    e_tag: result.meta.e_tag.clone(),
                    version: result.meta.version.clone(),
                };
                let bytes = result.bytes().await.map_err(dest_failure)?;
                Ok(Some((bytes.to_vec(), DocVersion::S3(version))))
            }
        }
    }

    /// CAS replace: `PutMode::Update`. `Error::Precondition` here is
    /// the same kind of determined outcome as `AlreadyExists` above —
    /// this call's own conditional request lost the race, which is
    /// exactly what a lease's stale heartbeat needs to learn — so
    /// instead of riding the shared recoverability rule (which would
    /// call it worth retrying, the right answer for an UNconditional
    /// put but not this one), it carries [`StaleDocVersion`] as the
    /// `DestinationError`'s SOURCE. That is what makes it a genuinely
    /// TYPED outcome the caller can [`super::kind::is_stale_version`]-
    /// check, rather than a fatal error indistinguishable from any
    /// other one except by reading its rendered text.
    pub(crate) async fn replace_doc_if(
        &self,
        name: &str,
        bytes: Vec<u8>,
        version: UpdateVersion,
    ) -> Result<DocVersion, DestinationError> {
        match self
            .store
            .put_opts(
                &self.object_key(name),
                bytes::Bytes::from(bytes).into(),
                PutOptions::from(PutMode::Update(version)),
            )
            .await
        {
            Ok(put) => Ok(DocVersion::S3(UpdateVersion::from(put))),
            Err(object_store::Error::Precondition { .. }) => {
                Err(DestinationError::fatal(StaleDocVersion {
                    name: name.to_owned(),
                }))
            }
            Err(e) => Err(dest_failure(e)),
        }
    }

    /// Delete where absence already counts as done — the lease's
    /// best-effort release must not fail just because a takeover
    /// already removed the document.
    pub(crate) async fn delete_doc(&self, name: &str) -> Result<(), DestinationError> {
        match self.store.delete(&self.object_key(name)).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(dest_failure(e)),
        }
    }
}

/// Sequential reader over a streaming GET, buffering the chunk in
/// flight. A transport failure mid-stream surfaces as a RETRYABLE io
/// kind when the shared rulebook says so, letting the consumer call it
/// transient instead of killing the run — recoverability preserved
/// across the io seam.
pub(crate) struct S3Reader {
    chunks: futures::stream::BoxStream<'static, object_store::Result<bytes::Bytes>>,
    held: bytes::Bytes,
    subject: String,
}

impl std::fmt::Debug for S3Reader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Reader")
            .field("subject", &self.subject)
            .finish_non_exhaustive()
    }
}

impl S3Reader {
    fn io_error(&self, cause: object_store::Error) -> std::io::Error {
        let kind = if rdlt_connector_sdk::spi::store::is_recoverable(&cause) {
            std::io::ErrorKind::ConnectionReset
        } else {
            std::io::ErrorKind::Other
        };
        std::io::Error::new(kind, format!("reading {}: {cause}", self.subject))
    }

    pub(crate) async fn read_full(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut done = 0;
        while done < buf.len() {
            if self.held.is_empty() {
                match self.chunks.next().await {
                    Some(Ok(chunk)) => self.held = chunk,
                    Some(Err(cause)) => return Err(self.io_error(cause)),
                    None => return Ok(done),
                }
            }
            let n = self.held.len().min(buf.len() - done);
            buf[done..done + n].copy_from_slice(&self.held[..n]);
            bytes::Buf::advance(&mut self.held, n);
            done += n;
        }
        Ok(done)
    }
}

/// The service's stamped modification time as ms since the epoch; a
/// pre-1970 stamp reads as absent instead of wrapping around.
fn epoch_millis(t: chrono::DateTime<chrono::Utc>) -> Option<u64> {
    u64::try_from(t.timestamp_millis()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The scope runs to the last separator before the first
    /// metacharacter; with no metacharacter the pattern IS the scope.
    #[test]
    fn the_listing_prefix_stops_at_the_first_metacharacter() {
        assert_eq!(
            S3Location::listing_scope("landed/2026/*.jsonl"),
            "landed/2026/"
        );
        assert_eq!(S3Location::listing_scope("landed/y=*/f.jsonl"), "landed/");
        assert_eq!(S3Location::listing_scope("*.jsonl"), "");
        assert_eq!(
            S3Location::listing_scope("plain/key.jsonl"),
            "plain/key.jsonl"
        );
    }

    /// Well-formed options produce a client — the boundary holds.
    #[test]
    fn a_valid_store_builds() {
        build_store(&S3Options::new("http://127.0.0.1:9000", "b", "ak", "sk")).expect("builds");
    }
}
