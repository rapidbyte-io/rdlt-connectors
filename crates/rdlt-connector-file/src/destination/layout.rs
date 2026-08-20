//! The layout vocabulary: every name this destination writes, the
//! path-safety rule, and the persisted commit log — all FROZEN (the
//! staged and final names must reproduce identically under WAL
//! replay, and the receipts are the exactly-once evidence).

use rdlt_connector_sdk::spi::error::DestinationError;

/// The persisted layout/commit-log version this build writes.
///
/// v2 (037 US4): the partition-directory encoding became injective
/// (`encode_partition_value`, replacing `path_safe`). This is a
/// greenfield format break, deliberately version-gated with NO
/// migration — a v1 (or absent-version, v0) manifest or commit log is
/// now REFUSED, not silently reinterpreted under the new encoding,
/// because the directory names it names may no longer exist or may
/// mean something else.
pub(super) const LAYOUT_FORMAT_VERSION: u32 = 2;

/// The hidden staging prefix.
pub(crate) const STAGING_DIR: &str = ".rdlt-staging";

/// The per-pipeline scope: a 12-hex identity hash, never the raw name.
pub(super) fn scope_of(pipeline: &str) -> String {
    rdlt_connector_sdk::spi::core::schema::ident_hash(pipeline, 12)
}

/// The state document's file name for one scope.
pub(super) fn state_file(scope: &str) -> String {
    format!("_rdlt_state.{scope}.json")
}

/// The commit log's file name for one scope.
pub(super) fn commits_file(scope: &str) -> String {
    format!("_rdlt_commits.{scope}.json")
}

/// The publish manifest's file name for one scope.
pub(super) fn manifest_file(scope: &str) -> String {
    format!("_rdlt_manifest.{scope}.json")
}

/// The session lease's file name for one scope (037 US2).
pub(super) fn lease_file(scope: &str) -> String {
    format!("_rdlt_lease.{scope}.json")
}

/// A staged part's tail under the staging prefix.
pub(super) fn staging_tail(scope: &str, load: &str, name: &str) -> String {
    format!("{STAGING_DIR}/{scope}/{load}/{name}")
}

/// A staged part's NAME under the load's staging directory: the table
/// as its OWN path segment, then `{slug}-{index}.{ext}` — deterministic
/// GIVEN the same part boundaries, so a crash-replay that splits its
/// rows the same way stages the same names it staged before. Since 034
/// the boundaries themselves need not reproduce (`roll_after_seconds`
/// is wall clock), so name determinism alone no longer carries
/// convergence — publish's [`Manifest`] sweep removes whatever a
/// differently-split predecessor left behind.
///
/// 030 review amendment to the gen-1 flat shape
/// `{load}-{table}-{slug}-{index}`: both table and slug may contain
/// `-`, so the flat join was AMBIGUOUS — (`events`, `us-east`) and
/// (`events-us`, `east`) collided to one staging file, the second
/// overwriting the first's bytes (the 029 colliding-path corruption
/// class, in the staging namespace). A path segment cannot contain
/// `/`, so the segment form is injective; the `{slug}-{index}` half is
/// unambiguous because the index is purely numeric and last. Staging
/// is TRANSIENT (reclaimed wholesale at open, never re-interpreted
/// across versions), so this is not a persisted-format change.
pub(super) fn staged_name(
    table: &str,
    partition: Option<&str>,
    index: usize,
    extension: &str,
) -> String {
    let slug = partition.unwrap_or("all");
    format!("{table}/{slug}-{index}.{extension}")
}

/// A published part's tail under the destination root. The index
/// counts per TABLE+PARTITION, so cross-table arrival order can never
/// change a final name.
pub(crate) fn final_tail(
    table: &str,
    partition: Option<&str>,
    load: &str,
    seq: u64,
    index: usize,
    extension: &str,
) -> String {
    match partition {
        Some(partition) => format!("{table}/{partition}/part-{load}-{seq}-{index}.{extension}"),
        None => format!("{table}/part-{load}-{seq}-{index}.{extension}"),
    }
}

/// Injective partition-value encoding (037 US4, layout v2). Survivors:
/// ASCII alphanumerics and `-` anywhere; `_` and `.` except at byte
/// position 0. Everything else — including `%` itself, `/`, control
/// bytes, every non-ASCII byte — becomes `%XX` (per UTF-8 byte,
/// uppercase hex). An empty value is `__empty__`.
///
/// Consequences the sentinels rely on: no encoded value ever begins
/// with `_` or `.`, so `__null__` / `__empty__` cannot collide with
/// any real encoded value; `/` never appears in the output
/// (`truncate::owns`'s depth rule depends on a partition slug never
/// itself containing a path separator); and dot-segments are
/// impossible — a partition value of `.` or `..` used to walk the
/// part OUT of its table directory via path resolution (030 review;
/// the retired `__dot__`/`__dotdot__` sentinels worked around it by
/// name). Here `.` at position 0 is never a survivor, so both encode
/// to a leading `%2E` and can never resolve as a directory-traversal
/// segment again.
///
/// Windows-reserved basenames (`CON`, `PRN`, `AUX`, `NUL`, `COM1`-`9`,
/// `LPT1`-`9`, case-insensitive, matched against the portion before
/// the first literal `.`) get their first byte escaped too, so the
/// written name is never one of those regardless of host OS. Because
/// every reserved prefix is pure ASCII letters — always survivors in
/// the ordinary path — this escaping can never coincide with a byte
/// this function would otherwise emit, so it does not disturb
/// injectivity.
pub(super) fn encode_partition_value(value: &str) -> String {
    if value.is_empty() {
        return "__empty__".to_owned();
    }
    let mut encoded = String::with_capacity(value.len());
    for (position, byte) in value.bytes().enumerate() {
        let survivor = byte.is_ascii_alphanumeric()
            || byte == b'-'
            || (position != 0 && (byte == b'_' || byte == b'.'));
        if survivor {
            encoded.push(byte as char);
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    escape_if_windows_reserved(encoded)
}

/// Basenames DOS/Windows treats as device names regardless of
/// extension, checked case-insensitively.
const WINDOWS_RESERVED_BASENAMES: &[&str] = &[
    "CON", "PRN", "AUX", "NUL", "COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8",
    "COM9", "LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9",
];

/// If `encoded`'s portion before the first literal `.` matches a
/// Windows-reserved basename, escape its first byte so the written
/// name is never reserved.
fn escape_if_windows_reserved(encoded: String) -> String {
    let basename = encoded.split('.').next().unwrap_or(&encoded);
    let is_reserved = WINDOWS_RESERVED_BASENAMES
        .iter()
        .any(|reserved| basename.eq_ignore_ascii_case(reserved));
    if !is_reserved {
        return encoded;
    }
    let bytes = encoded.as_bytes();
    let mut escaped = format!("%{:02X}", bytes[0]);
    escaped.push_str(&encoded[1..]);
    escaped
}

/// The NULL partition's directory.
pub(super) const NULL_PARTITION: &str = "__null__";

/// The persisted receipt log: the D3 planted-commit-log weld proof.
/// Receipts are retained for the LIFE of the destination — the SPI
/// commit contract is unconditional, and trimming would re-truncate
/// Replace targets on a redelivered trimmed load.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub(super) struct CommitLog {
    /// Absent in pre-versioning logs — decodes as v0, refused by
    /// [`version_gate`] since layout v2 (037 US4 D1: it also predates
    /// the current partition-directory encoding).
    #[serde(default)]
    pub format_version: u32,
    #[serde(default)]
    pub receipts: Vec<(String, u64)>,
}

/// The publish MANIFEST: the final names ONE commit intends to
/// publish, written durably before any of them is, so a retry of a
/// crashed attempt knows exactly which files its predecessor may have
/// left and removes the ones it no longer writes itself.
///
/// PIPELINE-SCOPED like the state doc and the commit log — which is
/// the safety property: final file names carry no pipeline marker and
/// load ids are only unique WITHIN a pipeline, so any sweep that
/// worked by parsing directory listings could, on a load-id collision
/// between two pipelines sharing one output table, delete the other
/// pipeline's rows. The manifest never names a file this pipeline did
/// not itself intend, so the sweep cannot either.
///
/// Only the LAST commit's manifest is kept. UNDER A WORKING WAL a
/// commit is only left behind once its receipt landed (replay
/// re-drives a pending commit under its ORIGINAL load id before
/// anything newer), so the superseded manifest has nothing left to
/// sweep. Without a workdir, or when a damaged WAL degrades to
/// re-extraction, the next attempt runs under a NEW load id, the old
/// manifest never matches, and its files are beyond this mechanism —
/// the file connector's cousin of iceberg's recorded N2. FIXED-BY-
/// REFUSAL (037 US3, same shape as iceberg's): the destination
/// declares `requires_durable_identity`, so the engine refuses to
/// start a run against it without a workdir — the no-WAL path this
/// comment describes cannot be reached from `rdlt run` at all.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub(super) struct Manifest {
    /// Absent in none — the manifest is new at layout v1.
    #[serde(default)]
    pub format_version: u32,
    #[serde(default)]
    pub load_id: String,
    #[serde(default)]
    pub commit_seq: u64,
    /// Final tails (relative to the output root) this commit publishes.
    #[serde(default)]
    pub names: Vec<String>,
}

/// The shared version gate for both persisted layout artifacts
/// (manifest, commit log). A STRICTLY newer version refuses as an
/// upgrade prompt, never a reset — resetting forgets receipts and
/// republishes. A STRICTLY older version — including absent, which
/// decodes as `0` (pre-versioning) — now ALSO refuses (037 US4 D1):
/// v2's partition-directory encoding is a greenfield format break
/// with no migration path, so an older document's file names can no
/// longer be trusted to mean what they say.
fn version_gate(doc: &str, file: &str, found: u32) -> Result<(), DestinationError> {
    if found > LAYOUT_FORMAT_VERSION {
        return Err(DestinationError::fatal(format!(
            "{doc} `{file}` format v{found} is newer than this build supports \
             (v{LAYOUT_FORMAT_VERSION}); upgrade rdlt instead of resetting"
        )));
    }
    if found < LAYOUT_FORMAT_VERSION {
        return Err(DestinationError::fatal(format!(
            "{doc} `{file}` format v{found} predates this build (v{LAYOUT_FORMAT_VERSION}): the \
             partition-directory encoding changed; point the destination at a fresh path or \
             delete the old output"
        )));
    }
    Ok(())
}

impl Manifest {
    /// Decode verbatim bytes; absent means "no prior attempt".
    pub(super) fn decode(
        bytes: Option<&[u8]>,
        file: &str,
    ) -> Result<Option<Self>, DestinationError> {
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let manifest: Self = serde_json::from_slice(bytes)
            .map_err(|e| DestinationError::fatal(format!("unreadable manifest `{file}`: {e}")))?;
        version_gate("manifest", file, manifest.format_version)?;
        Ok(Some(manifest))
    }
}

impl CommitLog {
    /// Decode verbatim bytes; absent means empty.
    pub(super) fn decode(bytes: Option<&[u8]>, file: &str) -> Result<Self, DestinationError> {
        let Some(bytes) = bytes else {
            return Ok(Self::default());
        };
        let log: Self = serde_json::from_slice(bytes)
            .map_err(|e| DestinationError::fatal(format!("unreadable commit log `{file}`: {e}")))?;
        log.check_readable(file)?;
        Ok(log)
    }

    /// See [`version_gate`]: newer refuses as an upgrade prompt, older
    /// (including absent/v0) now also refuses as predating v2's
    /// encoding break.
    pub(super) fn check_readable(&self, file: &str) -> Result<(), DestinationError> {
        version_gate("commit log", file, self.format_version)
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    /// The frozen name shapes, literally.
    #[test]
    fn every_name_shape_is_the_frozen_one() {
        assert_eq!(state_file("abc123def456"), "_rdlt_state.abc123def456.json");
        assert_eq!(commits_file("abc"), "_rdlt_commits.abc.json");
        assert_eq!(lease_file("abc"), "_rdlt_lease.abc.json");
        assert_eq!(
            staging_tail("abc", "load-1", "t/all-0.parquet"),
            ".rdlt-staging/abc/load-1/t/all-0.parquet"
        );
        assert_eq!(staged_name("t", None, 0, "parquet"), "t/all-0.parquet");
        assert_eq!(staged_name("t", Some("us"), 2, "jsonl"), "t/us-2.jsonl");
        // The 030 injectivity amendment: dashed table and slug pairs
        // that collided under the gen-1 flat join stay distinct.
        assert_ne!(
            staged_name("events", Some("us-east"), 0, "parquet"),
            staged_name("events-us", Some("east"), 0, "parquet"),
        );
        assert_eq!(
            final_tail("t", None, "l", 3, 0, "parquet"),
            "t/part-l-3-0.parquet"
        );
        assert_eq!(
            final_tail("t", Some("us"), "l", 3, 1, "jsonl"),
            "t/us/part-l-3-1.jsonl"
        );
        assert_eq!(scope_of("p").len(), 12, "12-hex scope");
    }

    /// The v2 encoding: exact-value pins for the survivors, the
    /// escape, the empty sentinel, and the sentinel-collision proof
    /// (`__null__` is never a possible ENCODED output, because a
    /// leading `_` always escapes).
    #[test]
    fn partition_encoding_is_injective_and_sentinel_safe() {
        assert_eq!(encode_partition_value("us-east"), "us-east");
        assert_eq!(encode_partition_value("a b/c"), "a%20b%2Fc");
        assert_eq!(encode_partition_value("a_b"), "a_b");
        assert_ne!(encode_partition_value("a/b"), encode_partition_value("a_b"));
        assert_eq!(encode_partition_value(""), "__empty__");
        assert_eq!(encode_partition_value("__null__"), "%5F_null__");
        assert!(encode_partition_value(".").starts_with('%'));
        assert!(encode_partition_value("..").starts_with('%'));
        assert!(encode_partition_value("CON").starts_with('%'));
    }

    proptest! {
        /// Injectivity, no path separator, and the sentinel-safety
        /// invariant, over arbitrary strings.
        #[test]
        fn encoding_is_injective_slashfree_and_never_leads_underscore(
            a in ".{0,40}", b in ".{0,40}"
        ) {
            let ea = encode_partition_value(&a);
            prop_assert!(!ea.contains('/'));
            prop_assert!(!ea.starts_with('_') || a.is_empty());
            prop_assert!(!ea.starts_with('.'));
            if a != b { prop_assert_ne!(ea, encode_partition_value(&b)); }
        }
    }

    /// The commit log: v0 (absent) and v1 (the retired encoding) both
    /// refuse as predating v2, the current version round-trips, and a
    /// strictly newer version refuses with the upgrade prompt.
    #[test]
    fn the_commit_log_versioning_is_upgrade_not_reset() {
        let err = CommitLog::decode(Some(br#"{"receipts": [["load-x", 1]]}"#), "log.json")
            .expect_err("absent format_version = v0 = also predates");
        assert!(
            format!("{err}").contains(
                "commit log `log.json` format v0 predates this build (v2): the \
                 partition-directory encoding changed; point the destination at a fresh path \
                 or delete the old output"
            ),
            "{err}"
        );

        let v1 = br#"{"format_version": 1, "receipts": [["load-x", 1]]}"#;
        let err = CommitLog::decode(Some(v1), "log.json").expect_err("v1 predates v2 too");
        assert!(
            format!("{err}").contains(
                "commit log `log.json` format v1 predates this build (v2): the \
                 partition-directory encoding changed; point the destination at a fresh path \
                 or delete the old output"
            ),
            "{err}"
        );

        let current = CommitLog {
            format_version: LAYOUT_FORMAT_VERSION,
            receipts: vec![("l".into(), 2)],
        };
        let bytes = serde_json::to_vec(&current).expect("encodes");
        assert_eq!(
            CommitLog::decode(Some(&bytes), "f").expect("round-trips"),
            current
        );

        let future = br#"{"format_version": 99, "receipts": []}"#;
        let err = CommitLog::decode(Some(future), "log.json").expect_err("refuses");
        assert!(
            format!("{err}").contains(
                "commit log `log.json` format v99 is newer than this build supports (v2); \
                 upgrade rdlt instead of resetting"
            ),
            "{err}"
        );

        let err = CommitLog::decode(Some(b"not json"), "log.json").expect_err("unreadable");
        assert!(
            format!("{err}").contains("unreadable commit log `log.json`"),
            "{err}"
        );
    }
}
