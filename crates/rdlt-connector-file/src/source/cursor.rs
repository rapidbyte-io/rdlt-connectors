//! The cursor rulebook: the persisted per-file progress record, the
//! two planners that turn a listing plus a cursor into read tasks,
//! and the resume checks that keep a remembered offset honest.
//!
//! The wire shape is a PERSISTED FORMAT (frozen): keys `done`/`size`/
//! `eol` with additive optional `mtime_ms`/`etag`/`tail_hash`/
//! `row_groups_hash`. Units are polymorphic by design — bytes for
//! plain jsonl, row groups for parquet, whole-file bytes for
//! csv/compressed — and complete means `done == size`.
//!
//! Entries are retained for the LIFE of the pipeline state,
//! deliberately never pruned: a pruned path that reappears re-reads
//! from zero and DUPLICATES under Append. The cost is ~150-250 bytes
//! per path ever seen; the operator's levers are a narrower pattern
//! or cleared state.

use std::collections::BTreeMap;

use rdlt_connector_sdk::spi::core::cursor::Cursor;
use rdlt_connector_sdk::spi::error::SourceError;

/// The persisted format version this crate writes.
const CURSOR_FORMAT_VERSION: u32 = 1;

/// The resume-verification window: the last `min(done, 4096)` consumed
/// bytes are re-hashed before any remembered offset is trusted.
pub(crate) const TAIL_WINDOW: u64 = 4096;

/// The whole persisted cursor: one record per path ever matched.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FileCursor {
    #[serde(default = "default_version")]
    pub format_version: u32,
    #[serde(default)]
    pub files: BTreeMap<String, FileProgress>,
}

fn default_version() -> u32 {
    CURSOR_FORMAT_VERSION
}

// Manual, because the serde default fn applies only at DESERIALIZE:
// a derived Default would mint version-0 cursors (the wire pin caught
// exactly that in this rewrite).
impl Default for FileCursor {
    fn default() -> Self {
        Self {
            format_version: CURSOR_FORMAT_VERSION,
            files: BTreeMap::new(),
        }
    }
}

/// One file's progress. The serde renames ARE the wire format; the
/// Rust names say what the numbers mean.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct FileProgress {
    /// Units consumed and checkpointed (bytes or row groups).
    #[serde(rename = "done")]
    pub done_units: u64,
    /// The file's total units as of the last listing.
    #[serde(rename = "size")]
    pub size_units: u64,
    /// Did consumption end at a record boundary? Pre-tripwire cursors
    /// lack the key; true is the benign reading.
    #[serde(rename = "eol", default = "default_eol")]
    pub ended_at_record_boundary: bool,
    /// Modification time in ms since epoch, SERIALIZED even when None
    /// (the frozen v1 wire shape). Local listings stamp the
    /// filesystem's; S3 listings stamp the service's `last_modified`,
    /// the rewrite tripwire's second leg beside the etag (docket S10 —
    /// an etag-less store would otherwise have no same-size rewrite
    /// detection at all).
    #[serde(default)]
    pub mtime_ms: Option<u64>,
    /// S3 etag from the listing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    /// blake3 hex of the consumed tail window — plain jsonl only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tail_hash: Option<String>,
    /// The parquet prefix digest — parquet only; additive at v1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_groups_hash: Option<String>,
}

fn default_eol() -> bool {
    true
}

/// One file's listing metadata, as the planners consume it.
#[derive(Debug, Clone, PartialEq)]
pub struct FileMeta {
    pub path: String,
    pub size_units: u64,
    pub mtime_ms: Option<u64>,
    pub etag: Option<String>,
}

/// What a resume must verify before its offset is trusted.
#[derive(Debug, Clone, PartialEq)]
pub enum ResumeCheck {
    /// Re-hash `window` bytes ending at the resume offset.
    TailBytes { window: u64, hash: String },
    /// Re-derive the parquet prefix digest over the consumed groups.
    RowGroupPrefix { hash: String },
}

/// One planned read.
#[derive(Debug, Clone, PartialEq)]
pub struct FileTask {
    /// The cursor key: the object key or the operator-named path.
    pub path: String,
    /// Where to actually READ — a staged local copy for fetched S3
    /// objects; absent means `path` itself.
    pub read_path: Option<String>,
    /// First unit to read.
    pub start: u64,
    pub size_units: u64,
    pub mtime_ms: Option<u64>,
    pub etag: Option<String>,
    pub resume_check: Option<ResumeCheck>,
}

impl FileCursor {
    /// Decode a persisted cursor; absent means empty; unreadable is
    /// typed (state is precious — silently starting over duplicates).
    /// A STRICTLY newer format refuses as an upgrade prompt for the
    /// same reason: a future shape would deserialize as EMPTY through
    /// the serde defaults and silently re-read everything (030 review
    /// — the commit log had this guard, the cursor did not).
    pub fn decode(cursor: Option<&Cursor>) -> Result<Self, SourceError> {
        let Some(cursor) = cursor else {
            return Ok(Self::default());
        };
        let decoded: Self = serde_json::from_value(cursor.as_value().clone())
            .map_err(|e| SourceError::fatal(format!("unreadable file cursor: {e}")))?;
        if decoded.format_version > CURSOR_FORMAT_VERSION {
            return Err(SourceError::fatal(format!(
                "file cursor format v{} is newer than this build supports \
                 (v{CURSOR_FORMAT_VERSION}); upgrade rdlt instead of clearing state",
                decoded.format_version
            )));
        }
        Ok(decoded)
    }

    /// Encode for the checkpoint channel.
    pub fn encode(&self) -> Cursor {
        Cursor::new(serde_json::to_value(self).expect("cursor serialization"))
    }

    /// Record progress for one path (insert or overwrite).
    pub fn record(&mut self, path: &str, progress: FileProgress) {
        self.files.insert(path.to_owned(), progress);
    }

    /// The byte/row-group planner (plain jsonl, parquet): fresh files
    /// start at zero, complete files skip, growth resumes behind a
    /// verification, and every impossible history is a typed refusal.
    pub fn plan(&self, listing: &[FileMeta]) -> Result<Vec<FileTask>, SourceError> {
        let mut tasks = Vec::new();
        for meta in listing {
            let Some(record) = self.files.get(&meta.path) else {
                tasks.push(fresh(meta));
                continue;
            };
            check_shrink(meta, record)?;
            check_rewrite(meta, record)?;
            if record.done_units < record.size_units || record.done_units < meta.size_units {
                if !record.ended_at_record_boundary {
                    return Err(SourceError::fatal(format!(
                        "file `{}` grew after a run that consumed an unterminated final \
                         line; the recorded offset {} points mid-record — clear it from \
                         the pipeline state or restore the file",
                        meta.path, record.done_units
                    )));
                }
                tasks.push(FileTask {
                    path: meta.path.clone(),
                    read_path: None,
                    start: record.done_units,
                    size_units: meta.size_units,
                    mtime_ms: meta.mtime_ms,
                    etag: meta.etag.clone(),
                    resume_check: resume_check_for(record),
                });
            }
            // done == size with silent tripwires: complete, skip.
        }
        Ok(tasks)
    }

    /// The whole-file planner (csv + compressed jsonl): these formats
    /// never grow in place, so ANY size change is a typed refusal;
    /// incomplete means re-read WHOLE from zero (crash re-delivery —
    /// exactly-once is the keyed merge/dedup layer's job).
    pub fn plan_whole(&self, listing: &[FileMeta]) -> Result<Vec<FileTask>, SourceError> {
        let mut tasks = Vec::new();
        for meta in listing {
            let Some(record) = self.files.get(&meta.path) else {
                tasks.push(fresh(meta));
                continue;
            };
            if meta.size_units != record.size_units {
                return Err(SourceError::fatal(format!(
                    "file `{}` changed size ({} → {}) — whole-file formats (csv, \
                     compressed) never grow in place; deliver new data as a new file, or \
                     clear this file from the pipeline state",
                    meta.path, record.size_units, meta.size_units
                )));
            }
            check_rewrite(meta, record)?;
            if record.done_units < record.size_units {
                tasks.push(fresh(meta));
            }
        }
        Ok(tasks)
    }
}

fn fresh(meta: &FileMeta) -> FileTask {
    FileTask {
        path: meta.path.clone(),
        read_path: None,
        start: 0,
        size_units: meta.size_units,
        mtime_ms: meta.mtime_ms,
        etag: meta.etag.clone(),
        resume_check: None,
    }
}

fn check_shrink(meta: &FileMeta, record: &FileProgress) -> Result<(), SourceError> {
    if meta.size_units < record.size_units || record.done_units > meta.size_units {
        return Err(SourceError::fatal(format!(
            "file `{}` shrank or was rewritten (recorded {} of {} bytes, now {}); \
             refusing to read from a stale offset — clear it from the pipeline state or \
             restore the file",
            meta.path, record.done_units, record.size_units, meta.size_units
        )));
    }
    Ok(())
}

/// The same-size rewrite tripwire: identical size with a DIFFERENT
/// etag (S3) or mtime (local) is a replacement, never progress. Both
/// sides must be present to speak — an absent pair is silent.
fn check_rewrite(meta: &FileMeta, record: &FileProgress) -> Result<(), SourceError> {
    if meta.size_units != record.size_units {
        return Ok(());
    }
    let etag_differs = matches!(
        (&meta.etag, &record.etag),
        (Some(a), Some(b)) if a != b
    );
    let mtime_differs = matches!(
        (meta.mtime_ms, record.mtime_ms),
        (Some(a), Some(b)) if a != b
    );
    if etag_differs {
        return Err(SourceError::fatal(format!(
            "file `{}` was rewritten in place (same size, different etag); refusing to \
             trust recorded progress — clear it from the pipeline state or restore the \
             object",
            meta.path
        )));
    }
    if mtime_differs {
        return Err(SourceError::fatal(format!(
            "file `{}` was rewritten in place (same size, but modified since the last \
             run); refusing to trust recorded progress — clear it from the pipeline \
             state or restore the file",
            meta.path
        )));
    }
    Ok(())
}

/// Which verification a resume arms. A record holding BOTH hashes was
/// written by two different readers — verifying either would verify
/// the wrong thing, so neither is trusted; a record holding none
/// (first post-upgrade resume) carries no check; zero consumed units
/// arm nothing.
pub(crate) fn resume_check_for(record: &FileProgress) -> Option<ResumeCheck> {
    if record.done_units == 0 {
        return None;
    }
    match (&record.tail_hash, &record.row_groups_hash) {
        (Some(_), Some(_)) | (None, None) => None,
        (Some(tail), None) => Some(ResumeCheck::TailBytes {
            window: record.done_units.min(TAIL_WINDOW),
            hash: tail.clone(),
        }),
        (None, Some(groups)) => Some(ResumeCheck::RowGroupPrefix {
            hash: groups.clone(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(path: &str, size: u64) -> FileMeta {
        FileMeta {
            path: path.into(),
            size_units: size,
            mtime_ms: Some(1_000),
            etag: None,
        }
    }

    /// THE WIRE SHAPE, pinned literally: the frozen keys, the
    /// serialized-when-None mtime, and the skipped-when-None optionals.
    #[test]
    fn the_persisted_wire_shape_is_the_frozen_one() {
        let mut cursor = FileCursor::default();
        cursor.record(
            "a.jsonl",
            FileProgress {
                done_units: 10,
                size_units: 20,
                ended_at_record_boundary: false,
                mtime_ms: None,
                etag: None,
                tail_hash: Some("abc".into()),
                row_groups_hash: None,
            },
        );
        let value = serde_json::to_value(&cursor).expect("encodes");
        assert_eq!(
            value,
            serde_json::json!({
                "format_version": 1,
                "files": {"a.jsonl": {
                    "done": 10, "size": 20, "eol": false,
                    "mtime_ms": null, "tail_hash": "abc",
                }},
            })
        );
    }

    /// Pre-tripwire cursors (no eol/mtime keys) decode with the benign
    /// defaults — the persisted format is forward- and backward-open.
    #[test]
    fn pre_tripwire_cursors_decode_with_defaults() {
        let cursor: FileCursor = serde_json::from_value(serde_json::json!({
            "format_version": 1,
            "files": {"old.jsonl": {"done": 5, "size": 5}},
        }))
        .expect("decodes");
        let record = &cursor.files["old.jsonl"];
        assert!(record.ended_at_record_boundary, "eol defaults true");
        assert!(record.mtime_ms.is_none() && record.tail_hash.is_none());
        // And an unknown future key is tolerated, not refused.
        let forward: FileCursor = serde_json::from_value(serde_json::json!({
            "format_version": 1,
            "files": {"new.jsonl": {"done": 1, "size": 1, "surprise": true}},
        }))
        .expect("open to additive keys");
        assert_eq!(forward.files["new.jsonl"].done_units, 1);
    }

    /// The planner matrix: fresh starts at zero, complete skips,
    /// growth resumes with the tail check, and each impossible history
    /// refuses with its frozen phrase.
    #[test]
    fn the_planner_matrix_holds() {
        let mut cursor = FileCursor::default();
        cursor.record(
            "done.jsonl",
            FileProgress {
                done_units: 20,
                size_units: 20,
                ended_at_record_boundary: true,
                mtime_ms: Some(1_000),
                etag: None,
                tail_hash: Some("h".into()),
                row_groups_hash: None,
            },
        );
        cursor.record(
            "grown.jsonl",
            FileProgress {
                done_units: 10,
                size_units: 10,
                ended_at_record_boundary: true,
                mtime_ms: Some(1_000),
                etag: None,
                tail_hash: Some("h".into()),
                row_groups_hash: None,
            },
        );

        let tasks = cursor
            .plan(&[meta("fresh.jsonl", 5), meta("done.jsonl", 20), {
                let mut m = meta("grown.jsonl", 30);
                m.mtime_ms = Some(2_000); // growth may change mtime; size differs so no tripwire
                m
            }])
            .expect("plans");
        assert_eq!(tasks.len(), 2, "complete skipped: {tasks:?}");
        assert_eq!((tasks[0].path.as_str(), tasks[0].start), ("fresh.jsonl", 0));
        assert_eq!(
            (tasks[1].path.as_str(), tasks[1].start),
            ("grown.jsonl", 10)
        );
        assert!(matches!(
            tasks[1].resume_check,
            Some(ResumeCheck::TailBytes { window: 10, .. })
        ));

        let err = cursor.plan(&[meta("done.jsonl", 5)]).expect_err("shrunk");
        assert!(format!("{err}").contains("shrank"), "{err}");

        let mut rewritten = meta("done.jsonl", 20);
        rewritten.mtime_ms = Some(9_999);
        let err = cursor.plan(&[rewritten]).expect_err("rewritten in place");
        assert!(
            format!("{err}")
                .contains("rewritten in place (same size, but modified since the last run)"),
            "{err}"
        );

        cursor.record(
            "torn.jsonl",
            FileProgress {
                done_units: 10,
                size_units: 10,
                ended_at_record_boundary: false,
                mtime_ms: Some(1_000),
                etag: None,
                tail_hash: None,
                row_groups_hash: None,
            },
        );
        let err = cursor
            .plan(&[meta("torn.jsonl", 30)])
            .expect_err("unterminated growth");
        assert!(format!("{err}").contains("points mid-record"), "{err}");
    }

    /// The whole-file planner: any size change refuses; incomplete
    /// re-reads WHOLE from zero.
    #[test]
    fn the_whole_file_planner_rereads_or_refuses() {
        let mut cursor = FileCursor::default();
        cursor.record(
            "half.csv",
            FileProgress {
                done_units: 3,
                size_units: 10,
                ended_at_record_boundary: true,
                mtime_ms: Some(1_000),
                etag: None,
                tail_hash: None,
                row_groups_hash: None,
            },
        );
        let tasks = cursor.plan_whole(&[meta("half.csv", 10)]).expect("plans");
        assert_eq!(
            (tasks[0].start, tasks[0].size_units),
            (0, 10),
            "whole re-read"
        );

        let err = cursor
            .plan_whole(&[meta("half.csv", 11)])
            .expect_err("grew");
        assert!(
            format!("{err}").contains("whole-file formats (csv, compressed) never grow in place"),
            "{err}"
        );
    }

    /// The both-hashes and no-hash records arm NO check; zero units arm
    /// nothing.
    #[test]
    fn ambiguous_records_arm_no_resume_check() {
        let both = FileProgress {
            done_units: 5,
            size_units: 9,
            ended_at_record_boundary: true,
            mtime_ms: None,
            etag: None,
            tail_hash: Some("t".into()),
            row_groups_hash: Some("g".into()),
        };
        assert!(resume_check_for(&both).is_none());
        let none = FileProgress {
            tail_hash: None,
            row_groups_hash: None,
            ..both.clone()
        };
        assert!(resume_check_for(&none).is_none());
        let zero = FileProgress {
            done_units: 0,
            tail_hash: Some("t".into()),
            row_groups_hash: None,
            ..both
        };
        assert!(resume_check_for(&zero).is_none());
    }

    /// Retention: recording never prunes — every path ever recorded
    /// stays for the life of the state.
    #[test]
    fn every_path_ever_recorded_is_retained() {
        let mut cursor = FileCursor::default();
        cursor.record(
            "a",
            FileProgress {
                done_units: 1,
                size_units: 1,
                ended_at_record_boundary: true,
                mtime_ms: None,
                etag: None,
                tail_hash: None,
                row_groups_hash: None,
            },
        );
        cursor.record(
            "b",
            FileProgress {
                done_units: 1,
                size_units: 1,
                ended_at_record_boundary: true,
                mtime_ms: None,
                etag: None,
                tail_hash: None,
                row_groups_hash: None,
            },
        );
        let decoded = FileCursor::decode(Some(&cursor.encode())).expect("round-trips");
        assert_eq!(decoded.files.len(), 2);
        assert_eq!(decoded, cursor);
    }

    /// A STRICTLY newer cursor format refuses as an upgrade prompt —
    /// a future shape would decode as EMPTY through the serde defaults
    /// and silently re-read everything (030 review).
    #[test]
    fn a_future_cursor_format_refuses_upgrade_not_reset() {
        let cursor = Cursor::new(serde_json::json!({"format_version": 9, "files": {}}));
        let err = FileCursor::decode(Some(&cursor))
            .expect_err("refused")
            .to_string();
        assert!(
            err.contains(
                "file cursor format v9 is newer than this build supports (v1); upgrade \
                 rdlt instead of clearing state"
            ),
            "{err}"
        );
    }
}
