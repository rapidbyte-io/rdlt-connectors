//! JSONL: newline-framed raw bytes pushed a slab at a time, a
//! checkpoint after every slab, and byte-offset resume guarded by a
//! rolling tail hash.
//!
//! No per-line allocation happens here. A slab is one `Vec<u8>` of
//! whole lines, framed with memchr and handed to `Bytes` without a
//! copy; UTF-8 checking belongs to whichever parse runs downstream.
//! [`Slabs`] owns the framing rules — emit only newline-terminated
//! content, hold back an unterminated remainder, surface the last
//! fragment when the stream runs dry — while [`Fill`] hides which
//! kind of reader supplies the bytes.

use bytes::Bytes;
use rdlt_connector_sdk::source::Feed;
use rdlt_connector_sdk::spi::SourceError;

use super::open_decoded;
use crate::format::{SLAB_BYTES, codec_of};
use crate::location::{ByteReader, Location, classify_read_error};
use crate::source::cursor::{FileCursor, FileProgress, FileTask, ResumeCheck, TAIL_WINDOW};

/// Where a slab's bytes come from. `pour` fills as much of `buf` as
/// the stream can currently supply and answers 0 exactly at the end
/// of the stream.
enum Fill<'a> {
    /// The seekable object reader (plain jsonl, local or S3).
    Object {
        reader: &'a mut ByteReader,
        path: &'a str,
    },
    /// A blocking decompressor stack (the whole-file codec path).
    Decoder {
        reader: Box<dyn std::io::Read + Send>,
        path: &'a str,
    },
    /// Test-only: one scripted chunk per call, so fixtures decide
    /// exactly where each fill ends.
    #[cfg(test)]
    Scripted(std::collections::VecDeque<Vec<u8>>),
}

impl Fill<'_> {
    async fn pour(&mut self, buf: &mut [u8]) -> Result<usize, SourceError> {
        match self {
            Fill::Object { reader, path } => reader
                .read_full(buf)
                .await
                .map_err(|e| classify_read_error(&format!("reading `{path}`"), e)),
            Fill::Decoder { reader, path } => {
                use std::io::Read as _;
                let mut have = 0;
                while have < buf.len() {
                    match reader.read(&mut buf[have..]) {
                        Ok(0) => break,
                        Ok(n) => have += n,
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                        Err(e) => {
                            return Err(SourceError::fatal(format!("reading `{path}`: {e}")));
                        }
                    }
                }
                Ok(have)
            }
            #[cfg(test)]
            Fill::Scripted(chunks) => {
                let Some(mut chunk) = chunks.pop_front() else {
                    return Ok(0);
                };
                let take = chunk.len().min(buf.len());
                buf[..take].copy_from_slice(&chunk[..take]);
                if take < chunk.len() {
                    chunk.drain(..take);
                    chunks.push_front(chunk);
                }
                Ok(take)
            }
        }
    }
}

/// Frames a byte stream into slabs of whole lines.
///
/// Every slab ends on a newline except possibly the last: when the
/// stream runs out holding an unterminated remainder, that remainder
/// comes out as the final line. Bytes past the last newline of a fill
/// are held until more arrive, and a single line longer than
/// [`SLAB_BYTES`] simply keeps growing until its newline shows up.
/// `None` means the stream is fully consumed.
struct Slabs {
    /// Bytes read but not yet emitted — never contains a newline.
    held: Vec<u8>,
    /// The fill source has answered 0.
    spent: bool,
    /// Nothing further will ever be emitted.
    finished: bool,
}

impl Slabs {
    fn new() -> Self {
        Self {
            held: Vec::new(),
            spent: false,
            finished: false,
        }
    }

    async fn next(&mut self, from: &mut Fill<'_>) -> Result<Option<Vec<u8>>, SourceError> {
        if self.finished {
            return Ok(None);
        }
        while !self.spent {
            let base = self.held.len();
            self.held.resize(base + SLAB_BYTES, 0);
            let got = from.pour(&mut self.held[base..]).await?;
            self.held.truncate(base + got);
            if got == 0 {
                self.spent = true;
            } else if memchr::memrchr(b'\n', &self.held[base..]).is_some() {
                break;
            }
        }
        let slab = match memchr::memrchr(b'\n', &self.held) {
            Some(last) => {
                let remainder = self.held.split_off(last + 1);
                std::mem::replace(&mut self.held, remainder)
            }
            // No newline anywhere: the stream is spent, and whatever
            // is held (possibly nothing) is the final unterminated
            // line.
            None => std::mem::take(&mut self.held),
        };
        if self.spent && self.held.is_empty() {
            self.finished = true;
        }
        Ok((!slab.is_empty()).then_some(slab))
    }
}

/// Fold freshly consumed bytes into the rolling tail, which always
/// holds the last `TAIL_WINDOW` bytes of everything read so far.
fn fold_into_tail(tail: &mut Vec<u8>, consumed: &[u8]) {
    let window = TAIL_WINDOW as usize;
    if consumed.len() >= window {
        tail.clear();
        tail.extend_from_slice(&consumed[consumed.len() - window..]);
    } else {
        let keep = window - consumed.len();
        if tail.len() > keep {
            tail.drain(..tail.len() - keep);
        }
        tail.extend_from_slice(consumed);
    }
}

fn hex_hash(bytes: &[u8]) -> String {
    blake3::hash(bytes).to_hex().to_string()
}

/// Read one byte-resume task from `location`. `Ok(false)` means the
/// host cancelled mid-stream; `Ok(true)` means the file completed.
pub(crate) async fn read_task(
    location: &Location,
    task: &FileTask,
    validate: bool,
    cursor: &mut FileCursor,
    feed: &mut Feed,
) -> Result<bool, SourceError> {
    // A row-group check on a record stream is refused outright: this
    // reader cannot evaluate it, and quietly dropping it would switch
    // resume verification off for exactly the runs that carry one.
    let expected = match task.resume_check.as_ref() {
        Some(ResumeCheck::RowGroupPrefix { .. }) => {
            return Err(SourceError::fatal(format!(
                "file `{}` recorded a row-group integrity value but is being read as a record stream; refusing to resume without a check this reader can evaluate — clear it from the pipeline state",
                task.path
            )));
        }
        Some(ResumeCheck::TailBytes { window, hash }) if task.start > 0 => {
            Some((*window, hash.as_str()))
        }
        Some(ResumeCheck::TailBytes { .. }) | None => None,
    };

    // The window is re-read on EVERY resume, verified or not. The
    // tail this run records must cover the full `min(done,
    // TAIL_WINDOW)` span the NEXT run will re-derive, and only the
    // bytes behind the offset can complete it. Seeding it solely on
    // the verified path left short tails whose hashes made the
    // following run reject a healthy append as a rewrite (030
    // review).
    let window = match expected {
        Some((window, _)) => window,
        None => task.start.min(TAIL_WINDOW),
    };
    let mut file = location.open_from(&task.path, task.start - window).await?;
    let mut tail = vec![0u8; window as usize];
    let seeded = file
        .read_full(&mut tail)
        .await
        .map_err(|e| classify_read_error(&format!("reading `{}`", task.path), e))?;
    tail.truncate(seeded);
    if let Some((window, hash)) = expected {
        // Byte count AND hash must both line up before the offset is
        // trusted: a genuine append matches and carries on; a
        // rewritten prefix stops the run.
        if seeded as u64 != window || hex_hash(&tail) != hash {
            return Err(SourceError::fatal(format!(
                "file `{}` was rewritten before the resume offset (the content preceding byte {} changed since the last run); refusing to read a stale tail — clear it from the pipeline state or restore the file",
                task.path, task.start
            )));
        }
    }

    let listed_size = task.size_units;
    let mut consumed = task.start;
    // The cursor notes whether the last consumed byte was a newline:
    // an unterminated final line loads fine NOW, but growth past a
    // mid-record offset has to be refused later.
    let mut on_boundary = true;

    let mut from = Fill::Object {
        reader: &mut file,
        path: &task.path,
    };
    let mut slabs = Slabs::new();
    while let Some(slab) = slabs.next(&mut from).await? {
        if validate {
            check_lines(&slab, consumed, &task.path)?;
        }
        on_boundary = slab.ends_with(b"\n");
        consumed += slab.len() as u64;
        fold_into_tail(&mut tail, &slab);

        if feed.raw_json(Bytes::from(slab)).await.is_break() {
            return Ok(false);
        }
        cursor.record(
            &task.path,
            FileProgress {
                done_units: consumed,
                size_units: listed_size.max(consumed),
                ended_at_record_boundary: on_boundary,
                mtime_ms: task.mtime_ms,
                etag: task.etag.clone(),
                tail_hash: Some(hex_hash(&tail)),
                row_groups_hash: None,
            },
        );
        if feed.checkpoint(cursor.encode()).await.is_break() {
            return Ok(false);
        }
    }

    // Running out before the listed size means the file shrank while
    // it was being read. Marking it complete would shrink the
    // recorded size with it, and the next run would skip the missing
    // bytes without a sound (030 review, docket S3) — refuse, like
    // every other history the cursor cannot explain.
    if consumed < listed_size {
        return Err(SourceError::fatal(format!(
            "file `{}` ended at byte {consumed} but its listing recorded {listed_size} — the file shrank while it was being read; clear it from the pipeline state or restore the file",
            task.path
        )));
    }
    let complete = FileProgress {
        done_units: consumed,
        size_units: consumed,
        ended_at_record_boundary: on_boundary,
        mtime_ms: task.mtime_ms,
        etag: task.etag.clone(),
        tail_hash: Some(hex_hash(&tail)),
        row_groups_hash: None,
    };
    // Checkpoint the completion only when it differs from the last
    // per-slab record — most loads end exactly there, and a run
    // should not close on a checkpoint that says nothing new (docket
    // S13).
    if cursor.files.get(&task.path) != Some(&complete) {
        cursor.record(&task.path, complete);
        if feed.checkpoint(cursor.encode()).await.is_break() {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Whole-file jsonl, for the compressed codecs: decode the entire
/// stream under the same slab framing and validation, then checkpoint
/// ONCE at completion. Compressed streams cannot seek, so a crash
/// mid-file re-delivers the whole file on retry — absorbing that is
/// the keyed merge/dedup layer's job.
pub(crate) async fn read_task_whole(
    task: &FileTask,
    validate: bool,
    cursor: &mut FileCursor,
    feed: &mut Feed,
) -> Result<bool, SourceError> {
    let read_path = task.read_path.as_deref().unwrap_or(&task.path);
    super::verify_local_snapshot(task)?;
    let reader = open_decoded(read_path, codec_of(&task.path))?;

    let mut from = Fill::Decoder {
        reader,
        path: &task.path,
    };
    let mut slabs = Slabs::new();
    // Decompressed bytes so far — only error messages care.
    let mut consumed = 0u64;
    while let Some(slab) = slabs.next(&mut from).await? {
        if validate {
            check_lines(&slab, consumed, &task.path)?;
        }
        consumed += slab.len() as u64;
        if feed.raw_json(Bytes::from(slab)).await.is_break() {
            return Ok(false);
        }
    }
    cursor.record(
        &task.path,
        FileProgress {
            done_units: task.size_units,
            size_units: task.size_units,
            ended_at_record_boundary: true,
            mtime_ms: task.mtime_ms,
            etag: task.etag.clone(),
            tail_hash: None, // whole-file units resume by re-delivery, never by tail
            row_groups_hash: None,
        },
    );
    if feed.checkpoint(cursor.encode()).await.is_break() {
        return Ok(false);
    }
    Ok(true)
}

/// Cheap structural check of every line in a slab: parse-and-discard,
/// no tree built. Bad input is reported here with the file and the
/// byte offset of the offending line's FIRST byte, rather than as a
/// parse failure deep inside the engine. Empty and whitespace-only
/// lines are separators, not records.
fn check_lines(slab: &[u8], slab_base: u64, path: &str) -> Result<(), SourceError> {
    let mut at = 0usize;
    while at < slab.len() {
        let end = memchr::memchr(b'\n', &slab[at..]).map_or(slab.len(), |nl| at + nl);
        let line = &slab[at..end];
        if !line.iter().all(u8::is_ascii_whitespace)
            && let Err(e) = serde_json::from_slice::<serde::de::IgnoredAny>(line)
        {
            return Err(SourceError::fatal(format!(
                "malformed JSON in `{path}` at byte offset {}: {e}",
                slab_base + at as u64
            )));
        }
        at = end + 1;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scripted(chunks: &[&[u8]]) -> Fill<'static> {
        Fill::Scripted(chunks.iter().map(|c| c.to_vec()).collect())
    }

    /// The framing rules end to end: each slab stops at a newline,
    /// held-back bytes join the next slab, and the unterminated
    /// remainder still comes out when the stream ends.
    #[tokio::test]
    async fn slabs_end_at_newlines_and_the_final_fragment_is_emitted() {
        let mut from = scripted(&[b"{\"a\":1}\n{\"b\"", b":2}\n{\"c\":3}"]);
        let mut slabs = Slabs::new();
        let one = slabs.next(&mut from).await.expect("slab").expect("some");
        assert_eq!(one, b"{\"a\":1}\n");
        let two = slabs.next(&mut from).await.expect("slab").expect("some");
        assert_eq!(two, b"{\"b\":2}\n");
        let three = slabs.next(&mut from).await.expect("slab").expect("some");
        assert_eq!(
            three, b"{\"c\":3}",
            "the unterminated final line still loads"
        );
        assert!(slabs.next(&mut from).await.expect("end").is_none());
    }

    /// The rolling tail holds exactly the last window of consumed
    /// bytes, whether a slab is smaller or larger than the window.
    #[test]
    fn the_tail_rolls_to_exactly_the_window() {
        let mut tail = Vec::new();
        fold_into_tail(&mut tail, &[1u8; 10]);
        assert_eq!(tail.len(), 10);
        fold_into_tail(&mut tail, &vec![2u8; TAIL_WINDOW as usize - 4]);
        assert_eq!(tail.len(), TAIL_WINDOW as usize);
        assert_eq!(&tail[..4], &[1, 1, 1, 1], "the old bytes' remainder leads");
        fold_into_tail(&mut tail, &vec![3u8; TAIL_WINDOW as usize + 100]);
        assert_eq!(tail.len(), TAIL_WINDOW as usize);
        assert!(
            tail.iter().all(|&b| b == 3),
            "a big slab replaces the window"
        );
    }

    /// A malformed line is reported against the file and its own
    /// FIRST byte's offset; whitespace-only lines are not records.
    #[test]
    fn validation_names_the_line_start_offset() {
        let slab = b"{\"ok\":1}\n   \nnot json\n";
        let err = check_lines(slab, 100, "f.jsonl").expect_err("malformed");
        assert!(
            format!("{err}").contains("malformed JSON in `f.jsonl` at byte offset 113"),
            "{err}"
        );
        check_lines(b"{\"ok\":1}\n", 0, "f.jsonl").expect("clean slab passes");
    }
}

#[cfg(test)]
mod read_task_pins {
    use rdlt_connector_sdk::source::Feed;
    use rdlt_connector_sdk::spi::{PushPayload, records_channel};

    use crate::location::Location;
    use crate::source::cursor::{FileCursor, FileTask};

    fn task_for(path: &std::path::Path, listed_size: u64) -> FileTask {
        FileTask {
            path: path.to_str().expect("utf8").to_owned(),
            read_path: None,
            start: 0,
            size_units: listed_size,
            mtime_ms: None,
            etag: None,
            resume_check: None,
        }
    }

    /// EOF short of the listing size refuses — recording it complete
    /// would let the next run skip the missing bytes silently (030
    /// review, docket S3: the mid-READ leg, distinct from the
    /// planner's shrink tripwire).
    #[tokio::test]
    async fn eof_short_of_the_listing_refuses_as_a_mid_read_shrink() {
        let dir = tempfile::tempdir().expect("dir");
        let file = dir.path().join("events.jsonl");
        std::fs::write(&file, b"{\"id\": 1}\n").expect("plant");
        let location = Location::local_dir(dir.path().to_path_buf()).expect("local");
        let (out, _hold) = records_channel(1 << 20);
        let mut feed = Feed::new(out);
        let mut cursor = FileCursor::default();

        let err = super::read_task(
            &location,
            &task_for(&file, 30),
            true,
            &mut cursor,
            &mut feed,
        )
        .await
        .expect_err("refused")
        .to_string();
        assert!(
            err.contains("ended at byte 10 but its listing recorded 30")
                && err.contains("shrank while it was being read"),
            "{err}"
        );
    }

    /// A load must not end on a checkpoint that says nothing new: the
    /// completion record is emitted only when it differs from the last
    /// per-slab record (030 review, docket S13).
    #[tokio::test]
    async fn the_completion_checkpoint_is_not_a_redundant_duplicate() {
        let dir = tempfile::tempdir().expect("dir");
        let file = dir.path().join("events.jsonl");
        std::fs::write(&file, b"{\"id\": 1}\n{\"id\": 2}\n").expect("plant");
        let location = Location::local_dir(dir.path().to_path_buf()).expect("local");
        let (out, mut incoming) = records_channel(1 << 20);
        let mut feed = Feed::new(out);
        let mut cursor = FileCursor::default();

        let done = super::read_task(
            &location,
            &task_for(&file, 20),
            true,
            &mut cursor,
            &mut feed,
        )
        .await
        .expect("reads");
        assert!(done);
        drop(feed);

        let mut checkpoints = 0;
        let mut slabs = 0;
        while let Some(push) = incoming.recv().await {
            match push.payload {
                PushPayload::Checkpoint(_) => checkpoints += 1,
                PushPayload::RawJson(_) => slabs += 1,
                _ => {}
            }
        }
        assert_eq!(slabs, 1, "two small lines arrive as one slab");
        assert_eq!(
            checkpoints, 1,
            "the completion record equals the last per-slab record and is suppressed"
        );
    }
}
