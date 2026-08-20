//! The whole-file readers' two entry checks: the pre-read re-stat
//! and the magic-verified codec open. The byte-resume and row-group
//! readers guard themselves with their own integrity checks (tail
//! hash, prefix digest) and do not pass through here.

use rdlt_connector_sdk::spi::error::SourceError;

use crate::format::{Codec, GZIP_MAGIC, ZSTD_MAGIC};
use crate::source::cursor::FileTask;

/// Refuse a local whole-file read whose target no longer matches its
/// listing entry. Local paths are opened LIVE at read time, so a file
/// swapped in the gap between planning and reading would be loaded
/// against a plan that never described it (030 review, docket S11: a
/// same-size replacement slid through exactly that gap this run).
/// Staged S3 fetches carry a `read_path` and are already immutable
/// copies, so they pass without a stat.
pub(crate) fn verify_local_snapshot(task: &FileTask) -> Result<(), SourceError> {
    if task.read_path.is_some() {
        return Ok(());
    }
    let meta = std::fs::metadata(&task.path)
        .map_err(|e| SourceError::fatal(format!("stat `{}`: {e}", task.path)))?;
    let seen_mtime = meta
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64);
    let size_differs = meta.len() != task.size_units;
    let mtime_differs = matches!(
        (task.mtime_ms, seen_mtime),
        (Some(listed), Some(seen)) if listed != seen
    );
    if size_differs || mtime_differs {
        // The evidence names whichever tripwire actually moved.
        let moved = if size_differs {
            format!("listed {} bytes, found {}", task.size_units, meta.len())
        } else {
            "same size, but modified since the listing".to_owned()
        };
        return Err(SourceError::fatal(format!(
            "file `{}` changed on disk between listing and read ({moved}); refusing to \
             load content the plan does not describe — retry the run",
            task.path
        )));
    }
    Ok(())
}

/// Open a local file, decoding through the codec its name declares.
/// The stream's first bytes are sniffed against the codec's magic
/// number before any decoder touches them: an extension that lies
/// about the content fails here with the bytes as evidence, instead
/// of surfacing as decoder garbage.
pub(crate) fn open_decoded(
    path: &str,
    codec: Codec,
) -> Result<Box<dyn std::io::Read + Send>, SourceError> {
    use std::io::Read as _;
    let mut file = std::fs::File::open(path)
        .map_err(|e| SourceError::fatal(format!("opening `{path}`: {e}")))?;
    let magic: &[u8] = match codec {
        Codec::Plain => return Ok(Box::new(file)),
        Codec::Gzip => &GZIP_MAGIC,
        Codec::Zstd => &ZSTD_MAGIC,
    };

    // Sniff up to four bytes, riding out short reads and EINTR.
    let mut head = [0u8; 4];
    let mut have = 0usize;
    while have < head.len() {
        match file.read(&mut head[have..]) {
            Ok(0) => break,
            Ok(n) => have += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(SourceError::fatal(format!("reading `{path}`: {e}"))),
        }
    }
    if have < magic.len() || head[..magic.len()] != *magic {
        return Err(SourceError::fatal(format!(
            "file `{path}` does not match its compression extension (magic bytes {:02x?}) — fix the name or the content",
            &head[..have]
        )));
    }

    // The sniffed prefix goes back in front of the file handle so the
    // decoder sees the stream from byte zero.
    let stream = std::io::Cursor::new(head[..have].to_vec()).chain(file);
    match codec {
        Codec::Gzip => Ok(Box::new(flate2::read::GzDecoder::new(stream))),
        Codec::Zstd => Ok(Box::new(
            zstd::stream::read::Decoder::new(stream)
                .map_err(|e| SourceError::fatal(format!("zstd `{path}`: {e}")))?,
        )),
        Codec::Plain => unreachable!("returned before the sniff"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Both halves of the magic check: a plain file behind a `.gz`
    /// name refuses with the observed bytes, and a genuinely gzipped
    /// one decodes back to its original content.
    #[test]
    fn lying_extensions_fail_with_the_observed_bytes() {
        use std::io::Read as _;
        let dir = tempfile::tempdir().expect("dir");
        let lying = dir.path().join("plain.jsonl.gz");
        std::fs::write(&lying, b"{}\n").expect("write");
        let Err(err) = open_decoded(lying.to_str().expect("utf8"), Codec::Gzip) else {
            panic!("magic mismatch must refuse");
        };
        let text = format!("{err}");
        assert!(
            text.contains("does not match its compression extension")
                && text.contains("fix the name or the content"),
            "{text}"
        );

        let honest = dir.path().join("real.jsonl.gz");
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut encoder, b"{\"a\":1}\n").expect("encode");
        std::fs::write(&honest, encoder.finish().expect("finish")).expect("write");
        let mut decoded = String::new();
        open_decoded(honest.to_str().expect("utf8"), Codec::Gzip)
            .expect("opens")
            .read_to_string(&mut decoded)
            .expect("decodes");
        assert_eq!(decoded, "{\"a\":1}\n");
    }

    fn snapshot_task(path: &str, size: u64, mtime_ms: Option<u64>) -> FileTask {
        FileTask {
            path: path.into(),
            read_path: None,
            start: 0,
            size_units: size,
            mtime_ms,
            etag: None,
            resume_check: None,
        }
    }

    /// The pre-read re-stat: a size or mtime that moved since the
    /// listing refuses with evidence naming which tripwire fired; a
    /// staged copy (read_path set) is already a snapshot and skips the
    /// check (030 review, docket S11).
    #[test]
    fn a_moved_file_refuses_before_the_whole_file_read() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("data.csv");
        std::fs::write(&path, b"1234567890").expect("plant");
        let path = path.to_str().expect("utf8");

        let err = verify_local_snapshot(&snapshot_task(path, 20, None))
            .expect_err("size moved")
            .to_string();
        assert!(
            err.contains("changed on disk between listing and read (listed 20 bytes, found 10)"),
            "{err}"
        );

        let stale_mtime = Some(1);
        let err = verify_local_snapshot(&snapshot_task(path, 10, stale_mtime))
            .expect_err("mtime moved")
            .to_string();
        assert!(
            err.contains("(same size, but modified since the listing)"),
            "{err}"
        );

        let mut staged = snapshot_task("does-not-exist", 999, None);
        staged.read_path = Some(path.to_owned());
        verify_local_snapshot(&staged).expect("a staged copy skips the re-stat");

        let meta = std::fs::metadata(path).expect("stat");
        let honest = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_millis() as u64);
        verify_local_snapshot(&snapshot_task(path, 10, honest)).expect("unchanged passes");
    }
}
