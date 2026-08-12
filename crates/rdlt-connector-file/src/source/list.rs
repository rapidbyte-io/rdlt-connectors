//! Resolving a stream's `path` on the local filesystem: the result is
//! the WHOLE matching set, sorted — a listing that cannot be completed
//! is an error, never a shorter list.
//!
//! Both location kinds share one tie-breaker for ambiguous input: if
//! the path names a file that exists, it is taken literally, glob
//! metacharacters and all — `events[prod].jsonl` names that file, not
//! a character class. Resolution happens once per run; anything
//! created afterwards belongs to the next one.

use rdlt_connector_sdk::spi::SourceError;

use super::cursor::FileMeta;

/// A path-or-glob into file metadata. The refusals are deliberate:
/// naming a file that is missing is an error, and naming a directory
/// earns a suggestion rather than an implicit expansion — the operator
/// declares what a stream reads, the connector does not guess. Only an
/// empty GLOB is an empty stream.
pub(crate) fn local_listing(pattern: &str) -> Result<Vec<FileMeta>, SourceError> {
    let mut found = resolve(pattern)?;
    found.sort();
    found.into_iter().map(describe).collect()
}

fn resolve(pattern: &str) -> Result<Vec<String>, SourceError> {
    let as_path = std::path::Path::new(pattern);
    if as_path.is_file() {
        return Ok(vec![pattern.to_owned()]);
    }
    if pattern.contains(['*', '?', '[']) {
        return expand_glob(pattern);
    }
    if as_path.is_dir() {
        return Err(SourceError::fatal(format!(
            "`{pattern}` is a directory, not a file — name a file, or use a pattern such as \
             `{pattern}/*.jsonl`"
        )));
    }
    Err(SourceError::fatal(format!(
        "file `{pattern}` does not exist"
    )))
}

fn expand_glob(pattern: &str) -> Result<Vec<String>, SourceError> {
    // No wildcard — `**` included — ever reaches into a dot-prefixed
    // name. That single option is what hides a file DESTINATION's
    // `.rdlt-staging` (uncommitted parts) from a source glob sweeping
    // the same directory; before it, a recursive glob would read
    // staged parts and read them AGAIN once published (030 review).
    // Spelling a dotfile out literally still works.
    let options = glob::MatchOptions {
        require_literal_leading_dot: true,
        ..Default::default()
    };
    let walk = glob::glob_with(pattern, options)
        .map_err(|e| SourceError::fatal(format!("invalid glob `{pattern}`: {e}")))?;
    let mut files = Vec::new();
    for entry in walk {
        match entry {
            Ok(path) if path.is_file() => files.push(path.to_string_lossy().into_owned()),
            // Directories can match a glob; only files enter a stream.
            Ok(_) => {}
            Err(e) => {
                return Err(SourceError::fatal(format!(
                    "expanding glob `{pattern}`: {e}; refusing to load a partial file list"
                )));
            }
        }
    }
    Ok(files)
}

fn describe(path: String) -> Result<FileMeta, SourceError> {
    let stat =
        std::fs::metadata(&path).map_err(|e| SourceError::fatal(format!("stat `{path}`: {e}")))?;
    let mtime_ms = stat
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64);
    Ok(FileMeta {
        path,
        size_units: stat.len(),
        mtime_ms,
        etag: None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The tie-breaker: metacharacters in the name of a file that
    /// EXISTS mean nothing — the path is the file.
    #[test]
    fn a_real_file_wins_over_its_own_metacharacters() {
        let dir = tempfile::tempdir().expect("dir");
        let tricky = dir.path().join("events[prod].jsonl");
        std::fs::write(&tricky, b"{}\n").expect("write");
        let listing = local_listing(tricky.to_str().expect("utf8")).expect("lists");
        assert_eq!(listing.len(), 1);
        assert_eq!(listing[0].size_units, 3);
        assert!(listing[0].mtime_ms.is_some() && listing[0].etag.is_none());
    }

    /// A glob yields files only, in sorted order — and matching
    /// nothing is an empty stream, not a failure.
    #[test]
    fn a_glob_yields_sorted_files_and_no_match_is_fine() {
        let dir = tempfile::tempdir().expect("dir");
        std::fs::write(dir.path().join("b.jsonl"), b"x").expect("write");
        std::fs::write(dir.path().join("a.jsonl"), b"x").expect("write");
        std::fs::create_dir(dir.path().join("c.jsonl")).expect("dir entry");
        let pattern = dir.path().join("*.jsonl");
        let listing = local_listing(pattern.to_str().expect("utf8")).expect("lists");
        let names: Vec<&str> = listing
            .iter()
            .map(|m| m.path.rsplit('/').next().expect("name"))
            .collect();
        assert_eq!(names, vec!["a.jsonl", "b.jsonl"], "sorted, files only");

        let empty = dir.path().join("*.csv");
        assert!(
            local_listing(empty.to_str().expect("utf8"))
                .expect("empty ok")
                .is_empty()
        );
    }

    /// Naming a directory earns the pattern suggestion; naming an
    /// absent file is refused — both with the frozen spellings.
    #[test]
    fn a_directory_suggests_a_pattern_and_an_absent_file_refuses() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().to_str().expect("utf8").to_owned();
        let err = local_listing(&path).expect_err("directory refused");
        assert!(
            format!("{err}").contains(&format!(
                "`{path}` is a directory, not a file — name a file, or use a pattern such as \
                 `{path}/*.jsonl`"
            )),
            "{err}"
        );
        let missing = format!("{path}/nope.jsonl");
        let err = local_listing(&missing).expect_err("missing refused");
        assert!(
            format!("{err}").contains(&format!("file `{missing}` does not exist")),
            "{err}"
        );
    }

    /// Dot-prefixed names stay invisible to every wildcard, `**`
    /// included — the guarantee that keeps a co-located destination's
    /// `.rdlt-staging` out of a source glob (030 review). Naming such
    /// a file literally still reads it.
    #[test]
    fn dot_prefixed_names_stay_out_of_wildcard_reach() {
        let dir = tempfile::tempdir().expect("dir");
        let staged = dir.path().join(".rdlt-staging/ab/load-1");
        std::fs::create_dir_all(&staged).expect("dirs");
        std::fs::write(staged.join("part.jsonl"), b"{}\n").expect("staged");
        std::fs::write(dir.path().join("real.jsonl"), b"{}\n").expect("real");

        let recursive = format!("{}/**/*.jsonl", dir.path().display());
        let names: Vec<String> = local_listing(&recursive)
            .expect("lists")
            .into_iter()
            .map(|m| m.path)
            .collect();
        assert_eq!(names.len(), 1, "{names:?}");
        assert!(names[0].ends_with("real.jsonl"));

        let literal = staged.join("part.jsonl");
        let named = local_listing(literal.to_str().expect("utf8")).expect("literal");
        assert_eq!(named.len(), 1);
    }
}
