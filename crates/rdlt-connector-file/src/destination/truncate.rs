//! Replace truncation: ownership-precise, over the ONE listing.
//!
//! Two rules, UNION, both reading `Location::keys_of_table` so local
//! and S3 can never diverge:
//!
//! - OWNED-PARTS (always): a tail at depth 1 or 2 (one partition dir at
//!   most) whose file name has the EXACT shape this destination writes
//!   — `part-<load>-<seq>-<index>.<ext>` with a known extension,
//!   numeric nonempty seq and index, nonempty load remainder. It reads
//!   NO configuration: a load that switched format or dropped
//!   `partition_by` still clears its predecessors. A bare `part-`
//!   prefix is NEVER enough — `part-0.parquet` and
//!   `part-00000-<uuid>-c000.snappy.parquet` are the default basenames
//!   of pyarrow/Spark/Hive/Delta, and a prefix test would delete a
//!   user's dataset.
//! - FROZEN plain-parquet (only local + parquet + unpartitioned):
//!   TOP-LEVEL `*.parquet` of ANY name — the exact pre-015 behavior,
//!   local-only by its stated scope; on an object store a user-placed
//!   `*.parquet` under the prefix is never ours.

use rdlt_connector_sdk::spi::DestinationError;

use super::DestFormat;
use crate::location::Location;

/// Delete every file Replace owns under `table`, returning how many.
pub(super) async fn truncate_table(
    location: &Location,
    table: &str,
    frozen_plain_parquet: bool,
) -> Result<usize, DestinationError> {
    let mut deleted = 0;
    for tail in location.keys_of_table(table).await? {
        if owns(&tail) || (frozen_plain_parquet && plain_top_level_parquet(&tail)) {
            location.delete_table_file(table, &tail).await?;
            deleted += 1;
        }
    }
    Ok(deleted)
}

/// The owned-parts shape rule over one tail (relative to the table
/// root).
pub(super) fn owns(tail: &str) -> bool {
    let mut components = tail.split('/');
    let (Some(first), second, None) = (components.next(), components.next(), components.next())
    else {
        return false; // depth 3+ — we never write more than one partition dir
    };
    let name = second.unwrap_or(first);
    if second.is_some() && first.is_empty() {
        return false;
    }
    let Some(stem) = name.strip_prefix("part-") else {
        return false;
    };
    // The extension must be EXACTLY one this destination writes — the
    // exhaustive iteration over `DestFormat::ALL` means a new format
    // variant extends ownership by construction.
    let Some(stem) = DestFormat::ALL
        .iter()
        .find_map(|f| stem.strip_suffix(&format!(".{}", f.extension())))
    else {
        return false;
    };
    // `<load>-<seq>-<index>`: the load id may itself contain dashes, so
    // parse from the RIGHT — index, then seq, both numeric nonempty;
    // whatever remains is the load and must be nonempty.
    let Some((rest, index)) = stem.rsplit_once('-') else {
        return false;
    };
    let Some((load, seq)) = rest.rsplit_once('-') else {
        return false;
    };
    let numeric = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    numeric(index) && numeric(seq) && !load.is_empty()
}

/// The frozen pre-015 rule: a TOP-LEVEL `*.parquet` of any name.
fn plain_top_level_parquet(tail: &str) -> bool {
    !tail.contains('/') && tail.ends_with(".parquet") && tail != ".parquet"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::destination::layout::final_tail;

    /// Every name this destination writes is owned — built from
    /// `final_tail` ITSELF, so the writer and the rule cannot drift.
    #[test]
    fn every_name_this_destination_writes_is_owned() {
        for format in DestFormat::ALL {
            for partition in [None, Some("eu"), Some("__null__"), Some("2024-01-01")] {
                for load in ["load-1", "a", "x-y-z-9"] {
                    let tail = final_tail("t", partition, load, 3, 0, format.extension());
                    let tail = tail.strip_prefix("t/").expect("under the table root");
                    assert!(owns(tail), "must own {tail}");
                }
            }
        }
    }

    /// The foreign basenames that a bare `part-` prefix test would
    /// have deleted: pyarrow, Spark/Delta, and near-miss shapes.
    #[test]
    fn a_foreign_dataset_is_never_ours() {
        for tail in [
            "part-0.parquet",                          // pyarrow default
            "part-00000-1a2b3c4d-c000.snappy.parquet", // Spark/Hive/Delta default
            "part-load-1-x.parquet",                   // non-numeric index
            "part-load-x-1.parquet",                   // non-numeric seq
            "part--1-0.parquet",                       // empty load
            "part-load-1-0.csv",                       // unknown extension
            "part-load-1-0.parquet.bak",               // trailing suffix
            "eu/west/part-load-1-0.parquet",           // depth 3
            "data.parquet",                            // no part- prefix
            "part-load-1-0",                           // no extension
        ] {
            assert!(!owns(tail), "must NOT own {tail}");
        }
        // The partitioned form of a foreign name is equally foreign.
        assert!(!owns("eu/part-0.parquet"));
        assert!(owns("eu/part-load-1-0.jsonl"));
    }

    /// The frozen rule adds ONLY top-level `*.parquet`.
    #[test]
    fn the_frozen_rule_is_top_level_parquet_only() {
        assert!(plain_top_level_parquet("data.parquet"));
        assert!(plain_top_level_parquet("part-0.parquet"));
        assert!(!plain_top_level_parquet("eu/data.parquet"));
        assert!(!plain_top_level_parquet("data.jsonl"));
        assert!(!plain_top_level_parquet(".parquet"));
    }
}
