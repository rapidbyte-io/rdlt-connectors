//! Does the shared scanner see what a plain text search sees?
//!
//! A scanner is the one piece of the crash-point machinery that fails
//! OPEN when wrong: it finds fewer sites and every assertion it feeds
//! still passes. So its own count is checked against an
//! independently-derived one before any registry trusts it. These rows
//! moved here with their crates at the 044 cut (they lived in
//! rdlt-testkit's own selfcheck while the connectors shared rdlt's
//! workspace); the committed per-crate counts are the ONLY guard
//! against a point deleted from code AND registry together —
//! `assert_registry_matches_sources` cannot catch that, and without
//! these rows the crash sweeps could silently narrow.

use std::path::Path;

/// Distinct crash-point names the scanner must find beside an arming call, per
/// crate. Recorded independently — by reading the sources — so that a scanner
/// which quietly stopped finding sites is caught.
///
/// These are DISTINCT NAMES, not call sites. A name armed at two places counts
/// once, because the registry lists names.
///
/// Some names are armed INDIRECTLY — the `crash_point!` takes a variable and the
/// literal lives at the constructor supplying it — so they are absent here by
/// design and covered instead by the "declared names must appear twice" half of
/// `assert_registry_matches_sources`.
const EXPECTED_DIRECT_NAMES: &[(&str, usize)] = &[
    // 12 since 034's review round 3 armed `pq.manifest.write` around
    // the publish manifest — the count moves WITH the registry, and
    // this line is the deliberate second place a new point must be
    // named. 14 since 037 US2's lease module armed `file.lease.acquire`
    // and `file.lease.release` (T6, `destination/lease.rs`).
    ("rdlt-connector-file", 14),
    ("rdlt-connector-rest", 3),
    ("rdlt-connector-iceberg", 3),
    ("rdlt-connector-duckdb", 2),
    ("rdlt-connector-oracle", 2),
    // Two `crash_point!` plus two `crash_at` — the crate that proves recognising
    // one arming spelling is not enough.
    ("rdlt-connector-snowflake", 4),
    // 11, not the 14 names its three registries declare: THREE points are armed
    // indirectly, so their literals sit at the constructor that supplies the name
    // rather than beside the macro —
    //   pg.src.mid_copy          a labelled struct  (source/connector.rs)
    //   pg.src.after_batch_push  a labelled struct  (source/connector.rs)
    //   cdc.snapshot.copy        a labelled struct  (source/cdc/read.rs)
    // Each was verified to appear twice — once declared, once where it arms — so
    // the "declared names appear twice" half of the assertion covers all three.
    // This crate is the reason that half exists: a set-equality design would
    // report these as missing and invite widening or shrinking to "fix" it.
    ("rdlt-connector-postgres", 11),
];

#[test]
fn the_scanner_finds_every_directly_armed_name() {
    let crates_dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("crates/ is the parent of this crate");
    for (crate_name, expected) in EXPECTED_DIRECT_NAMES {
        let src = crates_dir.join(crate_name).join("src");
        let found = rdlt_testkit::armed_crash_points(&src);
        assert_eq!(
            found.len(),
            *expected,
            "{crate_name}: scanner found {} distinct names, expected {expected}: {found:?}",
            found.len()
        );
    }
}

/// The vacuity guard: scanning a directory with no arming calls, against a
/// non-empty registry, must FAIL rather than agree.
///
/// This is the one way the whole registry check could itself pass while verifying
/// nothing — a mistyped path or an unrecognised arming spelling yields an empty
/// set, and an empty set trivially satisfies "everything armed is declared".
/// This crate's own `src/` is the armless directory: it deliberately contains
/// no connector code.
#[test]
#[should_panic(expected = "no crash-point sites found")]
fn scanning_nowhere_against_a_real_registry_fails() {
    let empty = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    rdlt_testkit::assert_registry_matches_sources(&empty, &[&["some.point"]]);
}
