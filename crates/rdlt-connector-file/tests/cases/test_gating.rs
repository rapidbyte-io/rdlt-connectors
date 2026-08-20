//! The registry-vs-sources pin, UNGATED: four registries over one
//! source tree, checked against their union (the 024 rule — never
//! "simplify" this to per-registry equality). `LEASE_FAIL_POINTS`
//! joined the union in 037 US2 T6: the lease module's two crash
//! points are armed in `lease.rs` from day one, before any sweep
//! drives them (that's T7/T8's job), and this scanner runs on every
//! `cargo nextest run` — an armed-but-undeclared name fails it
//! immediately.

use rdlt_connector_file::destination::{FAIL_POINTS, LEASE_FAIL_POINTS, S3_FAIL_POINTS};
use rdlt_connector_file::source;

#[test]
fn the_registries_match_the_sources() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    rdlt_testkit::scanner::assert_registry_matches_sources(
        &src,
        &[
            FAIL_POINTS,
            S3_FAIL_POINTS,
            LEASE_FAIL_POINTS,
            source::FAIL_POINTS,
        ],
    );
}
