//! The registry-vs-sources pin, UNGATED (the 030 pattern): the same
//! scan the failpoints-gated sweep binary runs, here so the default
//! gate catches a dead or unregistered crash point without the
//! feature. One registry over one source tree — never "simplify" the
//! union form away (the 024 rule).

use rdlt_connector_duckdb::destination::FAIL_POINTS;

#[test]
fn the_registry_matches_the_sources() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    rdlt_testkit::scanner::assert_registry_matches_sources(&src, &[FAIL_POINTS]);
}
