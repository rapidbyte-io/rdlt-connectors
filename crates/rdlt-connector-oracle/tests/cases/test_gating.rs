//! The registry-vs-sources pin, UNGATED (the 030/031 pattern): the
//! declared crash points are exactly the armed ones, checked without
//! the failpoints feature so a rename cannot hide behind it.

#[test]
fn the_registry_matches_the_sources() {
    rdlt_testkit::assert_registry_matches_sources(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .as_path(),
        &[rdlt_connector_oracle::source::FAIL_POINTS],
    );
}
