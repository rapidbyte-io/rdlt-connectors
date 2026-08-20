//! What the gate itself must see: the crash-point registry matches the
//! sources UNGATED (the sweep binary that also checks it is
//! failpoints-gated and spends container time — drift must fail a
//! routine gate), and the nextest live-group membership stays pinned.

/// Registry-vs-sources, in the routine gate.
#[test]
fn the_crash_registry_matches_the_sources_in_gate() {
    rdlt_testkit::scanner::assert_registry_matches_sources(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .as_path(),
        &[rdlt_connector_iceberg::destination::FAIL_POINTS],
    );
}

/// The nextest `iceberg-live` group bounds every container-booting
/// binary of THIS crate too (each cell boots its own Polaris JVM +
/// RUSTFS). The filter is spelled over the whole package; this pin
/// makes the membership explicit: the package's binaries are exactly
/// the integration umbrella and the by-hand sweep, so a new binary —
/// which would join the group silently — changes this list on purpose.
#[test]
fn the_live_group_membership_is_pinned() {
    let tests_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests");
    let mut actual: Vec<String> = std::fs::read_dir(&tests_dir)
        .expect("reading tests dir")
        .filter_map(|e| {
            let path = e.expect("entry").path();
            (path.extension()? == "rs")
                .then(|| path.file_stem()?.to_str().map(str::to_owned))
                .flatten()
        })
        .collect();
    actual.sort();
    assert_eq!(
        actual,
        vec!["crash_sweep".to_owned(), "integration".to_owned()],
        "the package's binaries changed — revisit the iceberg-live group filter"
    );
}
