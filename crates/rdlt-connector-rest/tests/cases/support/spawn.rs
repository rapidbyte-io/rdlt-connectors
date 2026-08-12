//! Locating the built `rdlt-connector-rest` bin — a thin
//! wrapper over the ONE spawn scaffold ([`rdlt_testkit::spawn`], 042
//! round-2 fix wave): the mechanics — `CARGO_TARGET_DIR` resolution,
//! the `RDLT_BUILD_CONNECTOR_BINS` guard, the stale-bin note, the
//! loud missing-bin refusal — live there once for every connector
//! suite.

use std::path::PathBuf;

/// The path to the built `rdlt-connector-rest` bin — see
/// [`rdlt_testkit::spawn::built_connector_bin`] for the guard and
/// build semantics.
pub(crate) fn built_bin() -> PathBuf {
    rdlt_testkit::spawn::built_connector_bin(
        env!("CARGO_MANIFEST_DIR"),
        // The package's own name, from cargo — a hardcoded literal
        // could drift from the real crate name (round-6 fix).
        env!("CARGO_PKG_NAME"),
    )
}
