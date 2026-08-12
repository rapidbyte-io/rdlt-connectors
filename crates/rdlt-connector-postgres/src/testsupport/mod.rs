//! THE test-access seam: everything the crate's own integration suites,
//! benches, and fuzz targets reach across the test-binary boundary, in one
//! place under one convention. Hidden — not a public API, no semver
//! guarantees.
//!
//! One module per zone (`source`, `destination`, `session`), plus `data`
//! for the shared representative fixtures both benches and fuzz entries
//! build from.

#[cfg(feature = "source")]
pub mod data;
#[cfg(feature = "destination")]
pub mod destination;
pub mod session;
#[cfg(feature = "source")]
pub mod source;
