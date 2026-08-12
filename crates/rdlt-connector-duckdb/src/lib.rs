//! The DuckDB destination, second generation — Arrow-native ingestion
//! with struct-preserving lowering, TEMP-table staging, and one
//! atomic transaction per commit that persists state with the data,
//! born on the rdlt connector sdk.
//!
//! Everything merge-shaped (strategies, dedup, scopes, scd2) is the
//! shared sqlcore vocabulary, executed here through the dialect seam.
//! A future source slots in beside the destination.

pub mod destination;
