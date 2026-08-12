//! The Iceberg REST-catalog destination, second generation — born on
//! the rdlt connector sdk.
//!
//! Rows land as parquet data files committed through Iceberg
//! snapshots, and exactly-once is SNAPSHOT-NATIVE: every data commit
//! stamps its identity into the snapshot summary, replay is detected by
//! scanning the table's own snapshot history, and pipeline state rides
//! as table properties on a marker table — the catalog is the only
//! store this destination needs.
//!
//! The underlying iceberg library is confined to ONE boundary module;
//! its types never cross this crate's public surface.

pub mod destination;
