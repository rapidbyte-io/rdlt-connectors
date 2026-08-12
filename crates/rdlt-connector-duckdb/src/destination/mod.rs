//! The DuckDB destination: one shared database instance, sdk-shelled.

mod catalog;
mod client;
mod config;
mod connector;
mod dialect;
mod load;
mod schema;

pub use config::{Config, ConfigError, config_schema};
#[doc(hidden)]
pub use connector::testhook;
pub use connector::{DuckDb, FAIL_POINTS};

/// The sdk shell around [`DuckDb`] — the destination's public form.
pub type Shell = rdlt_connector_sdk::destination::Shell<DuckDb>;
// The sqlcore vocabulary IS this destination's options vocabulary —
// re-exported so consumers spell it from here (facade parity).
pub use rdlt_connector_sqlcore::{
    AbsentPolicy, DedupSort, DestinationOptions, MergeStrategy, Scd2Options, SortOrder,
    TableOptions,
};
