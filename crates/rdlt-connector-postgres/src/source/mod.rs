//! The Postgres SOURCE: binary-COPY→Arrow extraction, catalog reflection,
//! query streams, cursor-column incremental, and (when the `cdc:` block is
//! configured) change data capture over logical replication.
//!
//! Layout, one noun per module: `connector` is the SPI face; `config` the
//! document vocabulary + local validation; `plan` the one catalog-dependent
//! validation gate; `reflect` the catalog round trip; `sql` the injection-
//! safe text assembly; `copy` the shared COPY read loop; `cursor/` the
//! incremental machinery; `cdc/` change data capture; `errors` the
//! phase-tagged taxonomy.

pub(crate) mod cdc;
pub mod config;
mod connector;
mod copy;
mod cursor;
mod errors;
#[cfg(feature = "failpoints")]
mod fail_points;
mod plan;
pub(crate) mod reflect;
mod sql;

pub use config::{Config, ConfigError, config_schema};
pub use connector::Postgres;

/// The SPI face: the sdk's shell over [`Postgres`]. `Shell::from_yaml`
/// (or `from_json`/`from_value`/`new`) is a running source in one call —
/// the parse-validate-assemble path lives in the sdk, once.
pub type Shell = rdlt_connector_sdk::source::Shell<Postgres>;

pub(crate) use connector::connect;

#[cfg(feature = "failpoints")]
pub use fail_points::{CDC_FAIL_POINTS, FAIL_POINTS};
