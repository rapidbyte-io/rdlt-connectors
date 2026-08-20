//! The Snowflake destination.
//!
//! One noun per module. `config` is the document vocabulary behind the
//! sdk's typed gate; `connector` the framework face and the crash
//! registry; `client` the single boundary with the Snowflake library;
//! `unit` the commit transaction and the DML-only discipline that keeps
//! it atomic; `catalog` what the account has been observed to hold;
//! `ddl` identifier and type spelling plus ensure rendering; `dialect`
//! the shared merge seam's spellings; `encode` batch→parquet; `stage`
//! parts and their journey through the service's storage; `load` the
//! sdk-`Backend` coordinator over all of them.

pub mod parts;

mod catalog;
pub(crate) mod client;
mod config;
mod connector;
mod ddl;
mod dialect;
mod encode;
mod load;
mod stage;
mod unit;

pub use config::{Auth, Config, ConfigError, KeyPair, Password, TableType, config_schema};
pub use connector::{FAIL_POINTS, Snowflake, testhook};
pub use load::Load;

/// The SPI face: the sdk's session choreography over [`Snowflake`].
/// `Shell::from_yaml` (or `from_json`/`from_value`/`new`) is a running
/// destination in one call.
pub type Shell = rdlt_connector_sdk::destination::Shell<Snowflake>;
