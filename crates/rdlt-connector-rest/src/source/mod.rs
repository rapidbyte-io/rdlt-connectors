//! The declarative REST source.
//!
//! Substrate first: [`config`] is the document vocabulary and its ONE
//! validation gate, [`select`] the JSONPath-subset selector both the
//! document and the read path use, [`http`] request execution (credential
//! attachment, pacing, bounded waits, classification). `read` drives the
//! paginated read loop over them; `connector` is the SPI face.

pub mod config;
mod connector;
#[cfg(feature = "failpoints")]
mod fail_points;
pub mod http;
pub mod read;
pub mod select;

pub use config::{Config, ConfigError, config_schema};
pub use connector::Rest;

/// The SPI face: the sdk's shell over [`Rest`]. `Shell::from_yaml(text)`
/// (or `from_json`/`from_value`/`new`) is a running source in one call —
/// the parse-validate-assemble path lives in the sdk, once.
pub type Shell = rdlt_connector_sdk::source::Shell<Rest>;
#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub use fail_points::FAIL_POINTS;
