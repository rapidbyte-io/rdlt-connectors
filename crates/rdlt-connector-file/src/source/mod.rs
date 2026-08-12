//! The source half: files as streams.

mod config;
mod connector;
pub mod cursor;
pub(crate) mod list;
pub(crate) mod read;

pub use config::{Config, ConfigError, CsvOptions, HintType, Stream, config_schema};
pub use connector::{FAIL_POINTS, File};

/// The canonical face: the sdk shell over [`File`].
pub type Shell = rdlt_connector_sdk::source::Shell<File>;
