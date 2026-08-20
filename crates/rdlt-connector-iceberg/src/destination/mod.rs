//! The destination: one module per noun, the sdk shell as the face.

mod client;
mod commit;
mod config;
mod connector;
mod load;
pub mod parquet;
mod partition;
pub mod parts;
mod schema;
mod state;
#[cfg(test)]
mod testsupport;
mod write;

pub use config::{
    Auth, BearerAuth, CatalogOptions, Config, ConfigError, Oauth2ClientCredentials, PartitionField,
    PartitionTransform, S3Override, StorageOptions, TableOptions, config_schema,
};
#[doc(hidden)]
pub use connector::testhook;
pub use connector::{FAIL_POINTS, Iceberg};
pub use load::Load;

/// The canonical face: the sdk shell over [`Iceberg`] — `from_yaml` /
/// `from_json` / `from_value` / `new` arrive from the sdk in one alias.
pub type Shell = rdlt_connector_sdk::destination::Shell<Iceberg>;
