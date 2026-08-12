//! Destination configuration: the builder embedders call, the document
//! vocabulary pipelines spell, and the shared sqlcore options both consume.
//!
//! The options vocabulary + validation live in rdlt-connector-sqlcore
//! (shared with every SQL destination) and are re-exported here under their
//! bare sqlcore names — the same spelling every SQL destination uses, so a
//! config document reads identically whichever one consumes it.

use rdlt_connector_sdk::spi::DestinationError;
use serde::{Deserialize, Serialize};

pub use rdlt_connector_sqlcore::{
    AbsentPolicy, DedupSort, DestinationOptions, MergeStrategy, Scd2Options, SortOrder,
    TableOptions,
};

/// The Postgres destination handle: connection string, target schema, TLS
/// posture, merge options. Built either programmatically (the builder) or
/// from a configuration document ([`Config`]).
#[derive(Debug, Clone)]
pub struct Postgres {
    pub(super) connection_string: String,
    pub(super) schema: String,
    pub(super) tls: Option<crate::tls::Policy>,
    pub(super) options: DestinationOptions,
}

impl Postgres {
    /// A handle over `connection_string` (e.g. `host=localhost
    /// user=postgres password=pw dbname=raw`). Nothing connects until the
    /// engine opens a load session.
    pub fn new(connection_string: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
            schema: "public".into(),
            tls: None,
            options: DestinationOptions::default(),
        }
    }

    /// Target schema (the pipeline vocabulary calls it `dataset`); created
    /// if missing.
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = schema.into();
        self
    }

    /// TLS posture — the SAME policy type the source uses; the two
    /// directions share one connect path.
    pub fn tls(mut self, policy: crate::tls::Policy) -> Self {
        self.tls = Some(policy);
        self
    }

    /// Strategy/hard-delete/SCD2 options. Validated here — errors name the
    /// field.
    pub fn options(mut self, options: DestinationOptions) -> Result<Self, DestinationError> {
        options.validate().map_err(DestinationError::fatal)?;
        self.options = options;
        Ok(self)
    }

    /// Wrap into the SPI face — the sdk's session choreography over
    /// [`super::Load`]. The builder composes the handle; this hands it to
    /// the engine.
    pub fn into_shell(self) -> super::Shell {
        rdlt_connector_sdk::destination::shell(self)
    }

    /// Build from a parsed configuration document.
    pub fn from_config(config: Config) -> Result<Self, ConfigError> {
        let options = DestinationOptions {
            merge_strategy: config.merge_strategy,
            tables: config.tables,
        };
        options.validate().map_err(ConfigError::Invalid)?;
        Ok(Self {
            connection_string: config.connection,
            schema: config.schema,
            tls: config.tls,
            options,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("parsing postgres destination config: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("parsing postgres destination JSON config: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid postgres destination config: {0}")]
    Invalid(String),
}

/// The destination configuration document — the same field vocabulary the
/// pipeline YAML's `destination: postgres:` block has always carried:
/// `conn`, `dataset`, `tls`, `merge_strategy`, `tables`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// libpq-style connection string/URL.
    #[serde(rename = "conn")]
    #[schemars(rename = "conn")]
    pub connection: String,
    /// Target schema (the document vocabulary calls it `dataset`); created
    /// if missing.
    ///
    /// OPTIONAL, defaulting to `public`. Worth stating plainly because
    /// the omission is silent and the consequence is a write to a
    /// different schema than intended: a document that leaves `dataset`
    /// out loads into `public`, it is not refused. Until 041 the
    /// pipeline YAML reached this connector through a hand-mirrored
    /// facade struct that REQUIRED the key, so a document missing it
    /// failed to parse; the facade now embeds this type directly (one
    /// vocabulary, no mirror to drift), which makes the library's
    /// long-standing default the document's default too. A relaxation,
    /// never a refusal — but a pipeline that relied on the old refusal
    /// to catch a typo'd or dropped `dataset:` will now run and land its
    /// tables in `public`. Spell the key.
    #[serde(rename = "dataset", default = "default_schema")]
    #[schemars(rename = "dataset")]
    pub schema: String,
    /// TLS posture (verify-* modes are expressible only here).
    #[serde(default)]
    pub tls: Option<crate::tls::Policy>,
    /// Default merge strategy for every merge table (sqlcore vocabulary).
    #[serde(default)]
    pub merge_strategy: Option<MergeStrategy>,
    /// Per-table strategy/hard-delete/SCD2 options (sqlcore vocabulary).
    #[serde(default)]
    pub tables: std::collections::BTreeMap<String, TableOptions>,
}

/// The [`Document`](rdlt_connector_sdk::config::Document) gate. Closing a
/// recorded asymmetry: generation 1's bare `Config::from_*` parsed WITHOUT
/// validating (only the handle constructors validated), so a config
/// document was checkable at one entry and not another. Under the sdk the
/// provided entry points all run this gate.
impl rdlt_connector_sdk::config::Document for Config {
    type Error = ConfigError;

    fn validate(&self) -> Result<(), ConfigError> {
        DestinationOptions {
            merge_strategy: self.merge_strategy,
            tables: self.tables.clone(),
        }
        .validate()
        .map_err(ConfigError::Invalid)
    }
}

fn default_schema() -> String {
    "public".into()
}

/// JSON Schema GENERATED from the config struct — the declared schema and
/// the parser cannot drift. Published through `ConnectorSpec`, so a
/// platform can render a destination form, not just a source one.
pub fn config_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(Config)).expect("schema serializes")
}

#[cfg(test)]
mod tests {
    use rdlt_connector_sdk::config::Document;

    use super::*;

    #[test]
    fn document_round_trips_and_validates() {
        let config = Config::from_yaml(
            r#"
conn: "host=h user=u"
dataset: raw
merge_strategy: upsert
tables:
  events:
    merge_strategy: delete_insert
"#,
        )
        .expect("document");
        let destination = Postgres::from_config(config).expect("parses");
        assert_eq!(destination.schema, "raw");
        assert_eq!(
            destination.options.merge_strategy,
            Some(MergeStrategy::Upsert)
        );
        // Dataset defaults to public; unknown fields refuse.
        let bare = Config::from_yaml("conn: \"host=h\"\n").expect("document");
        assert_eq!(
            Postgres::from_config(bare).expect("parses").schema,
            "public"
        );
        assert!(Config::from_yaml("conn: \"host=h\"\nghost: 1\n").is_err());
    }

    #[test]
    fn invalid_options_are_refused_at_parse() {
        // scd2 options without the scd2 strategy — sqlcore's validation
        // fires through the document entry point too, now at EVERY entry
        // (the sdk gate closed generation 1's non-validating from_*).
        let err = Config::from_yaml(
            "conn: \"host=h\"\ntables:\n  events:\n    scd2:\n      valid_from: vf\n",
        )
        .expect_err("invalid options refuse");
        assert!(matches!(err, ConfigError::Invalid(_)), "{err}");
    }

    #[test]
    fn schema_is_generated_from_the_struct() {
        let schema = config_schema();
        let properties = schema["properties"].as_object().expect("properties");
        for field in ["conn", "dataset", "tls", "merge_strategy", "tables"] {
            assert!(properties.contains_key(field), "{field}");
        }
    }
}
