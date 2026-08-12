//! The destination document: output path and location, the format
//! kinds, single-column partitioning, and the parquet tuning block.

use rdlt_connector_sdk::config::Document;
use rdlt_connector_sdk::spi::{ParquetOptions, PartOptions};

use crate::location::LocationOptions;

/// The whole destination document.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Config {
    /// Output directory (local) or key prefix (object store).
    pub path: String,
    /// Absent = the local filesystem.
    #[serde(default)]
    pub location: Option<LocationOptions>,
    /// The output kind; parquet is the default.
    #[serde(default)]
    pub format: DestFormat,
    /// ONE column; partition directories are BARE values
    /// (`<table>/<value>/part-...`), never Hive `col=value`; NULL
    /// lands under `__null__`. The column must exist at write time.
    #[serde(default)]
    pub partition_by: Option<String>,
    /// Parquet writer tuning; absent = the COMPRESSED defaults.
    #[serde(default)]
    pub parquet: Option<ParquetOptions>,
    /// How large an output part grows before it is closed; absent =
    /// the 128 MiB default. This is the OUTPUT size, measured after
    /// encoding — not `batch_policy`, which bounds Arrow memory.
    #[serde(default)]
    pub parts: Option<PartOptions>,
}

/// The output kinds this destination writes.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum DestFormat {
    #[default]
    Parquet,
    Jsonl,
}

impl DestFormat {
    /// Every kind with its extension — the truncation ownership rule
    /// iterates THIS list, so a new kind extends ownership by
    /// construction (the exhaustive match breaks compilation until it
    /// is added here).
    pub(crate) const ALL: &'static [DestFormat] = &[DestFormat::Parquet, DestFormat::Jsonl];

    pub(crate) fn extension(self) -> &'static str {
        match self {
            DestFormat::Parquet => "parquet",
            DestFormat::Jsonl => "jsonl",
        }
    }
}

/// Parse and validation failures, typed, with the frozen framings.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid file destination YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("invalid file destination JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid file destination config: {0}")]
    Invalid(String),
}

impl Document for Config {
    type Error = ConfigError;

    fn validate(&self) -> Result<(), ConfigError> {
        self.validate_in("file destination")
            .map_err(ConfigError::Invalid)
    }
}

impl Config {
    /// The programmatic seam.
    pub fn new(path: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            location: None,
            format: DestFormat::Parquet,
            partition_by: None,
            parquet: None,
            parts: None,
        }
    }

    pub fn with_location(mut self, location: LocationOptions) -> Self {
        self.location = Some(location);
        self
    }

    pub fn with_format(mut self, format: DestFormat) -> Self {
        self.format = format;
        self
    }

    pub fn with_partition_by(mut self, column: impl Into<String>) -> Self {
        self.partition_by = Some(column.into());
        self
    }

    pub fn with_parquet(mut self, parquet: ParquetOptions) -> Self {
        self.parquet = Some(parquet);
        self
    }

    /// The configured options, or the defaults — callers never read
    /// the raw field.
    pub fn parquet_options(&self) -> ParquetOptions {
        self.parquet.clone().unwrap_or_default()
    }

    /// The configured part sizing, or the 128 MiB default.
    pub fn part_options(&self) -> PartOptions {
        self.parts.unwrap_or_default()
    }

    pub fn with_parts(mut self, parts: PartOptions) -> Self {
        self.parts = Some(parts);
        self
    }

    /// The one gate, context-framed by the caller.
    pub(crate) fn validate_in(&self, context: &str) -> Result<(), String> {
        if self.path.is_empty() {
            return Err(format!("{context}: `path` must not be empty"));
        }
        if let Some(location) = &self.location {
            location.validate(context)?;
        }
        if self.partition_by.as_deref() == Some("") {
            return Err(format!("{context}: `partition_by` must name a column"));
        }
        if self.parquet.is_some() && self.format != DestFormat::Parquet {
            let fmt = match self.format {
                DestFormat::Parquet => unreachable!("guarded above"),
                DestFormat::Jsonl => "jsonl",
            };
            return Err(format!(
                "{context}: a `parquet` block is set but `format` is `{fmt}` — these settings \
                 only apply to parquet output; remove the block, or set `format: parquet`"
            ));
        }
        if let Some(parquet) = &self.parquet
            && let Err(e) = parquet.validate()
        {
            return Err(format!("{context}: {e}"));
        }
        // `parts` applies to BOTH formats — a JSONL part has a size
        // too — so unlike `parquet` there is no format rule here.
        if let Some(parts) = &self.parts
            && let Err(e) = parts.validate()
        {
            return Err(format!("{context}: {e}"));
        }
        Ok(())
    }
}

/// The generated declaration, from the same structs the parser reads.
pub fn config_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(Config)).expect("schema serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The refusals with their frozen spellings.
    #[test]
    fn every_refusal_carries_the_frozen_spelling() {
        let cases: Vec<(serde_json::Value, &str)> = vec![
            (
                serde_json::json!({"path": ""}),
                "file destination: `path` must not be empty",
            ),
            (
                serde_json::json!({"path": "out", "partition_by": ""}),
                "file destination: `partition_by` must name a column",
            ),
            (
                serde_json::json!({"path": "out", "format": "jsonl", "parquet": {}}),
                "file destination: a `parquet` block is set but `format` is `jsonl` — these \
                 settings only apply to parquet output; remove the block, or set `format: \
                 parquet`",
            ),
            (
                serde_json::json!({"path": "out", "location": {}}),
                "file destination: `location` block declares no storage kind (expected `s3`)",
            ),
            (
                serde_json::json!({"path": "out", "parquet": {"max_row_group_rows": 0}}),
                "file destination: `max_row_group_rows` is 0 — a row group must hold at least \
                 one row; remove the setting to use the default, or give a positive count",
            ),
        ];
        for (doc, want) in cases {
            let err = Config::from_value(doc).expect_err("refused");
            assert_eq!(
                format!("{err}"),
                format!("invalid file destination config: {want}")
            );
        }
    }

    /// The defaults: parquet format, no partitioning, and the
    /// COMPRESSED parquet defaults through the accessor.
    #[test]
    fn the_defaults_are_parquet_and_compressed() {
        let config = Config::from_value(serde_json::json!({"path": "out"})).expect("valid");
        assert_eq!(config.format, DestFormat::Parquet);
        assert!(config.partition_by.is_none());
        assert_eq!(config.parquet_options(), ParquetOptions::default());
    }

    /// Every kind is in ALL with its extension — the ownership rule's
    /// completeness guarantee.
    #[test]
    fn every_kind_is_in_the_ownership_list() {
        assert_eq!(DestFormat::ALL.len(), 2);
        assert_eq!(DestFormat::Parquet.extension(), "parquet");
        assert_eq!(DestFormat::Jsonl.extension(), "jsonl");
    }

    /// The format spellings round-trip.
    #[test]
    fn the_kind_spellings_round_trip() {
        for (text, format) in [
            ("parquet", DestFormat::Parquet),
            ("jsonl", DestFormat::Jsonl),
        ] {
            let parsed: DestFormat = serde_yaml::from_str(text).expect("parses");
            assert_eq!(parsed, format);
        }
    }
}
