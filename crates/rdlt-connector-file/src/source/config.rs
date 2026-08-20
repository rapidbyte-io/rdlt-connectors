//! The source document: stream vocabulary, validation, schema.
//!
//! A stream's format is DECLARED, never inferred from an extension;
//! everything an operator can get wrong is refused at the Document
//! gate with the field named. The type-hint names are the same
//! human-friendly spellings the REST source reads.

use std::collections::BTreeMap;

use rdlt_connector_sdk::config::Document;
use rdlt_connector_sdk::spi::core::types::LogicalType;

use crate::format::{Codec, Format, codec_of};
use crate::location::LocationOptions;

/// The whole source document: one or more streams.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Config {
    /// The streams to read; at least one.
    #[schemars(length(min = 1))]
    pub streams: Vec<Stream>,
}

/// One stream: a path (or glob) of files in one declared format.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Stream {
    pub name: String,
    /// Declared, never inferred: a `.csv` named `data.txt` reads fine,
    /// and a lying extension must not choose a parser.
    pub format: Format,
    /// Absent = the local filesystem.
    #[serde(default)]
    pub location: Option<LocationOptions>,
    /// CSV options — only with `format: csv`.
    #[serde(default)]
    pub csv: Option<CsvOptions>,
    /// Explicit file path or glob. A NAMED missing file is an error;
    /// an empty glob is an empty stream.
    pub path: String,
    /// Keyed identity for jsonl/csv rows; REFUSED on parquet (a
    /// structured format with no per-row identity to declare).
    #[serde(default)]
    pub primary_key: Option<Vec<String>>,
    /// Per-column type hints. CSV reads them directly, jsonl forwards
    /// them to the shredder; REFUSED on parquet (the file already
    /// carries its own types — see `Config::validate`).
    #[serde(default)]
    pub type_hints: BTreeMap<String, HintType>,
    /// Per-line JSON skim-validation; jsonl only. Absent resolves to
    /// the jsonl default of `true` at the connector boundary
    /// (`unwrap_or(true)`); an EXPLICIT value on a non-jsonl format is
    /// refused rather than silently ignored — see `Config::validate`.
    #[serde(default)]
    pub validate: Option<bool>,
}

/// CSV shape options.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct CsvOptions {
    #[serde(default = "default_delimiter")]
    pub delimiter: char,
    /// Without a header, columns are named `c0..cN`.
    #[serde(default = "default_header")]
    pub header: bool,
    #[serde(default = "default_quote")]
    pub quote: char,
}

fn default_delimiter() -> char {
    ','
}
fn default_header() -> bool {
    true
}
fn default_quote() -> char {
    '"'
}

impl Default for CsvOptions {
    fn default() -> Self {
        Self {
            delimiter: ',',
            header: true,
            quote: '"',
        }
    }
}

impl CsvOptions {
    /// The csv crate takes single bytes; a multi-byte char is refused
    /// naming which option.
    pub fn validate(&self, context: &str) -> Result<(), String> {
        for (name, value) in [("delimiter", self.delimiter), ("quote", self.quote)] {
            if !value.is_ascii() {
                return Err(format!(
                    "{context}: csv.{name} `{value}` must be a single ASCII byte"
                ));
            }
        }
        Ok(())
    }
}

/// The same human-friendly hint names as the REST source.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum HintType {
    Bool,
    Int64,
    Float64,
    Utf8,
    TimestampTz,
    Date,
    Time,
    Uuid,
    Json,
}

impl From<HintType> for LogicalType {
    fn from(hint: HintType) -> Self {
        match hint {
            HintType::Bool => LogicalType::Bool,
            HintType::Int64 => LogicalType::Int64,
            HintType::Float64 => LogicalType::Float64,
            HintType::Utf8 => LogicalType::Utf8,
            HintType::TimestampTz => LogicalType::TimestampTz,
            HintType::Date => LogicalType::Date,
            HintType::Time => LogicalType::Time,
            HintType::Uuid => LogicalType::Uuid,
            HintType::Json => LogicalType::Json,
        }
    }
}

/// Parse and validation failures, typed, with the frozen framings.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid file source YAML: {0}")]
    Yaml(#[from] serde_yaml_ng::Error),
    #[error("invalid file source JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid file source config: {0}")]
    Invalid(String),
}

impl Document for Config {
    type Error = ConfigError;

    fn validate(&self) -> Result<(), ConfigError> {
        self.validate()
    }
}

impl Config {
    /// The one gate; every refusal names the stream and the field.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let invalid = |message: String| Err(ConfigError::Invalid(message));
        if self.streams.is_empty() {
            return invalid("at least one stream is required".into());
        }
        // Duplicate names are refused at the gate (030 review, the 029
        // shared-table precedent): the reader resolves streams BY NAME,
        // so a duplicate would silently shadow its twin — and both
        // would feed one destination table under one part-index
        // sequence.
        let mut seen = std::collections::BTreeSet::new();
        for stream in &self.streams {
            if !seen.insert(stream.name.as_str()) {
                return invalid(format!(
                    "duplicate stream name `{}` — stream names must be unique; a \
                     duplicate is silently shadowed on read and both streams would \
                     write one destination table",
                    stream.name
                ));
            }
        }
        for stream in &self.streams {
            let context = format!("stream `{}`", stream.name);
            if let Some(location) = &stream.location
                && let Err(message) = location.validate(&context)
            {
                return invalid(message);
            }
            if stream.csv.is_some() && stream.format != Format::Csv {
                return invalid(format!(
                    "stream `{}`: a `csv` options block requires `format: csv`",
                    stream.name
                ));
            }
            if let Some(csv) = &stream.csv
                && let Err(message) = csv.validate(&context)
            {
                return invalid(message);
            }
            if stream.format == Format::Parquet {
                if stream.primary_key.is_some() {
                    return invalid(format!(
                        "stream `{}`: parquet streams are structured and cannot declare \
                         primary_key (no per-row identity)",
                        stream.name
                    ));
                }
                if codec_of(&stream.path) != Codec::Plain {
                    return invalid(format!(
                        "stream `{}`: parquet carries its own internal codecs — a \
                         compression extension on a parquet path is not supported",
                        stream.name
                    ));
                }
                if !stream.type_hints.is_empty() {
                    return invalid(format!(
                        "stream `{}`: type_hints are not honored for format parquet (the \
                         file carries its own types)",
                        stream.name
                    ));
                }
            }
            if stream.format != Format::Jsonl && stream.validate.is_some() {
                return invalid(format!(
                    "stream `{}`: validate applies only to format jsonl",
                    stream.name
                ));
            }
        }
        Ok(())
    }
}

/// The generated declaration — from the SAME structs the parser
/// reads, so declaration and parser cannot drift.
pub fn config_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(Config)).expect("schema serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal() -> serde_json::Value {
        serde_json::json!({
            "streams": [{"name": "events", "format": "jsonl", "path": "/data/*.jsonl"}]
        })
    }

    /// Wraps one stream body (already-valid YAML mapping contents,
    /// indentation-relative) as the sole entry of a `streams:` document.
    fn parse_stream_yaml(stream: &str) -> Result<Config, ConfigError> {
        let mut yaml = String::from("streams:\n");
        for (i, line) in stream.lines().enumerate() {
            yaml.push_str(if i == 0 { "  - " } else { "    " });
            yaml.push_str(line);
            yaml.push('\n');
        }
        Config::from_yaml(&yaml)
    }

    /// The smallest valid document, and what defaults fill in.
    #[test]
    fn a_minimal_document_parses_and_defaults_the_rest() {
        let config = Config::from_value(minimal()).expect("valid");
        let stream = &config.streams[0];
        assert!(stream.location.is_none() && stream.csv.is_none());
        assert!(stream.primary_key.is_none() && stream.type_hints.is_empty());
        assert!(
            stream.validate.is_none(),
            "absent stays unresolved; the connector boundary resolves jsonl to true"
        );
    }

    /// type_hints on parquet is refused, not silently ignored.
    #[test]
    fn type_hints_on_parquet_are_refused_not_ignored() {
        let err = parse_stream_yaml("name: t\nformat: parquet\npath: p\ntype_hints:\n  a: int64\n")
            .expect_err("parquet carries its own types");
        assert!(
            err.to_string().contains(
                "stream `t`: type_hints are not honored for format parquet (the file \
                 carries its own types)"
            ),
            "{err}"
        );
    }

    /// An explicit `validate` off jsonl is refused; the default (absent
    /// key) stays silently fine on csv/parquet.
    #[test]
    fn explicit_validate_off_jsonl_is_refused_and_the_default_is_not() {
        let err = parse_stream_yaml("name: t\nformat: csv\npath: p\nvalidate: true\n")
            .expect_err("validate is jsonl-only");
        assert!(
            err.to_string()
                .contains("stream `t`: validate applies only to format jsonl"),
            "{err}"
        );
        // The DEFAULT stays silent: an absent key on csv/parquet is fine.
        assert!(parse_stream_yaml("name: t\nformat: csv\npath: p\n").is_ok());
    }

    /// Every refusal with its frozen spelling.
    #[test]
    fn every_refusal_names_the_stream_with_the_frozen_spelling() {
        let cases: Vec<(serde_json::Value, &str)> = vec![
            (
                serde_json::json!({"streams": []}),
                "at least one stream is required",
            ),
            (
                serde_json::json!({"streams": [{
                    "name": "a", "format": "jsonl", "path": "p",
                    "csv": {},
                }]}),
                "stream `a`: a `csv` options block requires `format: csv`",
            ),
            (
                serde_json::json!({"streams": [{
                    "name": "a", "format": "csv", "path": "p",
                    "csv": {"delimiter": "→"},
                }]}),
                "stream `a`: csv.delimiter `→` must be a single ASCII byte",
            ),
            (
                serde_json::json!({"streams": [{
                    "name": "a", "format": "parquet", "path": "p.parquet",
                    "primary_key": ["id"],
                }]}),
                "stream `a`: parquet streams are structured and cannot declare primary_key (no \
                 per-row identity)",
            ),
            (
                serde_json::json!({"streams": [{
                    "name": "a", "format": "parquet", "path": "p.parquet.gz",
                }]}),
                "stream `a`: parquet carries its own internal codecs — a compression extension \
                 on a parquet path is not supported",
            ),
            (
                serde_json::json!({"streams": [{
                    "name": "a", "format": "jsonl", "path": "p",
                    "location": {},
                }]}),
                "stream `a`: `location` block declares no storage kind (expected `s3`)",
            ),
        ];
        for (doc, want) in cases {
            let err = Config::from_value(doc).expect_err("refused");
            assert_eq!(
                format!("{err}"),
                format!("invalid file source config: {want}")
            );
        }
    }

    /// The parser framings are frozen; unknown fields refuse.
    #[test]
    fn parser_failures_and_unknown_fields_refuse() {
        let err = Config::from_yaml(": not yaml").expect_err("refused");
        assert!(
            format!("{err}").starts_with("invalid file source YAML: "),
            "{err}"
        );
        let err = Config::from_json("{").expect_err("refused");
        assert!(
            format!("{err}").starts_with("invalid file source JSON: "),
            "{err}"
        );

        let mut doc = minimal();
        doc["streams"][0]["forma"] = serde_json::json!("typo");
        Config::from_value(doc).expect_err("unknown field refused");
    }

    /// Every hint name maps 1:1 onto its logical type.
    #[test]
    fn every_hint_maps_to_its_logical_type() {
        for (name, logical) in [
            ("bool", LogicalType::Bool),
            ("int64", LogicalType::Int64),
            ("float64", LogicalType::Float64),
            ("utf8", LogicalType::Utf8),
            ("timestamp_tz", LogicalType::TimestampTz),
            ("date", LogicalType::Date),
            ("time", LogicalType::Time),
            ("uuid", LogicalType::Uuid),
            ("json", LogicalType::Json),
        ] {
            let hint: HintType = serde_yaml_ng::from_str(name).expect("parses");
            assert_eq!(LogicalType::from(hint), logical, "{name}");
        }
    }

    /// The schema generates from the same structs and refuses what the
    /// parser refuses.
    #[test]
    fn the_schema_and_parser_agree_on_unknown_fields() {
        let schema = jsonschema::validator_for(&config_schema()).expect("valid schema");
        assert!(schema.validate(&minimal()).is_ok());
        let mut doc = minimal();
        doc["streams"][0]["forma"] = serde_json::json!("typo");
        assert!(schema.validate(&doc).is_err());
    }

    /// Duplicate stream names are refused at the gate — the reader
    /// resolves by name and would silently shadow the twin (030
    /// review, docket S1; the 029 shared-table precedent).
    #[test]
    fn duplicate_stream_names_are_refused() {
        let err = Config::from_value(serde_json::json!({
            "streams": [
                {"name": "events", "format": "jsonl", "path": "a/*.jsonl"},
                {"name": "events", "format": "jsonl", "path": "b/*.jsonl"},
            ]
        }))
        .expect_err("refused")
        .to_string();
        assert_eq!(
            err,
            "invalid file source config: duplicate stream name `events` — stream names \
             must be unique; a duplicate is silently shadowed on read and both streams \
             would write one destination table"
        );
    }
}
