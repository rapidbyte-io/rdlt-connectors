//! The destination document: database path, resource settings, and
//! the sqlcore merge vocabulary — the same shape the facade's YAML
//! arm always spoke, now a first-class parse-then-validate Document
//! (generation 1 predates the sdk and had only a builder).

use std::collections::BTreeMap;

use rdlt_connector_sdk::config::Document;
use rdlt_connector_sqlcore::{DestinationOptions, MergeStrategy, TableOptions};

/// The whole destination document.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Config {
    /// The database file; created if absent.
    pub path: std::path::PathBuf,
    /// Caps DuckDB's buffer/cache (its default is a fraction of
    /// SYSTEM RAM, which dominates pipeline RSS). Sugar for
    /// `settings: {memory_limit: …}`.
    #[serde(default)]
    pub memory_limit: Option<String>,
    /// The sqlcore default strategy; None = delete_insert, and the
    /// distinction is LOAD-BEARING (an EXPLICIT strategy under
    /// append/replace is a typed error).
    #[serde(default)]
    pub merge_strategy: Option<MergeStrategy>,
    /// Per-table sqlcore options.
    #[serde(default)]
    pub tables: Option<BTreeMap<String, TableOptions>>,
    /// `LOAD {name}` passthrough (dlt parity).
    #[serde(default)]
    pub extensions: Option<Vec<String>>,
    /// Raw `SET {key}='{value}'` passthrough.
    #[serde(default)]
    pub settings: Option<BTreeMap<String, String>>,
}

/// Parse and validation failures, typed, with the sdk from-text
/// framings (a NEW surface — generation 1 never parsed text).
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid duckdb destination YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("invalid duckdb destination JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid duckdb destination config: {0}")]
    Invalid(String),
}

impl Document for Config {
    type Error = ConfigError;

    fn validate(&self) -> Result<(), ConfigError> {
        let invalid = |m: String| Err(ConfigError::Invalid(m));
        if self.path.as_os_str().is_empty() {
            return invalid("`path` must not be empty".into());
        }
        // The bare-identifier rule guards the ONLY places this crate
        // interpolates un-quoted text into SQL; the spellings are gen
        // 1's, frozen. Values are escaped at connect ('' doubling).
        for key in self.settings.iter().flat_map(|s| s.keys()) {
            check_bare_identifier("setting", "keys", key).or_else(invalid)?;
            // Both spellings are user-supplied SETs and the later one
            // would silently win — refuse the ambiguity instead.
            // Case-insensitive: DuckDB SET keys are.
            if key.eq_ignore_ascii_case("memory_limit") && self.memory_limit.is_some() {
                return invalid(
                    "`settings.memory_limit` conflicts with the `memory_limit` field — \
                     set one, not both"
                        .into(),
                );
            }
        }
        for name in self.extensions.iter().flatten() {
            check_bare_identifier("extension", "names", name).or_else(invalid)?;
        }
        // The sqlcore vocabulary validates as one document.
        if let Err(e) = self.options().validate() {
            return invalid(e.to_string());
        }
        Ok(())
    }
}

fn check_bare_identifier(noun: &str, plural: &str, name: &str) -> Result<(), String> {
    let bare = !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if bare {
        Ok(())
    } else {
        Err(format!(
            "duckdb {noun} `{name}`: {plural} must be bare identifiers ([A-Za-z0-9_]) — \
             refusing to interpolate"
        ))
    }
}

impl Config {
    /// The programmatic seam.
    pub fn new(path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            path: path.into(),
            memory_limit: None,
            merge_strategy: None,
            tables: None,
            extensions: None,
            settings: None,
        }
    }

    /// The resolved sqlcore options document — callers never assemble
    /// it by hand.
    pub(crate) fn options(&self) -> DestinationOptions {
        DestinationOptions {
            merge_strategy: self.merge_strategy,
            tables: self.tables.clone().unwrap_or_default(),
        }
    }
}

/// The generated declaration, from the same structs the parser reads.
pub fn config_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(Config)).expect("schema serializes")
}
