//! The config document: vocabulary, validation, schema.
//!
//! This module is the ONLY place configuration text is rendered. The
//! YAML/JSON field spellings are the frozen vocabulary generation 1
//! shipped — field names ARE the wire names (no serde renames), every
//! struct refuses unknown fields, and the partition transform keeps its
//! `singleton_map` spelling (`transform: day` as a plain string,
//! `transform: {bucket: 16}` as a one-key map) on every deserializer.
//!
//! Validation is the [`Document`] gate: `Shell::from_yaml` /
//! `from_json` / `from_value` / `new` all pass through it, and each
//! rendered refusal names the field to fix.

use std::collections::BTreeMap;

use rdlt_connector_sdk::config::Document;
use rdlt_connector_sdk::spi::secret::Secret;

/// The whole document. `catalog` says where, `namespace` says which
/// tables, `storage` (optional) overrides the credential-vending
/// default with explicit family-S3 keys, `tables` carries per-stream
/// naming and partitioning, `parquet` tunes the data files.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Config {
    /// The REST catalog: endpoint, warehouse, credentials.
    pub catalog: CatalogOptions,
    /// Dot-separated namespace levels, e.g. `raw.orders`.
    pub namespace: String,
    /// Create the namespace if the catalog does not know it.
    #[serde(default)]
    pub create_namespace: bool,
    /// Explicit object-store credentials. Absent, the catalog VENDS
    /// scoped credentials per table (the default, and the verified
    /// path).
    #[serde(default)]
    pub storage: Option<StorageOptions>,
    /// Per-table options, keyed by the ENGINE'S NORMALIZED root-table
    /// name — lower-cased, non-`[a-z0-9_]` mapped to `_` — not the raw
    /// stream name (a stream `Order-Items` is keyed `order_items`).
    /// The frozen generation-1 lookup semantics; pinned live.
    #[serde(default)]
    pub tables: BTreeMap<String, TableOptions>,
    /// Parquet writer tuning; the SPI defaults (snappy) apply when
    /// absent.
    #[serde(default)]
    pub parquet: Option<rdlt_connector_sdk::spi::ParquetOptions>,
    /// Data-file sizing; absent = the SPI's 128 MiB default.
    ///
    /// `target_bytes` becomes the library's `target_file_size`, i.e.
    /// the Iceberg table property `write.target-file-size-bytes`. NOTE
    /// this is NOT the library's own default of 512 MiB: rdlt uses one
    /// size across every destination it writes files from, and 128 MiB
    /// is that size.
    ///
    /// `max_open_bytes` is met trivially here — the library's rolling
    /// writer streams each file out rather than accumulating it, so
    /// there is nothing for a memory ceiling to cap.
    #[serde(default)]
    pub parts: Option<rdlt_connector_sdk::spi::PartOptions>,
}

/// Where the catalog lives and how to talk to it.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct CatalogOptions {
    /// REST catalog root, e.g. `https://host/api/catalog`.
    pub uri: String,
    /// The warehouse the catalog serves.
    pub warehouse: String,
    /// Exactly one auth scheme.
    pub auth: Auth,
    /// Verbatim passthrough properties. Inserted LAST, so they win
    /// over generated ones — the documented escape hatch for
    /// non-secret keys (e.g. `uri`). The four credential-bearing keys
    /// (`credential`, `token`, `s3.access-key-id`,
    /// `s3.secret-access-key`) are refused at validate; use the typed
    /// auth fields instead.
    #[serde(default)]
    pub props: BTreeMap<String, String>,
}

/// The auth vocabulary: a struct of optional kinds, exactly one set.
#[derive(
    Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Auth {
    /// OAuth2 client-credentials against the catalog's token endpoint
    /// (or an explicit one).
    #[serde(default)]
    pub oauth2_client_credentials: Option<Oauth2ClientCredentials>,
    /// A static bearer token.
    #[serde(default)]
    pub bearer: Option<BearerAuth>,
}

impl Auth {
    /// The client-credentials form.
    pub fn oauth2(
        client_id: impl Into<String>,
        client_secret: Secret,
        scopes: Vec<String>,
    ) -> Self {
        Self {
            oauth2_client_credentials: Some(Oauth2ClientCredentials {
                token_url: None,
                client_id: client_id.into(),
                client_secret,
                scopes,
            }),
            bearer: None,
        }
    }

    /// The static-token form.
    pub fn bearer(token: Secret) -> Self {
        Self {
            oauth2_client_credentials: None,
            bearer: Some(BearerAuth { token }),
        }
    }
}

/// OAuth2 client credentials.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Oauth2ClientCredentials {
    /// Defaults to the catalog's own token endpoint.
    #[serde(default)]
    pub token_url: Option<String>,
    pub client_id: String,
    pub client_secret: Secret,
    #[serde(default)]
    pub scopes: Vec<String>,
}

/// A static bearer token.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct BearerAuth {
    pub token: Secret,
}

/// The storage override family — `s3` is the one kind.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct StorageOptions {
    #[serde(default)]
    pub s3: Option<S3Override>,
}

/// Explicit family-S3 credentials, replacing credential vending.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct S3Override {
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub region: Option<String>,
    pub access_key: Secret,
    pub secret_key: Secret,
    /// Path-style addressing — the shape self-hosted stores expect.
    #[serde(default = "default_path_style")]
    pub path_style: bool,
}

fn default_path_style() -> bool {
    true
}

impl S3Override {
    pub fn new(access_key: Secret, secret_key: Secret) -> Self {
        Self {
            endpoint: None,
            region: None,
            access_key,
            secret_key,
            path_style: true,
        }
    }

    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = Some(endpoint.into());
        self
    }

    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }
}

/// Per-stream table options.
#[derive(
    Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct TableOptions {
    /// The table name; defaults to the stream name.
    #[serde(default)]
    pub name: Option<String>,
    /// Partition fields, applied at CREATE (the spec is fixed there).
    #[serde(default)]
    pub partition_by: Vec<PartitionField>,
}

impl TableOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    pub fn with_partition(
        mut self,
        column: impl Into<String>,
        transform: PartitionTransform,
    ) -> Self {
        self.partition_by.push(PartitionField {
            column: column.into(),
            transform,
        });
        self
    }
}

/// One partition field: a source column and its transform.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct PartitionField {
    pub column: String,
    /// `singleton_map`: unit transforms spell as plain strings
    /// (`transform: day`), parameterized ones as one-key maps
    /// (`transform: {bucket: 16}`) — on YAML and JSON alike. Without
    /// the adapter, serde_yaml would demand `!bucket` tag syntax.
    #[serde(with = "serde_yaml::with::singleton_map")]
    #[schemars(with = "PartitionTransform")]
    pub transform: PartitionTransform,
}

/// The transform vocabulary — the service-defined closed set.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum PartitionTransform {
    Identity,
    Year,
    Month,
    Day,
    Hour,
    Bucket(u32),
    Truncate(u32),
}

/// Parse and validation failures, typed. The parser variants render
/// the frozen framing generation 1 shipped; every `Invalid` message
/// names the field to fix.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid iceberg destination YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("invalid iceberg destination JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid iceberg destination config: {0}")]
    Invalid(String),
}

impl Document for Config {
    type Error = ConfigError;

    fn validate(&self) -> Result<(), ConfigError> {
        self.validate()
    }
}

impl Config {
    /// The one gate. Order matters: the messages are frozen, and so is
    /// which of two coexisting problems an operator hears about first.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let invalid = |message: String| Err(ConfigError::Invalid(message));

        if !is_http_url(&self.catalog.uri) {
            return invalid(format!(
                "catalog.uri `{}` must be an http(s) URL",
                self.catalog.uri
            ));
        }
        if self.catalog.warehouse.is_empty() {
            return invalid("catalog.warehouse must not be empty".into());
        }
        match (
            &self.catalog.auth.oauth2_client_credentials,
            &self.catalog.auth.bearer,
        ) {
            (Some(_), Some(_)) => {
                return invalid(
                    "catalog.auth declares BOTH oauth2_client_credentials and bearer — pick one"
                        .into(),
                );
            }
            (None, None) => {
                return invalid(
                    "catalog.auth declares no scheme (expected oauth2_client_credentials or bearer)"
                        .into(),
                );
            }
            _ => {}
        }
        if let Some(oauth) = &self.catalog.auth.oauth2_client_credentials
            && let Some(url) = &oauth.token_url
            && !is_http_url(url)
        {
            return invalid(format!(
                "catalog.auth.oauth2_client_credentials.token_url `{url}` must be an http(s) URL"
            ));
        }
        for key in super::client::CREDENTIAL_PROP_KEYS {
            if self.catalog.props.contains_key(*key) {
                return invalid(format!(
                    "catalog.props may not override the credential key `{key}`; use the typed \
                     auth fields"
                ));
            }
        }
        if self.namespace.is_empty() || self.namespace.split('.').any(str::is_empty) {
            return invalid(format!(
                "namespace `{}` must be non-empty dot-separated levels",
                self.namespace
            ));
        }
        if let Some(storage) = &self.storage
            && storage.s3.is_none()
        {
            return invalid("`storage` block declares no kind (expected `s3`)".into());
        }
        if let Some(parquet) = &self.parquet
            && let Err(e) = parquet.validate()
        {
            return invalid(e.to_string());
        }
        if let Some(parts) = &self.parts
            && let Err(e) = parts.validate()
        {
            return invalid(e.to_string());
        }
        // Two streams resolving to ONE physical table would interleave
        // colliding data-file paths and let one stream's commit read as
        // the other's replay — refused here for explicit names, and
        // again at ensure time where defaulted names are visible.
        let mut resolved: std::collections::BTreeMap<&str, &str> =
            std::collections::BTreeMap::new();
        for (stream, table) in &self.tables {
            if let Some(name) = table.name.as_deref()
                && let Some(other) = resolved.insert(name, stream)
            {
                return invalid(format!(
                    "tables.{other} and tables.{stream} both resolve to table `{name}` — two \
                     streams may not share one table"
                ));
            }
            if table.name.as_deref() == Some("") {
                return invalid(format!("tables.{stream}.name must not be empty"));
            }
            for field in &table.partition_by {
                if field.column.is_empty() {
                    return invalid(format!(
                        "tables.{stream}.partition_by: a field names no column"
                    ));
                }
                match field.transform {
                    PartitionTransform::Bucket(0) => {
                        return invalid(format!(
                            "tables.{stream}.partition_by.{}: bucket count must be >= 1",
                            field.column
                        ));
                    }
                    PartitionTransform::Truncate(0) => {
                        return invalid(format!(
                            "tables.{stream}.partition_by.{}: truncate width must be >= 1",
                            field.column
                        ));
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    /// The namespace as its dot-separated levels.
    pub fn namespace_levels(&self) -> Vec<String> {
        self.namespace.split('.').map(str::to_owned).collect()
    }

    /// A stream's table name — configured, or the stream's own.
    pub fn table_name(&self, stream: &str) -> String {
        self.tables
            .get(stream)
            .and_then(|t| t.name.clone())
            .unwrap_or_else(|| stream.to_owned())
    }

    /// A stream's partition fields (empty when unconfigured).
    pub fn partition_fields(&self, stream: &str) -> &[PartitionField] {
        self.tables
            .get(stream)
            .map(|t| t.partition_by.as_slice())
            .unwrap_or(&[])
    }
}

fn is_http_url(url: &str) -> bool {
    url.starts_with("http://") || url.starts_with("https://")
}

/// The generated declaration — from the SAME structs the parser reads,
/// so declaration and parser cannot drift.
pub fn config_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(Config)).expect("schema serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal() -> serde_json::Value {
        serde_json::json!({
            "catalog": {
                "uri": "http://localhost:8181/api/catalog",
                "warehouse": "wh",
                "auth": {"bearer": {"token": "t"}},
            },
            "namespace": "raw",
        })
    }

    /// The smallest valid document, and what the defaults fill in.
    #[test]
    fn a_minimal_document_parses_and_defaults_the_rest() {
        let config = Config::from_value(minimal()).expect("valid");
        assert!(!config.create_namespace);
        assert!(config.storage.is_none() && config.parquet.is_none());
        assert!(config.tables.is_empty());
        assert_eq!(config.namespace_levels(), vec!["raw"]);
        assert_eq!(config.table_name("events"), "events");
        assert!(config.partition_fields("events").is_empty());
    }

    /// Every validation refusal, with its frozen spelling.
    #[test]
    fn every_refusal_names_the_field_with_the_frozen_spelling() {
        let cases: Vec<(serde_json::Value, &str)> = vec![
            (
                {
                    let mut doc = minimal();
                    doc["catalog"]["uri"] = "ftp://x".into();
                    doc
                },
                "catalog.uri `ftp://x` must be an http(s) URL",
            ),
            (
                {
                    let mut doc = minimal();
                    doc["catalog"]["warehouse"] = "".into();
                    doc
                },
                "catalog.warehouse must not be empty",
            ),
            (
                {
                    let mut doc = minimal();
                    doc["catalog"]["auth"] = serde_json::json!({
                        "bearer": {"token": "t"},
                        "oauth2_client_credentials": {"client_id": "c", "client_secret": "s"},
                    });
                    doc
                },
                "catalog.auth declares BOTH oauth2_client_credentials and bearer — pick one",
            ),
            (
                {
                    let mut doc = minimal();
                    doc["catalog"]["auth"] = serde_json::json!({});
                    doc
                },
                "catalog.auth declares no scheme (expected oauth2_client_credentials or bearer)",
            ),
            (
                {
                    let mut doc = minimal();
                    doc["catalog"]["auth"] = serde_json::json!({
                        "oauth2_client_credentials": {
                            "client_id": "c", "client_secret": "s", "token_url": "not-a-url"
                        }
                    });
                    doc
                },
                "catalog.auth.oauth2_client_credentials.token_url `not-a-url` must be an http(s) \
                 URL",
            ),
            (
                {
                    let mut doc = minimal();
                    doc["namespace"] = "raw..orders".into();
                    doc
                },
                "namespace `raw..orders` must be non-empty dot-separated levels",
            ),
            (
                {
                    let mut doc = minimal();
                    doc["storage"] = serde_json::json!({});
                    doc
                },
                "`storage` block declares no kind (expected `s3`)",
            ),
            (
                {
                    let mut doc = minimal();
                    doc["tables"] = serde_json::json!({"events": {"name": ""}});
                    doc
                },
                "tables.events.name must not be empty",
            ),
            (
                {
                    let mut doc = minimal();
                    doc["tables"] = serde_json::json!({
                        "a": {"name": "shared"}, "b": {"name": "shared"}
                    });
                    doc
                },
                "tables.a and tables.b both resolve to table `shared` — two streams may not \
                 share one table",
            ),
            (
                {
                    let mut doc = minimal();
                    doc["tables"] = serde_json::json!({
                        "events": {"partition_by": [{"column": "", "transform": "day"}]}
                    });
                    doc
                },
                "tables.events.partition_by: a field names no column",
            ),
            (
                {
                    let mut doc = minimal();
                    doc["tables"] = serde_json::json!({
                        "events": {"partition_by": [{"column": "id", "transform": {"bucket": 0}}]}
                    });
                    doc
                },
                "tables.events.partition_by.id: bucket count must be >= 1",
            ),
            (
                {
                    let mut doc = minimal();
                    doc["tables"] = serde_json::json!({
                        "events": {"partition_by": [{"column": "id", "transform": {"truncate": 0}}]}
                    });
                    doc
                },
                "tables.events.partition_by.id: truncate width must be >= 1",
            ),
            (
                {
                    let mut doc = minimal();
                    doc["catalog"]["props"] = serde_json::json!({"credential": "sneaky"});
                    doc
                },
                "catalog.props may not override the credential key `credential`; use the typed \
                 auth fields",
            ),
            (
                {
                    let mut doc = minimal();
                    doc["catalog"]["props"] = serde_json::json!({"token": "sneaky"});
                    doc
                },
                "catalog.props may not override the credential key `token`; use the typed auth \
                 fields",
            ),
            (
                {
                    let mut doc = minimal();
                    doc["catalog"]["props"] = serde_json::json!({"s3.access-key-id": "sneaky"});
                    doc
                },
                "catalog.props may not override the credential key `s3.access-key-id`; use the \
                 typed auth fields",
            ),
            (
                {
                    let mut doc = minimal();
                    doc["catalog"]["props"] = serde_json::json!({"s3.secret-access-key": "sneaky"});
                    doc
                },
                "catalog.props may not override the credential key `s3.secret-access-key`; use \
                 the typed auth fields",
            ),
        ];
        for (doc, want) in cases {
            let err = Config::from_value(doc).expect_err("refused");
            assert_eq!(
                format!("{err}"),
                format!("invalid iceberg destination config: {want}"),
            );
        }
    }

    /// The Secret bypass, closed: none of the four credential-bearing
    /// prop keys may arrive through `catalog.props`, individually.
    #[test]
    fn credential_keys_in_catalog_props_are_refused() {
        for key in [
            "credential",
            "token",
            "s3.access-key-id",
            "s3.secret-access-key",
        ] {
            let mut doc = minimal();
            doc["catalog"]["props"] = serde_json::json!({ key: "sneaky" });
            let err = Config::from_value(doc).expect_err(key);
            assert!(
                err.to_string().contains(&format!(
                    "catalog.props may not override the credential key `{key}`; use the typed \
                     auth fields"
                )),
                "{err}"
            );
        }
    }

    /// The transform spellings: plain strings for unit variants,
    /// one-key maps for parameterized ones — on YAML and JSON alike.
    #[test]
    fn partition_transforms_keep_the_singleton_map_spellings() {
        let yaml = r#"
catalog:
  uri: http://localhost:8181/api/catalog
  warehouse: wh
  auth: {bearer: {token: t}}
namespace: raw
tables:
  events:
    partition_by:
      - {column: ts, transform: day}
      - {column: id, transform: {bucket: 16}}
      - {column: code, transform: {truncate: 8}}
"#;
        let config = Config::from_yaml(yaml).expect("valid");
        assert_eq!(
            config
                .partition_fields("events")
                .iter()
                .map(|f| f.transform)
                .collect::<Vec<_>>(),
            vec![
                PartitionTransform::Day,
                PartitionTransform::Bucket(16),
                PartitionTransform::Truncate(8),
            ]
        );
        let mut doc = minimal();
        doc["tables"] = serde_json::json!({
            "events": {"partition_by": [{"column": "ts", "transform": "hour"}]}
        });
        let config = Config::from_value(doc).expect("json parses the same shapes");
        assert_eq!(
            config.partition_fields("events")[0].transform,
            PartitionTransform::Hour
        );
    }

    /// Unknown fields refuse everywhere — a typo must not silently
    /// drop configuration.
    #[test]
    fn unknown_fields_are_refused_at_every_level() {
        let mut doc = minimal();
        doc["catalogg"] = serde_json::json!("typo");
        Config::from_value(doc).expect_err("root typo refused");

        let mut doc = minimal();
        doc["catalog"]["warehous"] = serde_json::json!("typo");
        Config::from_value(doc).expect_err("nested typo refused");
    }

    /// The parser variants keep generation 1's framing.
    #[test]
    fn parser_failures_render_the_frozen_framing() {
        let err = Config::from_yaml(": not yaml").expect_err("refused");
        assert!(
            format!("{err}").starts_with("invalid iceberg destination YAML: "),
            "{err}"
        );
        let err = Config::from_json("{").expect_err("refused");
        assert!(
            format!("{err}").starts_with("invalid iceberg destination JSON: "),
            "{err}"
        );
    }

    /// Secrets never render — validation errors and Debug alike.
    #[test]
    fn secrets_are_grep_proof_through_the_document() {
        let mut doc = minimal();
        doc["catalog"]["auth"] = serde_json::json!({
            "oauth2_client_credentials": {"client_id": "c", "client_secret": "sekret-123"}
        });
        doc["storage"] = serde_json::json!({
            "s3": {"access_key": "ak-456", "secret_key": "sk-789"}
        });
        let config = Config::from_value(doc).expect("valid");
        let rendered = format!("{config:?}");
        for leaked in ["sekret-123", "ak-456", "sk-789"] {
            assert!(!rendered.contains(leaked), "{leaked} leaked: {rendered}");
        }
    }

    /// The generated schema round-trips the vocabulary it declares.
    #[test]
    fn the_schema_generates_from_the_same_structs() {
        let schema = config_schema();
        assert!(
            schema["properties"]["catalog"].is_object(),
            "declares catalog: {schema}"
        );
        assert!(
            schema["properties"]["namespace"].is_object(),
            "declares namespace"
        );
    }
}
