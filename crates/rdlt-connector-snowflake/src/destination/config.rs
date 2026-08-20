//! The configuration document.
//!
//! Everything a pipeline can say to this destination lives in one closed
//! vocabulary: who to connect as (`account`, `user`, one `auth` method),
//! where to write (`database`, `schema`, always both — statements are
//! fully qualified so a changed server-side default cannot retarget a
//! pipeline), how to run (`warehouse`, `role`, `session_parameters`,
//! `query_tag`, `host` for PrivateLink fronts, `table_type`), and the
//! option set every SQL destination shares, flattened in.
//!
//! The sdk's [`Document`] supplies the entry points; there is no path
//! that parses without validating. The error is typed at every entry —
//! generation 1 returned `Result<_, String>` from hand-rolled
//! constructors, an asymmetry 027 recorded and this generation closes.
//! Refusals name the field at fault, because editing that field is
//! always the user's next action.

use super::parts;
use std::collections::BTreeMap;

use rdlt_connector_sdk::config::Document;
use rdlt_connector_sdk::pem::Material;
use rdlt_connector_sdk::spi::secret::Secret;
use rdlt_connector_sqlcore::DestinationOptions;
use serde::{Deserialize, Serialize};

/// The destination's configuration document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Config {
    /// Account identifier as it appears in the host name (`MYORG-MYACCT`
    /// for `myorg-myacct.snowflakecomputing.com`). A pasted URL is
    /// refused rather than mangled into a host that resolves nowhere.
    pub account: String,
    /// Login name.
    pub user: String,
    /// Exactly one authentication method.
    pub auth: Auth,
    /// Database to write into. Required even when the user has a
    /// server-side default — see the module doc.
    pub database: String,
    /// Schema to write into. Required for the same reason.
    pub schema: String,
    /// Warehouse to run on. Absent means the user's default; a load with
    /// neither fails typed at the service.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub warehouse: Option<String>,
    /// Role to assume. Absent means the user's default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    /// Permanent (default) or transient tables — see [`TableType`].
    #[serde(default)]
    pub table_type: TableType,
    /// Session parameters applied verbatim at connect time.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub session_parameters: BTreeMap<String, String>,
    /// A `QUERY_TAG`, so the pipeline's statements are attributable in
    /// the account's query history.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query_tag: Option<String>,
    /// Overrides the host derived from `account` — PrivateLink and
    /// similar deployments front the account under their own name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
    /// Staged-part sizing; absent = the SPI's 128 MiB default.
    ///
    /// The service's own guidance is 100-250 MB compressed per file
    /// for load parallelism, so the shared default sits inside its
    /// recommended band. Rows accumulate in memory until a part
    /// closes, which is what `max_open_bytes` bounds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parts: Option<parts::Options>,
    /// The shared SQL-destination options (merge strategy, hard delete,
    /// dedup sort, merge scope, scd2), flattened so the document reads
    /// identically on every SQL destination.
    #[serde(default, flatten)]
    pub options: DestinationOptions,
}

/// Exactly one way in. A struct of optional methods rather than an enum,
/// the connector family's convention: a future scheme is an ADDITIVE
/// field and the YAML keeps its natural `auth: {key_pair: {…}}` shape.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Auth {
    /// Key-pair JWT — the method the service recommends for unattended
    /// use and the one this connector is verified against.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_pair: Option<KeyPair>,
    /// Password. The service enforces MFA on password sign-ins and
    /// refuses them outright for `TYPE = SERVICE` users — present for
    /// parity, not recommended.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<Password>,
    /// A caller-supplied OAuth access token; acquiring and refreshing
    /// it is the caller's business.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub oauth_token: Option<Secret>,
    /// A programmatic access token. The drivers present these on the
    /// password channel — verified live, not assumed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pat: Option<Secret>,
}

/// Key-pair (JWT) authentication.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct KeyPair {
    /// The private key: a path to a `.p8` file, or inline PEM text.
    pub private_key: Material,
    /// Required exactly when the key is encrypted. The mismatch
    /// surfaces at CONNECT time through the library, wrapped in the
    /// connect identity frame — parse-time validation cannot check it
    /// earlier without reading the key file, which may not exist where
    /// the document is validated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub passphrase: Option<Secret>,
}

/// Password authentication, with [`Auth::password`]'s caveats.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Password {
    /// The account password.
    pub password: Secret,
    /// An MFA passcode, where the account requires one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub passcode: Option<Secret>,
}

/// Whether created tables are transient.
///
/// Transient tables skip the seven-day fail-safe — the cost lever a
/// re-loadable pipeline target most often wants. Applied to rdlt's own
/// bookkeeping tables too, so one choice governs everything the
/// pipeline creates.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TableType {
    /// Standard tables: Time Travel plus fail-safe.
    #[default]
    Permanent,
    /// No fail-safe period.
    Transient,
}

/// A rejected configuration, naming what to edit.
///
/// The two parser variants carry the parser's rendered text — the same
/// spelling the generation-1 `String` era produced, now behind a type
/// (which is also what keeps this enum `Clone + PartialEq + Eq`).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConfigError {
    /// YAML text that did not parse as the document shape.
    Yaml(String),
    /// JSON text (or a value) that did not parse as the document shape.
    Json(String),
    /// A required field was blank.
    Missing {
        /// The field, spelled as the document spells it.
        field: &'static str,
    },
    /// The `auth` block named no method, or several.
    Auth {
        /// What was wrong, in the user's terms.
        detail: String,
    },
    /// A field's value cannot be used as given.
    Invalid {
        /// The field, spelled as the document spells it.
        field: &'static str,
        /// Why not.
        detail: String,
    },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // Unprefixed on purpose: the parser's own text is the frozen
            // spelling here.
            Self::Yaml(detail) | Self::Json(detail) => write!(f, "{detail}"),
            Self::Missing { field } => write!(f, "snowflake: `{field}` is required"),
            Self::Auth { detail } => write!(f, "snowflake: `auth` {detail}"),
            Self::Invalid { field, detail } => write!(f, "snowflake: `{field}` {detail}"),
        }
    }
}

impl std::error::Error for ConfigError {}

impl From<serde_yaml_ng::Error> for ConfigError {
    fn from(error: serde_yaml_ng::Error) -> Self {
        Self::Yaml(error.to_string())
    }
}

impl From<serde_json::Error> for ConfigError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error.to_string())
    }
}

/// The sdk gate: parse (any entry), then THIS, with no way around it.
impl Document for Config {
    type Error = ConfigError;

    fn validate(&self) -> Result<(), ConfigError> {
        Config::validate(self)
    }
}

impl Config {
    /// Everything checkable without a connection.
    ///
    /// Also inherent, not only the trait method: assembly calls it, and
    /// a caller holding a config value should not need a trait import
    /// to check one.
    pub fn validate(&self) -> Result<(), ConfigError> {
        for (field, value) in [
            ("account", &self.account),
            ("user", &self.user),
            ("database", &self.database),
            ("schema", &self.schema),
        ] {
            if value.trim().is_empty() {
                return Err(ConfigError::Missing { field });
            }
        }
        // The one mistake worth intercepting by shape: the console URL
        // pasted where the identifier belongs. Left alone it derives a
        // host that resolves to nothing, with the error far from the
        // cause.
        if self.account.contains("://") || self.account.contains('/') {
            return Err(ConfigError::Invalid {
                field: "account",
                detail: "is the account identifier (`MYORG-MYACCT`), not a URL".to_owned(),
            });
        }
        if self.account.contains(".snowflakecomputing.com") {
            return Err(ConfigError::Invalid {
                field: "account",
                detail: "is the account identifier alone; the host is derived from it".to_owned(),
            });
        }
        self.auth.validate()?;
        // The shared option rules fire at PARSE, not first at ensure —
        // generation 1 left this seam unvalidated (a document with
        // contradictory merge options parsed clean and failed mid-load);
        // the fresh suite exposed the gap and this closes it, with
        // sqlcore's own frozen sentence as the detail.
        if let Some(parts) = &self.parts {
            parts.validate().map_err(|e| ConfigError::Invalid {
                field: "parts",
                detail: e.to_string(),
            })?;
        }
        self.options
            .validate()
            .map_err(|detail| ConfigError::Invalid {
                field: "tables",
                detail,
            })
    }

    /// The host this configuration addresses — derived from `account`
    /// unless `host` overrides it. Stated once so every caller agrees.
    pub fn host(&self) -> String {
        self.host
            .clone()
            .unwrap_or_else(|| format!("{}.snowflakecomputing.com", self.account.to_lowercase()))
    }
}

impl Auth {
    /// Zero methods cannot connect; several mean the author believes
    /// something untrue about which credential is in use. Both are
    /// refused by name rather than silently resolved.
    fn validate(&self) -> Result<(), ConfigError> {
        let named: Vec<&str> = [
            self.key_pair.is_some().then_some("key_pair"),
            self.password.is_some().then_some("password"),
            self.oauth_token.is_some().then_some("oauth_token"),
            self.pat.is_some().then_some("pat"),
        ]
        .into_iter()
        .flatten()
        .collect();
        match named.as_slice() {
            [_] => Ok(()),
            [] => Err(ConfigError::Auth {
                detail: "names no method (expected one of key_pair, password, oauth_token, pat)"
                    .to_owned(),
            }),
            several => Err(ConfigError::Auth {
                detail: format!(
                    "names {} methods ({}); set exactly one",
                    several.len(),
                    several.join(", ")
                ),
            }),
        }
    }

    /// Key-pair JWT.
    ///
    /// The constructors exist because the vocabulary is
    /// `#[non_exhaustive]`: without them an embedder could deserialize
    /// a config but never build one, and the library API reaches
    /// everything the CLI reaches.
    pub fn key_pair(key_pair: KeyPair) -> Self {
        Self {
            key_pair: Some(key_pair),
            ..Self::default()
        }
    }

    /// Password, with [`Password`]'s caveats.
    pub fn password(password: Password) -> Self {
        Self {
            password: Some(password),
            ..Self::default()
        }
    }

    /// A caller-supplied OAuth access token.
    pub fn oauth_token(token: impl Into<Secret>) -> Self {
        Self {
            oauth_token: Some(token.into()),
            ..Self::default()
        }
    }

    /// A programmatic access token.
    pub fn pat(token: impl Into<Secret>) -> Self {
        Self {
            pat: Some(token.into()),
            ..Self::default()
        }
    }
}

impl KeyPair {
    /// A key that needs no passphrase.
    pub fn new(private_key: impl Into<Material>) -> Self {
        Self {
            private_key: private_key.into(),
            passphrase: None,
        }
    }

    /// The passphrase an encrypted key requires.
    pub fn with_passphrase(mut self, passphrase: impl Into<Secret>) -> Self {
        self.passphrase = Some(passphrase.into());
        self
    }
}

impl Password {
    /// A password without an MFA passcode.
    pub fn new(password: impl Into<Secret>) -> Self {
        Self {
            password: password.into(),
            passcode: None,
        }
    }

    /// The MFA passcode the account requires.
    pub fn with_passcode(mut self, passcode: impl Into<Secret>) -> Self {
        self.passcode = Some(passcode.into());
        self
    }
}

/// The JSON Schema, generated from the same structs the parser reads —
/// declaration and parser cannot drift.
pub fn config_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(Config)).expect("a generated schema serializes")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn minimal() -> serde_json::Value {
        serde_json::json!({
            "account": "MYORG-MYACCT",
            "user": "LOADER",
            "auth": {"key_pair": {"private_key": "/k.p8"}},
            "database": "ANALYTICS",
            "schema": "RAW",
        })
    }

    /// The smallest valid document, and what defaults fill in.
    #[test]
    fn a_minimal_document_parses_and_defaults_the_rest() {
        let config = Config::from_value(minimal()).expect("valid");
        assert_eq!(config.table_type, TableType::Permanent);
        assert!(config.warehouse.is_none() && config.role.is_none());
        assert!(config.session_parameters.is_empty());
        assert_eq!(config.host(), "myorg-myacct.snowflakecomputing.com");
    }

    /// Unknown fields refuse (typos must not be silently ignored), and
    /// the refusal is now TYPED — the parser's text behind the Json
    /// variant, spelled exactly as the String era spelled it.
    #[test]
    fn an_unknown_field_is_refused_and_typed() {
        let mut doc = minimal();
        doc["wharehouse"] = serde_json::json!("TYPO_WH");
        let err = Config::from_value(doc).expect_err("the typo is refused");
        assert!(matches!(err, ConfigError::Json(_)), "{err:?}");
        assert!(err.to_string().contains("wharehouse"), "{err}");
    }

    /// The YAML entry absorbs its parser the same way; the rendered
    /// text is the parser's own, bare.
    #[test]
    fn yaml_parse_failures_are_typed_and_render_bare() {
        let err = Config::from_yaml(": not yaml").expect_err("refused");
        assert!(matches!(err, ConfigError::Yaml(_)), "{err:?}");
        let parser_text = serde_yaml_ng::from_str::<Config>(": not yaml")
            .expect_err("parse fails")
            .to_string();
        assert_eq!(err.to_string(), parser_text);
    }

    /// Blank required fields are refused naming the field.
    #[test]
    fn required_fields_are_named_when_blank() {
        for field in ["account", "user", "database", "schema"] {
            let mut doc = minimal();
            doc[field] = serde_json::json!("  ");
            let err = Config::from_value(doc).expect_err("blank refused");
            assert_eq!(err, ConfigError::Missing { field }, "{err}");
            assert!(err.to_string().contains(field), "{err}");
        }
    }

    /// A pasted URL (either form) is intercepted where the identifier
    /// belongs.
    #[test]
    fn a_pasted_url_is_refused_where_an_identifier_belongs() {
        for wrong in [
            "https://myorg-myacct.snowflakecomputing.com",
            "myorg-myacct.snowflakecomputing.com",
        ] {
            let mut doc = minimal();
            doc["account"] = serde_json::json!(wrong);
            let err = Config::from_value(doc).expect_err("not an identifier");
            assert!(err.to_string().contains("account"), "{err}");
        }
    }

    /// The host override wins over the derivation.
    #[test]
    fn the_host_override_replaces_the_derived_name() {
        let mut doc = minimal();
        doc["host"] = serde_json::json!("acct.privatelink.snowflakecomputing.com");
        let config = Config::from_value(doc).expect("valid");
        assert_eq!(config.host(), "acct.privatelink.snowflakecomputing.com");
    }

    /// Zero methods and several methods both refuse, each naming what it
    /// saw.
    #[test]
    fn auth_must_name_exactly_one_method() {
        let mut none = minimal();
        none["auth"] = serde_json::json!({});
        let err = Config::from_value(none).expect_err("no method");
        assert!(err.to_string().contains("names no method"), "{err}");

        let mut two = minimal();
        two["auth"] = serde_json::json!({
            "key_pair": {"private_key": "/k.p8"},
            "pat": "tok",
        });
        let rendered = Config::from_value(two)
            .expect_err("two methods")
            .to_string();
        assert!(
            rendered.contains("key_pair") && rendered.contains("pat"),
            "{rendered}"
        );
    }

    /// Every method the vocabulary declares parses, alone.
    #[test]
    fn every_auth_method_parses() {
        for auth in [
            serde_json::json!({"key_pair": {"private_key": "/k.p8"}}),
            serde_json::json!({"key_pair": {"private_key": "/k.p8", "passphrase": "s"}}),
            serde_json::json!({"password": {"password": "p"}}),
            serde_json::json!({"password": {"password": "p", "passcode": "123456"}}),
            serde_json::json!({"oauth_token": "tok"}),
            serde_json::json!({"pat": "tok"}),
        ] {
            let mut doc = minimal();
            doc["auth"] = auth.clone();
            Config::from_value(doc).unwrap_or_else(|e| panic!("{auth}: {e}"));
        }
    }

    /// No credential material reaches Debug output — for any method.
    #[test]
    fn no_secret_reaches_debug_output() {
        for auth in [
            serde_json::json!({"key_pair": {"private_key": "-----BEGIN X-----", "passphrase": "PASSPHRASE-LEAK"}}),
            serde_json::json!({"password": {"password": "PW-LEAK", "passcode": "PC-LEAK"}}),
            serde_json::json!({"oauth_token": "OAUTH-LEAK"}),
            serde_json::json!({"pat": "PAT-LEAK"}),
        ] {
            let mut doc = minimal();
            doc["auth"] = auth;
            let rendered = format!("{:?}", Config::from_value(doc).expect("valid"));
            for leak in [
                "PASSPHRASE-LEAK",
                "PW-LEAK",
                "PC-LEAK",
                "OAUTH-LEAK",
                "PAT-LEAK",
            ] {
                assert!(!rendered.contains(leak), "{leak} rendered: {rendered}");
            }
        }
    }

    /// The key accepts both spellings: a path, or inline PEM.
    #[test]
    fn the_private_key_may_be_a_path_or_inline_pem() {
        let mut inline = minimal();
        inline["auth"] = serde_json::json!({
            "key_pair": {"private_key": "-----BEGIN PRIVATE KEY-----\nabc\n"}
        });
        let inline = Config::from_value(inline).expect("valid");
        assert!(
            inline
                .auth
                .key_pair
                .expect("key pair")
                .private_key
                .is_inline()
        );

        let path = Config::from_value(minimal()).expect("valid");
        assert!(
            !path
                .auth
                .key_pair
                .expect("key pair")
                .private_key
                .is_inline()
        );
    }

    /// The shared options ride the SAME flattened spelling as every
    /// other SQL destination.
    #[test]
    fn the_shared_option_vocabulary_is_flattened_into_the_document() {
        let mut doc = minimal();
        doc["merge_strategy"] = serde_json::json!("upsert");
        let config = Config::from_value(doc).expect("valid");
        assert!(config.options.merge_strategy.is_some());
    }

    /// The generated schema names the vocabulary it was generated from.
    #[test]
    fn the_schema_generates_and_names_the_vocabulary() {
        let rendered = serde_json::to_string(&config_schema()).expect("render");
        for field in ["account", "auth", "key_pair", "table_type", "query_tag"] {
            assert!(rendered.contains(field), "schema omits `{field}`");
        }
    }
}
