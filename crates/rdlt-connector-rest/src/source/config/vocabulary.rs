//! Every config type with its FROZEN document spelling — field names, enum
//! tags, compat forms, and defaults are the operator contract. Evolution is
//! ADDITIVE: every older spelling parses unchanged; superseded fields
//! remain as documented aliases.
//!
//! Configs are DATA — no callbacks — so a platform can render, validate,
//! and store them, and [`config_schema`] is GENERATED from these structs so
//! the declared schema and the parser cannot drift.

use std::collections::BTreeMap;

use rdlt_connector_sdk::spi::core::types::LogicalType;
use rdlt_connector_sdk::spi::secret::Secret;
use serde::{Deserialize, Serialize};

/// The whole source document: connection-wide settings plus one entry per
/// stream.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Config {
    /// e.g. `https://api.example.com`
    pub base_url: String,
    // The compat module accepts BOTH the natural `auth: {bearer: {token: …}}`
    // singleton-map form (YAML and JSON) AND the older YAML tagged form
    // `auth: !bearer`, so the older tagged spelling parses unchanged.
    #[serde(default, with = "auth_compat")]
    #[schemars(with = "Auth")]
    pub auth: Auth,
    /// Source-level default headers, merged UNDER per-stream headers.
    ///
    /// Plain strings, printed VERBATIM in derived `Debug` — a credential put
    /// here is NOT redacted. Credentials belong under `auth:`, where they are
    /// `Secret`-wrapped and masked; validation rejects the credential-bearing
    /// header names here for exactly this reason.
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// Source-level default query params, merged UNDER per-stream params.
    ///
    /// Plain strings, printed VERBATIM in derived `Debug` like
    /// [`headers`](Self::headers). A credential belongs under `auth:` (the
    /// `api_key` scheme with `location: query`) — never hard-coded here.
    #[serde(default)]
    pub params: BTreeMap<String, String>,
    /// Parent-child fan-out cap (streams themselves read sequentially).
    #[serde(default = "default_max_concurrency")]
    pub max_concurrency: u32,
    /// Pacing floor between request sends, milliseconds.
    #[serde(default)]
    pub min_request_interval_ms: u64,
    /// Max in-source Retry-After wait; beyond it the rate-limit error
    /// surfaces to the engine's retry budget.
    #[serde(default = "default_retry_after_cap")]
    pub retry_after_cap_secs: u64,
    /// Read deadline for a request: the longest the source will wait for the
    /// server to produce MORE bytes. The bound resets after each successful
    /// read, so it catches a server that accepts a connection and then
    /// stalls without capping a transfer that is making continuous progress.
    ///
    /// `0` is REFUSED rather than read as "disabled": the guarantee is that
    /// no configuration produces an unbounded wait.
    #[serde(default = "default_request_timeout")]
    pub request_timeout_secs: u64,
    /// Loop guard: a stream exceeding this many pages is a typed error.
    #[serde(default = "default_max_pages")]
    pub max_pages: u64,
    pub streams: Vec<Stream>,
}

pub(crate) fn default_max_concurrency() -> u32 {
    1
}
pub(crate) fn default_retry_after_cap() -> u64 {
    300
}
pub(crate) fn default_request_timeout() -> u64 {
    300
}
pub(crate) fn default_max_pages() -> u64 {
    10_000
}

/// Auth field (de)serialization: singleton-map form in and out, PLUS the
/// older YAML tagged spelling (`auth: !bearer`) on the way in — the form
/// serde_yaml itself produces for a plain externally-tagged enum.
mod auth_compat {
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serializer};

    use super::Auth;

    pub fn serialize<S: Serializer>(auth: &Auth, serializer: S) -> Result<S::Ok, S::Error> {
        serde_yaml_ng::with::singleton_map::serialize(auth, serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Auth, D::Error> {
        // Buffer into a format-agnostic value first: YAML keeps `!tags` as
        // Value::Tagged; JSON and singleton-map YAML land as mappings.
        let value = serde_yaml_ng::Value::deserialize(deserializer)?;
        if matches!(value, serde_yaml_ng::Value::Tagged(_)) {
            Auth::deserialize(value).map_err(D::Error::custom)
        } else {
            serde_yaml_ng::with::singleton_map::deserialize(value).map_err(D::Error::custom)
        }
    }
}

/// How requests authenticate. Every credential field is [`Secret`]-wrapped:
/// masked in Debug/Display/errors, revealed only at request attachment.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Auth {
    #[default]
    None,
    /// `Authorization: Bearer <token>`
    Bearer {
        token: Secret,
    },
    /// Arbitrary header.
    Header {
        name: String,
        value: Secret,
    },
    Basic {
        username: String,
        password: Secret,
    },
    /// A named credential in a header or query parameter.
    ApiKey {
        name: String,
        key: Secret,
        #[serde(default)]
        location: KeyLocation,
    },
    /// OAuth2 client-credentials grant: lazy token fetch, cached with an
    /// expiry margin, single-flight refresh, ONE 401 re-fetch then fatal.
    #[serde(rename = "oauth2_client_credentials")]
    OAuth2ClientCredentials {
        token_url: String,
        client_id: String,
        client_secret: Secret,
        #[serde(default)]
        scopes: Vec<String>,
        #[serde(default)]
        audience: Option<String>,
        #[serde(default = "default_expiry_margin")]
        expiry_margin_secs: u64,
    },
}

pub(crate) fn default_expiry_margin() -> u64 {
    60
}

/// Where an `api_key` credential rides.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum KeyLocation {
    #[default]
    Header,
    Query,
}

/// HTTP method; `post` permits (and pagination may require) `body`.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum Method {
    #[default]
    Get,
    Post,
}

/// One stream: an endpoint, how it paginates, and how its records land.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Stream {
    /// Stream name (becomes the root table name).
    pub name: String,
    /// Path joined onto `base_url`, e.g. `/issues`. May carry `{placeholder}`
    /// tokens when a `parent` block declares them.
    pub path: String,
    #[serde(default)]
    pub method: Method,
    /// JSON body template (POST only). Pagination params for POST-cursor
    /// APIs are set INTO this body under their declared names.
    #[serde(default)]
    pub body: Option<serde_json::Value>,
    /// Extra query parameters sent with every request.
    #[serde(default)]
    pub params: BTreeMap<String, String>,
    /// Per-stream headers, merged OVER source-level headers.
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// Selector for the records array: dot paths + `[*]` wildcards + `[N]`
    /// indices (e.g. `data.items[*]`). Omitted = the body IS the array
    /// (that case streams bytes straight through — the perf path).
    #[serde(default)]
    pub records_path: Option<String>,
    #[serde(default)]
    pub pagination: Pagination,
    /// Incremental block (supersedes the flat aliases below).
    #[serde(default)]
    pub incremental: Option<Incremental>,
    /// Legacy ALIAS for `incremental.cursor_field`.
    #[serde(default)]
    pub cursor_field: Option<String>,
    /// Legacy ALIAS for `incremental.start_param`.
    #[serde(default)]
    pub cursor_param: Option<String>,
    /// Declared handling for specific responses; anything undeclared keeps
    /// the typed-error posture. First match wins.
    #[serde(default)]
    pub response_actions: Vec<ResponseAction>,
    /// Parent-child linkage: this stream fans out per parent record.
    #[serde(default)]
    pub parent: Option<Parent>,
    #[serde(default)]
    pub primary_key: Option<Vec<String>>,
    /// Per-column logical types overriding inference,
    /// e.g. `created_at: timestamp_tz`.
    #[serde(default)]
    pub type_hints: BTreeMap<String, TypeHint>,
}

impl Stream {
    /// The effective incremental configuration: the block, or the legacy
    /// flat aliases assembled into one (validation forbids mixing them).
    pub fn effective_incremental(&self) -> Option<Incremental> {
        if let Some(incremental) = &self.incremental {
            return Some(incremental.clone());
        }
        self.cursor_field.as_ref().map(|field| Incremental {
            cursor_field: field.clone(),
            start_param: self.cursor_param.clone(),
            end_param: None,
            end_value: None,
            initial_value: None,
        })
    }
}

/// The seven pagination families. The document spelling is the `type` tag;
/// `type: cursor` is the BODY-cursor family's frozen spelling.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case", tag = "type")]
#[non_exhaustive]
pub enum Pagination {
    /// Single request.
    #[default]
    None,
    /// `?<page_param>=N`, starting at `start`, until a page returns no
    /// records (or the declared total is reached).
    Page {
        #[serde(default = "default_page_param")]
        page_param: String,
        #[serde(default = "default_page_start")]
        start: u64,
        /// Optional stop: selector to the total-page count in the response.
        #[serde(default)]
        total_pages_path: Option<String>,
        /// Optional stop: selector to the total-record count.
        #[serde(default)]
        total_count_path: Option<String>,
    },
    /// `?<offset_param>=N&<limit_param>=page_size` until a short page (or
    /// the declared total is reached).
    Offset {
        #[serde(default = "default_offset_param")]
        offset_param: String,
        #[serde(default = "default_limit_param")]
        limit_param: String,
        page_size: u64,
        #[serde(default)]
        total_count_path: Option<String>,
    },
    /// A cursor in the response body feeds the next request's param;
    /// terminates when the cursor is absent or null. The config spelling
    /// stays `type: cursor`; the variant is named for its body-cursor
    /// semantics.
    #[serde(rename = "cursor")]
    BodyCursor {
        /// Selector to the cursor value in the response body.
        cursor_path: String,
        cursor_param: String,
    },
    /// A cursor in a response HEADER feeds the next request's param;
    /// terminates when the header is absent.
    HeaderCursor {
        header: String,
        cursor_param: String,
    },
    /// The response body carries the next page's URL (absolute or
    /// relative); terminates when absent or null.
    NextUrl { next_url_path: String },
    /// RFC5988 `Link: <url>; rel="next"`; terminates when no next link.
    LinkHeader,
}

impl Pagination {
    /// Every selector path this family carries, each labeled for error
    /// messages — the one home for the variant knowledge that config
    /// validation (which checks they parse) and paginator construction both
    /// reach for.
    pub(crate) fn selector_paths(&self) -> Vec<(&'static str, &str)> {
        match self {
            Pagination::Page {
                total_pages_path,
                total_count_path,
                ..
            } => {
                let mut paths = Vec::new();
                if let Some(path) = total_pages_path {
                    paths.push(("pagination.total_pages_path", path.as_str()));
                }
                if let Some(path) = total_count_path {
                    paths.push(("pagination.total_count_path", path.as_str()));
                }
                paths
            }
            Pagination::Offset {
                total_count_path, ..
            } => total_count_path
                .as_deref()
                .map(|path| ("pagination.total_count_path", path))
                .into_iter()
                .collect(),
            Pagination::BodyCursor { cursor_path, .. } => {
                vec![("pagination.cursor_path", cursor_path.as_str())]
            }
            Pagination::NextUrl { next_url_path } => {
                vec![("pagination.next_url_path", next_url_path.as_str())]
            }
            Pagination::None | Pagination::HeaderCursor { .. } | Pagination::LinkHeader => vec![],
        }
    }
}

pub(crate) fn default_page_param() -> String {
    "page".into()
}
pub(crate) fn default_page_start() -> u64 {
    1
}
pub(crate) fn default_offset_param() -> String {
    "offset".into()
}
pub(crate) fn default_limit_param() -> String {
    "limit".into()
}

/// Incremental cursor binding: max-observed value, checkpoint after rows,
/// resume via the engine's committed cursor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Incremental {
    /// Record field carrying the cursor value.
    pub cursor_field: String,
    /// Request parameter carrying the resume value ("only records after").
    #[serde(default)]
    pub start_param: Option<String>,
    /// Optional closed-window upper bound parameter. Requires `end_value`.
    #[serde(default)]
    pub end_param: Option<String>,
    /// Explicit value for `end_param` (closed windows take explicit values;
    /// "now" is never synthesized).
    #[serde(default)]
    pub end_value: Option<String>,
    /// First-run lower bound when no cursor is committed yet.
    #[serde(default)]
    pub initial_value: Option<String>,
}

/// Declared response handling — an ALLOW-list over the typed-error default.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct ResponseAction {
    /// HTTP status this action matches (absent = any status).
    #[serde(default)]
    pub status: Option<u16>,
    /// Substring match over the first 64KiB of the body (absent = any body).
    #[serde(default)]
    pub content_contains: Option<String>,
    /// What to do when this action matches. The config key stays `action`.
    #[serde(rename = "action")]
    pub kind: ActionKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum ActionKind {
    /// Treat as an empty page (pagination still terminates per its family).
    Ignore,
    /// Clean end of the stream.
    EndStream,
    /// Explicit fatal (documents intent).
    Error,
}

/// Parent-child linkage: `{placeholder}` tokens in path/params/body resolve
/// from each parent record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Parent {
    /// The parent stream's name (must be a declared stream).
    pub stream: String,
    /// placeholder token → parent record field (dot path).
    pub placeholders: BTreeMap<String, String>,
    /// Parent fields embedded into child records as `_parent_<name>`.
    #[serde(default)]
    pub include: Vec<String>,
}

/// Human-friendly hint names in YAML, mapped onto the engine's logical
/// types.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TypeHint {
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

impl From<TypeHint> for LogicalType {
    fn from(hint: TypeHint) -> Self {
        match hint {
            TypeHint::Bool => LogicalType::Bool,
            TypeHint::Int64 => LogicalType::Int64,
            TypeHint::Float64 => LogicalType::Float64,
            TypeHint::Utf8 => LogicalType::Utf8,
            TypeHint::TimestampTz => LogicalType::TimestampTz,
            TypeHint::Date => LogicalType::Date,
            TypeHint::Time => LogicalType::Time,
            TypeHint::Uuid => LogicalType::Uuid,
            TypeHint::Json => LogicalType::Json,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid REST source YAML: {0}")]
    Yaml(#[from] serde_yaml_ng::Error),
    #[error("invalid REST source JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid REST source config: {0}")]
    Invalid(String),
}

/// JSON Schema GENERATED from the config structs — the declared schema and
/// the parser cannot drift.
pub fn config_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(Config)).expect("schema serializes")
}
