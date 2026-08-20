//! The source document: connection facts, and streams over tables —
//! parse-then-validate through the sdk Document gate.

use rdlt_connector_sdk::config::Document;
use rdlt_connector_sdk::spi::secret::Secret;

/// The whole source document.
/// NOT `Serialize` — deliberately. `password` is a `Secret`, whose
/// serde impl is `transparent` over the String, so a derived
/// `Serialize` would print the credential in full to anything that
/// dumped a config. `Debug` is redacted; serialization has no such
/// guard, so the capability is simply absent.
#[derive(Debug, Clone, PartialEq, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Config {
    /// The database host.
    pub host: String,
    /// The listener port.
    #[serde(default = "default_port")]
    pub port: u16,
    /// The service name (a PDB service such as `FREEPDB1`).
    /// Exactly one of `service` or `sid` is required.
    #[serde(default)]
    pub service: Option<String>,
    /// The legacy SID, for instances that predate service names —
    /// the shape older estates still hand out.
    #[serde(default)]
    pub sid: Option<String>,
    pub user: String,
    pub password: Secret,
    /// Connection and fetch tuning. Absent means the defaults.
    #[serde(default)]
    pub tuning: Tuning,
    /// The streams to read; at least one.
    #[schemars(length(min = 1))]
    pub streams: Vec<Stream>,
}

/// The knobs an Oracle operator expects to turn.
///
/// The names are rdlt's, but each one is the JDBC parameter an Oracle
/// estate already tunes, so a known-good JDBC string translates
/// directly:
/// `defaultRowPrefetch` → `page_rows`,
/// `oracle.net.CONNECT_TIMEOUT` → `connect_timeout_ms`,
/// `oracle.jdbc.ReadTimeout` → `read_timeout_ms`,
/// and `oracle.jdbc.implicitStatementCacheSize` → `statement_cache`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Tuning {
    /// Rows the CLIENT prefetches per round trip.
    ///
    /// This is a throughput knob, not a correctness one: the read
    /// streams a single cursor whatever the value, so raising it
    /// trades client memory for fewer round trips. Absent means the
    /// driver's own default.
    #[serde(default)]
    pub page_rows: Option<u32>,
    /// How long a connect attempt may take.
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_ms: u64,
    /// How long ONE statement may take before it is abandoned. The
    /// connection is dropped when it fires — a statement that timed
    /// out has left the protocol mid-conversation.
    #[serde(default = "default_read_timeout")]
    pub read_timeout_ms: u64,
    /// Rows per Arrow batch pushed downstream.
    ///
    /// The batch is the unit of backpressure AND of the checkpoint
    /// that follows it, so this trades memory against how much work a
    /// crash costs. It is not a protocol limit — the old page size
    /// was, because a reply had to fit one network packet.
    #[serde(default = "default_batch_rows")]
    pub batch_rows: u32,
    /// Statements the driver may keep open for reuse.
    #[serde(default = "default_statement_cache")]
    pub statement_cache: u32,
    /// Dead-connection detection interval, in MINUTES, or `0` for
    /// off.
    ///
    /// Minutes, not seconds, because that is the granularity Oracle's
    /// `EXPIRE_TIME` accepts — naming it `_secs` would have been a
    /// knob whose units were a lie. A firewall or NAT that reaps an
    /// idle connection does so SILENTLY, and the read then waits out
    /// its whole `read_timeout_ms` against a socket nothing will
    /// answer. This is `oracle.net.keepAlive` / `EXPIRE_TIME`.
    #[serde(default = "default_keepalive")]
    pub keepalive_minutes: u64,
}

/// One minute — under the 5-minute idle timeout common to firewalls
/// and NAT, and the smallest value Oracle acts on.
fn default_keepalive() -> u64 {
    1
}

fn default_connect_timeout() -> u64 {
    60_000
}
fn default_read_timeout() -> u64 {
    600_000
}
fn default_batch_rows() -> u32 {
    8_192
}
fn default_statement_cache() -> u32 {
    20
}

impl Default for Tuning {
    fn default() -> Self {
        Self {
            page_rows: None,
            connect_timeout_ms: default_connect_timeout(),
            read_timeout_ms: default_read_timeout(),
            batch_rows: default_batch_rows(),
            statement_cache: default_statement_cache(),
            keepalive_minutes: default_keepalive(),
        }
    }
}

fn default_port() -> u16 {
    1521
}

/// One stream: a table read incrementally by an optional cursor
/// column.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Stream {
    pub name: String,
    /// The table to read. Bare names fold UPPERCASE (Oracle's own
    /// rule); the connector always emits the quoted form.
    pub table: String,
    /// Watermark column for incremental reads (numeric or timestamp).
    #[serde(default)]
    pub cursor: Option<String>,
    /// Keyed identity for the engine's merge/dedup layers.
    ///
    /// REQUIRED for `Merge`. Every oracle stream is `structured`
    /// (rows cross as Arrow), and the engine refuses to plan a Merge
    /// on a structured stream with no declared key — there is no
    /// content-hash fallback for one. Omit this only for `Append` or
    /// `Replace`.
    #[serde(default)]
    pub primary_key: Option<Vec<String>>,
}

/// Parse and validation failures, typed, with the sdk from-text
/// framings.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid oracle source YAML: {0}")]
    Yaml(#[from] serde_yaml_ng::Error),
    #[error("invalid oracle source JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid oracle source config: {0}")]
    Invalid(String),
}

impl Config {
    /// The Easy Connect descriptor the driver takes.
    ///
    /// A SID is spelled with a colon rather than a slash — the legacy
    /// form older estates still hand out.
    pub(crate) fn connect_string(&self) -> String {
        // Easy Connect Plus carries the two network knobs that have
        // no API on the connection: CONNECT_TIMEOUT bounds a connect
        // against a black-holed host, EXPIRE_TIME is Oracle's own
        // dead-connection detection (the keepalive). Both are
        // expressed in the units the descriptor accepts — seconds
        // and minutes respectively.
        let mut params = vec![format!(
            "connect_timeout={}",
            self.tuning.connect_timeout_ms.div_ceil(1_000).max(1)
        )];
        if self.tuning.keepalive_minutes > 0 {
            params.push(format!("expire_time={}", self.tuning.keepalive_minutes));
        }
        let query = format!("?{}", params.join("&"));
        match (&self.service, &self.sid) {
            (Some(service), _) => {
                format!("//{}:{}/{}{query}", self.host, self.port, service)
            }
            // The legacy SID form is not Easy Connect Plus, so it
            // takes no parameters.
            (_, Some(sid)) => format!("{}:{}:{}", self.host, self.port, sid),
            _ => format!("//{}:{}{query}", self.host, self.port),
        }
    }
}

impl Document for Config {
    type Error = ConfigError;

    fn validate(&self) -> Result<(), ConfigError> {
        let invalid = |m: String| Err(ConfigError::Invalid(m));
        if self.host.is_empty() {
            return invalid("`host` must not be empty".into());
        }
        match (&self.service, &self.sid) {
            (Some(s), None) if !s.is_empty() => {}
            (None, Some(s)) if !s.is_empty() => {}
            (Some(_), Some(_)) => {
                return invalid(
                    "`service` and `sid` are two ways to name one instance — set one".into(),
                );
            }
            _ => {
                return invalid("one of `service` (modern) or `sid` (legacy) is required".into());
            }
        }
        if self.user.is_empty() {
            return invalid("`user` must not be empty".into());
        }
        if self.password.reveal().is_empty() {
            return invalid("`password` must not be empty".into());
        }
        if self.tuning.page_rows == Some(0) {
            return invalid("`tuning.page_rows` is 0 — a page must hold at least one row".into());
        }
        if self.tuning.batch_rows == 0 {
            return invalid("`tuning.batch_rows` is 0 — a batch must hold at least one row".into());
        }
        if self.tuning.keepalive_minutes > 1_440 {
            return invalid(format!(
                "`tuning.keepalive_minutes` is {} — the supported range is 0 (off) to 1440",
                self.tuning.keepalive_minutes
            ));
        }
        if self.streams.is_empty() {
            return invalid("at least one stream is required".into());
        }
        // Duplicate names are refused at the gate (the 029-031
        // shared-table precedent): the reader resolves streams by
        // name, and a duplicate is silently shadowed on read.
        let mut seen = std::collections::BTreeSet::new();
        for stream in &self.streams {
            if !seen.insert(stream.name.as_str()) {
                return invalid(format!(
                    "duplicate stream name `{}` — stream names must be unique",
                    stream.name
                ));
            }
            if stream.name.is_empty() {
                return invalid("stream names must not be empty".into());
            }
            if stream.table.is_empty() {
                return invalid(format!(
                    "stream `{}`: `table` must not be empty",
                    stream.name
                ));
            }
            if stream.cursor.as_deref() == Some("") {
                return invalid(format!(
                    "stream `{}`: `cursor` is empty — omit it to read the stream in full",
                    stream.name
                ));
            }
            // `primary_key: []` is not "no key": the engine matches
            // on a NON-EMPTY key list and otherwise falls back to
            // hashing the whole row, so two versions of one business
            // row become two identities and dedup keeps both — with
            // no diagnostic at any layer. Omitting the field means
            // that on purpose; an empty list means it by accident.
            if stream.primary_key.as_deref() == Some(&[]) {
                return invalid(format!(
                    "stream `{}`: `primary_key` is an empty list — omit the field entirely \
                     (Append/Replace need no key), or name the columns (Merge requires them)",
                    stream.name
                ));
            }
        }
        Ok(())
    }
}

/// The generated declaration, from the same structs the parser reads.
pub fn config_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(Config)).expect("schema serializes")
}
