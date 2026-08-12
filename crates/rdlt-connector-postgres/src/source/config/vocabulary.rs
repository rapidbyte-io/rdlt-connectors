//! The configuration vocabulary: every struct and enum a pipeline YAML (or
//! an embedder's JSON document) can spell, with its serde and schema
//! plumbing. The rules that judge a parsed document live in `validate`.
//!
//! Serde spellings are the FROZEN document vocabulary; where a Rust
//! identifier improves on a frozen spelling, `#[serde(rename)]` carries the
//! document form (`conn` ↔ `connection`).

use serde::{Deserialize, Serialize};

/// Per-column type-hint vocabulary — defined beside the closed conversion
/// table it selects from (the crate-internal type rulebook); this is its
/// public spelling.
pub use crate::types::map::TypeHint;

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("parsing postgres source config: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("parsing postgres source JSON config: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid postgres source config: {0}")]
    Invalid(String),
}

/// The source configuration document.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// libpq-style connection string/URL; `sslmode` up to `require` may be
    /// set here (verify-* modes go in the `tls` block).
    #[serde(rename = "conn")]
    #[schemars(rename = "conn")]
    pub connection: String,
    /// Reflection scope; bare table names below resolve inside it.
    #[serde(default = "default_schema")]
    pub schema: String,
    /// Include views and materialized views in schema-wide discovery.
    #[serde(default)]
    pub include_views: bool,
    /// Table selection, three-valued. PRESENT ⇒ exactly this list and
    /// nothing else — including the empty list, which selects no tables and
    /// leaves `queries` as the run's only streams. ABSENT ⇒ discover every
    /// table in `schema`, which is why a pipeline that declares queries and
    /// omits this field also receives every table alongside them.
    #[serde(default)]
    pub tables: Option<Vec<TableConfig>>,
    /// Query streams: a stream per SQL statement, schema DESCRIBED by the
    /// database; always executed as `SELECT * FROM (sql) AS q` (read-only
    /// enforced by subquery rules).
    #[serde(default)]
    pub queries: Vec<QueryConfig>,
    /// TLS posture: full sslmode matrix; verify-* modes are expressible only
    /// here (conn-string sslmode covers disable/prefer/require).
    /// Contradicting an explicit conn sslmode is a config error.
    #[serde(default)]
    pub tls: Option<crate::tls::Policy>,
    /// CDC via logical replication: when present, EVERY configured table is
    /// captured through the replication slot instead of cursor-column
    /// incremental (the two are mutually exclusive per table). Query streams
    /// are unaffected.
    #[serde(default)]
    pub cdc: Option<CdcConfig>,
    /// Decoder cuts a RecordBatch at this many buffered bytes.
    #[serde(default = "default_batch_target_bytes")]
    pub batch_target_bytes: usize,
    /// Secondary cut: maximum rows per batch.
    #[serde(default = "default_batch_max_rows")]
    pub batch_max_rows: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TableConfig {
    /// Bare table name; `schema` owns qualification (qualified names
    /// rejected).
    pub name: String,
    #[serde(default)]
    pub cursor: Option<CursorConfig>,
    /// Overrides the reflected primary key (dedup/merge key source).
    #[serde(default)]
    pub primary_key: Option<Vec<String>>,
    /// Mutually exclusive with `excluded_columns`.
    #[serde(default)]
    pub included_columns: Option<Vec<String>>,
    #[serde(default)]
    pub excluded_columns: Option<Vec<String>>,
    /// Per-column type-hint overrides: a CLOSED conversion table; unknown
    /// columns or undefined (source → hint) pairs are typed config errors at
    /// open.
    #[serde(default)]
    pub type_hints: std::collections::BTreeMap<String, TypeHint>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct QueryConfig {
    /// Stream name — unique across tables AND queries.
    pub name: String,
    /// The SELECT/CTE statement (wrapped as a subquery at execution).
    pub sql: String,
    #[serde(default)]
    pub cursor: Option<CursorConfig>,
    /// Declared key (nothing to reflect): dedup keys + merge.
    #[serde(default)]
    pub primary_key: Option<Vec<String>>,
    #[serde(default)]
    pub type_hints: std::collections::BTreeMap<String, TypeHint>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CursorConfig {
    /// Must exist on the table with a cursor-capable type (validated at
    /// open).
    pub column: String,
    /// Typed literal for the first run (absent ⇒ full initial load).
    #[serde(default)]
    pub initial_value: Option<String>,
    #[serde(default = "default_boundary")]
    pub boundary: Bound,
    #[serde(default)]
    pub direction: Direction,
    /// Optional upper bound (typed literal, exclusive under `max` unless
    /// `end_bound: inclusive`).
    #[serde(default)]
    pub end_value: Option<String>,
    /// Upper-bound semantics: `exclusive` (default) or `inclusive` — rows
    /// exactly AT `end_value` load. A read filter only; never resume state.
    #[serde(default = "default_end_bound")]
    pub end_bound: Bound,
    #[serde(default)]
    pub nulls: NullPolicy,
    /// Attribution window: each RESUMED run widens the read window this far
    /// behind the watermark so late-committed rows are captured. Requires a
    /// closed boundary and a primary key; the saved watermark is never
    /// lowered.
    #[serde(default)]
    pub lag: Option<Lag>,
}

/// Lag vocabulary: `"90s"`/`"5m"`/`"2h"`/`"1d"` (time cursors; whole days
/// for `date`) or a plain positive magnitude (`"1000"`, `"0.5"`) for
/// integer/decimal cursors. Config form is the literal string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Lag {
    /// Whole seconds (from a duration form).
    Duration { seconds: u64 },
    /// Validated positive numeric literal for numeric cursors.
    Magnitude(String),
}

impl std::str::FromStr for Lag {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let trimmed = text.trim();
        if let Some(unit) = trimmed.chars().last().filter(|c| "smhd".contains(*c)) {
            let count: u64 = trimmed[..trimmed.len() - 1]
                .parse()
                .map_err(|e| format!("lag `{trimmed}`: {e}"))?;
            if count == 0 {
                return Err(format!("lag `{trimmed}` must be positive"));
            }
            let seconds_per_unit = match unit {
                's' => 1,
                'm' => 60,
                'h' => 3600,
                _ => 86400,
            };
            return Ok(Self::Duration {
                seconds: count * seconds_per_unit,
            });
        }
        let numeric = !trimmed.is_empty()
            && trimmed.chars().all(|c| c.is_ascii_digit() || c == '.')
            && trimmed.parse::<f64>().is_ok_and(|value| value > 0.0);
        if numeric {
            Ok(Self::Magnitude(trimmed.to_string()))
        } else {
            Err(format!(
                "lag `{trimmed}` is neither a duration (\"90s\", \"5m\", \"2h\", \"1d\") \
                 nor a positive magnitude"
            ))
        }
    }
}

impl std::fmt::Display for Lag {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Duration { seconds } => write!(formatter, "{seconds}s"),
            Self::Magnitude(magnitude) => formatter.write_str(magnitude),
        }
    }
}

impl serde::Serialize for Lag {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for Lag {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

impl schemars::JsonSchema for Lag {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Lag".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        // Mirrors `FromStr` exactly — the string vocabulary IS the config
        // form.
        schemars::json_schema!({
            "type": "string",
            "description": "Attribution window: a duration (\"90s\", \"5m\", \
                            \"2h\", \"1d\") for time cursors, or a positive \
                            magnitude for numeric cursors",
            "pattern": "^([0-9]+[smhd]|[0-9]+(\\.[0-9]+)?)$"
        })
    }
}

impl Lag {
    /// The SQL delta subtracted from (direction max) or added to (min) the
    /// watermark, per cursor family. Err = the pair is undefined — surfaced
    /// as a typed open-time error naming the column.
    pub(crate) fn sql_delta(&self, kind: crate::types::Kind) -> Result<String, String> {
        use crate::types::Kind;
        match (self, kind) {
            (Self::Duration { seconds }, Kind::TimestampTz | Kind::TimestampNaive) => {
                Ok(format!("INTERVAL '{seconds} seconds'"))
            }
            (Self::Duration { seconds }, Kind::Date) if seconds % 86_400 == 0 => {
                Ok(format!("{}::int4", seconds / 86_400))
            }
            (Self::Duration { .. }, Kind::Date) => {
                Err("date cursors take whole-day lags (e.g. \"2d\")".into())
            }
            (Self::Magnitude(magnitude), Kind::Int16 | Kind::Int32 | Kind::Int64) => {
                if magnitude.contains('.') {
                    Err("integer cursors take integer lags".into())
                } else {
                    Ok(format!("{magnitude}::int8"))
                }
            }
            (Self::Magnitude(magnitude), Kind::Decimal { .. }) => {
                Ok(format!("'{magnitude}'::numeric"))
            }
            (Self::Duration { .. }, Kind::Int16 | Kind::Int32 | Kind::Int64) => {
                Err("integer cursors take a plain magnitude lag, not a duration".into())
            }
            (Self::Magnitude(_), Kind::TimestampTz | Kind::TimestampNaive | Kind::Date) => {
                Err("time cursors take a duration lag (\"90s\", \"5m\", \"1d\")".into())
            }
            _ => Err("lag is not defined for this cursor type".into()),
        }
    }
}

/// Edge semantics for a cursor-window bound — ONE vocabulary for both the
/// resume boundary and the optional end bound. Defaults differ per field
/// (resume: inclusive, so watermark-equal rows re-fetch and dedup; end:
/// exclusive), carried by per-field serde defaults.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Bound {
    /// `>=` / `<=` — rows exactly AT the edge value load. As the resume
    /// boundary this re-fetches watermark-equal rows, deduped via boundary
    /// keys.
    Inclusive,
    /// `>` / `<` — the edge value itself is excluded. As the resume boundary
    /// this skips dedup: safe only for strictly monotonic cursors.
    Exclusive,
}

fn default_boundary() -> Bound {
    Bound::Inclusive
}

fn default_end_bound() -> Bound {
    Bound::Exclusive
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Direction {
    /// Ascending cursor, watermark = max seen.
    #[default]
    Max,
    /// Descending cursor, watermark = min seen.
    Min,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum NullPolicy {
    /// NULL-cursor rows are filtered out (`IS NOT NULL`).
    #[default]
    Exclude,
    /// NULL-cursor rows are included on every run (`… OR cursor IS NULL`).
    Include,
    /// A NULL cursor value is a DATA-CONTRACT violation: the run fails with
    /// a typed error naming stream and column — for pipelines that treat
    /// NULL `updated_at` as a bug.
    Error,
}

/// CDC block: slot + publication are USER-OWNED server resources — rdlt
/// creates them only under `create_if_missing` (idempotently) and NEVER
/// drops either. The flag-column collision check needs reflection and runs
/// at open.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CdcConfig {
    /// Replication slot name; single consumer (concurrent use is a typed
    /// error naming the holding pid).
    pub slot: String,
    /// Publication; must cover every CDC table (preflighted at open).
    pub publication: String,
    /// Create slot and publication when absent. rdlt never drops them.
    #[serde(default)]
    pub create_if_missing: bool,
    #[serde(default)]
    pub mode: CdcMode,
    /// Tail-mode quiet wait between chunks (duration forms only: "1s",
    /// "5m", "2h", "1d").
    #[serde(default = "default_idle_wait")]
    pub idle_wait: Wait,
    /// Deletion-flag column emitted on every CDC stream: NULL for
    /// insert/update rows, TRUE for deletes (a destination's `hard_delete`
    /// turns it into real deletions).
    #[serde(default = "default_flag_column")]
    pub flag_column: String,
    /// `off` = never advance the slot's acknowledged position (debugging /
    /// fan-in staging) — the server retains WAL indefinitely; documented.
    #[serde(default)]
    pub ack: AckMode,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum CdcMode {
    /// Consume the backlog to the run-start WAL position, then finish
    /// (cron-able).
    #[default]
    Catchup,
    /// Chunked catch-up loop until cancelled, `idle_wait` between quiet
    /// chunks.
    Tail,
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum AckMode {
    /// Advance the slot once per run, after every stream committed.
    #[default]
    Auto,
    Off,
}

/// A wait interval: the duration vocabulary ("1s", "5m", "2h", "1d") WITHOUT
/// the magnitude forms. Config form is the literal string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Wait {
    pub seconds: u64,
}

impl std::str::FromStr for Wait {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        match text.parse::<Lag>() {
            Ok(Lag::Duration { seconds }) => Ok(Self { seconds }),
            _ => Err(format!(
                "wait `{text}` must be a duration (\"1s\", \"5m\", \"2h\", \"1d\")"
            )),
        }
    }
}

impl std::fmt::Display for Wait {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}s", self.seconds)
    }
}

impl serde::Serialize for Wait {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for Wait {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        text.parse().map_err(serde::de::Error::custom)
    }
}

impl schemars::JsonSchema for Wait {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Wait".into()
    }

    fn json_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "description": "A wait interval: \"1s\", \"5m\", \"2h\", \"1d\"",
            "pattern": "^[0-9]+[smhd]$"
        })
    }
}

fn default_idle_wait() -> Wait {
    Wait { seconds: 1 }
}
fn default_flag_column() -> String {
    "_rdlt_deleted".into()
}
fn default_schema() -> String {
    "public".into()
}
fn default_batch_target_bytes() -> usize {
    8 << 20
}
fn default_batch_max_rows() -> usize {
    65_536
}

/// JSON Schema GENERATED from the config structs — the declared schema and
/// the parser cannot drift.
pub fn config_schema() -> serde_json::Value {
    serde_json::to_value(schemars::schema_for!(Config)).expect("schema serializes")
}
