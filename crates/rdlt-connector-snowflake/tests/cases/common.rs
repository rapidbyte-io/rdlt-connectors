//! The credential gate for the live cells — this connector's own
//! convention, so it lives with its consumers rather than in the
//! connector-agnostic testkit.
//!
//! ONE posture, no environment override — the container gate's rule
//! with credential presence in place of runtime presence: without an
//! account every live cell skips VISIBLY and the offline cells carry
//! the burden of proof; with credentials in the documented place the
//! same gate runs the cells against the real service. The net against
//! a quietly-skipping leg is count discipline, not a knob.
//!
//! Nothing here copies a credential VALUE anywhere beyond the config it
//! hands the connector: values live in the environment or under
//! `~/.config/rdlt/snowflake/`, never in the repository.

// Each cases module uses its own subset; unused items are expected per
// module rather than dead code.
#![allow(dead_code)]

use std::path::PathBuf;

/// Where the file half of the convention lives.
fn config_dir() -> Option<PathBuf> {
    std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config/rdlt/snowflake"))
}

/// One entry's lookup, behind a trait so the RESOLUTION RULES are
/// testable without mutating the process environment (which this
/// workspace could not do anyway: setting env vars is `unsafe`, and
/// unsafe is denied).
pub trait Lookup {
    /// An environment variable's value, if set and non-empty.
    fn env(&self, var: &str) -> Option<String>;
    /// A file's contents under the convention directory.
    fn file(&self, name: &str) -> Option<String>;
}

/// The real convention: environment, then the config directory.
#[derive(Debug, Clone, Copy, Default)]
pub struct RealLookup;

impl Lookup for RealLookup {
    fn env(&self, var: &str) -> Option<String> {
        let value = std::env::var_os(var)?.to_string_lossy().trim().to_owned();
        (!value.is_empty()).then_some(value)
    }

    fn file(&self, name: &str) -> Option<String> {
        let value = std::fs::read_to_string(config_dir()?.join(name)).ok()?;
        let value = value.trim().to_owned();
        (!value.is_empty()).then_some(value)
    }
}

/// Environment first (a CI or one-off run overrides without editing the
/// user's files), trimmed (an editor's trailing newline is not part of
/// a credential).
fn env_then_file(lookup: &dyn Lookup, var: &str, file: &str) -> Option<String> {
    lookup.env(var).or_else(|| lookup.file(file))
}

/// Everything a live cell needs to reach the qual account.
///
/// Deliberately NOT a connector config: each cell builds its own
/// document from these parts, which is itself part of what the cells
/// prove.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SnowflakeCreds {
    /// Account identifier, as the host name spells it.
    pub account: String,
    /// Login name.
    pub user: String,
    /// Path to the private key file.
    pub private_key_path: String,
    /// Passphrase, when the key is encrypted.
    pub passphrase: Option<String>,
    /// Database the cells may create and drop schemas in.
    pub database: String,
    /// Warehouse, when configured.
    pub warehouse: Option<String>,
    /// Role, when configured.
    pub role: Option<String>,
}

/// Resolve the credentials, or `None` — and a cell returning early on
/// `None` PASSES, which is the hazard worth naming: a leg that can only
/// skip proves nothing, so the offline cells demonstrate behavior and
/// these confirm it live.
pub fn credentials() -> Option<SnowflakeCreds> {
    credentials_with(&RealLookup)
}

/// [`credentials`] against a supplied lookup — the testable form.
pub fn credentials_with(lookup: &dyn Lookup) -> Option<SnowflakeCreds> {
    let resolved = resolve(lookup);
    if resolved.is_none() {
        announce_skip("no account credentials in the environment or ~/.config/rdlt/snowflake/");
    }
    resolved
}

fn resolve(lookup: &dyn Lookup) -> Option<SnowflakeCreds> {
    // The key entry is a PATH — the file IS the credential, so unlike
    // every other entry it is never read from a convention file's
    // contents.
    let key_path = lookup
        .env("RDLT_SNOWFLAKE_PRIVATE_KEY_PATH")
        .or_else(|| {
            Some(
                config_dir()?
                    .join("rdlt_qual_key.p8")
                    .to_string_lossy()
                    .into_owned(),
            )
        })
        .filter(|path| std::path::Path::new(path).exists())?;
    Some(SnowflakeCreds {
        account: env_then_file(lookup, "RDLT_SNOWFLAKE_ACCOUNT", "account")?,
        user: env_then_file(lookup, "RDLT_SNOWFLAKE_USER", "user")?,
        private_key_path: key_path,
        passphrase: env_then_file(lookup, "RDLT_SNOWFLAKE_KEY_PASSPHRASE", "passphrase"),
        database: env_then_file(lookup, "RDLT_SNOWFLAKE_DATABASE", "database")?,
        warehouse: env_then_file(lookup, "RDLT_SNOWFLAKE_WAREHOUSE", "warehouse"),
        role: env_then_file(lookup, "RDLT_SNOWFLAKE_ROLE", "role"),
    })
}

/// Say once — out loud — that the live cells are not running. Without
/// it, a skipped suite and an executed one are both green and
/// indistinguishable. Once per process, so it stays a notice.
fn announce_skip(reason: &str) {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        eprintln!("SKIP: snowflake live legs — {reason}");
    });
}

/// Which extra credential an auth-matrix cell wants — each method gates
/// on its OWN entry, so a missing PAT skips only the PAT leg.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TokenKind {
    /// A programmatic access token.
    Pat,
    /// A caller-supplied OAuth access token.
    OauthToken,
    /// A password, for the password-capable test user.
    Password,
}

/// Resolve one extra credential.
pub fn token(kind: TokenKind) -> Option<String> {
    token_with(&RealLookup, kind)
}

/// [`token`] against a supplied lookup — the testable form.
pub fn token_with(lookup: &dyn Lookup, kind: TokenKind) -> Option<String> {
    match kind {
        TokenKind::Pat => env_then_file(lookup, "RDLT_SNOWFLAKE_PAT", "pat"),
        TokenKind::OauthToken => env_then_file(lookup, "RDLT_SNOWFLAKE_OAUTH_TOKEN", "oauth-token"),
        TokenKind::Password => env_then_file(lookup, "RDLT_SNOWFLAKE_PASSWORD", "password"),
    }
}

/// A scratch schema unique to one run, so parallel runs and crashed
/// predecessors cannot collide. Uppercase — the catalog's image.
pub fn scratch_schema(label: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or_default();
    format!("RDLT_T_{}_{nanos:X}", label.to_ascii_uppercase())
}

/// A config document for the qual account targeting `schema`.
pub fn config_for(creds: &SnowflakeCreds, schema: &str) -> serde_json::Value {
    let mut auth_key_pair = serde_json::json!({"private_key": creds.private_key_path});
    if let Some(passphrase) = &creds.passphrase {
        auth_key_pair["passphrase"] = serde_json::json!(passphrase);
    }
    let mut doc = serde_json::json!({
        "account": creds.account,
        "user": creds.user,
        "auth": {"key_pair": auth_key_pair},
        "database": creds.database,
        "schema": schema,
    });
    if let Some(warehouse) = &creds.warehouse {
        doc["warehouse"] = serde_json::json!(warehouse);
    }
    if let Some(role) = &creds.role {
        doc["role"] = serde_json::json!(role);
    }
    doc
}

/// A keyed STRUCTURED source pushing Arrow batches — what upsert/scd2
/// require (json rows would ride the shredder and be refused). Each
/// read pushes the whole feed: snapshot semantics, no checkpoints, so a
/// second run of the same pipeline re-delivers rather than resuming.
pub struct KeyedSource {
    /// (id, note, gone) rows.
    pub rows: Vec<(i64, &'static str, bool)>,
}

#[async_trait::async_trait]
impl rdlt_connector_sdk::spi::Source for KeyedSource {
    fn spec(&self) -> rdlt_connector_sdk::spi::ConnectorSpec {
        rdlt_connector_sdk::spi::ConnectorSpec::new("keyed-test-source", "0.0.0")
    }

    async fn streams(
        &self,
    ) -> Result<Vec<rdlt_connector_sdk::spi::StreamSpec>, rdlt_connector_sdk::spi::SourceError>
    {
        Ok(vec![
            rdlt_connector_sdk::spi::StreamSpec::new("events")
                .with_structured()
                .with_primary_key(["id".to_owned()]),
        ])
    }

    async fn read(
        &self,
        mut request: rdlt_connector_sdk::spi::ReadRequest,
    ) -> Result<(), rdlt_connector_sdk::spi::SourceError> {
        use std::sync::Arc;
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false),
            arrow_schema::Field::new("note", arrow_schema::DataType::Utf8, true),
            arrow_schema::Field::new("gone", arrow_schema::DataType::Boolean, true),
        ]));
        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(arrow_array::Int64Array::from(
                    self.rows.iter().map(|(id, _, _)| *id).collect::<Vec<_>>(),
                )),
                Arc::new(arrow_array::StringArray::from(
                    self.rows
                        .iter()
                        .map(|(_, note, _)| *note)
                        .collect::<Vec<_>>(),
                )),
                Arc::new(arrow_array::BooleanArray::from(
                    self.rows
                        .iter()
                        .map(|(_, _, gone)| *gone)
                        .collect::<Vec<_>>(),
                )),
            ],
        )
        .expect("a well-formed batch");
        let _ = request.out.arrow(batch).await;
        Ok(())
    }
}
