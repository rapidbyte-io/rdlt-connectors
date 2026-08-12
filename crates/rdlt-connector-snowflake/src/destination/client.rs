//! The one boundary with the Snowflake library.
//!
//! Every library type stops at this file: what crosses out is the
//! [`Executor`] trait, SPI errors, and strings. Swapping the library is
//! a change here, nowhere else — the same one-boundary rule the duckdb
//! crate set and 022 adopted.
//!
//! Three concerns live behind the boundary: opening a session from
//! configuration (every auth method mapped in one place), running
//! statements and reading their results in the few shapes the protocol
//! needs, and turning a library error into the SPI taxonomy.

use async_trait::async_trait;
use rdlt_connector_sdk::spi::DestinationError;
use snowflake_connector_rs::{
    AuthConfig, Client, ClientConfig, DynamicRow, EndpointConfig, Error as SfError, ErrorKind,
    KeyPairConfig, PasswordConfig, Session, SessionConfig,
};

use super::config::{Auth, Config};

/// The service's code for a MERGE whose source carried two rows for one
/// target key — the closest thing to a unique violation on a service
/// that enforces no uniqueness. The merge path exchanges it for the
/// shared diagnosis; nothing ever matches the message text around it.
pub(crate) const DUPLICATE_ROW_IN_DML: &str = "100090";

/// Server codes that earn a retry.
///
/// A short allowlist, grown by observation: an unrecognised server
/// error is a SQL or schema problem far more often than a blip, and
/// retrying those spends the budget to change nothing.
const TRANSIENT_SERVER_CODES: &[&str] = &[
    // Statement aborted by a warehouse resize/suspend/resume under it;
    // the same statement succeeds on the resumed warehouse.
    "000629", "000630", // Concurrent DDL on one object; a retry serialises behind it.
    "000625",
];

/// What a caller may run against the service, in the result shapes the
/// protocol actually consumes.
///
/// A trait rather than the session type so statement-economy tests can
/// script and count without a warehouse, and so the unit's DML-only
/// wrapper can refuse by wrapping ANY executor.
#[async_trait]
pub(crate) trait Executor: Send + Sync {
    /// Run a statement; discard its result.
    async fn execute(&self, sql: &str) -> Result<(), DestinationError>;

    /// One integer, first column of the first row — every protocol
    /// probe's shape.
    async fn scalar_u64(&self, sql: &str, binds: &[&str]) -> Result<u64, DestinationError>;

    /// Total a named column over every row — the COPY verification's
    /// shape (one row per file; the check is the sum).
    async fn sum_column(&self, sql: &str, column: &str) -> Result<u64, DestinationError>;

    /// Named columns from EVERY row, rendered as text.
    ///
    /// The upload verification's shape, and the reason a scalar or a
    /// total cannot serve it: a multi-file upload reports SUCCESS
    /// overall while one row says a file failed, so correctness is a
    /// property of each row. Column names resolve case-insensitively —
    /// the service names result columns in its own case, and pinning
    /// that case here would make a check silently read nothing if it
    /// changed. Values come back as owned strings: this is the
    /// boundary, and nothing behind it crosses out.
    async fn rows(
        &self,
        sql: &str,
        binds: &[&str],
        columns: &[&str],
    ) -> Result<Vec<Vec<String>>, DestinationError>;
}

/// Open a session. Every auth method the vocabulary accepts maps here
/// and nowhere else.
pub(crate) async fn connect(config: &Config) -> Result<Box<dyn Executor>, DestinationError> {
    let auth = library_auth(&config.auth)?;
    let mut client = ClientConfig::new(config.user.clone(), config.account.clone(), auth);

    if config.host.is_some() {
        // Only when asked: a PrivateLink deployment fronts the account
        // under its own name.
        let url = format!("https://{}", config.host())
            .parse()
            .map_err(|e| DestinationError::fatal(format!("snowflake: `host` is not a URL: {e}")))?;
        client = client.with_endpoint(EndpointConfig::custom_base_url(url));
    }

    let mut session = SessionConfig::new()
        .with_database(config.database.clone())
        .with_schema(config.schema.clone());
    if let Some(warehouse) = &config.warehouse {
        session = session.with_warehouse(warehouse.clone());
    }
    if let Some(role) = &config.role {
        session = session.with_role(role.clone());
    }
    for (key, value) in &config.session_parameters {
        session = session.with_session_parameter(key.clone(), value.clone().into());
    }
    if let Some(tag) = &config.query_tag {
        session = session.with_session_parameter("QUERY_TAG", tag.clone().into());
    }

    // Both connect steps carry the identity, so whichever fails tells
    // the operator WHICH credential to fix without opening the pipeline
    // document. The service's own auth errors say what went wrong but
    // never who was trying.
    let with_identity = |err: SfError| {
        let transient = is_transient(&err);
        let failure = ConnectFailure {
            account: config.account.clone(),
            user: config.user.clone(),
            source: err,
        };
        if transient {
            DestinationError::transient(failure)
        } else {
            DestinationError::fatal(failure)
        }
    };
    let client = Client::new(client.with_session(session)).map_err(with_identity)?;
    Ok(Box::new(SessionExecutor {
        session: client.create_session().await.map_err(with_identity)?,
    }))
}

/// Translate a library error into the SPI taxonomy.
///
/// The discriminators are `ErrorKind` and, for server errors, the
/// structured code — never rendered text, which the service may change
/// and would take the retry policy with it.
pub(crate) fn classify(err: SfError) -> DestinationError {
    if is_transient(&err) {
        DestinationError::transient(err)
    } else {
        DestinationError::fatal(err)
    }
}

/// The retry decision alone — the connect path wraps identity around an
/// error before classifying, and the rule must not be restated there.
fn is_transient(err: &SfError) -> bool {
    transient_rule(err.kind(), err.snowflake_code())
}

/// The decision table itself, over the two discriminators — split from
/// the error so it pins without a library error to carry it.
fn transient_rule(kind: ErrorKind, server_code: Option<&str>) -> bool {
    match kind {
        // The work is unchanged; the next attempt is a fresh session.
        ErrorKind::Network | ErrorKind::Timeout | ErrorKind::SessionExpired => true,
        ErrorKind::Server => server_code.is_some_and(|code| TRANSIENT_SERVER_CODES.contains(&code)),
        // Config/Auth/Cancelled/Protocol/BindEncode/Decode/Internal/
        // Other: a retry cannot change these. Auth happens before any
        // SQL and carries no server code — which is why KIND, not code,
        // is the first discriminator.
        _ => false,
    }
}

/// The structured code inside an already-classified error, if any.
///
/// Walks the source chain rather than reading the top: context this
/// crate adds (the connect identity, a merge frame) must not hide the
/// code that handling keys on.
pub(crate) fn code_in(err: &DestinationError) -> Option<String> {
    let mut source: Option<&(dyn std::error::Error + 'static)> = match err {
        DestinationError::Fatal(e) | DestinationError::Transient(e) => Some(e.as_ref()),
        DestinationError::RateLimited { source, .. } => Some(source.as_ref()),
        _ => None,
    };
    while let Some(error) = source {
        if let Some(code) = error
            .downcast_ref::<SfError>()
            .and_then(SfError::snowflake_code)
        {
            return Some(code.to_owned());
        }
        source = error.source();
    }
    None
}

/// A connect failure carrying who was connecting.
#[derive(Debug)]
struct ConnectFailure {
    account: String,
    user: String,
    source: SfError,
}

impl std::fmt::Display for ConnectFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "snowflake: connecting as user `{}` on account `{}` failed: {}",
            self.user, self.account, self.source
        )
    }
}

impl std::error::Error for ConnectFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// The config's auth block, mapped onto the library's.
///
/// A programmatic access token rides the PASSWORD channel — a fact
/// verified against a live account; the OAuth channel rejects PATs.
fn library_auth(auth: &Auth) -> Result<AuthConfig, DestinationError> {
    if let Some(key_pair) = &auth.key_pair {
        let pem = key_pair.private_key.read_to_string().map_err(|e| {
            DestinationError::fatal(format!(
                "snowflake: `auth.key_pair.private_key` unreadable: {e}"
            ))
        })?;
        return Ok(match &key_pair.passphrase {
            Some(passphrase) => AuthConfig::key_pair(KeyPairConfig::from_encrypted_pem(
                pem,
                passphrase.reveal().as_bytes().to_vec(),
            )),
            None => AuthConfig::key_pair(KeyPairConfig::from_pem(pem)),
        });
    }
    if let Some(password) = &auth.password {
        let mut config = PasswordConfig::from(password.password.reveal().to_owned());
        if let Some(passcode) = &password.passcode {
            config = config.with_passcode(passcode.reveal().to_owned());
        }
        return Ok(AuthConfig::password(config));
    }
    if let Some(token) = &auth.oauth_token {
        return Ok(AuthConfig::oauth(token.reveal().to_owned()));
    }
    if let Some(pat) = &auth.pat {
        return Ok(AuthConfig::password(pat.reveal().to_owned()));
    }
    // A validated config cannot get here; its own gate refuses an empty
    // auth block with a message naming the alternatives.
    Err(DestinationError::fatal(
        "snowflake: `auth` names no authentication method",
    ))
}

/// The live session behind the trait.
struct SessionExecutor {
    session: Session,
}

impl SessionExecutor {
    /// Run one statement with positional binds and collect the result.
    ///
    /// The library's bind builder is a typestate (the first bind changes
    /// the statement's type), so the first is split from the rest; the
    /// values are owned before the await because a borrowed bind cannot
    /// outlive the polling frame.
    async fn query(
        &self,
        sql: &str,
        binds: &[&str],
    ) -> Result<snowflake_connector_rs::ResultTable, DestinationError> {
        let owned: Vec<String> = binds.iter().map(|b| (*b).to_owned()).collect();
        let statement = snowflake_connector_rs::Statement::from(sql.to_owned());
        let cursor = match owned.split_first() {
            None => self.session.query(statement).await,
            Some((first, rest)) => {
                let mut bound = statement.bind(first.clone());
                for bind in rest {
                    bound = bound.bind(bind.clone());
                }
                self.session.query(bound).await
            }
        };
        cursor
            .map_err(classify)?
            .collect_table()
            .await
            .map_err(classify)
    }
}

#[async_trait]
impl Executor for SessionExecutor {
    async fn execute(&self, sql: &str) -> Result<(), DestinationError> {
        self.session
            .query(sql.to_owned())
            .await
            .map(|_| ())
            .map_err(classify)
    }

    async fn scalar_u64(&self, sql: &str, binds: &[&str]) -> Result<u64, DestinationError> {
        let table = self.query(sql, binds).await?;
        table
            .rows::<DynamicRow>()
            .map_err(classify)?
            .next()
            .transpose()
            .map_err(classify)?
            .and_then(|row| row.value_at(0).ok().and_then(cell_as_u64))
            .ok_or_else(|| {
                DestinationError::fatal(format!(
                    "snowflake: expected one integer from `{sql}`, got nothing usable"
                ))
            })
    }

    async fn sum_column(&self, sql: &str, column: &str) -> Result<u64, DestinationError> {
        let table = self.query(sql, &[]).await?;
        let mut total = 0;
        for row in table.rows::<DynamicRow>().map_err(classify)? {
            let row = row.map_err(classify)?;
            let index = index_of(&row, sql, column)?;
            let cell = row.value_at(index).map_err(|e| {
                DestinationError::fatal(format!("snowflake: reading `{column}`: {e}"))
            })?;
            total += cell_as_u64(cell).ok_or_else(|| {
                DestinationError::fatal(format!(
                    "snowflake: `{column}` is not a count in the result of `{sql}`"
                ))
            })?;
        }
        Ok(total)
    }

    async fn rows(
        &self,
        sql: &str,
        binds: &[&str],
        columns: &[&str],
    ) -> Result<Vec<Vec<String>>, DestinationError> {
        let table = self.query(sql, binds).await?;
        let mut out = Vec::new();
        for row in table.rows::<DynamicRow>().map_err(classify)? {
            let row = row.map_err(classify)?;
            let mut values = Vec::with_capacity(columns.len());
            for wanted in columns {
                let index = index_of(&row, sql, wanted)?;
                let cell = row.value_at(index).map_err(|e| {
                    DestinationError::fatal(format!("snowflake: reading `{wanted}`: {e}"))
                })?;
                values.push(cell_as_display(cell));
            }
            out.push(values);
        }
        Ok(out)
    }
}

/// Resolve a wanted column against one row's own schema,
/// case-insensitively (see [`Executor::rows`]).
fn index_of(row: &DynamicRow, sql: &str, wanted: &str) -> Result<usize, DestinationError> {
    row.schema()
        .columns()
        .iter()
        .find(|c| c.name().eq_ignore_ascii_case(wanted))
        .map(|c| c.index())
        .ok_or_else(|| {
            DestinationError::fatal(format!(
                "snowflake: the result of `{sql}` carries no `{wanted}` column"
            ))
        })
}

/// A count, however the library represents it: Snowflake NUMBERs arrive
/// as integers or fixed-point decimals depending on the query, and a
/// probe must not fail on the representation. Booleans count too —
/// `SELECT EXISTS (…)` is the shared planner's stage probe, the service
/// answers it BOOLEAN (measured live: the statement compiles and the
/// cell arrives as a Boolean, which the numeric arms alone refused,
/// killing every full-feed merge publish — an inherited generation-1
/// defect, since it read the same probe through the same arms).
fn cell_as_u64(cell: &snowflake_connector_rs::CellValue) -> Option<u64> {
    use snowflake_connector_rs::CellValue;
    match cell {
        CellValue::Integer(v) => u64::try_from(*v).ok(),
        CellValue::Decimal(d) => d.to_string().parse().ok(),
        CellValue::Boolean(b) => Some(u64::from(*b)),
        CellValue::String(s) => match s.as_str() {
            "true" => Some(1),
            "false" => Some(0),
            _ => s.parse().ok(),
        },
        _ => None,
    }
}

/// Any cell as text. An upload result mixes words and sizes in one row,
/// and a caller comparing a status against a known word should not care
/// which is which. Null renders empty: an absent message is not an
/// error, and callers key on the status column.
fn cell_as_display(cell: &snowflake_connector_rs::CellValue) -> String {
    use snowflake_connector_rs::CellValue;
    match cell {
        CellValue::Null => String::new(),
        CellValue::String(s) => s.clone(),
        CellValue::Integer(v) => v.to_string(),
        CellValue::Decimal(d) => d.to_string(),
        CellValue::Boolean(b) => b.to_string(),
        CellValue::Float(f) => f.to_string(),
        other => format!("{other:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The whole retry decision table, pinned: kind first, then — for
    /// server errors only — the code allowlist. Emptying the allowlist
    /// or dropping a kind arm must fail HERE, because no live cell can
    /// provoke a warehouse resize on demand.
    #[test]
    fn the_transient_decision_table_holds_kind_first_then_code() {
        for kind in [
            ErrorKind::Network,
            ErrorKind::Timeout,
            ErrorKind::SessionExpired,
        ] {
            assert!(transient_rule(kind, None), "{kind:?} retries codeless");
            assert!(
                transient_rule(kind, Some("999999")),
                "{kind:?} retries whatever the code"
            );
        }
        for code in TRANSIENT_SERVER_CODES {
            assert!(
                transient_rule(ErrorKind::Server, Some(code)),
                "allowlisted server code {code} retries"
            );
        }
        assert!(!transient_rule(ErrorKind::Server, Some("000904")));
        assert!(!transient_rule(ErrorKind::Server, None));
        for kind in [ErrorKind::Config, ErrorKind::Auth, ErrorKind::Cancelled] {
            assert!(
                !transient_rule(kind, Some("000629")),
                "{kind:?} never retries — kind decides before code"
            );
        }
    }

    /// The allowlist is the recorded one: warehouse resize/suspend
    /// aborts and concurrent-DDL serialisation, nothing else.
    #[test]
    fn the_transient_allowlist_is_exactly_the_recorded_three() {
        assert_eq!(TRANSIENT_SERVER_CODES, &["000629", "000630", "000625"]);
    }

    /// A count arrives however the service spells it — including the
    /// BOOLEAN a `SELECT EXISTS` probe answers with (measured live; the
    /// numeric-only arms refused it and killed every full-feed merge
    /// publish, in both generations).
    #[test]
    fn a_count_is_read_from_every_representation_the_service_uses() {
        use snowflake_connector_rs::CellValue;
        assert_eq!(cell_as_u64(&CellValue::Integer(3)), Some(3));
        assert_eq!(cell_as_u64(&CellValue::Boolean(true)), Some(1));
        assert_eq!(cell_as_u64(&CellValue::Boolean(false)), Some(0));
        assert_eq!(cell_as_u64(&CellValue::String("true".to_owned())), Some(1));
        assert_eq!(cell_as_u64(&CellValue::String("7".to_owned())), Some(7));
        assert_eq!(cell_as_u64(&CellValue::String("maybe".to_owned())), None);
        assert_eq!(
            cell_as_u64(&CellValue::Integer(-1)),
            None,
            "no negative count"
        );
    }
}
