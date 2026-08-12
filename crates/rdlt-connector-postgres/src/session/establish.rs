//! From parsed connection to live prepared client: the one handshake path
//! for plaintext and TLS, the session profiles, and the detached driver
//! task that keeps the socket alive.

use tokio_postgres::config::SslMode;

use super::classify::{ConnectError, classify_connect_error, is_transient_at_connect};
use super::connection_string::{Parsed, parse};
use crate::tls;

/// How a session is prepared after the handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    /// Server defaults — reads, reflection, DDL.
    Plain,
    /// Pins the GUCs the pgoutput TEXT forms depend on (`datestyle`,
    /// `bytea_output`): a role whose defaults differ must not change what
    /// the CDC feed looks like.
    CdcControl,
}

impl Profile {
    fn statements(self) -> Option<&'static str> {
        match self {
            Profile::Plain => None,
            Profile::CdcControl => Some("SET datestyle = 'ISO'; SET bytea_output = 'hex'"),
        }
    }
}

/// A live prepared connection. Derefs to the driver client — the wrapper
/// exists so "how a connection came to be" has exactly one owner, not to
/// hide the driver; every query still speaks `tokio_postgres::Client`.
#[derive(Debug)]
pub struct Connection {
    client: tokio_postgres::Client,
}

impl std::ops::Deref for Connection {
    type Target = tokio_postgres::Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

/// How establishing a session can fail — and the ONE place retry-worthiness
/// is decided for connection failures.
#[derive(Debug, thiserror::Error)]
pub enum EstablishError {
    #[error("tls config: {0}")]
    Config(#[from] tls::ConfigError),
    #[error("connect: {0}")]
    Connect(#[from] ConnectError),
    /// The post-handshake session [`Profile`] statements failed — carried as
    /// its own variant so callers that OWN the profile (the CDC control path)
    /// can tag it with their phase and stream instead of inheriting the
    /// connect framing.
    #[error("session profile: {detail}")]
    Profile { detail: String, transient: bool },
}

impl EstablishError {
    /// Whether the engine's retry loop can ever help: config errors never,
    /// connect failures only when network-shaped (TLS verification failures
    /// don't heal — retries don't mint certificates), profile failures by
    /// the connect-time SQLSTATE rule they were judged with.
    pub(crate) fn is_transient(&self) -> bool {
        match self {
            EstablishError::Config(_) => false,
            EstablishError::Connect(error) => error.transient,
            EstablishError::Profile { transient, .. } => *transient,
        }
    }

    /// The failure alone, without the variant framing — what error adapters
    /// hand to their own taxonomies. The enum's `Display` prefixes exist for
    /// typed matching in tests, not for operators: rendering them into a
    /// phase-tagged message would say "connect: " twice in two spellings.
    pub(crate) fn detail(&self) -> String {
        match self {
            EstablishError::Config(inner) => inner.to_string(),
            EstablishError::Connect(inner) => inner.to_string(),
            EstablishError::Profile { detail, .. } => detail.clone(),
        }
    }
}

/// The whole path: parse the connection string, resolve the policy, shake
/// hands, apply the profile. Every consumer of a user connection string ends
/// up here.
pub async fn establish(
    connection_string: &str,
    tls_override: Option<&tls::Policy>,
    profile: Profile,
) -> Result<Connection, EstablishError> {
    let parsed = parse(connection_string, tls_override)?;
    let connection = connect(&parsed).await?;
    if let Some(statements) = profile.statements() {
        connection
            .client
            .batch_execute(statements)
            .await
            .map_err(|error| EstablishError::Profile {
                detail: super::classify::describe(&error),
                transient: is_transient_at_connect(&error),
            })?;
    }
    Ok(connection)
}

/// The handshake half, callable with an already-parsed connection (the TLS
/// matrix tests exercise policies directly). Verifying modes never fall back
/// to plaintext; `disable` never negotiates TLS.
pub async fn connect(parsed: &Parsed) -> Result<Connection, EstablishError> {
    let mut driver = parsed.driver.clone();
    if parsed.tls.mode.wants_encryption() && parsed.tls.mode != tls::Mode::Prefer {
        driver.ssl_mode(SslMode::Require);
    }
    if parsed.tls.mode == tls::Mode::Disable {
        driver.ssl_mode(SslMode::Disable);
    }
    match tls::build(&parsed.tls).map_err(EstablishError::Config)? {
        None => handshake(&driver, tokio_postgres::NoTls).await,
        Some(client_config) => {
            handshake(
                &driver,
                tokio_postgres_rustls::MakeRustlsConnect::new(client_config),
            )
            .await
        }
    }
}

/// Connect with a chosen TLS maker — generic so the plaintext and rustls
/// paths are ONE path. (Two copies differing only in the connector value
/// would need their error classification and driver spawning kept in step by
/// hand.)
async fn handshake<T>(
    driver: &tokio_postgres::Config,
    tls_connector: T,
) -> Result<Connection, EstablishError>
where
    T: tokio_postgres::tls::MakeTlsConnect<tokio_postgres::Socket>,
    T::Stream: Send + 'static,
    T::TlsConnect: Send,
    <T::TlsConnect as tokio_postgres::tls::TlsConnect<tokio_postgres::Socket>>::Future: Send,
{
    let (client, io_driver) = driver
        .connect(tls_connector)
        .await
        .map_err(|error| EstablishError::Connect(classify_connect_error(&error)))?;
    // The io-driver future owns the socket: when it stops, every later
    // statement on this client fails with a generic "connection closed" that
    // names nothing. Its terminal error is the only description of WHY — a
    // TLS reset, a server shutdown, an idle timeout — so it is logged, not
    // discarded. Not propagated: nothing awaits this task, and by the time
    // it ends the failure has already surfaced through the client.
    tokio::spawn(async move {
        if let Err(error) = io_driver.await {
            tracing::warn!(error = %error, "postgres connection driver stopped");
        }
    });
    Ok(Connection { client })
}
