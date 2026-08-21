//! The framework face: [`Rest`] implements the sdk's `SourceConnector`
//! over a validated [`Config`] and one shared [`http::Client`]; the sdk's
//! shell provides the SPI ([`super::Shell`]).

use async_trait::async_trait;
use rdlt_connector_sdk::source::{Feed, SourceConnector};
use rdlt_connector_sdk::spi::{
    core::cursor::Cursor, core::id::StreamName, error::SourceError, source::StreamSpec,
};

use super::config::{self, Config, Stream};
use super::http::{Client, Credentials};
use super::read;

/// The declarative REST source: one validated config document, one HTTP
/// client whose read deadline covers every request the source makes.
#[derive(Debug)]
pub struct Rest {
    config: Config,
    client: Client,
}

impl Rest {
    fn stream_config(&self, name: &StreamName) -> Option<&Stream> {
        self.config.streams.iter().find(|s| s.name == name.as_str())
    }
}

#[async_trait]
impl SourceConnector for Rest {
    // Reverse-DNS, not bare `rest` (039 T6's id rule, adopted at 042):
    // NAME is the connector id the wire handshake reports and the
    // client verifies by STRICT equality against a
    // `Requirement.id` — and D-039-1 keys discovery on the
    // id's last segment (`io.rapidbyte.rest` → binary
    // `rdlt-connector-rest` on PATH), so the id, the reported identity
    // and the binary name all derive from this one const.
    const NAME: &'static str = "io.rapidbyte.rest";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");

    type Config = Config;

    /// Wire up the HTTP client for a validated config.
    fn assemble(config: Config) -> Result<Self, config::ConfigError> {
        // ONE reqwest client for the whole source, so the deadline covers
        // the token fetch as well as the data requests. Its construction is
        // fallible — `reqwest::Client::new()` PANICS when the TLS backend cannot
        // initialise, which is an environment problem an embedder should
        // receive as an error.
        //
        // `read_timeout`, NOT `timeout`. The bound exists to catch a server
        // that accepts a connection and then stalls, and `read_timeout`
        // resets after each successful read — so it bounds every wait
        // without capping a transfer that is making continuous progress. A
        // total deadline would kill a large page mid-download, and since
        // that failure is transient the engine would restart and hit the
        // same wall on every attempt.
        //
        // Redirects are PINNED same-origin. reqwest's default policy
        // follows up to ten hops anywhere, stripping only the standard
        // auth headers — but this source also authenticates with custom
        // header names and query-located api keys, which survive every
        // redirect by construction. One 3xx to an attacker host would
        // deliver them, so a hop that changes origin refuses instead;
        // the hop cap stays at reqwest's own default.
        let http = reqwest::Client::builder()
            .read_timeout(std::time::Duration::from_secs(config.request_timeout_secs))
            .redirect(reqwest::redirect::Policy::custom(|attempt| {
                const MAX_REDIRECTS: usize = 10;
                if attempt.previous().len() >= MAX_REDIRECTS {
                    return attempt.error("too many redirects");
                }
                match attempt.previous().last() {
                    Some(previous)
                        if crate::source::http::origin::same_origin(previous, attempt.url()) =>
                    {
                        attempt.follow()
                    }
                    _ => attempt.error(
                        "redirect leaves the request's origin — credentials are pinned \
                         to the origin the config named",
                    ),
                }
            }))
            .build()
            .map_err(|e| config::ConfigError::Invalid(format!("building the HTTP client: {e}")))?;
        let client = Client::new(
            Credentials::new(config.auth.clone(), http.clone()),
            http,
            config
                .headers
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            config
                .params
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            config.min_request_interval_ms,
            config.retry_after_cap_secs,
        );
        Ok(Self { config, client })
    }

    fn config_schema() -> Option<serde_json::Value> {
        Some(config::config_schema())
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(self
            .config
            .streams
            .iter()
            .map(|stream| {
                let mut spec = StreamSpec::new(stream.name.as_str());
                if let Some(key) = &stream.primary_key {
                    spec = spec.with_primary_key(key.iter().cloned());
                }
                if let Some(incremental) = stream.effective_incremental() {
                    spec = spec.with_cursor_field(incremental.cursor_field);
                }
                for (column, hint) in &stream.type_hints {
                    spec = spec.with_type_hint(column.clone(), (*hint).into());
                }
                spec
            })
            .collect())
    }

    async fn read_stream(
        &self,
        stream: &StreamSpec,
        since: Option<Cursor>,
        feed: &mut Feed,
    ) -> Result<(), SourceError> {
        let stream = self
            .stream_config(&stream.name)
            .ok_or_else(|| SourceError::fatal(format!("unknown stream {}", stream.name)))?;
        read::deliver(&self.config, &self.client, stream, since.as_ref(), feed).await
    }
}
