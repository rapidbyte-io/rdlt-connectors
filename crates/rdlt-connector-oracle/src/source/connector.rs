//! The source connector: config in, streams out.

use async_trait::async_trait;
use rdlt_connector_sdk::source::{Feed, SourceConnector};
use rdlt_connector_sdk::spi::core::{cursor::Cursor, id::StreamName};
use rdlt_connector_sdk::spi::{error::SourceError, source::StreamSpec};

use super::client::Client;
use super::config::{self, Config, Stream};
use super::cursor::OracleCursor;
use super::read::read_stream;

/// The crash points this source arms — exported so the sweep
/// iterates exactly this list. These spellings are frozen.
pub const FAIL_POINTS: &[&str] = &["ora.query", "ora.checkpoint"];

/// The Oracle source.
#[derive(Debug, Clone)]
pub struct Oracle {
    config: Config,
}

impl Oracle {
    fn stream_config(&self, name: &StreamName) -> Option<&Stream> {
        self.config.streams.iter().find(|s| s.name == name.as_str())
    }
}

#[async_trait]
impl SourceConnector for Oracle {
    // The connector id, reverse-DNS (039 T6): the strict-equality
    // handshake verification and the runtime's last-segment binary
    // discovery (`io.rapidbyte.oracle` → `rdlt-connector-oracle` on
    // PATH) both derive from this one const.
    const NAME: &'static str = "io.rapidbyte.oracle";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");

    type Config = Config;

    fn assemble(config: Config) -> Result<Self, config::ConfigError> {
        Ok(Self { config })
    }

    fn config_schema() -> Option<serde_json::Value> {
        Some(config::config_schema())
    }

    /// A cheap connectivity probe — connect and let it go.
    async fn check(&self) -> Result<(), SourceError> {
        Client::connect(&self.config).await.map(|_| ())
    }

    /// Declare each stream, INCLUDING its Arrow schema.
    ///
    /// The rows themselves now cross as Arrow, so the schema travels
    /// with the batch and the engine needs no hints — under the old
    /// NDJSON transport the exact types were derived here and then
    /// thrown away, and every decimal landed as TEXT downstream.
    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        let client = Client::connect(&self.config).await?;
        let mut specs = Vec::with_capacity(self.config.streams.len());
        for stream in &self.config.streams {
            // STRUCTURED: the rows cross as Arrow, and the engine
            // refuses Arrow on a stream that has not said so
            // ("source pushed Arrow batches on a stream not declared
            // `structured`"). The whole suite passed without this —
            // it only shows up end-to-end, which is what the
            // benchmark caught.
            let mut spec = StreamSpec::new(stream.name.as_str()).with_structured();
            if let Some(key) = &stream.primary_key {
                spec = spec.with_primary_key(key.iter().cloned());
            }
            if let Some(cursor) = &stream.cursor {
                spec = spec.with_cursor_field(cursor.clone());
            }
            let described = super::read::describe(
                &client,
                &stream.name,
                &super::client::quote_table(&stream.table),
            )
            .await?;
            // The schema is DERIVED here but not attached: an Arrow
            // batch carries its own, so hints would be a second copy
            // that could disagree with it. What this buys is an early
            // refusal — a table holding a type the rulebook has no
            // mapping for fails at discovery, naming the column,
            // rather than part-way through a load.
            super::schema::schema_of(&described).map_err(|e| {
                SourceError::fatal(format!("stream `{}`: `{}`: {e}", stream.name, stream.table))
            })?;
            specs.push(spec);
        }
        Ok(specs)
    }

    async fn read_stream(
        &self,
        stream: &StreamSpec,
        since: Option<Cursor>,
        feed: &mut Feed,
    ) -> Result<(), SourceError> {
        let Some(config) = self.stream_config(&stream.name) else {
            return Err(SourceError::fatal(format!(
                "unknown stream {}",
                stream.name
            )));
        };
        let mut cursor = OracleCursor::decode(since.as_ref())?;
        let client = Client::connect(&self.config).await?;
        read_stream(&client, &self.config, config, &mut cursor, feed).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The registry carries its frozen spellings.
    #[test]
    fn the_registry_is_the_frozen_pair() {
        assert_eq!(FAIL_POINTS, &["ora.query", "ora.checkpoint"]);
    }
}
