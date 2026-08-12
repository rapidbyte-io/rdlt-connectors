//! The source connector: construction and the framework face (the sdk's
//! `SourceConnector`; the SPI comes from [`super::Shell`]). Both faces are
//! thin dispatch — validation lives in `plan`, incremental resolution in
//! `cursor::prepare`, SQL in `sql`, the read loop in `copy`.

use std::collections::BTreeMap;

use async_trait::async_trait;
use rdlt_connector_sdk::source::{Feed, SourceConnector};
use rdlt_connector_sdk::spi::core::crash_point;
use rdlt_connector_sdk::spi::{Cursor, SourceError, StreamSpec};

use super::config::{self, Config};
use super::errors::{self, Phase};
use super::{cdc, copy, cursor, plan, reflect, sql};
use crate::session;
use crate::types::Column;

/// The Postgres source.
#[derive(Debug)]
pub struct Postgres {
    config: Config,
    /// Reflection runs once per run: `streams()` fills it, every `read()`
    /// reuses it. Drift after this point surfaces as typed errors.
    reflected: tokio::sync::OnceCell<BTreeMap<String, reflect::Table>>,
    /// CDC run state (slot lifecycle, shared snapshot view, ack floors).
    cdc_runtime: cdc::Runtime,
}

impl Postgres {
    async fn reflected(&self) -> Result<&BTreeMap<String, reflect::Table>, SourceError> {
        self.reflected
            .get_or_try_init(|| async {
                let connection = connect(&self.config).await?;
                let mut tables = reflect::reflect(&connection, &self.config).await?;
                // Query streams: described once per run, same cache.
                for query in &self.config.queries {
                    if tables.contains_key(&query.name) {
                        return Err(errors::fatal(
                            Phase::Reflect,
                            Some(&query.name),
                            "query stream name collides with a reflected table",
                        ));
                    }
                    let described =
                        reflect::describe_query(&connection, &query.name, &query.sql).await?;
                    tables.insert(query.name.clone(), described);
                }
                Ok(tables)
            })
            .await
    }

    /// The effective per-stream config: a listed table's, or a query's
    /// (viewed through the table-config shape so hints/cursor machinery
    /// applies unchanged).
    fn stream_config(&self, name: &str) -> Option<config::TableConfig> {
        self.config
            .table_config(name)
            .cloned()
            .or_else(|| self.config.synthesized_table_config(name))
    }

    /// The stream names this run serves, in publish order: listed tables in
    /// config order (else every reflected relation), then query streams.
    fn stream_names<'a>(&'a self, reflected: &'a BTreeMap<String, reflect::Table>) -> Vec<&'a str> {
        let mut names: Vec<&str> = match &self.config.tables {
            Some(listed) => listed.iter().map(|table| table.name.as_str()).collect(),
            None => reflected
                .keys()
                .map(String::as_str)
                .filter(|name| self.config.query_config(name).is_none())
                .collect(),
        };
        names.extend(self.config.queries.iter().map(|query| query.name.as_str()));
        names
    }

    /// The CDC-covered stream set: every TABLE stream (query streams are
    /// never CDC), in declaration order.
    fn cdc_tables(&self, reflected: &BTreeMap<String, reflect::Table>) -> Vec<String> {
        match &self.config.tables {
            Some(listed) => listed.iter().map(|table| table.name.clone()).collect(),
            None => reflected
                .keys()
                .filter(|name| self.config.query_config(name).is_none())
                .cloned()
                .collect(),
        }
    }

    /// Resolve one stream's table + validated facts. A missing entry is
    /// typed, never an index panic.
    fn stream_facts(
        &self,
        reflected: &BTreeMap<String, reflect::Table>,
        name: &str,
    ) -> Result<(plan::Facts, Option<config::TableConfig>), SourceError> {
        let table = reflected.get(name).ok_or_else(|| {
            errors::fatal(Phase::Reflect, Some(name), "stream has no reflected table")
        })?;
        let stream_config = self.stream_config(name);
        let facts = plan::resolve(table, stream_config.as_ref(), name)?;
        Ok((facts, stream_config))
    }
}

/// Open one connection through the shared session path: connection-string parse failure
/// is Fatal (never the Transient retry path); verification failures are
/// Fatal (retries don't mint certificates), network-shaped failures
/// Transient. Enforcement lives HERE so it holds even for configs built
/// without `validate` (`Config` has pub fields).
pub(crate) async fn connect(config: &Config) -> Result<session::Connection, SourceError> {
    session::establish(
        &config.connection,
        config.tls.as_ref(),
        session::Profile::Plain,
    )
    .await
    .map_err(errors::establish_failure)
}

#[async_trait]
impl SourceConnector for Postgres {
    // Reverse-DNS, not bare `postgres` (039 T6's id rule, adopted at
    // 041): NAME is the connector id the wire handshake reports and the
    // client verifies by STRICT equality against a
    // `ConnectorRequirement.id` — and D-039-1 keys discovery on the
    // id's last segment (`io.rapidbyte.postgres` → binary
    // `rdlt-connector-postgres` on PATH), so the id, the reported
    // identity and the binary name all derive from this one const.
    const NAME: &'static str = "io.rapidbyte.postgres";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");

    type Config = Config;

    fn assemble(config: Config) -> Result<Self, config::ConfigError> {
        Ok(Self {
            config,
            reflected: tokio::sync::OnceCell::new(),
            cdc_runtime: cdc::Runtime::new(),
        })
    }

    fn config_schema() -> Option<serde_json::Value> {
        Some(config::config_schema())
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        let reflected = self.reflected().await?;
        let names = self.stream_names(reflected);
        // Discovery alongside declared queries is legitimate but rarely
        // intended: the run delivers every table in the schema IN ADDITION
        // to the queries, which multiplies the volume moved without changing
        // what the queries produce. Announce it with the remedy, since the
        // extra streams are otherwise only visible in the row totals.
        if self.config.tables.is_none() && !self.config.queries.is_empty() {
            let discovered: Vec<&str> = names
                .iter()
                .copied()
                .filter(|name| self.config.query_config(name).is_none())
                .collect();
            if !discovered.is_empty() {
                tracing::warn!(
                    schema = %self.config.schema,
                    count = discovered.len(),
                    tables = %discovered.join(", "),
                    "`tables` is not set, so schema discovery adds these tables to the \
                     declared queries; set `tables: []` to deliver only the queries, or \
                     list the tables to read"
                );
            }
        }
        let mut specs = Vec::with_capacity(names.len());
        for name in names {
            let (facts, _) = self.stream_facts(reflected, name)?;
            // CDC table streams: keyed structured streams, the key from the
            // replica-identity preflight; cursor config is impossible here
            // (refused at parse). Query streams stay on their own machinery.
            if let Some(cdc_config) = &self.config.cdc
                && self.config.query_config(name).is_none()
            {
                let cdc_tables = self.cdc_tables(reflected);
                let identities = self
                    .cdc_runtime
                    .identities(&self.config, cdc_config, reflected, &cdc_tables)
                    .await?;
                let identity = identities.get(name).ok_or_else(|| {
                    errors::fatal(Phase::Reflect, Some(name), "stream is not a CDC table")
                })?;
                // The merge key must survive the column selection — delete
                // records are key-only, so an excluded key column would
                // strand them.
                if let Some(missing) = identity
                    .key
                    .iter()
                    .find(|key| !facts.columns.iter().any(|column| &&column.name == key))
                {
                    return Err(errors::fatal(
                        Phase::Reflect,
                        Some(name),
                        format!(
                            "replica-identity key column `{missing}` is excluded \
                             by the column selection — CDC delete records carry \
                             only key columns"
                        ),
                    ));
                }
                specs.push(
                    StreamSpec::new(name)
                        .with_structured()
                        .with_primary_key(identity.key.clone()),
                );
                continue;
            }
            let mut spec = StreamSpec::new(name).with_structured();
            if !facts.primary_key.is_empty() {
                spec = spec.with_primary_key(facts.primary_key.clone());
            }
            if let Some(cursor) = &facts.cursor {
                spec = spec.with_cursor_field(cursor.config.column.clone());
            }
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
        let name = stream.name.as_str().to_owned();
        let reflected = self.reflected().await?;
        let (facts, _) = self.stream_facts(reflected, &name)?;
        // Lossy visibility: each documented-lossy column announces itself
        // ONCE per read on a dedicated target, so embedders can subscribe to
        // `rdlt::lossy` without log-scraping.
        for column in &facts.columns {
            if column.mapping.documented_lossy {
                tracing::warn!(
                    target: "rdlt::lossy",
                    stream = %name,
                    column = %column.name,
                    source_type = %column.type_name,
                    "documented-lossy mapping: values arrive via a textual or \
                     normalizing conversion"
                );
            }
        }
        crash_point!(
            "pg.src.after_reflect",
            Err(errors::fatal(
                Phase::Reflect,
                Some(&name),
                "injected: after reflect"
            ))
        );

        // CDC dispatch: table streams read through the snapshot/change-pass
        // machinery; everything below is the non-CDC path.
        if let Some(cdc_config) = &self.config.cdc
            && self.config.query_config(&name).is_none()
        {
            let cdc_tables = self.cdc_tables(reflected);
            let identities = self
                .cdc_runtime
                .identities(&self.config, cdc_config, reflected, &cdc_tables)
                .await?;
            let identity = identities.get(&name).ok_or_else(|| {
                errors::fatal(Phase::Reflect, Some(&name), "stream is not a CDC table")
            })?;
            let decode_columns: Vec<Column> = facts
                .columns
                .iter()
                .map(|column| Column {
                    name: column.name.clone(),
                    kind: column.mapping.kind,
                    not_null: column.not_null,
                })
                .collect();
            let context = cdc::StreamContext {
                config: &self.config,
                cdc: cdc_config,
                identity,
                columns: &decode_columns,
            };
            let reflected_columns: Vec<&reflect::Column> = facts.columns.iter().collect();
            return cdc::read_stream(
                &self.cdc_runtime,
                &context,
                &cdc_tables,
                &reflected_columns,
                stream,
                since,
                feed,
            )
            .await;
        }

        let mut incremental = cursor::prepare(&facts, since.as_ref(), &name)?;
        let (where_sql, order_sql) = match &incremental {
            Some(plan) => (plan.where_sql.clone(), plan.order_sql.clone()),
            None => (String::new(), String::new()),
        };
        let column_refs: Vec<&reflect::Column> = facts.columns.iter().collect();
        let select = match self.config.query_config(&name) {
            // Query stream: the ONE wrapped form is the FROM clause.
            Some(query) => sql::select_from(
                &sql::subquery(&query.sql),
                &column_refs,
                &where_sql,
                &order_sql,
            ),
            None => sql::select(
                &self.config.schema,
                &name,
                &column_refs,
                &where_sql,
                &order_sql,
            ),
        };
        let copy_sql = sql::copy(&select);

        let connection = connect(&self.config).await?;
        let decode_columns: Vec<Column> = facts
            .columns
            .iter()
            .map(|column| Column {
                name: column.name.clone(),
                kind: column.mapping.kind,
                not_null: column.not_null,
            })
            .collect();
        let mut decoder = crate::types::binary::Decoder::new(
            decode_columns,
            self.config.batch_target_bytes,
            self.config.batch_max_rows,
        )
        .map_err(|e| errors::fatal(Phase::Decode, Some(&name), e))?;
        let mut pushed_any = false;
        let completed = copy::stream(
            &connection,
            &copy_sql,
            &mut decoder,
            feed,
            &name,
            copy::CrashSite {
                label: "pg.src.mid_copy",
                detail: "injected: connection lost mid-COPY",
            },
            |batch| tracked_pushes(&mut incremental, batch, &mut pushed_any),
        )
        .await?;
        if !completed {
            return Ok(()); // cancellation; dropping the client aborts the COPY
        }
        if !pushed_any && feed.arrow(decoder.empty_batch()).await.is_break() {
            return Ok(()); // still cancellation
        }
        match incremental {
            None => {
                // Snapshot (cursor-less) streams never checkpoint: every run
                // is a full read by definition; no meaningful resume cursor.
                tracing::debug!(table = %name, rows = decoder.rows_decoded(), "snapshot complete");
            }
            Some(plan) => {
                crash_point!(
                    "pg.src.before_checkpoint",
                    Err(errors::fatal(
                        Phase::Copy,
                        Some(&name),
                        "injected: before final checkpoint"
                    ))
                );
                if let Some(state) = plan.tracker.final_state(plan.keep_final_keys)
                    && feed.checkpoint(state.encode()).await.is_break()
                {
                    return Ok(());
                }
                tracing::debug!(
                    table = %name,
                    rows = decoder.rows_decoded(),
                    deduped = plan.tracker.deduped_rows,
                    "incremental read complete"
                );
            }
        }
        Ok(())
    }
}

/// The plain read's per-batch transform for [`copy::stream`]: route each
/// decoded batch through the incremental tracker (dedup → push →
/// intermediate checkpoint, in that order) and return the ordered pushes.
/// The `Push::Crash` between the batch push and its checkpoint is the
/// `pg.src.after_batch_push` site — it fires only after a non-empty batch
/// was pushed. Snapshot (tracker-less) streams push the batch straight
/// through.
fn tracked_pushes(
    incremental: &mut Option<cursor::Incremental>,
    batch: arrow_array::RecordBatch,
    pushed_any: &mut bool,
) -> Result<Vec<copy::Push>, SourceError> {
    use copy::Push;
    match incremental {
        None => {
            *pushed_any = true;
            Ok(vec![Push::Arrow(batch)])
        }
        Some(plan) => {
            let (surviving, checkpoint) = plan.tracker.process(batch)?;
            let mut pushes = Vec::new();
            if let Some(surviving) = surviving {
                *pushed_any = true;
                pushes.push(Push::Arrow(surviving));
                pushes.push(Push::Crash(
                    "pg.src.after_batch_push",
                    "injected: after batch push",
                ));
            }
            if let Some(state) = checkpoint {
                pushes.push(Push::Checkpoint(state.encode()));
            }
            Ok(pushes)
        }
    }
}

#[cfg(test)]
mod tests {
    #[cfg(feature = "failpoints")]
    use super::*;

    #[cfg(feature = "failpoints")]
    #[test]
    fn crash_points_actually_fire() {
        fn site() -> Result<(), SourceError> {
            crash_point!(
                "pg.src.after_reflect",
                Err(errors::fatal(Phase::Reflect, None, "probe"))
            );
            Ok(())
        }
        rdlt_connector_sdk::spi::core::failpoint::fail::cfg("pg.src.after_reflect", "return")
            .unwrap();
        let fired = site().is_err();
        rdlt_connector_sdk::spi::core::failpoint::fail::remove("pg.src.after_reflect");
        assert!(fired, "armed crash_point must fire");
    }
}
