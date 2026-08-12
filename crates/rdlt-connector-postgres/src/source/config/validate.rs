//! The entry points and the gate: every constructor parses, then runs the
//! same local validation — shape rules checkable without a database. Rules
//! that need the live catalog run at open, against reflection (`plan`).

use std::collections::BTreeSet;

use rdlt_connector_sdk::config::Document;

use super::vocabulary::*;

/// The [`Document`] gate: the sdk's provided `from_yaml`/`from_json`/
/// `from_value` parse and then run THIS — local validation split by
/// concern (connection, cursors, CDC, stream selection), each owning a
/// coherent slice of the same typed errors, with the crate's own frozen
/// refusal spellings.
impl Document for Config {
    type Error = ConfigError;

    fn validate(&self) -> Result<(), ConfigError> {
        self.validate_connection()?;
        self.validate_cursors()?;
        self.validate_cdc()?;
        self.validate_streams()?;
        Ok(())
    }
}

impl Config {
    /// Top-level connection + batching scalars.
    fn validate_connection(&self) -> Result<(), ConfigError> {
        let invalid = |message: String| Err(ConfigError::Invalid(message));
        if self.connection.trim().is_empty() {
            return invalid("`conn` must not be empty".into());
        }
        // Parse failure = FATAL config error, up front — a malformed
        // connection string must never reach the Transient/retry path. The
        // shared session gate also translates libpq's TLS parameters and
        // names every rejected parameter — no bare parse errors.
        if let Err(e) = crate::session::parse(&self.connection, self.tls.as_ref()) {
            return invalid(e.to_string());
        }
        if self.schema.trim().is_empty() {
            return invalid("`schema` must not be empty".into());
        }
        if self.batch_target_bytes == 0 || self.batch_max_rows == 0 {
            return invalid("batch knobs must be positive".into());
        }
        Ok(())
    }

    /// Cursor-shape rules checkable without the catalog.
    fn validate_cursors(&self) -> Result<(), ConfigError> {
        let invalid = |message: String| Err(ConfigError::Invalid(message));
        for cursor in self
            .tables
            .iter()
            .flatten()
            .filter_map(|table| table.cursor.as_ref())
            .chain(
                self.queries
                    .iter()
                    .filter_map(|query| query.cursor.as_ref()),
            )
        {
            if cursor.lag.is_some() && cursor.boundary == Bound::Exclusive {
                return invalid(format!(
                    "cursor `{}`: lag requires an INCLUSIVE boundary (an exclusive boundary \
                     skips the dedup that makes the lag window safe): ",
                    cursor.column
                ));
            }
        }
        Ok(())
    }

    /// The CDC block: required non-empty names + the cursor/cdc exclusivity.
    fn validate_cdc(&self) -> Result<(), ConfigError> {
        let invalid = |message: String| Err(ConfigError::Invalid(message));
        if let Some(cdc) = &self.cdc {
            for (field, value) in [
                ("cdc.slot", &cdc.slot),
                ("cdc.publication", &cdc.publication),
                ("cdc.flag_column", &cdc.flag_column),
            ] {
                if value.trim().is_empty() {
                    return invalid(format!("`{field}` must not be empty"));
                }
            }
            // CDC captures the CONFIGURED tables; selecting none captures
            // nothing, so the slot would never be preflighted or advanced
            // and the block would behave as if it were absent.
            if self.tables.as_ref().is_some_and(Vec::is_empty) {
                return invalid(
                    "`cdc` is configured but `tables` is empty — change data capture reads \
                     the configured tables, so selecting none captures nothing; list the \
                     tables to capture or remove the `cdc` block"
                        .into(),
                );
            }
            // CDC covers every configured table; a cursor block on any of
            // them is a contradiction, not an override.
            if let Some(table) = self
                .tables
                .iter()
                .flatten()
                .find(|table| table.cursor.is_some())
            {
                return invalid(format!(
                    "table `{}`: `cursor` and `cdc` are mutually exclusive — with a \
                     `cdc:` block every configured table is captured through the \
                     replication slot",
                    table.name
                ));
            }
        }
        Ok(())
    }

    /// Query + table stream selection: unique names, no qualified table
    /// names, exclusive column selections, non-empty overrides.
    fn validate_streams(&self) -> Result<(), ConfigError> {
        let invalid = |message: String| Err(ConfigError::Invalid(message));
        {
            let mut names = BTreeSet::new();
            if let Some(tables) = &self.tables {
                for table in tables {
                    names.insert(table.name.as_str());
                }
            }
            for query in &self.queries {
                if query.name.trim().is_empty() {
                    return invalid("query with empty name".into());
                }
                if query.sql.trim().is_empty() {
                    return invalid(format!("query `{}`: empty sql", query.name));
                }
                if !names.insert(query.name.as_str()) {
                    return invalid(format!(
                        "stream name `{}` used by more than one table/query",
                        query.name
                    ));
                }
                if let Some(primary_key) = &query.primary_key
                    && primary_key.is_empty()
                {
                    return invalid(format!(
                        "query `{}`: primary_key present but empty",
                        query.name
                    ));
                }
            }
        }
        if let Some(tables) = &self.tables {
            // An empty list selects no tables — legitimate alongside
            // queries, and the only way to say "deliver the declared queries
            // and nothing else". With no queries either, the run would
            // select nothing at all and silently move zero rows, so that
            // combination is refused here rather than at read time.
            if tables.is_empty() && self.queries.is_empty() {
                return invalid(
                    "no streams selected: `tables` is empty and no `queries` are declared — \
                     list the tables to read, declare a query, or omit `tables` to discover \
                     every table in the schema"
                        .into(),
                );
            }
            let mut seen = BTreeSet::new();
            for table in tables {
                if table.name.contains('.') {
                    return invalid(format!(
                        "table `{}`: schema-qualified names are rejected; `schema` owns \
                         qualification",
                        table.name
                    ));
                }
                if table.name.trim().is_empty() {
                    return invalid("table with empty name".into());
                }
                if !seen.insert(table.name.as_str()) {
                    return invalid(format!("table `{}` listed twice", table.name));
                }
                if table.included_columns.is_some() && table.excluded_columns.is_some() {
                    return invalid(format!(
                        "table `{}`: included_columns and excluded_columns are mutually \
                         exclusive",
                        table.name
                    ));
                }
                if let Some(selection) = table
                    .included_columns
                    .as_deref()
                    .or(table.excluded_columns.as_deref())
                    && selection.is_empty()
                {
                    return invalid(format!(
                        "table `{}`: column selection present but empty",
                        table.name
                    ));
                }
                if let Some(primary_key) = &table.primary_key
                    && primary_key.is_empty()
                {
                    return invalid(format!(
                        "table `{}`: primary_key present but empty",
                        table.name
                    ));
                }
            }
        }
        Ok(())
    }

    /// The per-table config for a stream name, when the user listed tables.
    pub(crate) fn table_config(&self, name: &str) -> Option<&TableConfig> {
        self.tables
            .as_ref()?
            .iter()
            .find(|table| table.name == name)
    }

    pub(crate) fn query_config(&self, name: &str) -> Option<&QueryConfig> {
        self.queries.iter().find(|query| query.name == name)
    }

    /// A query stream's config viewed through the table-config shape, so the
    /// hint/selection/cursor machinery applies unchanged.
    pub(crate) fn synthesized_table_config(&self, name: &str) -> Option<TableConfig> {
        let query = self.query_config(name)?;
        Some(TableConfig {
            name: query.name.clone(),
            cursor: query.cursor.clone(),
            primary_key: query.primary_key.clone(),
            included_columns: None,
            excluded_columns: None,
            type_hints: query.type_hints.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use rdlt_connector_sdk::config::Document;

    use super::*;

    #[test]
    fn full_document_round_trips() {
        let config = Config::from_yaml(
            r#"
conn: "postgresql://u:p@localhost/db"
schema: sales
include_views: true
batch_target_bytes: 1048576
batch_max_rows: 1000
tables:
  - name: orders
    cursor:
      column: updated_at
      initial_value: "2026-01-01T00:00:00Z"
      boundary: exclusive
      direction: min
      end_value: "2027-01-01T00:00:00Z"
      nulls: include
    primary_key: [id]
    excluded_columns: [internal_notes]
  - name: customers
"#,
        )
        .expect("full config");
        let orders = config.table_config("orders").expect("orders");
        let cursor = orders.cursor.as_ref().expect("cursor");
        assert_eq!(cursor.boundary, Bound::Exclusive);
        assert_eq!(cursor.direction, Direction::Min);
        assert_eq!(cursor.nulls, NullPolicy::Include);
        assert!(
            config
                .table_config("customers")
                .expect("customers")
                .cursor
                .is_none()
        );
    }

    #[test]
    fn lag_vocabulary_round_trips_and_rejects() {
        use crate::types::Kind;
        // Duration forms.
        assert_eq!("90s".parse::<Lag>().unwrap(), Lag::Duration { seconds: 90 });
        assert_eq!("5m".parse::<Lag>().unwrap(), Lag::Duration { seconds: 300 });
        assert_eq!(
            "2h".parse::<Lag>().unwrap(),
            Lag::Duration { seconds: 7200 }
        );
        assert_eq!(
            "1d".parse::<Lag>().unwrap(),
            Lag::Duration { seconds: 86_400 }
        );
        // Magnitudes.
        assert_eq!(
            "1000".parse::<Lag>().unwrap(),
            Lag::Magnitude("1000".into())
        );
        assert_eq!("0.5".parse::<Lag>().unwrap(), Lag::Magnitude("0.5".into()));
        // Rejections: zero, negative, garbage, empty.
        for bad in ["0s", "-5m", "soon", "", "5 m"] {
            assert!(bad.parse::<Lag>().is_err(), "{bad}");
        }
        // Display round-trips through FromStr semantically.
        let lag: Lag = "5m".parse().unwrap();
        assert_eq!(lag.to_string().parse::<Lag>().unwrap(), lag);

        // sql_delta family matrix.
        let five_minutes = Lag::Duration { seconds: 300 };
        assert_eq!(
            five_minutes.sql_delta(Kind::TimestampTz).unwrap(),
            "INTERVAL '300 seconds'"
        );
        let two_days = Lag::Duration { seconds: 172_800 };
        assert_eq!(two_days.sql_delta(Kind::Date).unwrap(), "2::int4");
        assert!(
            five_minutes.sql_delta(Kind::Date).is_err(),
            "sub-day on date"
        );
        let thousand = Lag::Magnitude("1000".into());
        assert_eq!(thousand.sql_delta(Kind::Int64).unwrap(), "1000::int8");
        let half = Lag::Magnitude("0.5".into());
        assert!(half.sql_delta(Kind::Int64).is_err(), "fractional on int");
        assert_eq!(
            half.sql_delta(Kind::Decimal {
                precision: 10,
                scale: 2
            })
            .unwrap(),
            "'0.5'::numeric"
        );
        // Undefined families and unit mismatches.
        assert!(five_minutes.sql_delta(Kind::Text).is_err(), "text cursor");
        assert!(thousand.sql_delta(Kind::TimestampNaive).is_err());
        assert!(five_minutes.sql_delta(Kind::Int64).is_err());
    }

    #[test]
    fn yaml_conn_spelling_is_frozen() {
        // The document says `conn`; the Rust field is `connection`.
        let config = Config::from_yaml("conn: \"host=h user=u\"\n").expect("parses");
        assert_eq!(config.connection, "host=h user=u");
        // The old spelling is the ONLY spelling.
        assert!(Config::from_yaml("connection: \"host=h user=u\"\n").is_err());
    }
}
