//! THE stream validation gate: every catalog-dependent fact about a stream
//! is checked HERE, exactly once, with exactly one error text per fact —
//! `streams()` and `read()` both consume the same [`Facts`], so which SPI
//! call the engine makes first can never change which error a
//! misconfiguration produces.
//!
//! One config-shape fact is deliberately re-checked here as well: lag's
//! inclusive-boundary requirement, first refused at document parse. The
//! repeat is defense in depth, not drift — `Config` has public fields, so a
//! hand-built configuration never went through `validate()`, and this gate
//! is the last stop before data moves.

use rdlt_connector_sdk::spi::error::SourceError;

use crate::source::config::{Bound, CursorConfig, TableConfig};
use crate::source::errors::{self, Phase};
use crate::source::reflect;
use crate::types::Kind;

/// Everything `streams()` and `read()` need to know about one stream, fully
/// validated.
#[derive(Debug)]
pub(crate) struct Facts {
    /// Selected columns with hints applied, in reflected order.
    pub(crate) columns: Vec<reflect::Column>,
    /// Declared override or reflected primary key; empty when neither
    /// exists.
    pub(crate) primary_key: Vec<String>,
    pub(crate) cursor: Option<CursorFacts>,
}

/// The validated cursor facts for an incremental stream.
#[derive(Debug)]
pub(crate) struct CursorFacts {
    /// Index of the cursor column within [`Facts::columns`].
    pub(crate) index: usize,
    pub(crate) kind: Kind,
    pub(crate) config: CursorConfig,
}

/// Resolve one stream's facts against reflection. Typed and early — every
/// failure fires before any data moves, whichever SPI face asked.
pub(crate) fn resolve(
    table: &reflect::Table,
    table_config: Option<&TableConfig>,
    stream: &str,
) -> Result<Facts, SourceError> {
    let columns = reflect::hinted_columns(table, table_config)?;
    let primary_key = table.effective_primary_key(table_config);
    let cursor = match table_config.and_then(|config| config.cursor.as_ref()) {
        None => None,
        Some(cursor_config) => {
            // One fact, one error text: selected AND cursor-capable
            // post-hints (a hint may change capability in either direction).
            let (index, column) = columns
                .iter()
                .enumerate()
                .find(|(_, column)| column.name == cursor_config.column)
                .ok_or_else(|| {
                    errors::fatal(
                        Phase::Reflect,
                        Some(stream),
                        format!(
                            "cursor column `{}` is not a selected column",
                            cursor_config.column
                        ),
                    )
                })?;
            if !column.mapping.cursor_capable {
                return Err(errors::fatal(
                    Phase::Reflect,
                    Some(stream),
                    format!(
                        "cursor column `{}` is not cursor-capable after type mapping/hints",
                        cursor_config.column
                    ),
                ));
            }
            let kind = column.mapping.kind;
            // Lag prerequisites, all typed and early: inclusive boundary
            // (also caught at config parse), a defined cursor-family delta,
            // and a primary key so the keyed-Merge dedup path exists.
            if let Some(lag) = &cursor_config.lag {
                if cursor_config.boundary == Bound::Exclusive {
                    return Err(errors::fatal(
                        Phase::Reflect,
                        Some(stream),
                        format!(
                            "cursor `{}`: lag requires an inclusive boundary",
                            cursor_config.column
                        ),
                    ));
                }
                if let Err(detail) = lag.sql_delta(kind) {
                    return Err(errors::fatal(
                        Phase::Reflect,
                        Some(stream),
                        format!(
                            "cursor lag on `{}` ({}): {detail}",
                            cursor_config.column, column.type_name
                        ),
                    ));
                }
                if primary_key.is_empty() {
                    return Err(errors::fatal(
                        Phase::Reflect,
                        Some(stream),
                        format!(
                            "cursor `{}`: lag requires a primary key (reflected or \
                             declared) — window rows re-deliver and dedup by key under \
                             Merge",
                            cursor_config.column
                        ),
                    ));
                }
            }
            Some(CursorFacts {
                index,
                kind,
                config: cursor_config.clone(),
            })
        }
    };
    Ok(Facts {
        columns,
        primary_key,
        cursor,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::config::{Direction, Lag, NullPolicy};
    use crate::source::reflect::tests::table;
    use crate::types::map::oid;

    fn cursor(column: &str, lag: Option<Lag>, boundary: Bound) -> CursorConfig {
        CursorConfig {
            column: column.into(),
            initial_value: None,
            boundary,
            direction: Direction::Max,
            end_value: None,
            end_bound: Bound::Exclusive,
            nulls: NullPolicy::Exclude,
            lag,
        }
    }

    fn config_with(cursor_config: CursorConfig) -> TableConfig {
        TableConfig {
            name: "t".into(),
            cursor: Some(cursor_config),
            primary_key: None,
            included_columns: None,
            excluded_columns: None,
            type_hints: Default::default(),
        }
    }

    #[test]
    fn cursor_facts_resolve_once_with_one_error_per_fact() {
        let reflected = table(&[("id", oid::INT8, true), ("flag", oid::BOOL, false)]);
        // Happy path: index + kind land.
        let facts = resolve(
            &reflected,
            Some(&config_with(cursor("id", None, Bound::Inclusive))),
            "t",
        )
        .expect("resolves");
        let cursor_facts = facts.cursor.expect("cursor facts");
        assert_eq!(cursor_facts.index, 0);
        assert_eq!(cursor_facts.kind, Kind::Int64);
        assert_eq!(facts.primary_key, ["id"]);
        // Not selected.
        let error = resolve(
            &reflected,
            Some(&config_with(cursor("ghost", None, Bound::Inclusive))),
            "t",
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("not a selected column"), "{error}");
        // Not capable.
        let error = resolve(
            &reflected,
            Some(&config_with(cursor("flag", None, Bound::Inclusive))),
            "t",
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("not cursor-capable"), "{error}");
    }

    #[test]
    fn lag_prerequisites_each_have_their_own_error() {
        let keyed = table(&[("id", oid::INT8, true)]);
        let magnitude = Lag::Magnitude("10".into());
        // Exclusive boundary.
        let error = resolve(
            &keyed,
            Some(&config_with(cursor(
                "id",
                Some(magnitude.clone()),
                Bound::Exclusive,
            ))),
            "t",
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("inclusive boundary"), "{error}");
        // Undefined family delta (duration on an integer cursor).
        let error = resolve(
            &keyed,
            Some(&config_with(cursor(
                "id",
                Some(Lag::Duration { seconds: 60 }),
                Bound::Inclusive,
            ))),
            "t",
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("magnitude"), "{error}");
        // No primary key.
        let keyless = table(&[("id", oid::INT8, false)]);
        let error = resolve(
            &keyless,
            Some(&config_with(cursor(
                "id",
                Some(magnitude),
                Bound::Inclusive,
            ))),
            "t",
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("requires a primary key"), "{error}");
    }
}
