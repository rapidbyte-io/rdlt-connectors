//! Turn validated stream facts + stored resume state into the ready-to-read
//! incremental plan: the boundary/order SQL that shapes the COPY subselect,
//! plus the tracker that dedups re-fetched boundary rows and emits resume
//! checkpoints. Built once per `read()`.
//!
//! Validation happened in `plan::resolve` — this module only DECODES state
//! and RENDERS the window; the one failure it can add is a corrupt stored
//! cursor.

use rdlt_connector_sdk::spi::{Cursor, SourceError};

use super::state::State;
use super::tracker::{Spec, Tracker};
use crate::source::config::{Bound, Direction, NullPolicy};
use crate::source::errors::{self, Phase};
use crate::source::plan::Facts;
use crate::source::sql;
use crate::types::{Scalar, text};

/// The resolved incremental read plan for a cursor stream.
pub(crate) struct Incremental {
    pub(crate) tracker: Tracker,
    /// Whether the FINAL checkpoint keeps its boundary keys (`boundary:
    /// inclusive`); intermediate checkpoints always do.
    pub(crate) keep_final_keys: bool,
    pub(crate) where_sql: String,
    pub(crate) order_sql: String,
}

/// Resolve the incremental plan from validated facts: decode the
/// resume/config watermarks, apply the lag window, render the boundary
/// matrix, and build the dedup tracker.
pub(crate) fn prepare(
    facts: &Facts,
    since: Option<&Cursor>,
    stream: &str,
) -> Result<Option<Incremental>, SourceError> {
    let Some(cursor) = &facts.cursor else {
        return Ok(None);
    };
    let config = &cursor.config;
    let direction_max = config.direction == Direction::Max;
    let stored = match since {
        Some(since) => Some(State::decode(since, stream)?),
        None => None,
    };
    // A decimal watermark compares as (scaled, scale) pairs, which is only
    // a numeric ordering while the scale is FIXED — the documented cursor
    // assumption. If the column's scale changed between runs (ALTER TYPE,
    // a changed decimal hint), silently comparing across scales would read
    // an advancing stream as regressed and stop checkpointing; refuse
    // loudly instead.
    if let Some(state) = &stored
        && let crate::types::Kind::Decimal { scale, .. } = cursor.kind
        && let Scalar::Decimal {
            scale: stored_scale,
            ..
        } = &state.watermark
        && *stored_scale != scale
    {
        return Err(errors::fatal(
            Phase::Reflect,
            Some(stream),
            format!(
                "stored cursor watermark carries numeric scale {stored_scale} but the                  column now has scale {scale} — the cursor column's type changed; reset                  the pipeline state to take a fresh initial load"
            ),
        ));
    }
    let parse_literal = |text_value: &str| -> Result<Scalar, SourceError> {
        text::parse_scalar(cursor.kind, text_value)
            .map_err(|detail| errors::fatal(Phase::Reflect, Some(stream), detail))
    };
    // Lower bound: stored state — closed (>= + dedup) iff it carries
    // boundary keys, which every checkpoint does except an open-boundary
    // final; else the configured initial_value under the configured
    // boundary.
    let inclusive_default = config.boundary == Bound::Inclusive;
    let lower: Option<(Scalar, bool)> = match &stored {
        Some(state) => Some((state.watermark.clone(), !state.boundary_keys.is_empty())),
        None => match &config.initial_value {
            Some(text_value) => Some((parse_literal(text_value)?, inclusive_default)),
            None => None,
        },
    };
    let upper: Option<Scalar> = match &config.end_value {
        Some(text_value) => Some(parse_literal(text_value)?),
        None => None,
    };
    // Lag: widen the RESUMED window only — run 1 (no watermark) is
    // unaffected, and the saved watermark is never lowered. The window
    // re-read forces closed semantics regardless of how the stored state's
    // boundary flag landed. `sql_delta` was validated by `plan::resolve`;
    // re-deriving it here cannot produce a different answer.
    let lag_delta: Option<String> = match (&config.lag, &stored) {
        (Some(lag), Some(_)) => Some(lag.sql_delta(cursor.kind).map_err(|detail| {
            errors::fatal(
                Phase::Reflect,
                Some(stream),
                format!("cursor lag on `{}`: {detail}", config.column),
            )
        })?),
        _ => None,
    };
    let lower = match (lower, &lag_delta) {
        (Some((watermark, _)), Some(_)) => Some((watermark, true)),
        (other, _) => other,
    };
    let clauses = sql::incremental_clauses(
        &config.column,
        direction_max,
        lower
            .as_ref()
            .map(|(watermark, closed)| (watermark, *closed)),
        lag_delta.as_deref(),
        upper
            .as_ref()
            .map(|value| (value, config.end_bound == Bound::Inclusive)),
        // `error` keeps NULL rows IN the read (like include) so the tracker
        // can raise on them.
        config.nulls != NullPolicy::Exclude,
        matches!(cursor.kind, crate::types::Kind::Text),
    );
    // Row keys: primary-key columns present in the selection; otherwise
    // whole-row hashing.
    let key_columns: Option<Vec<usize>> = if facts.primary_key.is_empty() {
        None
    } else {
        facts
            .primary_key
            .iter()
            .map(|key| facts.columns.iter().position(|column| &column.name == key))
            .collect()
    };
    let tracker = Tracker::new(Spec {
        cursor_index: cursor.index,
        kind: cursor.kind,
        direction_max,
        stored,
        key_columns,
        null_error: (config.nulls == NullPolicy::Error)
            .then(|| (stream.to_owned(), config.column.clone())),
        stream: stream.to_owned(),
        cursor_column: config.column.clone(),
    });
    Ok(Some(Incremental {
        tracker,
        keep_final_keys: inclusive_default,
        where_sql: clauses.where_sql,
        order_sql: clauses.order_sql,
    }))
}
