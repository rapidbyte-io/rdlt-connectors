//! The CDC read dispatch: the snapshot pass (first run, no cursor) and the
//! bounded change-catch-up pass, phase by phase, with the run-state lock
//! held only where run state is actually touched.

use futures::TryStreamExt;

use rdlt_connector_sdk::source::Feed;
use rdlt_connector_sdk::spi::core::crash_point;
use rdlt_connector_sdk::spi::{core::cursor::Cursor, error::SourceError, source::StreamSpec};

use crate::session::Connection;
use crate::source::config::{CdcConfig, CdcMode, Config};
use crate::source::errors::{self, Phase};
use crate::source::{copy, sql};
use crate::types::Column;

use super::apply::{Apply, Emit, batch_of};
use super::runtime::{Identity, Resume, Runtime};
use super::{ack, pgoutput, slot, tail};

/// The per-stream read context: everything fixed for the whole of one
/// stream's read — the source config, the CDC block, the resolved replica
/// identity, and the decode columns. Built by the connector and threaded
/// through the pass/apply signatures so they stay small.
pub(crate) struct StreamContext<'a> {
    pub(crate) config: &'a Config,
    pub(crate) cdc: &'a CdcConfig,
    pub(crate) identity: &'a Identity,
    pub(crate) columns: &'a [Column],
}

/// The CDC read dispatch: snapshot pass (no cursor) or change pass. Any
/// error drops the run's cached connections — the engine's TRANSIENT
/// in-run retries re-enter with this same [`Runtime`], and a dead snapshot
/// or control connection must never be reused across attempts.
pub(crate) async fn read_stream(
    runtime: &Runtime,
    context: &StreamContext<'_>,
    cdc_tables: &[String],
    reflected_columns: &[&crate::source::reflect::Column],
    stream: &StreamSpec,
    since: Option<Cursor>,
    feed: &mut Feed,
) -> Result<(), SourceError> {
    let result = dispatch(
        runtime,
        context,
        cdc_tables,
        reflected_columns,
        stream,
        since,
        feed,
    )
    .await;
    if result.is_err() {
        runtime.state.lock().await.drop_connections();
    }
    result
}

async fn dispatch(
    runtime: &Runtime,
    context: &StreamContext<'_>,
    cdc_tables: &[String],
    reflected_columns: &[&crate::source::reflect::Column],
    stream: &StreamSpec,
    since: Option<Cursor>,
    feed: &mut Feed,
) -> Result<(), SourceError> {
    let &StreamContext { config, cdc, .. } = context;
    let stream = stream.name.as_str().to_owned();

    // ---- lifecycle, under the state lock ----
    let mut state = runtime.state.lock().await;
    if state.pending.is_none() {
        state.pending = Some(cdc_tables.iter().cloned().collect());
    }
    let control = state.control(config, &stream).await?;
    if state.ensured.is_none() {
        crash_point!(
            "cdc.slot.create",
            Err(errors::fatal(
                Phase::Slot,
                Some(&stream),
                "injected: before slot ensure"
            ))
        );
        let outcome = slot::ensure(&control, cdc, &config.schema, cdc_tables).await?;
        state.ensured = Some(outcome);
    }
    let ensured = state.ensured.expect("ensured just initialized");

    let since = match &since {
        Some(cursor) => Some(Resume::decode(cursor, &stream)?.cdc_lsn),
        None => None,
    };
    // A slot created THIS run starts at its consistent point — it cannot
    // cover a resuming stream's history. Peeking would silently skip every
    // change in (since, consistent_point): typed error, never a gap.
    if let Some(since) = since
        && ensured.created_slot
        && let Some(point) = ensured.consistent_point
        && since < point
    {
        return Err(errors::fatal(
            Phase::Slot,
            Some(&stream),
            format!(
                "replication slot `{}` was created THIS run at {} but this \
                 stream resumes from {} — the feed cannot cover that gap; \
                 reset the pipeline state so the stream takes a fresh \
                 snapshot instead of resuming past a recreated slot",
                cdc.slot,
                slot::render_lsn(point),
                slot::render_lsn(since)
            ),
        ));
    }

    let cursor = match since {
        None => {
            // ---- snapshot pass ----
            // Cursor start: the consistent point when THIS run created the
            // slot; otherwise the shared snapshot's visibility horizon —
            // the WAL position read BEFORE its transaction began. Either
            // way: no gap, and the overlap window applies twice and
            // converges.
            let start = match ensured.consistent_point {
                Some(point) => point,
                None => match state.snapshot_horizon {
                    Some(horizon) => horizon,
                    None => slot::current_wal_lsn(&control).await?,
                },
            };
            let snapshot = state.snapshot(config, &stream, start).await?;
            drop(state); // COPY + pushes run WITHOUT the state lock
            if snapshot_pass(&snapshot, context, &stream, reflected_columns, feed).await?
                == PassOutcome::Cancelled
            {
                return Ok(());
            }
            if feed
                .checkpoint(Resume { cdc_lsn: start }.encode())
                .await
                .is_break()
            {
                return Ok(());
            }
            state = runtime.state.lock().await;
            state.ack_floor.insert(stream.clone(), start);
            start
        }
        Some(since) => {
            // ---- change pass ----
            let target = match state.target {
                Some(target) => target,
                None => {
                    let target = slot::current_wal_lsn(&control).await?;
                    state.target = Some(target);
                    target
                }
            };
            state.ack_floor.insert(stream.clone(), since);
            drop(state); // the pass pushes batches WITHOUT the state lock
            if target > since {
                let outcome = change_pass(&control, context, &stream, since, target, feed).await?;
                if outcome == PassOutcome::Cancelled {
                    return Ok(());
                }
            }
            state = runtime.state.lock().await;
            target.max(since)
        }
    };

    // ---- run completion + ack ----
    state.final_cursor.insert(stream.clone(), cursor);
    let drained = {
        let pending = state.pending.as_mut().expect("pending initialized");
        pending.remove(&stream);
        pending.is_empty()
    };
    if drained {
        state.close_snapshot();
        let ack_floor = state.ack_floor.values().min().copied();
        let committed_floor = state.final_cursor.values().min().copied();
        ack::acknowledge_and_report(&control, cdc, ack_floor, committed_floor, &stream).await?;
    }
    if cdc.mode == CdcMode::Tail {
        drop(state);
        return tail::tail_loop(control, context, &stream, cursor, feed).await;
    }
    Ok(())
}

#[derive(PartialEq)]
pub(super) enum PassOutcome {
    Complete,
    Cancelled,
}

/// The snapshot pass: COPY the whole table through the shared
/// repeatable-read view, flag-wrapping each batch (rows are upserts, flag
/// NULL). `cdc.snapshot.copy` is the loop's mid-stream crash site.
async fn snapshot_pass(
    snapshot: &Connection,
    context: &StreamContext<'_>,
    stream: &str,
    reflected_columns: &[&crate::source::reflect::Column],
    feed: &mut Feed,
) -> Result<PassOutcome, SourceError> {
    let select = sql::select(&context.config.schema, stream, reflected_columns, "", "");
    let copy_sql = sql::copy(&select);
    let mut decoder = crate::types::binary::Decoder::new(
        context.columns.to_vec(),
        context.config.batch_target_bytes,
        context.config.batch_max_rows,
    )
    .map_err(|e| errors::fatal(Phase::Decode, Some(stream), e))?;
    let mut pushed_any = false;
    let flag_column = context.cdc.flag_column.clone();
    let completed = copy::stream(
        snapshot,
        &copy_sql,
        &mut decoder,
        feed,
        stream,
        copy::CrashSite {
            label: "cdc.snapshot.copy",
            detail: "injected: connection lost mid-snapshot",
        },
        |batch| {
            pushed_any = true;
            Ok(vec![copy::Push::Arrow(with_null_flag(batch, &flag_column))])
        },
    )
    .await?;
    if !completed {
        return Ok(PassOutcome::Cancelled);
    }
    if !pushed_any {
        // Schema-bearing empty batch: columns + flag, all nullable.
        let empty = batch_of(context.columns, &context.cdc.flag_column, &[], &[])
            .map_err(|e| errors::fatal(Phase::Decode, Some(stream), e))?;
        if feed.arrow(empty).await.is_break() {
            return Ok(PassOutcome::Cancelled);
        }
    }
    Ok(PassOutcome::Complete)
}

/// One bounded catch-up pass for one stream: peek `(since, target]` as a
/// server-side row stream, decode pgoutput, keep this table's changes,
/// batch, checkpoint at commit positions only.
pub(super) async fn change_pass(
    control: &Connection,
    context: &StreamContext<'_>,
    stream: &str,
    since: u64,
    target: u64,
    feed: &mut Feed,
) -> Result<PassOutcome, SourceError> {
    crash_point!(
        "cdc.stream.peek",
        Err(errors::transient(
            Phase::Slot,
            Some(stream),
            "injected: peek connection lost"
        ))
    );
    // ONE canonical peek: `slot::peek` owns the SQL, the parameter binding,
    // and the LSN parsing (it classifies its errors slot-scoped, so they
    // carry no table name — a peek reads every table's changes and filters
    // its own). The stream is consumed row-by-row, decoding each change as
    // it lands rather than buffering the whole range.
    let changes = slot::peek(control, context.cdc, target).await?;
    futures::pin_mut!(changes);

    let mut apply = Apply::new(context, stream, since)?;
    while let Some(change) = changes.try_next().await? {
        let message = pgoutput::parse(&change.data)
            .map_err(|e| errors::fatal(Phase::Decode, Some(stream), e))?;
        if !deliver(apply.on_message(change.lsn, message)?, feed).await? {
            return Ok(PassOutcome::Cancelled);
        }
    }
    if !deliver(apply.finish(target)?, feed).await? {
        return Ok(PassOutcome::Cancelled);
    }
    Ok(PassOutcome::Complete)
}

/// Deliver one apply step's emitted actions in order. Returns `false` at
/// the first engine cancellation (a closed batch or checkpoint channel) so
/// the caller can stop the pass cleanly.
async fn deliver(emits: Vec<Emit>, feed: &mut Feed) -> Result<bool, SourceError> {
    for action in emits {
        let delivered = match action {
            Emit::Batch(batch) => feed.arrow(batch).await.is_continue(),
            Emit::Checkpoint(lsn) => feed
                .checkpoint(Resume { cdc_lsn: lsn }.encode())
                .await
                .is_continue(),
        };
        if !delivered {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Snapshot batches ride the binary COPY decoder; give them the SAME shape
/// as change batches: every field nullable + the trailing flag column
/// (NULL — snapshot rows are upserts).
fn with_null_flag(batch: arrow_array::RecordBatch, flag_column: &str) -> arrow_array::RecordBatch {
    use arrow_schema::{DataType, Field, Schema};
    let mut fields: Vec<Field> = batch
        .schema()
        .fields()
        .iter()
        .map(|field| field.as_ref().clone().with_nullable(true))
        .collect();
    fields.push(Field::new(flag_column, DataType::Boolean, true));
    let mut arrays = batch.columns().to_vec();
    arrays.push(std::sync::Arc::new(arrow_array::BooleanArray::from(vec![
        None::<bool>;
        batch.num_rows()
    ])));
    arrow_array::RecordBatch::try_new(std::sync::Arc::new(Schema::new(fields)), arrays)
        .expect("flag append preserves shape")
}
