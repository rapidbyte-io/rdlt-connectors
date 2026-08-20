//! The shared binary-COPY read loop, common to the plain snapshot read and
//! the CDC snapshot pass. Both open a server-side `COPY … TO STDOUT BINARY`,
//! feed every socket chunk through a [`Decoder`], and push each completed
//! batch downstream with identical error mapping (connection loss Transient,
//! decode faults Fatal) and one mid-stream crash point.
//!
//! The per-batch work differs (the CDC snapshot flag-wraps and pushes; the
//! plain read runs the incremental tracker, pushes, and checkpoints), so it
//! is a caller-supplied SYNC `transform` that returns the ordered [`Push`]
//! actions the loop then awaits. Keeping the transform synchronous — rather
//! than an async closure that borrows across `.await` — is what lets the
//! loop stay `Send` in the async-trait `read()` path.

use arrow_array::RecordBatch;
use futures::TryStreamExt;
use rdlt_connector_sdk::source::Feed;
use rdlt_connector_sdk::spi::core::crash_point;
use rdlt_connector_sdk::spi::{core::cursor::Cursor, error::SourceError};
use tokio_postgres::Client;

use crate::source::errors::{self, Phase};
use crate::types::binary::Decoder;

/// The mid-stream crash site a caller owns: the failpoint name plus the
/// injected transient detail. `pg.src.mid_copy` for the plain read,
/// `cdc.snapshot.copy` for CDC.
// The fields are read only by `crash_point!`, which compiles to nothing
// without the failpoints feature.
#[cfg_attr(not(feature = "failpoints"), allow(dead_code))]
pub(crate) struct CrashSite {
    pub(crate) label: &'static str,
    pub(crate) detail: &'static str,
}

/// One action the loop performs for a decoded batch, in order. The transform
/// returns these; the loop awaits the pushes and fires the crash points.
pub(crate) enum Push {
    /// Hand an Arrow batch downstream (engine cancellation stops the loop).
    Arrow(RecordBatch),
    /// Emit a resume checkpoint.
    Checkpoint(Cursor),
    /// Fire a named post-batch crash point (failpoints only) — the plain
    /// read's `pg.src.after_batch_push`, which lands BETWEEN a batch push
    /// and its checkpoint. The second field is the injected transient
    /// detail.
    Crash(&'static str, &'static str),
}

/// Run `copy_sql` on `client`, decode through `decoder`, and for each
/// completed batch (including the finish tail) call `transform` and perform
/// the [`Push`] actions it returns. Returns `true` when the whole stream was
/// consumed, `false` at the first engine cancellation.
// `crash` feeds a `crash_point!` that compiles to nothing without the
// failpoints feature, leaving it unused in normal builds.
#[cfg_attr(not(feature = "failpoints"), allow(unused_variables))]
pub(crate) async fn stream<F>(
    client: &Client,
    copy_sql: &str,
    decoder: &mut Decoder,
    feed: &mut Feed,
    stream_name: &str,
    crash: CrashSite,
    mut transform: F,
) -> Result<bool, SourceError>
where
    F: FnMut(RecordBatch) -> Result<Vec<Push>, SourceError>,
{
    let copy_stream = client
        .copy_out(copy_sql)
        .await
        .map_err(|e| errors::classify(Phase::Copy, Some(stream_name), &e))?;
    futures::pin_mut!(copy_stream);
    loop {
        let chunk = copy_stream
            .try_next()
            .await
            .map_err(|e| errors::classify(Phase::Copy, Some(stream_name), &e))?;
        let Some(chunk) = chunk else { break };
        // Simulated mid-stream connection loss: Transient — the ENGINE
        // retries the whole read from committed state.
        crash_point!(
            crash.label,
            Err(errors::transient(
                Phase::Copy,
                Some(stream_name),
                crash.detail
            ))
        );
        let batches = decoder
            .feed(&chunk)
            .map_err(|e| errors::fatal(Phase::Decode, Some(stream_name), e))?;
        for batch in batches {
            if !perform(transform(batch)?, feed).await? {
                return Ok(false);
            }
        }
    }
    if let Some(tail) = decoder
        .finish()
        .map_err(|e| errors::fatal(Phase::Decode, Some(stream_name), e))?
        && !perform(transform(tail)?, feed).await?
    {
        return Ok(false);
    }
    Ok(true)
}

/// Await one batch's push actions in order. `false` = engine cancellation.
#[cfg_attr(not(feature = "failpoints"), allow(unused_variables))]
async fn perform(pushes: Vec<Push>, feed: &mut Feed) -> Result<bool, SourceError> {
    for push in pushes {
        match push {
            Push::Arrow(batch) => {
                if feed.arrow(batch).await.is_break() {
                    return Ok(false);
                }
            }
            Push::Checkpoint(cursor) => {
                if feed.checkpoint(cursor).await.is_break() {
                    return Ok(false);
                }
            }
            Push::Crash(label, detail) => {
                // Post-batch injection (plain read only): Transient,
                // tableless.
                crash_point!(label, Err(errors::transient(Phase::Copy, None, detail)));
            }
        }
    }
    Ok(true)
}
