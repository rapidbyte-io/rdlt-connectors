//! Continuous tail mode: a chunked loop of bounded catch-up passes that
//! keeps following the change feed after the initial catch-up completes.

use std::sync::Arc;

use rdlt_connector_sdk::source::Feed;
use rdlt_connector_sdk::spi::SourceError;

use crate::session::Connection;

use super::read::{PassOutcome, StreamContext, change_pass};
use super::runtime::Resume;
use super::slot;

/// A tail acks nothing beyond its first pass: acking is safe only up to
/// DESTINATION-COMMITTED positions, and no such feedback exists in-run (the
/// engine's own WAL is best-effort — its damage path re-extracts from
/// cursors, so acking pushed-but-uncommitted positions would turn "slower,
/// never wrong" into data loss). The server therefore retains WAL for the
/// tail's whole life; warn once past this span so operators cycle the tail
/// (the next run's ack reclaims retention).
const TAIL_UNACKED_WARN_BYTES: u64 = 256 << 20;

/// Continuous tail: a chunked loop of bounded catch-ups — each chunk pins
/// ITS OWN current position, checkpoints flow per chunk, and a quiet chunk
/// idles `idle_wait`. Cancellation is observed at commit boundaries: the
/// per-chunk checkpoint probe (always a commit/target position) fails the
/// moment the engine closes the channel — no new SPI surface needed. Chunks
/// never hold the run-state lock: concurrent tail streams share the control
/// connection via Arc.
pub(super) async fn tail_loop(
    control: Arc<Connection>,
    context: &StreamContext<'_>,
    stream: &str,
    mut cursor: u64,
    feed: &mut Feed,
) -> Result<(), SourceError> {
    let confirmed = slot::confirmed_flush_lsn(&control, &context.cdc.slot).await?;
    let mut retention_warned = false;
    loop {
        if feed
            .checkpoint(Resume { cdc_lsn: cursor }.encode())
            .await
            .is_break()
        {
            return Ok(()); // cancellation, at a commit boundary
        }
        let target = slot::current_wal_lsn(&control).await?;
        let quiet = if target > cursor {
            let outcome = change_pass(&control, context, stream, cursor, target, feed).await?;
            if outcome == PassOutcome::Cancelled {
                return Ok(());
            }
            cursor = target;
            false
        } else {
            true
        };
        let unacked = cursor.saturating_sub(confirmed);
        if !retention_warned && unacked > TAIL_UNACKED_WARN_BYTES {
            retention_warned = true;
            tracing::warn!(
                target: "rdlt::cdc",
                unacked_bytes = unacked,
                "tail mode: the slot's acknowledged position is fixed for the \
                 life of this run and the server retains WAL behind it — cycle \
                 the tail (restart the run) so the next run's ack reclaims \
                 retention"
            );
        }
        if quiet {
            tokio::time::sleep(std::time::Duration::from_secs(
                context.cdc.idle_wait.seconds,
            ))
            .await;
        }
    }
}
