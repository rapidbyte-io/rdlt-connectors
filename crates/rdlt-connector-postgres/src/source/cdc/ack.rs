//! The acknowledgement + lag surface: advance the slot's confirmed position
//! to the destination-committed floor, and report replication lag on the
//! dedicated `rdlt::cdc` tracing target. Called at run completion by the
//! dispatch, and kept alive by the tail loop — both paths, one module.

use rdlt_connector_sdk::spi::SourceError;
use rdlt_connector_sdk::spi::core::crash_point;

use crate::session::Connection;
use crate::source::config::{AckMode, CdcConfig};

use super::slot;

/// Run-completion housekeeping once every CDC stream has drained its pass:
/// advance the slot's acknowledged position to the min
/// destination-committed floor (Auto ack only), then emit the
/// replication-lag report.
// `stream` feeds only the crash-point error, which compiles to nothing
// without the failpoints feature.
#[cfg_attr(not(feature = "failpoints"), allow(unused_variables))]
pub(super) async fn acknowledge_and_report(
    control: &Connection,
    cdc: &CdcConfig,
    ack_floor: Option<u64>,
    committed_floor: Option<u64>,
    stream: &str,
) -> Result<(), SourceError> {
    if cdc.ack == AckMode::Auto
        && let Some(floor) = ack_floor
    {
        crash_point!(
            "cdc.ack.advance",
            Err(crate::source::errors::fatal(
                crate::source::errors::Phase::Slot,
                Some(stream),
                "injected: before slot advance"
            ))
        );
        let confirmed = slot::confirmed_flush_lsn(control, &cdc.slot).await?;
        if floor > confirmed {
            slot::advance(control, &cdc.slot, floor).await?;
        }
    }
    // Replication lag: how far the live feed is ahead of the least-advanced
    // stream at run completion — LSN delta in bytes on the dedicated
    // `rdlt::cdc` target (embedders subscribe, no log-scraping), plus a
    // wall-clock delta when the server tracks commit timestamps.
    let head = slot::current_wal_lsn(control).await?;
    if let Some(committed) = committed_floor {
        let lag_bytes = head.saturating_sub(committed);
        match commit_time_lag_seconds(control).await {
            Some(lag_seconds) => tracing::info!(
                target: "rdlt::cdc",
                lag_bytes,
                lag_seconds,
                "replication lag at run completion"
            ),
            None => tracing::info!(
                target: "rdlt::cdc",
                lag_bytes,
                "replication lag at run completion"
            ),
        }
    }
    Ok(())
}

/// Wall-clock replication lag, only when the server exposes it
/// (`track_commit_timestamp = on`); `None` — never a guess — otherwise.
async fn commit_time_lag_seconds(connection: &Connection) -> Option<f64> {
    let tracked: String = connection
        .query_one("SHOW track_commit_timestamp", &[])
        .await
        .ok()?
        .get(0);
    if tracked != "on" {
        return None;
    }
    connection
        .query_one(
            "SELECT extract(epoch FROM clock_timestamp() - \
             (pg_last_committed_xact()).timestamp)::float8",
            &[],
        )
        .await
        .ok()?
        .get::<_, Option<f64>>(0)
}
