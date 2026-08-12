//! The destination half: files as an exactly-once target.
//!
//! Staging lives under `.rdlt-staging/{scope}/{load}/`, receipts
//! forever in `_rdlt_commits.{scope}.json`, state beside them — the
//! 4-phase commit is the `load` module's doc comment.

mod config;
mod connector;
mod inspect;
mod layout;
// 037 US2 T7: `Lease` is wired into `Load` (acquire on open, before
// `prepare_staging`; `check_still_held` at the top of `write` and
// `publish`; `release` at session `close`) — see `load.rs`. `close`
// itself runs in two modes (the engine propagates its result on a
// successful run, discards it best-effort on abandonment, per the
// final-review wave's item 3); `release` is internally best-effort on
// EITHER mode — it is never the source of a close failure.
mod lease;
mod load;
mod stage;
mod truncate;

pub use config::{Config, ConfigError, DestFormat, config_schema};
#[doc(hidden)]
pub use connector::testhook;
pub use connector::{FAIL_POINTS, File, LEASE_FAIL_POINTS, S3_FAIL_POINTS};
pub(crate) use layout::STAGING_DIR;

/// The sdk shell around [`File`] — the destination's public form.
pub type Shell = rdlt_connector_sdk::destination::Shell<File>;

/// The CANONICAL local-parquet spelling — supported by name, not a
/// deprecated alias (bench, CLI, and crash-sweep tooling consume it).
pub type ParquetDir = Shell;
