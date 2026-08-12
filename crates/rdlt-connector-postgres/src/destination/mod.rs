//! The Postgres DESTINATION: binary-protocol COPY, one transaction per
//! commit unit, sqlcore-planned publishes, exactly-once receipts. Receives
//! FLATTENED schemas — `structs: false` makes the engine lower nested
//! objects at the seam. Depends on the SPI, sqlcore, and this crate's
//! substrate only.
//!
//! Layout, one owned state per module: `connector` the framework face +
//! `connect`; `config` the handle/builder + document vocabulary; `catalog`
//! what has been ensured and the DDL that ensured it; `unit` the
//! commit-unit transaction; `write` the COPY wire loop; `executor`
//! planned-step execution; `load` the sdk-`Backend` coordinator over all
//! of them; `dialect` the SQL-text seam; `errors` the phase-tagged
//! taxonomy. The SPI face is [`Shell`], the sdk's session choreography
//! over [`Load`].
//!
//! # What a unit transaction costs
//!
//! Append and Replace rows COPY straight into their target inside one
//! transaction per commit unit, instead of landing in a stage table and
//! being moved by `INSERT … SELECT` at publish. Every row is written once
//! rather than twice. Merge is unchanged: its arms join delivered rows
//! against the target, so it genuinely needs the stage.
//!
//! That trade has four consequences worth knowing before running rdlt
//! against a busy database. None of them blocks a load; all of them are
//! about what ELSE the database can do while one is running.
//!
//! - **A Replace target is locked for the whole load, not just the
//!   publish.** `TRUNCATE` takes ACCESS EXCLUSIVE and holds it until the
//!   unit commits. Readers of that table block for that whole window.
//! - **Vacuum falls behind while a unit is open.** The transaction pins the
//!   oldest row version the whole DATABASE may reclaim — not just this
//!   table's.
//! - **A stalled load holds both at once.** Commit cadence is the control:
//!   more frequent commit units mean shorter transactions and shorter
//!   locks.
//! - **Constraint violations surface at `write`, not at publish.** The
//!   server enforces the target's constraints during the COPY, so a bad row
//!   fails at the batch that carried it and names the row.

pub(crate) mod catalog;
pub mod config;
mod connector;
pub(crate) mod dialect;
mod errors;
mod executor;
#[cfg(feature = "failpoints")]
mod fail_points;
mod load;
pub(crate) mod unit;
mod write;

pub use config::{
    AbsentPolicy, Config, ConfigError, DedupSort, DestinationOptions, MergeStrategy, Postgres,
    Scd2Options, SortOrder, TableOptions, config_schema,
};
pub use load::Load;

/// The SPI face: the sdk's shell over [`Postgres`]. `Shell::from_yaml`
/// (or `from_json`/`from_value`/`new`) is a running destination in one
/// call; a builder-constructed handle wraps via
/// `rdlt_connector_sdk::destination::shell`.
pub type Shell = rdlt_connector_sdk::destination::Shell<Postgres>;

#[cfg(feature = "failpoints")]
pub use fail_points::FAIL_POINTS;
