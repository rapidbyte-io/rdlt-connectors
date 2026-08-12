//! Change data capture via logical replication, over the ordinary SQL
//! connection (no replication-protocol client): slot-first snapshot through
//! ONE repeatable-read view, bounded catch-up passes over
//! `pg_logical_slot_peek_binary_changes`, commit-boundary checkpoints, and
//! an acknowledgement that trails one run behind.
//!
//! Layout, one noun per module: `runtime` the run-scoped state + the
//! replica-identity preflight; `slot` the slot/publication lifecycle and
//! the peek/advance SQL; `pgoutput` the hand-rolled protocol-v1 parser
//! (fuzzed); `apply` the per-stream transaction-buffered state machine;
//! `read` the dispatch (snapshot pass / change pass); `ack` the
//! acknowledgement + lag surface; `tail` the continuous chunked loop.

mod ack;
mod apply;
mod pgoutput;
mod read;
mod runtime;
pub mod slot;
mod tail;

pub(crate) use pgoutput::parse as parse_pgoutput;
pub(crate) use read::{StreamContext, read_stream};
pub(crate) use runtime::Runtime;
