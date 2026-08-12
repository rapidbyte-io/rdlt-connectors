//! Support for suites that spawn the REAL connector binary. One noun
//! per module: `spawn` locates (and, under the Makefile's env var,
//! builds) the bin; `probe` counts a live database file's committed
//! rows from OUTSIDE the connector's process.

pub(crate) mod probe;
pub(crate) mod spawn;
