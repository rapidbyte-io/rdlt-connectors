//! Support for suites that spawn the REAL connector binary. One noun
//! per module: `spawn` locates (and, under the Makefile's env var,
//! builds) the bin. The read-back probe needs no module of its own —
//! the catalog is multi-writer and `common::LiveProbe` already counts
//! through the fixture's snapshot oracle.

pub(crate) mod spawn;
