//! The read layer: one submodule per format, plus the shared entry
//! checks under their own noun.

pub(crate) mod checks;
pub(crate) mod csv;
pub(crate) mod jsonl;
pub(crate) mod parquet;

pub(crate) use checks::{open_decoded, verify_local_snapshot};
