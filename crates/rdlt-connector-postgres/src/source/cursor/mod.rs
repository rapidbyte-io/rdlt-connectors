//! Incremental cursor machinery, one noun per file: `state` is the persisted
//! resume state (versioned wire shape, `format_version`-gated — see its
//! module docs, 037 US4) and its Arrow extraction; `tracker` dedups
//! re-fetched boundary rows and emits resume checkpoints; `prepare` turns
//! validated stream facts + stored state into the ready-to-read plan.

mod prepare;
mod state;
mod tracker;

pub(crate) use prepare::{Incremental, prepare};
