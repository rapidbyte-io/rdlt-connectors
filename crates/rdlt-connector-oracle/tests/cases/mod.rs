pub mod common;
#[cfg(feature = "spawn-bins")]
mod support;
#[cfg(feature = "spawn-bins")]
mod test_certify_wire;
mod test_document;
mod test_gating;
#[cfg(feature = "spawn-bins")]
mod test_kill_wire;
mod test_live;
#[cfg(feature = "spawn-bins")]
mod test_spawned_bin;
