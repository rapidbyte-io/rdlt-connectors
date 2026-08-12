pub mod common;
#[cfg(feature = "spawn-bins")]
mod support;
#[cfg(feature = "spawn-bins")]
mod test_certify_wire;
mod test_concurrency;
mod test_conformance;
mod test_document;
mod test_evolution;
mod test_exactly_once;
mod test_gating;
mod test_ingestion;
mod test_interop;
#[cfg(feature = "spawn-bins")]
mod test_kill_wire;
mod test_partitioning;
mod test_parts;
mod test_providers;
mod test_quickstart;
#[cfg(feature = "spawn-bins")]
mod test_spawned_bin;
