//! The case files, one suite per noun. Suites land as their subsystems do.

mod cdc_rig;
mod common;
#[cfg(feature = "spawn-bins")]
mod support;
mod test_cdc_cycle;
mod test_cdc_identity;
mod test_cdc_recovery;
mod test_cdc_slot;
#[cfg(feature = "spawn-bins")]
mod test_cdc_wire;
#[cfg(feature = "spawn-bins")]
mod test_certify_wire;
mod test_config;
mod test_config_schema;
mod test_copy_wire_pin;
mod test_destination_conformance;
mod test_destination_recovery;
mod test_differential;
mod test_direct_publish;
mod test_golden_ensure_sql;
mod test_golden_sql;
mod test_golden_unit_sql;
mod test_incremental;
#[cfg(feature = "spawn-bins")]
mod test_kill_wire;
mod test_merge_refinements;
mod test_merge_strategies;
mod test_native_types;
mod test_option_edges;
mod test_query_streams;
mod test_reflect;
mod test_scd2;
mod test_source_conformance;
#[cfg(feature = "spawn-bins")]
mod test_spawned_bin;
mod test_tls_matrix;
mod test_unit_isolation;
