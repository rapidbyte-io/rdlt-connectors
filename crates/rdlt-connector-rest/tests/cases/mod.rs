//! Table of contents for the suites; shared plumbing lives in `common`.

mod common;
#[cfg(feature = "spawn-bins")]
mod support;
mod test_actions;
mod test_auth;
#[cfg(feature = "spawn-bins")]
mod test_certify_wire;
mod test_children;
mod test_config_schema;
mod test_conformance;
#[cfg(feature = "spawn-bins")]
mod test_kill_wire;
mod test_pagination;
mod test_pokeapi_live;
mod test_robustness;
#[cfg(feature = "spawn-bins")]
mod test_spawned_bin;
