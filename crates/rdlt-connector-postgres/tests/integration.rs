//! THE compile root for the v2 postgres test surface: every suite lives
//! under `cases/` as a `test_<noun>` module. Only binaries a gate selects BY
//! NAME keep their own roots: `source_crash_sweep`,
//! `destination_crash_sweep`, `cdc_crash_sweep` (the sweep targets) and
//! `memory_bound` (the heavy target).

mod cases;
