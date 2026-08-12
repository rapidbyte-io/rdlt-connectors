//! THE compile root for the v2 REST test surface: every suite lives under
//! `cases/` as a `test_<noun>` module. Only the binary a gate selects BY
//! NAME keeps its own root: `sweep` (the crash-sweep target).

mod cases;
