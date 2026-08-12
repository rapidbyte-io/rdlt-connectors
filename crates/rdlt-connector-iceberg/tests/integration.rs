//! The one integration root: every suite is a module under `cases/`.
//! Container-backed cells probe the runtime and SKIP visibly without
//! it; the crash sweep is its own failpoints-gated binary.

mod cases;
