//! The one integration root: every suite is a module under `cases/`.
//! Local-filesystem cells run anywhere; RUSTFS-backed cells probe the
//! container runtime and SKIP visibly without it; the crash sweep is
//! its own failpoints-gated binary.

mod cases;
