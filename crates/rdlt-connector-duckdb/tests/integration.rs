//! The one integration root: every suite is a module under `cases/`.
//! The differential oracle self-skips without a container runtime; the
//! crash sweep is its own failpoints-gated binary.

mod cases;
