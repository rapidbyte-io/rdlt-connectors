//! The crash-point registry the sweep pins and iterates — verbatim from the
//! module root; the testkit registry scanner verifies it against this
//! crate's own sources.

/// Fail-point registry: every `crash_point!` site in the source
/// read/checkpoint path. The crash sweep pins and iterates exactly this
/// list, both passes.
#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub const FAIL_POINTS: &[&str] = &[
    "pg.src.after_reflect",
    "pg.src.mid_copy",
    "pg.src.after_batch_push",
    "pg.src.before_checkpoint",
];

/// CDC fail-point registry — the CDC sweep pins and iterates exactly this
/// list, both passes.
#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub const CDC_FAIL_POINTS: &[&str] = &[
    "cdc.slot.create",
    "cdc.snapshot.copy",
    "cdc.stream.peek",
    "cdc.ack.advance",
];
