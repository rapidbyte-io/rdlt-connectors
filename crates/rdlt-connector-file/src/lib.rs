//! The file family, second generation — local and S3 files as BOTH a
//! source and a destination, born on the rdlt connector sdk.
//!
//! The source's resume integrity is the tail-hash cursor rulebook: a
//! persisted per-file progress record whose offsets are trusted only
//! after the file's tail bytes re-hash to what the cursor remembered.
//! The destination is direct-to-storage exactly-once: parts stage
//! under a hidden prefix and publish atomically behind a durable
//! receipt log, with Replace truncation owning exactly the shapes it
//! wrote.

pub mod destination;
mod format;
mod location;
pub mod source;

pub use format::Format;
pub use location::{LocationOptions, S3Options};
