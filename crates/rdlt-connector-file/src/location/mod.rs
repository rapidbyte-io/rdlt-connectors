//! Where files live: the Local | S3 vocabulary and the shared IO
//! primitives both halves speak.

mod kind;
mod options;
mod s3;
pub(crate) mod store;

pub(crate) use kind::classify_read_error;
pub(crate) use kind::{ByteReader, Location};
// `DocVersion` is named here (037 US2 review round 1, C2): `lease.rs`'s
// `takeover` helper takes one as a plain function PARAMETER
// (`version: DocVersion`), and a parameter's type must be spelled —
// unlike the earlier callers, which only ever destructured or
// threaded a version through without ever writing the type name. An
// unused `pub(crate) use` fails this crate's own lint gate (see
// kind.rs's `is_stale_version` doc for the same rule) — so this stays
// re-exported only because there is now a real caller.
pub(crate) use kind::{CreateDoc, DocVersion, is_stale_version};
pub use options::{LocationOptions, S3Options};
