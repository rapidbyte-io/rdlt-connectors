//! One home for "a connected Postgres session", shared by the source, the
//! destination, and CDC — so the three cannot drift on how a connection is
//! made, prepared, or judged when it fails.
//!
//! The module owns the whole path from a user's connection string to a live
//! prepared client: `connection_string` parses both libpq spellings and
//! extracts the TLS parameters, `establish` resolves the policy, performs
//! the handshake, and applies a session [`Profile`]; `classify` renders
//! driver failures with their full meaning and decides retry-worthiness in
//! exactly one place. Callers hold a [`Connection`] and never see the
//! parse → resolve → handshake → pin sequence.

mod classify;
mod connection_string;
mod establish;

pub(crate) use classify::describe;
#[cfg(any(feature = "destination", test))]
pub(crate) use classify::is_permanent_at_statement;
#[cfg(any(feature = "source", test))]
pub(crate) use classify::is_transient_at_connect;
pub use classify::{ConnectError, TlsFailure};
// `pub` items inside this `pub(crate)` module are NOT dead visibility:
// they feed the doc-hidden `testsupport::session` re-export chain, which is
// the one public path the integration suites reach them through.
pub use connection_string::{Parsed, parse};
pub use establish::{Connection, EstablishError, Profile, connect, establish};
