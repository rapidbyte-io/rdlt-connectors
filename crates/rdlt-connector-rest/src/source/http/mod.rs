//! HTTP execution: request build, credential attachment, classification,
//! pacing, bounded Retry-After waits. The source classifies errors and the
//! ENGINE retries; any in-source wait is bounded by construction, never a
//! free retry loop.

pub(crate) mod classify;
mod client;
mod credentials;

pub use client::Client;
pub use credentials::Credentials;
