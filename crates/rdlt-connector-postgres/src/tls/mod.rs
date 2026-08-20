//! Connection security for both Postgres connectors: the policy vocabulary,
//! its resolution rules, and the rustls machinery that enforces it.
//!
//! One policy type, consumed by one connect path (`session`), so the source
//! and the destination cannot drift on what a TLS posture means. Modes carry
//! libpq semantics: `require` encrypts WITHOUT validating the certificate
//! (the ecosystem's long-standing meaning — documented loudly on the
//! variant), `verify_full` is the production recommendation.
//!
//! `policy` holds the config-shaped vocabulary and the agree-or-error
//! resolution rules; `client_config` turns a resolved [`Policy`] into a
//! rustls `ClientConfig` before any connection exists; `verify` quarantines
//! the deliberately-weaker certificate verifiers the libpq levels demand.
//!
//! [`Policy`], [`Mode`], [`ConfigError`] and [`Material`] are embedder API —
//! the YAML `tls:` block and destination builders speak them. Everything
//! else is crate-internal.

mod client_config;
mod policy;
mod verify;

pub use policy::{ConfigError, Mode, Policy};
pub use rdlt_connector_sdk::pem::Material;

pub(crate) use client_config::build;
pub(crate) use policy::{resolve, validate_credentials};
