//! Session-side test access: the parse gate and the raw connect path, for
//! the TLS matrix and connection-string suites that must exercise them
//! without a full pipeline.

pub use crate::session::{
    ConnectError, Connection, EstablishError, Parsed, Profile, TlsFailure, connect, establish,
    parse,
};
