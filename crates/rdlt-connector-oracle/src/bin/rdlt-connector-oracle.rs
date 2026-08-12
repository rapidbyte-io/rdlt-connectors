//! `rdlt-connector-oracle` — the oracle connector as an out-of-process
//! protocol server (ADR 0001). D-039-1's discovery convention resolves
//! the connector id `io.rapidbyte.oracle` to THIS binary name on PATH;
//! a provider spawns it with `--role=source`, reads the one stdout
//! handshake line, and everything after is the wire protocol.
//!
//! SOURCE-ONLY: this crate has no destination half, so `ServeRole`
//! carries the one variant and `--role=destination` fails as an
//! unrecognized value (clap's stderr + exit 2) — an arg error before
//! any serve machinery, never a half-served role.
//!
//! THE PRE-SPAWN CLIENT PROBE, this bin's one departure from the
//! template: the `oracle` driver dlopens an Oracle Client library at
//! RUNTIME (the build needed none), and without the probe a clientless
//! machine died INSIDE the serve — after the handshake line, as an
//! opaque wire error the provider could only report as a dead
//! connector. So the probe runs between clap's arg gate and the
//! handshake: a missing client is one typed stderr line naming the
//! library and the install hint, then the serve-error exit code, with
//! stdout still EMPTY — the provider sees a clean pre-handshake
//! refusal, never a malformed handshake.
//!
//! Behavior contract (pinned in this crate's spawn-bins suite, not
//! clap's usage text): missing/invalid args → clap's stderr + exit 2;
//! `--version` prints the crate version; a missing client or a serve
//! error → one stderr line + exit 1.

use rdlt_connector_oracle::source::{Oracle, client_available};

/// The pre-handshake client probe (this bin's one insertion into the
/// shared serve-main template): BEFORE any stdout byte — the handshake
/// line has not printed, so the provider reads a refusal, not a
/// malformed handshake.
fn check_client() {
    if !client_available() {
        eprintln!(
            "rdlt-connector-oracle: no usable Oracle Client library — this connector wraps \
             ODPI-C, which dlopens libclntsh at RUNTIME (the build needed none); the library \
             is missing, broken, or its version is unsupported. Install or update Oracle \
             Instant Client and put its directory on LD_LIBRARY_PATH."
        );
        std::process::exit(1);
    }
}

rdlt_connector_sdk::serve_main! {
    about: "rdlt oracle connector — a protocol server (ADR 0001)",
    preflight: check_client(),
    roles: {
        Source => rdlt_connector_sdk::serve::source::source::<Oracle>(),
    }
}
