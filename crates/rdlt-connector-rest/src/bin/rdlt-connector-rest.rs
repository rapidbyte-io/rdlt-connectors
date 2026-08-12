//! `rdlt-connector-rest` — the rest connector as an out-of-process
//! protocol server (ADR 0001). D-039-1's discovery convention resolves
//! the connector id `io.rapidbyte.rest` to THIS binary name on PATH; a
//! provider spawns it with `--role=source`, reads the one stdout
//! handshake line, and everything after is the wire protocol.
//!
//! SOURCE-ONLY: this crate has no destination half, so `ServeRole`
//! carries the one variant and `--role=destination` fails as an
//! unrecognized value (clap's stderr + exit 2) — an arg error before
//! any serve machinery, never a half-served role.
//!
//! Behavior contract (pinned in this crate's spawn-bins suite, not
//! clap's usage text): missing/invalid args → clap's stderr + exit 2;
//! `--version` prints the crate version; a serve error → one stderr
//! line + exit 1.

use rdlt_connector_rest::source::Rest;

rdlt_connector_sdk::serve_main! {
    about: "rdlt rest connector — a protocol server (ADR 0001)",
    roles: {
        Source => rdlt_connector_sdk::serve::source::source::<Rest>(),
    }
}
