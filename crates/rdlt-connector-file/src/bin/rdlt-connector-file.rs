//! `rdlt-connector-file` — the file connector as an out-of-process
//! protocol server (ADR 0001). D-039-1's discovery convention resolves
//! the connector id `io.rapidbyte.file` to THIS binary name on PATH; a
//! provider spawns it with `--role=<source|destination>`, reads the one
//! stdout handshake line, and everything after is the wire protocol.
//!
//! Behavior contract (pinned in rdlt-runtime's spawn-bins suite, not
//! clap's usage text): missing/invalid args → clap's stderr + exit 2;
//! `--version` prints the crate version; a serve error → one stderr
//! line + exit 1.

use rdlt_connector_file::destination::File as FileDestination;
use rdlt_connector_file::source::File as FileSource;

rdlt_connector_sdk::serve_main! {
    about: "rdlt file connector — a protocol server (ADR 0001)",
    roles: {
        Source => rdlt_connector_sdk::serve::source::source::<FileSource>(),
        Destination => rdlt_connector_sdk::serve::destination::destination::<FileDestination>(),
    }
}
