//! `rdlt-connector-postgres` — the postgres connector as an
//! out-of-process protocol server (ADR 0001). D-039-1's discovery
//! convention resolves the connector id `io.rapidbyte.postgres` to THIS
//! binary name on PATH; a provider spawns it with
//! `--role=<source|destination>`, reads the one stdout handshake line,
//! and everything after is the wire protocol.
//!
//! Behavior contract (pinned in this crate's spawn-bins suite, not
//! clap's usage text): missing/invalid args → clap's stderr + exit 2;
//! `--version` prints the crate version; a serve error → one stderr
//! line + exit 1.

use rdlt_connector_postgres::destination::Postgres as PgDestination;
use rdlt_connector_postgres::source::Postgres as PgSource;

rdlt_connector_sdk::serve_main! {
    about: "rdlt postgres connector — a protocol server (ADR 0001)",
    roles: {
        Source => rdlt_connector_sdk::serve::source::run::<PgSource>(),
        Destination => rdlt_connector_sdk::serve::destination::run::<PgDestination>(),
    }
}
