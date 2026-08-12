//! `rdlt-connector-snowflake` — the snowflake destination as an
//! out-of-process protocol server (ADR 0001; D-039-3's enabling half —
//! the in-process publish-blocker dissolves at the later swap, not
//! here). D-039-1's discovery convention resolves the connector id
//! `io.rapidbyte.snowflake` to THIS binary name on PATH.
//!
//! Destination-only on purpose: the crate has no source half, so the
//! role enum carries exactly one value — `--role=source` is refused by
//! clap as an invalid value (exit 2), the same class as a typo.
//!
//! Behavior contract (pinned in rdlt-runtime's spawn-bins suite, not
//! clap's usage text): missing/invalid args → clap's stderr + exit 2;
//! `--version` prints the crate version; a serve error → one stderr
//! line + exit 1.

use rdlt_connector_snowflake::destination::Snowflake;

rdlt_connector_sdk::serve_main! {
    about: "rdlt snowflake connector (destination) — a protocol server (ADR 0001)",
    roles: {
        Destination => rdlt_connector_sdk::serve::destination::destination::<Snowflake>(),
    }
}
