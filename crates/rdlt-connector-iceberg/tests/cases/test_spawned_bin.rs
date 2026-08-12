//! The `rdlt-connector-iceberg` BIN, spawned for real (042): the
//! config-free `Spec` RPC answers with the reverse-DNS identity, plus
//! the bin's pinned arg behavior (exit codes and the version string;
//! clap's usage TEXT is deliberately unasserted — clap owns it).
//!
//! These are the pins for THE IDENTITY RULE (039 T6) reaching iceberg:
//! the connector's `NAME` const IS its connector id, spelled
//! reverse-DNS (`io.rapidbyte.iceberg`), so the strict-equality
//! handshake verification (D-039-2) and D-039-1's last-segment binary
//! discovery (`io.rapidbyte.iceberg` → binary `rdlt-connector-iceberg`
//! on PATH) both derive from one const. This crate is
//! DESTINATION-ONLY, so `--role=source` is an ARG error (clap's exit
//! 2), pinned beside the nonsense role. All three cells are offline —
//! `Spec` answers before any catalog is dialed, so no fixture and no
//! skip surface.

use rdlt_runtime::Role;

use super::support::spawn::built_bin;

/// The destination half answers the config-free `Spec` RPC through the
/// provider (spawn → handshake line → dial → Spec; the provider owns
/// the whole lifecycle including socket cleanup) and reports the
/// reverse-DNS id — the one `NAME` const, exact.
#[tokio::test]
async fn the_iceberg_bin_answers_the_spec_rpc() {
    rdlt_certify::assert_spec_identity(
        &built_bin(),
        Role::Destination,
        "io.rapidbyte.iceberg",
        env!("CARGO_PKG_VERSION"),
    )
    .await;
}

/// The pinned arg contract, through the shared helper
/// ([`rdlt_certify::assert_bin_arg_contract`]): no args and a bogus
/// role are clap's exit 2, each unserved role is refused at the arg
/// gate, and `--version`/`--help` behave with the crate version in the
/// output.
#[test]
fn the_arg_contract_holds() {
    rdlt_certify::assert_bin_arg_contract(&built_bin(), &["source"], env!("CARGO_PKG_VERSION"));
}
