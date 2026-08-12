//! The `rdlt-connector-rest` BIN, spawned for real (042): the
//! config-free `Spec` RPC answers for the SOURCE role with the
//! reverse-DNS identity, plus the bin's pinned arg behavior (exit codes
//! and the version string; clap's usage TEXT is deliberately
//! unasserted — clap owns it).
//!
//! These are the pins for THE IDENTITY RULE (039 T6) reaching rest:
//! the connector's `NAME` const IS its connector id, spelled
//! reverse-DNS (`io.rapidbyte.rest`), so the strict-equality handshake
//! verification (D-039-2) and D-039-1's last-segment binary discovery
//! (`io.rapidbyte.rest` → binary `rdlt-connector-rest` on PATH) both
//! derive from one const. SOURCE-ONLY: the crate has no destination
//! half, so `--role=destination` is an unrecognized VALUE and exits 2
//! at clap's arg gate — pinned below beside the other arg contracts.
//! No server anywhere here: `Spec` is the bin's static identity,
//! before any handshake.

use rdlt_runtime::Role;

use super::support::spawn::built_bin;

/// The source half answers the config-free `Spec` RPC through the
/// provider (spawn → handshake line → dial → Spec; the provider owns
/// the whole lifecycle including socket cleanup) and reports the
/// reverse-DNS id — the one `NAME` const, exact.
#[tokio::test]
async fn the_rest_bin_answers_the_spec_rpc_for_the_source_role() {
    rdlt_certify::assert_spec_identity(
        &built_bin(),
        Role::Source,
        "io.rapidbyte.rest",
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
    rdlt_certify::assert_bin_arg_contract(
        &built_bin(),
        &["destination"],
        env!("CARGO_PKG_VERSION"),
    );
}
