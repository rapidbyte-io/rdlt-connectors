//! The `rdlt-connector-postgres` BIN, spawned for real (041): the
//! config-free `Spec` RPC answers for BOTH roles with the reverse-DNS
//! identity, plus the bin's pinned arg behavior (exit codes and the
//! version string; clap's usage TEXT is deliberately unasserted — clap
//! owns it).
//!
//! These are the pins for THE IDENTITY RULE (039 T6) reaching postgres:
//! the connector's `NAME` const IS its connector id, spelled reverse-DNS
//! (`io.rapidbyte.postgres`), so the strict-equality handshake
//! verification (D-039-2) and D-039-1's last-segment binary discovery
//! (`io.rapidbyte.postgres` → binary `rdlt-connector-postgres` on PATH)
//! both derive from one const. No container and no credentials anywhere
//! here: `Spec` is the bin's static identity, before any handshake.

use rdlt_runtime::provider::Role;

use super::support::spawn::built_bin;

/// Both halves answer the config-free `Spec` RPC through the provider
/// (spawn → handshake line → dial → Spec; the provider owns the whole
/// lifecycle including socket cleanup) and each reports the reverse-DNS
/// id — the one `NAME` const, exact.
#[tokio::test]
async fn the_postgres_bin_answers_the_spec_rpc_for_both_roles() {
    for role in [Role::Source, Role::Destination] {
        rdlt_certify::contract::assert_spec_identity(
            &built_bin(),
            role,
            "io.rapidbyte.postgres",
            env!("CARGO_PKG_VERSION"),
        )
        .await;
    }
}

/// The pinned arg contract, through the shared helper
/// ([`rdlt_certify::contract::assert_bin_arg_contract`]): no args and a bogus
/// role are clap's exit 2, each unserved role is refused at the arg
/// gate, and `--version`/`--help` behave with the crate version in the
/// output.
#[test]
fn the_arg_contract_holds() {
    rdlt_certify::contract::assert_bin_arg_contract(&built_bin(), &[], env!("CARGO_PKG_VERSION"));
}
