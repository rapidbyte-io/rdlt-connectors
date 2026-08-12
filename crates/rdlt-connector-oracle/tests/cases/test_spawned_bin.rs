//! The `rdlt-connector-oracle` BIN, spawned for real (042): the
//! config-free `Spec` RPC answers for the SOURCE role with the
//! reverse-DNS identity, plus the bin's pinned arg behavior (exit
//! codes and the version string; clap's usage TEXT is deliberately
//! unasserted — clap owns it).
//!
//! These are the pins for THE IDENTITY RULE (039 T6) reaching oracle:
//! the connector's `NAME` const IS its connector id, spelled
//! reverse-DNS (`io.rapidbyte.oracle`), so the strict-equality
//! handshake verification (D-039-2) and D-039-1's last-segment binary
//! discovery (`io.rapidbyte.oracle` → binary `rdlt-connector-oracle`
//! on PATH) both derive from one const. SOURCE-ONLY: the crate has no
//! destination half, so `--role=destination` is an unrecognized VALUE
//! and exits 2 at clap's arg gate — pinned below beside the other arg
//! contracts.
//!
//! THE PRE-SPAWN CLIENT PROBE is this suite's oracle-specific pin,
//! asserted from BOTH sides: the driver dlopens an Oracle Client at
//! RUNTIME, and the bin probes for one between clap's arg gate and
//! the handshake line. A machine WITHOUT a client must see the typed
//! stderr refusal with stdout EMPTY and the serve-error exit code —
//! never an opaque death after a half-printed handshake — and a
//! machine WITH one must see the ordinary handshake. Each arm has a
//! subject on exactly one kind of machine, so its counterpart
//! announces the skip (024's skip-not-fail rule); the arg-contract
//! cells sit BEFORE the probe in the bin and run everywhere.

use rdlt_connector_oracle::source::client_available;
use rdlt_runtime::Role;

use super::support::spawn::built_bin;

/// The refusal, byte-for-byte: the bin's one stderr line when no
/// USABLE client is loadable (missing, broken, or an unsupported
/// version — the whole DPI load family), naming the library and the
/// install hint. Frozen — the operator-facing spelling of the whole
/// probe.
const REFUSAL: &str = "rdlt-connector-oracle: no usable Oracle Client library — this connector wraps \
     ODPI-C, which dlopens libclntsh at RUNTIME (the build needed none); the library \
     is missing, broken, or its version is unsupported. Install or update Oracle \
     Instant Client and put its directory on LD_LIBRARY_PATH.\n";

/// The source half answers the config-free `Spec` RPC through the
/// provider (spawn → handshake line → dial → Spec; the provider owns
/// the whole lifecycle including socket cleanup) and reports the
/// reverse-DNS id — the one `NAME` const, exact. Needs a loadable
/// client: the bin's probe sits before the handshake, so on a
/// clientless machine this arm's subject is the OTHER cell's.
#[tokio::test]
async fn with_a_client_the_bin_answers_the_spec_rpc_for_the_source_role() {
    if !client_available() {
        eprintln!(
            "SKIP: no Oracle Client library — the bin refuses before the handshake \
             (the refusal cell covers this machine); the Spec RPC arm not run"
        );
        return;
    }
    rdlt_certify::assert_spec_identity(
        &built_bin(),
        Role::Source,
        "io.rapidbyte.oracle",
        env!("CARGO_PKG_VERSION"),
    )
    .await;
}

/// THE REFUSAL ARM: on a machine without a client, `--role=source` is
/// one typed stderr line (frozen, full-string), an EMPTY stdout — no
/// handshake byte ever printed — and the serve-error exit code, 1.
/// The probe's whole point is that the provider reads this as a clean
/// pre-handshake refusal rather than an opaque spawn death; a machine
/// WITH a client has no subject for it and says so.
#[test]
fn without_a_client_the_bin_refuses_before_the_handshake() {
    if client_available() {
        eprintln!(
            "SKIP: an Oracle Client library is loadable — the refusal arm has no \
             subject on this machine (the Spec RPC cell covers it); not run"
        );
        return;
    }
    let output = std::process::Command::new(built_bin())
        .arg("--role=source")
        .output()
        .expect("the bin runs");

    assert_eq!(
        output.status.code(),
        Some(1),
        "a missing client is the serve-error exit code"
    );
    assert!(
        output.stdout.is_empty(),
        "the refusal precedes ANY stdout byte — a partial handshake would be an opaque \
         spawn death to the provider; got {:?}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8(output.stderr).expect("the refusal is UTF-8");
    assert_eq!(stderr, REFUSAL, "the refusal spelling is frozen");
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
