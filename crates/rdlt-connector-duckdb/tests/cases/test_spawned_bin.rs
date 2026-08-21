//! The `rdlt-connector-duckdb` BIN, spawned for real (042): the
//! config-free `Spec` RPC answers with the reverse-DNS identity, plus
//! the bin's pinned arg behavior (exit codes and the version string;
//! clap's usage TEXT is deliberately unasserted — clap owns it).
//!
//! These are the pins for THE IDENTITY RULE (039 T6) reaching duckdb:
//! the connector's `NAME` const IS its connector id, spelled
//! reverse-DNS (`io.rapidbyte.duckdb`), so the strict-equality
//! handshake verification (D-039-2) and D-039-1's last-segment binary
//! discovery (`io.rapidbyte.duckdb` → binary `rdlt-connector-duckdb`
//! on PATH) both derive from one const. This crate is
//! DESTINATION-ONLY, so `--role=source` is an ARG error (clap's exit
//! 2), pinned beside the nonsense role.
//!
//! Plus the cross-process cell (D-042-2's operator story, measured
//! live on every run): a SECOND spawned connector pointed at a
//! database file a FIRST live connector holds read-write is refused at
//! its handshake, the refusal classified FATAL on the wire — an
//! embedder sees a typed terminal error carrying duckdb's own lock
//! diagnosis, never an infinite retry.

use rdlt_runtime::provider::Classification;
use rdlt_runtime::{
    local::Local, provider::ClientError, provider::Error as ProviderError, provider::Provider,
    provider::Requirement, provider::Role,
};
use serde_json::json;

use super::support::spawn::built_bin;

/// The destination half answers the config-free `Spec` RPC through the
/// provider (spawn → handshake line → dial → Spec; the provider owns
/// the whole lifecycle including socket cleanup) and reports the
/// reverse-DNS id — the one `NAME` const, exact.
#[tokio::test]
async fn the_duckdb_bin_answers_the_spec_rpc() {
    rdlt_certify::contract::assert_spec_identity(
        &built_bin(),
        Role::Destination,
        "io.rapidbyte.duckdb",
        env!("CARGO_PKG_VERSION"),
    )
    .await;
}

/// THE CROSS-PROCESS CELL (D-042-2, live): connector 1 handshakes and
/// holds the database file read-write for its whole life; connector 2,
/// spawned against the SAME file, is refused at ITS handshake with a
/// FATAL classification on the wire and duckdb's own lock diagnosis in
/// the message. This is the live re-measurement of the `classify`
/// unit pin's spelling — the `Could not set lock on file` template
/// comes from the service on every run, never from a fixture — and the
/// proof the refusal is terminal: a fatal handshake refusal reaches an
/// embedder as a typed error, not a retry loop.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_spawned_connector_on_a_held_file_is_refused_fatal() {
    let bin = built_bin();
    let dir = tempfile::tempdir().expect("dir");
    let config = json!({ "path": dir.path().join("held.duckdb") });
    let provider = Local::new();
    let requirement = Requirement::new("io.rapidbyte.duckdb").with_path(&bin);

    let first = provider
        .destination(&requirement, &config)
        .await
        .expect("the first connector opens the file and holds it");

    let error = provider
        .destination(&requirement, &config)
        .await
        .expect_err("the second connector must be refused, not admitted");
    match error {
        ProviderError::Client(ClientError::Handshake {
            classification,
            message,
            ..
        }) => {
            assert_eq!(
                classification,
                Classification::Fatal,
                "the refusal's classification travels the wire as FATAL"
            );
            assert!(
                message.contains("Could not set lock on file"),
                "the refusal carries duckdb's own lock diagnosis: {message}"
            );
        }
        other => panic!("expected a handshake refusal, got {other:?}"),
    }
    drop(first);
}

/// The pinned arg contract, through the shared helper
/// ([`rdlt_certify::contract::assert_bin_arg_contract`]): no args and a bogus
/// role are clap's exit 2, each unserved role is refused at the arg
/// gate, and `--version`/`--help` behave with the crate version in the
/// output.
#[test]
fn the_arg_contract_holds() {
    rdlt_certify::contract::assert_bin_arg_contract(
        &built_bin(),
        &["source"],
        env!("CARGO_PKG_VERSION"),
    );
}
