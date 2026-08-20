//! Classification against REAL service errors — a mock cannot say what
//! Snowflake actually returns, so these cells provoke failures and
//! check what this crate makes of them.

use rdlt_connector_sdk::spi::error::DestinationError;
use rdlt_connector_snowflake::destination::testhook;

use super::common::{config_for, credentials};

/// A statement against an object that does not exist is FATAL (a retry
/// cannot mint the table), and the structured code stays reachable
/// through the classified error — the seam the merge diagnosis keys on.
#[tokio::test]
async fn a_missing_object_is_fatal_and_carries_its_code() {
    let Some(creds) = credentials() else { return };
    let config = {
        use rdlt_connector_sdk::config::Document;
        rdlt_connector_snowflake::destination::Config::from_value(config_for(&creds, "PUBLIC"))
            .expect("valid")
    };

    let err = testhook::connect_and_run(&config, "SELECT * FROM \"RDLT_DOES_NOT_EXIST_ANYWHERE\"")
        .await
        .expect_err("the table does not exist");
    assert!(
        matches!(err, DestinationError::Fatal(_)),
        "not retryable: {err:?}"
    );
    assert!(
        testhook::classify_live_error(&err).is_some(),
        "the structured code survives classification: {err}"
    );
}

/// A wrong credential is FATAL with the identity in the message — the
/// operator learns WHICH credential to fix without opening the
/// pipeline document. (The bad key is synthetic; no real secret is
/// harmed.)
#[tokio::test]
async fn a_bad_credential_fails_fatal_naming_the_identity() {
    let Some(creds) = credentials() else { return };
    let mut doc = config_for(&creds, "PUBLIC");
    doc["auth"] = serde_json::json!({
        "key_pair": {"private_key":
            "-----BEGIN PRIVATE KEY-----\nnot-a-real-key\n-----END PRIVATE KEY-----\n"}
    });
    let config = {
        use rdlt_connector_sdk::config::Document;
        rdlt_connector_snowflake::destination::Config::from_value(doc).expect("valid")
    };

    let err = testhook::connect_and_run(&config, "SELECT 1")
        .await
        .expect_err("the key is garbage");
    let rendered = err.to_string();
    assert!(
        matches!(err, DestinationError::Fatal(_)),
        "retrying cannot fix a credential: {err:?}"
    );
    assert!(
        rendered.contains(&config.user) || rendered.contains(&config.account),
        "the identity is named: {rendered}"
    );
}
