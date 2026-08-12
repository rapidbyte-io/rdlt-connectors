//! The auth matrix, each method against the real account. Key-pair
//! rides the main credential gate; the other methods each gate on their
//! OWN entry, so a missing PAT skips only the PAT leg. The password and
//! OAuth legs stay UNPERFORMED without their entries — the same
//! standing call 022 and 023 recorded: the legs are written, the skip
//! is announced, and a credential entry turns them green with zero code
//! change.

use rdlt_connector_snowflake::destination::testhook;
use serde_json::json;

use super::common::{TokenKind, config_for, credentials, token};

/// One probe statement proves a session opened under the method.
async fn probe(doc: serde_json::Value) {
    let config = {
        use rdlt_connector_sdk::config::Document;
        rdlt_connector_snowflake::destination::Config::from_value(doc).expect("valid")
    };
    let one = testhook::connect_and_run(&config, "SELECT 1")
        .await
        .expect("the session runs");
    assert_eq!(one, "1");
}

#[tokio::test]
async fn key_pair_authentication_opens_a_session() {
    let Some(creds) = credentials() else { return };
    probe(config_for(&creds, "PUBLIC")).await;
}

#[tokio::test]
async fn a_programmatic_access_token_rides_the_password_channel() {
    let Some(creds) = credentials() else { return };
    let Some(pat) = token(TokenKind::Pat) else {
        eprintln!("SKIP: snowflake PAT leg — no RDLT_SNOWFLAKE_PAT entry");
        return;
    };
    let mut doc = config_for(&creds, "PUBLIC");
    doc["auth"] = json!({"pat": pat});
    probe(doc).await;
}

#[tokio::test]
async fn an_oauth_token_opens_a_session() {
    let Some(creds) = credentials() else { return };
    let Some(oauth) = token(TokenKind::OauthToken) else {
        eprintln!("SKIP: snowflake OAuth leg — no RDLT_SNOWFLAKE_OAUTH_TOKEN entry");
        return;
    };
    let mut doc = config_for(&creds, "PUBLIC");
    doc["auth"] = json!({"oauth_token": oauth});
    probe(doc).await;
}

#[tokio::test]
async fn a_password_opens_a_session() {
    let Some(creds) = credentials() else { return };
    let Some(password) = token(TokenKind::Password) else {
        eprintln!("SKIP: snowflake password leg — no RDLT_SNOWFLAKE_PASSWORD entry");
        return;
    };
    let mut doc = config_for(&creds, "PUBLIC");
    doc["auth"] = json!({"password": {"password": password}});
    probe(doc).await;
}
