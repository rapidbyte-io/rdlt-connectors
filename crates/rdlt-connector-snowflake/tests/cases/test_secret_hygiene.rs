//! No credential value survives into anything a user can see: not a
//! validation error, not a parser error (serde quotes the document it
//! was reading — a channel this crate does not write but must still not
//! leak), not Debug output.

use rdlt_connector_sdk::config::Document;
use rdlt_connector_snowflake::destination::Config;
use serde_json::json;

/// Every auth method, each carrying a marked secret.
fn every_auth() -> Vec<(&'static str, serde_json::Value)> {
    vec![
        (
            "key_pair",
            json!({"key_pair": {"private_key": "-----BEGIN X-----", "passphrase": "LEAK-PASSPHRASE"}}),
        ),
        (
            "password",
            json!({"password": {"password": "LEAK-PW", "passcode": "LEAK-PC"}}),
        ),
        ("oauth_token", json!({"oauth_token": "LEAK-OAUTH"})),
        ("pat", json!({"pat": "LEAK-PAT"})),
    ]
}

const MARKERS: &[&str] = &[
    "LEAK-PASSPHRASE",
    "LEAK-PW",
    "LEAK-PC",
    "LEAK-OAUTH",
    "LEAK-PAT",
];

fn assert_clean(what: &str, method: &str, rendered: &str) {
    for marker in MARKERS {
        assert!(
            !rendered.contains(marker),
            "{what} for `{method}` leaks {marker}: {rendered}"
        );
    }
}

/// A validation failure's rendering carries no secret, whichever method
/// the document used.
#[test]
fn validation_errors_carry_no_secret() {
    for (method, auth) in every_auth() {
        let doc = json!({
            "account": "https://not-an-identifier.example",
            "user": "U", "database": "D", "schema": "S",
            "auth": auth,
        });
        let err = Config::from_value(doc).expect_err("the URL account refuses");
        assert_clean("a validation error", method, &err.to_string());
    }
}

/// A parser failure's rendering carries no secret either — serde's own
/// text is the risk here, since it quotes the input.
#[test]
fn parse_errors_carry_no_secret() {
    for (method, auth) in every_auth() {
        let doc = format!(
            r#"{{"account":"A","user":"U","database":"D","schema":"S","typo":1,"auth":{auth}}}"#
        );
        let err = Config::from_json(&doc).expect_err("the typo refuses");
        assert_clean("a parse error", method, &err.to_string());
    }
}

/// Debug output of a parsed config carries no secret.
#[test]
fn debug_output_carries_no_secret() {
    for (method, auth) in every_auth() {
        let doc = json!({
            "account": "A", "user": "U", "database": "D", "schema": "S",
            "auth": auth,
        });
        let config = Config::from_value(doc).expect("valid");
        assert_clean("Debug output", method, &format!("{config:?}"));
    }
}
