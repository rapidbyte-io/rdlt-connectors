//! The document gate: the schema and the parser agree, and every
//! refusal names its field.

use rdlt_connector_oracle::source::{self, Config, Shell};
use rdlt_connector_sdk::config::Document;

fn base() -> serde_json::Value {
    serde_json::json!({
        "host": "db.example", "service": "FREEPDB1",
        "user": "rdlt", "password": "pw",
        "streams": [{"name": "events", "table": "EVENTS"}]
    })
}

/// The generated schema accepts what the parser accepts, and refuses
/// what it refuses — they come from the same structs.
#[test]
fn the_schema_and_parser_agree() {
    let schema = jsonschema::validator_for(&source::config_schema()).expect("compiles");
    let good = base();
    assert!(schema.is_valid(&good));
    assert!(Shell::from_value(good).is_ok());

    let mut typo = base();
    typo["strems"] = serde_json::json!([]);
    assert!(
        !schema.is_valid(&typo),
        "deny_unknown_fields reaches the schema"
    );
    assert!(Shell::from_value(typo).is_err());
}

/// The port defaults to Oracle's registered listener port, and the
/// tuning defaults are the JDBC-equivalent values an Oracle operator
/// expects.
#[test]
fn the_defaults_match_the_jdbc_equivalents() {
    let config = Config::from_value(base()).expect("valid");
    assert_eq!(config.port, 1521);
    assert_eq!(
        config.tuning.connect_timeout_ms, 60_000,
        "oracle.net.CONNECT_TIMEOUT"
    );
    assert_eq!(
        config.tuning.read_timeout_ms, 600_000,
        "oracle.jdbc.ReadTimeout"
    );
    assert_eq!(config.tuning.statement_cache, 20);
    assert!(
        config.tuning.page_rows.is_none(),
        "derived from widths unless set"
    );
}

/// A legacy instance is named by SID; an operator's known-good JDBC
/// tuning translates field for field.
#[test]
fn a_legacy_sid_instance_with_jdbc_tuning_parses() {
    let mut document = base();
    document["service"] = serde_json::Value::Null;
    document.as_object_mut().expect("object").remove("service");
    document["sid"] = "ORCL".into();
    document["tuning"] = serde_json::json!({
        "page_rows": 25,                 // defaultRowPrefetch=25 & oracle.jdbc.ReadTimeout=600000
    // & oracle.net.CONNECT_TIMEOUT=60000 & oracle.net.keepAlive=true    // oracle.jdbc.defaultLobPrefetchSize
        "read_timeout_ms": 600_000,      // oracle.jdbc.ReadTimeout
        "connect_timeout_ms": 60_000,    // oracle.net.CONNECT_TIMEOUT
    });
    let config = Config::from_value(document).expect("valid legacy document");
    assert_eq!(config.sid.as_deref(), Some("ORCL"));
    assert_eq!(config.tuning.page_rows, Some(25));
}

/// Every refusal names its field; duplicates are refused at the gate
/// (the reader resolves streams by name and would shadow the twin).
#[test]
fn refusals_name_their_field() {
    let cases: Vec<(serde_json::Value, &str)> = vec![
        (
            {
                let mut v = base();
                v["host"] = "".into();
                v
            },
            "`host` must not be empty",
        ),
        (
            {
                let mut v = base();
                v["service"] = "".into();
                v
            },
            "one of `service` (modern) or `sid` (legacy) is required",
        ),
        (
            {
                let mut v = base();
                v["sid"] = "ORCL".into();
                v
            },
            "`service` and `sid` are two ways to name one instance",
        ),
        (
            {
                let mut v = base();
                v["tuning"] = serde_json::json!({"page_rows": 0});
                v
            },
            "`tuning.page_rows` is 0",
        ),
        (
            {
                let mut v = base();
                v["streams"] = serde_json::json!([]);
                v
            },
            "at least one stream is required",
        ),
        (
            {
                let mut v = base();
                v["streams"] = serde_json::json!([
                    {"name": "a", "table": "T"},
                    {"name": "a", "table": "U"}
                ]);
                v
            },
            "duplicate stream name `a`",
        ),
        (
            {
                let mut v = base();
                v["streams"] = serde_json::json!([{"name": "a", "table": ""}]);
                v
            },
            "stream `a`: `table` must not be empty",
        ),
    ];
    for (document, needle) in cases {
        let err = Config::from_value(document)
            .expect_err("refused")
            .to_string();
        assert!(
            err.starts_with("invalid oracle source config: ") && err.contains(needle),
            "{err}"
        );
    }
}

/// The password never renders.
#[test]
fn the_password_is_grep_proof() {
    let config = Config::from_value(base()).expect("valid");
    assert!(!format!("{config:?}").contains("pw"));
}

/// The tuning defaults are the documented ones.
///
/// This asserts VALUES only, and says so: it is NOT evidence that a
/// knob reaches anything. An earlier version of this doc claimed it
/// was, which is exactly how six inert knobs survived a review —
/// what actually catches that is `every_tuning_knob_has_a_consumer`
/// below.
#[test]
fn tuning_defaults_are_the_documented_ones() {
    let tuning = rdlt_connector_oracle::source::Tuning::default();
    assert_eq!(tuning.page_rows, None, "the driver's own prefetch");
    assert_eq!(tuning.batch_rows, 8_192);
    assert_eq!(tuning.connect_timeout_ms, 60_000);
    assert_eq!(tuning.read_timeout_ms, 600_000);
    assert_eq!(tuning.statement_cache, 20);
    assert_eq!(tuning.keepalive_minutes, 1, "minutes: EXPIRE_TIME's unit");
}

/// EVERY knob has a consumer in the source tree.
///
/// The defect this guards is the one that has now escaped THREE
/// reviews: a field that parses, is documented as a control, and
/// reaches nothing. A knob with no consumer is worse than an absent
/// one — it reads as a working control.
#[test]
fn every_tuning_knob_has_a_consumer() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut body = String::new();
    for entry in std::fs::read_dir(src.join("source")).expect("source dir") {
        let path = entry.expect("entry").path();
        if path.extension().is_some_and(|e| e == "rs") {
            body.push_str(&std::fs::read_to_string(&path).expect("read"));
        }
    }
    // A CONSUMER is a field ACCESS (`tuning.<knob>`), not a mention:
    // the declaration is `pub <knob>:` and the default fn names the
    // knob too, so counting mentions would pass on the declaration
    // alone. Note `connect_timeout_ms` is legitimately consumed
    // inside config.rs itself, by `connect_string`.
    for knob in [
        "page_rows",
        "batch_rows",
        "connect_timeout_ms",
        "read_timeout_ms",
        "statement_cache",
        "keepalive_minutes",
    ] {
        assert!(
            body.contains(&format!("tuning.{knob}")),
            "`tuning.{knob}` is never read — it parses, it is documented as a control, \
             and it controls nothing"
        );
    }
}

/// The owner's JDBC parameter string maps onto the document.
#[test]
fn the_jdbc_parameter_vocabulary_maps() {
    // defaultRowPrefetch=25 & oracle.jdbc.ReadTimeout=600000
    // & oracle.net.CONNECT_TIMEOUT=60000 & oracle.net.keepAlive=true
    // & oracle.jdbc.ReadTimeout=600000 & oracle.net.CONNECT_TIMEOUT=60000
    // & oracle.net.keepAlive=true
    let value = serde_json::json!({
        "host": "db.example.com",
        "service": "ORCL",
        "user": "rdlt",
        "password": "pw",
        "tuning": {
            "page_rows": 25,
            "read_timeout_ms": 600_000,
            "connect_timeout_ms": 60_000,
            "keepalive_minutes": 1
        },
        "streams": [{"name": "s", "table": "T"}]
    });
    let config = Config::from_value(value).expect("the JDBC vocabulary maps");
    assert_eq!(config.tuning.page_rows, Some(25));
    assert_eq!(config.tuning.keepalive_minutes, 1);

    // `useFetchSizeWithLongColumn` has NO counterpart and needs none:
    // it exists because the JDBC driver could not stream LONG columns
    // alongside a fetch size. LONG is deprecated by Oracle in favour
    // of CLOB, and this connector reads LOBs through locators, which
    // is not coupled to the page size at all.
    assert!(
        Config::from_value(serde_json::json!({
            "host": "h", "service": "S", "user": "u", "password": "p",
            "tuning": {"use_fetch_size_with_long_column": true},
            "streams": [{"name": "s", "table": "T"}]
        }))
        .is_err(),
        "an unknown tuning key is refused, not silently ignored"
    );
}
