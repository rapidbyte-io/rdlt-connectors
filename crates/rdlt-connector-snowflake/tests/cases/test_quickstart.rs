//! The README's quickstart document is executable truth: it parses into
//! a running destination, and the vocabulary 023 REMOVED stays removed —
//! a document carrying it is refused by name, not silently ignored.

use rdlt_connector_snowflake::destination::{ConfigError, Shell};

/// The quickstart, as the README spells it (keep the two in step).
const QUICKSTART: &str = r#"
account: "MYORG-MYACCT"
user: LOADER
auth:
  key_pair:
    private_key: /etc/rdlt/keys/loader.p8
database: ANALYTICS
schema: RAW
warehouse: LOAD_WH
merge_strategy: upsert
"#;

/// The quickstart parses through the one gate into a running shell.
#[test]
fn the_quickstart_document_parses_into_a_destination() {
    let shell = Shell::from_yaml(QUICKSTART).expect("the README's quickstart must parse");
    drop(shell);
}

/// 023 deleted the external-stage vocabulary (`storage:` and friends);
/// a document still carrying it must be refused NAMING the leftover
/// field, so its author knows which lines to delete.
#[test]
fn removed_stage_vocabulary_is_refused_by_name() {
    let with_stage = format!("{QUICKSTART}stage:\n  bucket: s3://old\n");
    let err = Shell::from_yaml(&with_stage).expect_err("removed vocabulary refuses");
    assert!(matches!(err, ConfigError::Yaml(_)), "{err:?}");
    assert!(
        err.to_string().contains("stage"),
        "the refusal names the field to delete: {err}"
    );
}
