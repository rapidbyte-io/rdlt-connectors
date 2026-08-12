//! The examples are LOAD-BEARING: every `examples/*/pipeline.yaml`
//! must parse through the real Spec gate, every connector reference in
//! it — rich spelling or `connector:` — must desugar to a known
//! reverse-DNS id, and every resolved config must pass that connector's
//! own Document gate; each connector's designated REFERENCE example
//! must additionally mention EVERY field of that connector's config
//! schema, active or commented, so "the full configuration is visible
//! in examples/" stays a property the gate holds rather than a promise
//! that rots. No spawn in the default gate: whether the documents RUN
//! against real binaries is the spawn suites' business; this holds the
//! document language itself against the examples.

use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};

use rdlt::pipeline_spec::{ConfigSource, ConnectorRef};
use rdlt_connector_sdk::config::Document;

/// Every rich spelling the pipeline document exposes. The expected ids
/// are derived through the production desugar table rather than copied
/// into this test.
const CONNECTOR_SPELLINGS: &[&str] = &[
    "rest",
    "oracle",
    "file",
    "postgres",
    "duckdb",
    "iceberg",
    "snowflake",
];

fn examples_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples")
}

/// Push one resolved config through the named connector's own Document
/// gate, exactly as the connector does when a spawned run handshakes.
fn validate_config(example: &str, role: &str, reference: &ConnectorRef) {
    let ConfigSource::Inline(config) = &reference.config else {
        panic!("{example}: the {role} config must resolve to an inline document");
    };
    let config = config.clone();
    let outcome = match (reference.id.as_str(), role) {
        ("io.rapidbyte.rest", "source") => rdlt_connector_rest::source::Config::from_value(config)
            .map(|_| ())
            .map_err(|error| error.to_string()),
        ("io.rapidbyte.oracle", "source") => {
            rdlt_connector_oracle::source::Config::from_value(config)
                .map(|_| ())
                .map_err(|error| error.to_string())
        }
        ("io.rapidbyte.file", "source") => rdlt_connector_file::source::Config::from_value(config)
            .map(|_| ())
            .map_err(|error| error.to_string()),
        ("io.rapidbyte.postgres", "source") => {
            rdlt_connector_postgres::source::Config::from_value(config)
                .map(|_| ())
                .map_err(|error| error.to_string())
        }
        ("io.rapidbyte.duckdb", "destination") => {
            rdlt_connector_duckdb::destination::Config::from_value(config)
                .map(|_| ())
                .map_err(|error| error.to_string())
        }
        ("io.rapidbyte.postgres", "destination") => {
            rdlt_connector_postgres::destination::Config::from_value(config)
                .map(|_| ())
                .map_err(|error| error.to_string())
        }
        ("io.rapidbyte.file", "destination") => {
            rdlt_connector_file::destination::Config::from_value(config)
                .map(|_| ())
                .map_err(|error| error.to_string())
        }
        ("io.rapidbyte.iceberg", "destination") => {
            rdlt_connector_iceberg::destination::Config::from_value(config)
                .map(|_| ())
                .map_err(|error| error.to_string())
        }
        ("io.rapidbyte.snowflake", "destination") => {
            rdlt_connector_snowflake::destination::Config::from_value(config)
                .map(|_| ())
                .map_err(|error| error.to_string())
        }
        other => panic!("{example}: no connector config gate is wired for {other:?}"),
    };
    if let Err(error) = outcome {
        panic!(
            "{example}: the {role} config fails {}'s own document gate: {error}",
            reference.id
        );
    }
}

/// Every example directory carries a pipeline.yaml that parses, and
/// both sides desugar to a shipped connector id and pass that
/// connector's config gate. The count is pinned so a deleted or
/// unreadable example fails rather than shrinking the property.
#[test]
fn every_example_pipeline_parses_desugars_and_passes_connector_gates() {
    let known_ids: HashSet<_> = CONNECTOR_SPELLINGS
        .iter()
        .map(|spelling| {
            rdlt::pipeline_spec::connector_id(spelling)
                .unwrap_or_else(|| panic!("`{spelling}` must have a desugar-table row"))
        })
        .collect();
    let mut seen = 0;
    for entry in std::fs::read_dir(examples_dir()).expect("examples/ exists") {
        let entry = entry.expect("entry");
        let dir = entry.path();
        let path = dir.join("pipeline.yaml");
        if !path.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("{name}/pipeline.yaml must read: {e}"));
        let spec: rdlt::pipeline_spec::Spec = serde_yaml::from_str(&text)
            .unwrap_or_else(|e| panic!("{name}/pipeline.yaml must parse: {e}"));
        // The pipeline-level halves of the build gate that need no
        // spawn: the commit-policy threshold rule is the SAME check
        // build_pipeline runs before any connector resolves, and a
        // merge key must be non-empty (the builder's refusal, pinned
        // in builder_errors). The capability half — a write mode the
        // destination refuses — needs a live connector and stays with
        // the spawn suites.
        if let Some(policy) = &spec.commit_policy {
            policy
                .check()
                .unwrap_or_else(|e| panic!("{name}: commit_policy must pass the build gate: {e}"));
        }
        if let Some(rdlt::pipeline_spec::WriteModeSpec::Merge { key }) = &spec.write_mode {
            assert!(!key.is_empty(), "{name}: a merge write mode needs a key");
        }
        // The example's own directory is the base, so a path-form
        // config resolves relative to the files beside it.
        let source = spec
            .source
            .desugar(&dir)
            .unwrap_or_else(|e| panic!("{name}: the source must desugar: {e}"));
        let destination = spec
            .destination
            .desugar(&dir)
            .unwrap_or_else(|e| panic!("{name}: the destination must desugar: {e}"));
        for (role, reference) in [("source", &source), ("destination", &destination)] {
            assert!(
                known_ids.contains(reference.id.as_str()),
                "{name}: `{}` is not a shipped connector's id",
                reference.id
            );
            validate_config(&name, role, reference);
        }
        seen += 1;
    }
    assert_eq!(seen, 7, "every example directory carries a pipeline.yaml");
}

/// Each connector's designated REFERENCE example, with the schema(s)
/// its pipeline must mention in full. A connector may appear in
/// several examples; the full-vocabulary claim binds only its
/// designated one. The schemas come straight from the connector
/// crates — the same documents a spawned `rdlt schema` serves.
fn reference_map() -> Vec<(&'static str, Vec<serde_json::Value>)> {
    vec![
        (
            "pokemon-to-jsonl",
            vec![rdlt_connector_rest::source::config_schema()],
        ),
        (
            "oracle-to-jsonl",
            vec![rdlt_connector_oracle::source::config_schema()],
        ),
        (
            "postgres-to-parquet",
            vec![
                rdlt_connector_postgres::source::config_schema(),
                rdlt_connector_file::destination::config_schema(),
            ],
        ),
        (
            "jsonl-to-postgres",
            vec![
                rdlt_connector_file::source::config_schema(),
                rdlt_connector_postgres::destination::config_schema(),
            ],
        ),
        (
            "csv-to-duckdb",
            vec![rdlt_connector_duckdb::destination::config_schema()],
        ),
        (
            "postgres-to-iceberg",
            vec![rdlt_connector_iceberg::destination::config_schema()],
        ),
        (
            "jsonl-to-snowflake",
            vec![rdlt_connector_snowflake::destination::config_schema()],
        ),
    ]
}

/// Collect every property name reachable in a config schema, walking
/// nested objects, arrays, unions and `$defs`.
fn property_names(schema: &serde_json::Value, out: &mut BTreeSet<String>) {
    if let Some(props) = schema.get("properties").and_then(|p| p.as_object()) {
        for (name, sub) in props {
            out.insert(name.clone());
            property_names(sub, out);
        }
    }
    for key in ["items", "additionalProperties"] {
        if let Some(sub) = schema.get(key) {
            property_names(sub, out);
        }
    }
    for key in ["anyOf", "oneOf", "allOf"] {
        if let Some(list) = schema.get(key).and_then(|l| l.as_array()) {
            for sub in list {
                property_names(sub, out);
            }
        }
    }
    if let Some(defs) = schema.get("$defs").and_then(|d| d.as_object()) {
        for sub in defs.values() {
            property_names(sub, out);
        }
    }
}

/// The reference map IS the example set: an example directory this
/// test does not know is either missing its designated-schema entry or
/// is dead weight — both are findings.
#[test]
fn the_reference_map_matches_the_example_directories() {
    let mut on_disk: Vec<String> = std::fs::read_dir(examples_dir())
        .expect("examples/ exists")
        .filter_map(|entry| {
            let entry = entry.expect("entry");
            entry
                .path()
                .join("pipeline.yaml")
                .is_file()
                .then(|| entry.file_name().to_string_lossy().into_owned())
        })
        .collect();
    on_disk.sort();
    let mut mapped: Vec<String> = reference_map()
        .into_iter()
        .map(|(name, _)| name.to_owned())
        .collect();
    mapped.sort();
    assert_eq!(on_disk, mapped);
}

/// Does `text` mention `field` as a whole word? A raw substring check
/// would let one field vouch for another it merely contains — a new
/// `ssl` field counted as documented by the existing `sslmode`, a
/// `key` by `primary_key` — re-opening exactly the rot this gate
/// exists to prevent. Hyphens and dots count as word characters so a
/// compound token in prose or a URL (`append-mode`, `ssl-mode=`)
/// cannot vouch for a field embedded in it either; config fields
/// themselves are underscore-spelled, so no legitimate mention is a
/// hyphenated or dotted compound.
fn mentions_field(text: &str, field: &str) -> bool {
    let is_word = |c: char| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.';
    text.match_indices(field).any(|(at, _)| {
        let clear_before = text[..at].chars().next_back().is_none_or(|c| !is_word(c));
        let clear_after = text[at + field.len()..]
            .chars()
            .next()
            .is_none_or(|c| !is_word(c));
        clear_before && clear_after
    })
}

/// THE FULL-CONFIGURATION GUARANTEE: each designated example mentions
/// every field of its connector's schema, in an active line or a
/// commented one. A field added to a config struct without a home in
/// its reference example fails HERE — which is the entire point of
/// this file.
#[test]
fn each_reference_example_mentions_every_schema_field() {
    for (name, schemas) in reference_map() {
        let text = std::fs::read_to_string(examples_dir().join(name).join("pipeline.yaml"))
            .expect("read example");
        let mut fields = BTreeSet::new();
        for schema in &schemas {
            property_names(schema, &mut fields);
        }
        let missing: Vec<&String> = fields
            .iter()
            .filter(|field| !mentions_field(&text, field))
            .collect();
        assert!(
            missing.is_empty(),
            "{name}/pipeline.yaml is the reference for its connector but never mentions: \
             {missing:?} — show each field, active or commented"
        );
    }
}
