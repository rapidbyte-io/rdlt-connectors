//! Shared harness for the wiremock suites: build a source from YAML, read
//! one stream, collect rows + checkpoints.
#![allow(dead_code)]

use rdlt_connector_rest::source::Shell;
use rdlt_connector_sdk::spi::{
    Cursor, PushPayload, ReadRequest, Source, SourceError, records_channel,
};

pub struct ReadOutcome {
    pub rows: Vec<serde_json::Value>,
    pub checkpoints: Vec<Cursor>,
    pub result: Result<(), SourceError>,
}

/// Read `stream` fully; panics only on harness plumbing, never on the
/// read.
pub async fn read_stream(yaml: &str, stream: &str, since: Option<Cursor>) -> ReadOutcome {
    let source = Shell::from_yaml(yaml).expect("config parses");
    let (out, mut input) = records_channel(1 << 20);
    let specs = source.streams().await.expect("streams");
    let spec = specs
        .iter()
        .find(|s| s.name.as_str() == stream)
        .unwrap_or_else(|| panic!("stream {stream} declared"))
        .clone();
    let read = tokio::spawn(async move { source.read(ReadRequest::new(spec, since, out)).await });

    let mut rows = Vec::new();
    let mut checkpoints = Vec::new();
    while let Some(push) = input.recv().await {
        match push.payload {
            PushPayload::RawJson(bytes) => {
                let value: serde_json::Value = serde_json::from_slice(&bytes).expect("valid json");
                rows.extend(value.as_array().expect("array").clone());
            }
            PushPayload::Checkpoint(cursor) => checkpoints.push(cursor),
            _ => {}
        }
    }
    let result = read.await.expect("join");
    ReadOutcome {
        rows,
        checkpoints,
        result,
    }
}

/// Convenience: read expecting success; returns the rows.
pub async fn read_ok(yaml: &str, stream: &str) -> Vec<serde_json::Value> {
    let outcome = read_stream(yaml, stream, None).await;
    outcome.result.expect("read succeeds");
    outcome.rows
}

/// Convenience: read expecting a typed error; returns its rendering.
pub async fn read_err(yaml: &str, stream: &str) -> String {
    let outcome = read_stream(yaml, stream, None).await;
    outcome.result.expect_err("read fails typed").to_string()
}

/// One-stream REST config document: `base_url` plus a single stream named
/// `name` at `path`. `extra` is the stream's remaining fields as raw YAML
/// (records_path, pagination, response_actions, method/body, incremental,
/// …); every line is indented to the stream's field column here, so
/// callers write it flush-left and state only their per-test delta.
/// `base_url` is always quoted — valid both for a live server URI and for
/// a literal like `http://x` used by parse-only cases.
pub fn stream_yaml(base_url: &str, name: &str, path: &str, extra: &str) -> String {
    let mut doc =
        format!("base_url: \"{base_url}\"\nstreams:\n  - name: {name}\n    path: {path}\n");
    for line in extra.lines() {
        if line.is_empty() {
            doc.push('\n');
        } else {
            doc.push_str("    ");
            doc.push_str(line);
            doc.push('\n');
        }
    }
    doc
}
