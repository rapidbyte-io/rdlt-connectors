//! Shared helpers: planted local files, config builders, and row
//! counting through the destination's own testhook.

use std::path::Path;

use rdlt_connector_sdk::config::Document;

use rdlt_connector_file::{destination, source};

/// Plant one file under the directory, creating parents.
pub fn plant(dir: &Path, name: &str, bytes: &[u8]) {
    let path = dir.join(name);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("parents");
    }
    std::fs::write(path, bytes).expect("plant");
}

/// A one-stream jsonl source document over a glob under `dir`.
pub fn jsonl_source(dir: &Path, pattern: &str) -> source::Config {
    source::Config::from_value(serde_json::json!({
        "streams": [{
            "name": "events",
            "format": "jsonl",
            "path": dir.join(pattern).to_str().expect("utf8 temp path"),
        }]
    }))
    .expect("valid source document")
}

/// A local parquet destination config over `dir`.
pub fn local_dest(dir: &Path) -> destination::Config {
    destination::Config::new(dir.to_str().expect("utf8 temp path"))
}
