//! Cross-connector end-to-end: files INTO another destination through
//! the engine — the coverage generation 1 carried in its e2e binaries,
//! and the binary the gate's `TARGET=e2e` selection (`binary(/e2e/)`)
//! reaches. The swap emptied that selection once — the 024
//! empty-selection-must-fail discipline caught it — so this binary's
//! NAME is load-bearing.

use rdlt_connector_duckdb::destination as duck;
use rdlt_connector_file::source;
use rdlt_engine::config::Config as EngineConfig;
use rdlt_engine::engine::Engine;

fn plant(dir: &std::path::Path, name: &str, bytes: &[u8]) {
    let path = dir.join(name);
    std::fs::create_dir_all(path.parent().expect("parent")).expect("dirs");
    std::fs::write(path, bytes).expect("plant");
}

fn jsonl_source(dir: &std::path::Path, pattern: &str) -> source::Shell {
    source::Shell::from_value(serde_json::json!({
        "streams": [{
            "name": "events",
            "format": "jsonl",
            "path": dir.join(pattern).to_str().expect("utf8"),
        }]
    }))
    .expect("valid")
}

/// Planted jsonl → duckdb: exact totals across two runs (the second
/// reads only the delta) — the file source driving a FOREIGN
/// destination's exactly-once path.
#[tokio::test]
async fn jsonl_to_duckdb_lands_exact_totals_and_resumes() {
    let input = tempfile::tempdir().expect("input");
    let out = tempfile::tempdir().expect("out");
    let workdir = tempfile::tempdir().expect("workdir");
    let db = out.path().join("e2e.duckdb");
    plant(input.path(), "data/a.jsonl", b"{\"id\": 1}\n{\"id\": 2}\n");

    // Each run opens its own shell (the engine consumes it — dropped when
    // run returns, so the sequential re-open is legal); the counts go
    // through the config-keyed READ-ONLY testhook oracle.
    let run = |db: std::path::PathBuf, workdir: std::path::PathBuf, input: std::path::PathBuf| async move {
        let dest = duck::Shell::new(duck::Config::new(db)).expect("open");
        Engine::new(
            EngineConfig::new("file-e2e-duckdb").with_workdir(workdir.join("wal")),
            jsonl_source(&input, "data/*.jsonl"),
            dest,
        )
        .run()
        .await
        .expect("the load settles");
    };
    run(db.clone(), workdir.path().into(), input.path().into()).await;
    assert_eq!(
        duck::testhook::count_rows(&duck::Config::new(&db), "events").expect("count"),
        2
    );

    plant(input.path(), "data/b.jsonl", b"{\"id\": 3}\n");
    run(db.clone(), workdir.path().into(), input.path().into()).await;
    assert_eq!(
        duck::testhook::count_rows(&duck::Config::new(&db), "events").expect("count"),
        3,
        "the second run read only the new file"
    );
}
