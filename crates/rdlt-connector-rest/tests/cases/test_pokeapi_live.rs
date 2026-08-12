//! Feature 014 FR-013: the LIVE PokeAPI cell — one real public API proves
//! the world. Env-gated (`RDLT_NET=1`; SKIPPED, not failed, without it);
//! declarative config only; STRUCTURAL asserts (public data drifts);
//! conservative pacing (good-citizen against a real host).
//!
//! Exercises for real what the mocks prove in principle: next_url
//! pagination (`next` in the body), a records selector (`results`), and a
//! parent-child detail stream (`/pokemon/{name}`).

use rdlt_connector_duckdb::destination as duck;
use rdlt_connector_rest::source::Shell;
use rdlt_engine::{Engine, EngineConfig};

fn net_gated() -> bool {
    if std::env::var("RDLT_NET").as_deref() != Ok("1") {
        eprintln!("skipped: set RDLT_NET=1 to run the live PokeAPI cell");
        return false;
    }
    true
}

/// Parent-child against the real API: one bounded list page fans out to
/// per-pokemon detail requests; the detail's `abilities` array is the child
/// record set (bounded volume, real placeholder resolution).
#[tokio::test(flavor = "multi_thread")]
async fn pokeapi_list_and_details_through_the_engine() {
    if !net_gated() {
        return;
    }
    let yaml = r#"
base_url: https://pokeapi.co/api/v2
min_request_interval_ms: 100
streams:
  - name: pokemon_list
    path: /pokemon
    params: {limit: "20"}
    records_path: results
  - name: pokemon_detail
    path: /pokemon/{name}
    records_path: abilities
    parent:
      stream: pokemon_list
      placeholders: {name: name}
      include: [name]
"#;
    let source = Shell::from_yaml(yaml).expect("config");
    let dir = tempfile::tempdir().unwrap();
    let dest_config = duck::Config::new(dir.path().join("poke.duckdb"));
    let dest = duck::Shell::new(dest_config.clone()).unwrap();
    let mut config = EngineConfig::new("pokeapi");
    config = config.with_workdir(dir.path().join("work"));
    Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect("live run");

    // STRUCTURAL asserts (counts drift with the Pokédex, structure doesn't).
    let list = duck::testhook::count_rows(&dest_config, "pokemon_list").unwrap();
    assert!(list > 0 && list <= 20, "one bounded list page, got {list}");
    let details = duck::testhook::count_rows(&dest_config, "pokemon_detail").unwrap();
    assert!(
        details >= list,
        "every pokemon has ≥1 ability row ({details} < {list})"
    );
    // Parent resolution proven: the embedded parent name is set everywhere.
    assert_eq!(
        duck::testhook::query_string(
            &dest_config,
            "SELECT count(*)::VARCHAR FROM pokemon_detail WHERE _parent_name IS NULL"
        )
        .unwrap(),
        "0"
    );
}

/// The next_url chain against the real API — a small resource
/// (/pokemon-color, ~10 entries) with limit=4 gives a 3-page chain that
/// terminates NATURALLY (last page: next=null) within polite volume.
#[tokio::test(flavor = "multi_thread")]
async fn pokeapi_next_url_chain_terminates() {
    if !net_gated() {
        return;
    }
    let yaml = r#"
base_url: https://pokeapi.co/api/v2
min_request_interval_ms: 100
streams:
  - name: colors
    path: /pokemon-color
    params: {limit: "4"}
    records_path: results
    pagination: {type: next_url, next_url_path: next}
"#;
    let source = Shell::from_yaml(yaml).expect("config");
    let dir = tempfile::tempdir().unwrap();
    let dest_config = duck::Config::new(dir.path().join("colors.duckdb"));
    let dest = duck::Shell::new(dest_config.clone()).unwrap();
    let mut config = EngineConfig::new("poke-colors");
    config = config.with_workdir(dir.path().join("work"));
    Engine::new(config, source, dest.clone())
        .run()
        .await
        .expect("live run");
    let n = duck::testhook::count_rows(&dest_config, "colors").unwrap();
    assert!(n > 8, "the full chain was walked (> 2 pages), got {n}");
}
