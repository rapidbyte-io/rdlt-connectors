//! The one-dependency rule, enforced in the connectors' own house: a
//! connector crate carries EXACTLY ONE rdlt runtime dependency — the
//! sdk — and reaches the SPI through its `spi` re-export.
//! `rdlt-connector-sqlcore` is the RECORDED exception for SQL
//! destinations (shared merge-core substrate), and `rdlt-testkit` is
//! tolerated OPTIONAL-ONLY (a connector shipping container fixtures
//! behind a `fixtures` feature routes them through the testkit by
//! design, and an optional dep must live in `[dependencies]` to be
//! feature-gated). Dev-dependencies are exempt — the verification half.
//!
//! Judged from `cargo metadata` rather than manifest text: the resolved
//! graph is what a consumer receives, and metadata cannot be fooled by
//! an unusual spelling. Every `rdlt-connector-*` member except sqlcore
//! (substrate, not a connector) is bound — derived from the workspace,
//! never a hand-kept list, so a new connector crate is in jurisdiction
//! the moment it joins the members table.

use serde_json::Value;
use std::path::Path;

/// The workspace root: two levels above this crate's manifest.
fn workspace_root() -> &'static Path {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/examples-gate sits two levels below the workspace root")
}

fn metadata() -> Value {
    let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
    let output = std::process::Command::new(cargo)
        .current_dir(workspace_root())
        .args(["metadata", "--format-version", "1", "--all-features"])
        .output()
        .expect("cargo metadata spawns");
    assert!(
        output.status.success(),
        "cargo metadata failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("cargo metadata emits JSON")
}

#[test]
fn connector_members_depend_on_the_sdk_alone() {
    let meta = metadata();
    let member_ids: Vec<&str> = meta["workspace_members"]
        .as_array()
        .expect("workspace_members is a list")
        .iter()
        .map(|id| id.as_str().expect("a member id is a string"))
        .collect();
    let packages = meta["packages"].as_array().expect("packages is a list");

    let mut bound = 0;
    for package in packages {
        let id = package["id"].as_str().expect("a package id");
        let name = package["name"].as_str().expect("a package name");
        if !member_ids.contains(&id)
            || !name.starts_with("rdlt-connector-")
            || name == "rdlt-connector-sqlcore"
        {
            continue;
        }
        bound += 1;
        for dep in package["dependencies"].as_array().expect("dependencies") {
            // Normal edges only: `kind` is null for them, "dev"/"build"
            // otherwise. Dev edges never reach a consumer.
            if !dep["kind"].is_null() {
                continue;
            }
            let dep_name = dep["name"].as_str().expect("a dependency name");
            if !dep_name.starts_with("rdlt") {
                continue;
            }
            let optional = dep["optional"].as_bool().unwrap_or(false);
            let allowed = matches!(dep_name, "rdlt-connector-sdk" | "rdlt-connector-sqlcore")
                || (dep_name == "rdlt-testkit" && optional);
            assert!(
                allowed,
                "{name}: rdlt runtime dependency `{dep_name}` beyond the allowed \
                 set — the SPI and vocabulary are reached through the sdk's `spi` \
                 re-export, never directly (rdlt-testkit is tolerated ONLY as an \
                 optional fixtures-feature dep)"
            );
        }
        assert!(
            package["dependencies"]
                .as_array()
                .expect("dependencies")
                .iter()
                .any(|dep| dep["kind"].is_null() && dep["name"] == "rdlt-connector-sdk"),
            "{name}: a connector member depends on the sdk"
        );
    }
    // The vacuity guard: a filter bug reading zero connectors must fail,
    // and a connector leaving or joining shows up as a count change here
    // beside its own diff.
    assert_eq!(bound, 7, "every first-party connector member is bound");
}

/// sqlcore's own shape, pinned separately: substrate sits UNDER the
/// connectors, so its one rdlt edge is the SPI itself — never the sdk,
/// never a connector.
#[test]
fn sqlcore_depends_on_the_spi_alone() {
    let meta = metadata();
    let package = meta["packages"]
        .as_array()
        .expect("packages")
        .iter()
        .find(|p| p["name"] == "rdlt-connector-sqlcore")
        .expect("sqlcore is a member");
    for dep in package["dependencies"].as_array().expect("dependencies") {
        if !dep["kind"].is_null() {
            continue;
        }
        let dep_name = dep["name"].as_str().expect("a dependency name");
        if dep_name.starts_with("rdlt") {
            assert_eq!(
                dep_name, "rdlt-connector",
                "sqlcore reaches rdlt through the SPI alone"
            );
        }
    }
}
