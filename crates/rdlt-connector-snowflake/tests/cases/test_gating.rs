//! The credential gate's resolution rules, proven without touching the
//! process environment (a fake lookup stands in — mutating real env
//! vars is `unsafe`, and unsafe is denied).

use std::collections::BTreeMap;

use super::common::{Lookup, TokenKind, credentials_with, scratch_schema, token_with};

/// A scripted convention: some env vars, some files.
#[derive(Default)]
struct Fake {
    env: BTreeMap<&'static str, &'static str>,
    files: BTreeMap<&'static str, &'static str>,
}

impl Lookup for Fake {
    fn env(&self, var: &str) -> Option<String> {
        self.env
            .get(var)
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty())
    }
    fn file(&self, name: &str) -> Option<String> {
        self.files
            .get(name)
            .map(|v| v.trim().to_owned())
            .filter(|v| !v.is_empty())
    }
}

/// Tokens resolve their own entries, environment first, file as the
/// fallback — and each kind reads ONLY its own spelling.
#[test]
fn tokens_resolve_env_first_then_file_per_kind() {
    let both = Fake {
        env: [("RDLT_SNOWFLAKE_PAT", "env-pat")].into(),
        files: [("pat", "file-pat"), ("oauth-token", "file-oauth")].into(),
    };
    assert_eq!(
        token_with(&both, TokenKind::Pat).as_deref(),
        Some("env-pat"),
        "the environment wins"
    );
    assert_eq!(
        token_with(&both, TokenKind::OauthToken).as_deref(),
        Some("file-oauth"),
        "the file serves when the environment is silent"
    );
    assert_eq!(
        token_with(&both, TokenKind::Password),
        None,
        "a kind with neither entry resolves to nothing"
    );
}

/// Whitespace-only values count as absent — an editor's trailing
/// newline is not a credential, and an entry of spaces is not one
/// either.
#[test]
fn blank_values_count_as_absent() {
    let blank = Fake {
        env: [("RDLT_SNOWFLAKE_PAT", "   ")].into(),
        files: [("pat", "\n")].into(),
    };
    assert_eq!(token_with(&blank, TokenKind::Pat), None);
}

/// A convention missing any REQUIRED entry resolves to no credentials
/// at all — the gate is all-or-nothing, so a half-configured
/// environment skips visibly rather than failing mid-cell.
#[test]
fn missing_required_entries_mean_no_credentials() {
    // Everything except the account: still nothing.
    let partial = Fake {
        files: [("user", "U"), ("database", "D")].into(),
        ..Fake::default()
    };
    assert!(credentials_with(&partial).is_none());
}

/// Scratch schemas are unique per call and uppercase (the catalog's
/// image), so parallel runs and dead runs cannot collide.
#[test]
fn scratch_schemas_are_unique_and_uppercase() {
    let one = scratch_schema("merge");
    let two = scratch_schema("merge");
    assert_ne!(one, two);
    assert!(one.starts_with("RDLT_T_MERGE_"), "{one}");
    assert_eq!(one, one.to_uppercase(), "{one}");
}

/// The crash-point registry agrees with the sources — checked HERE, in
/// the ungated suite, because the sweep binary that also checks it is
/// feature-gated and by-hand: registry-vs-source drift must fail a
/// routine gate, not wait for the next manual sweep.
#[test]
fn the_crash_registry_matches_the_sources_in_gate() {
    rdlt_testkit::assert_registry_matches_sources(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .as_path(),
        &[rdlt_connector_snowflake::destination::FAIL_POINTS],
    );
}
