//! THE S2 SKIP-ACKNOWLEDGMENT WIRE PIN (044 T3 carry): the certifier
//! CLI's `--accept-skips` surface — clap parse → pass-through →
//! exit-code fold — pinned end to end over the real wire. The leg
//! lived in rdlt-certify's own CLI suite while the file connector was
//! that repository's spawn subject; the reference connector cannot
//! carry it (its source has no snapshot shape — a config whose path
//! matches nothing is a read error there, not an honest S2 skip), so
//! the pin moves here with the one connector whose glob-shaped source
//! produces it naturally.
//!
//! The certifier bin is INSTALLED from the LOCKED rdlt revision
//! (Cargo.lock's), so the CLI this suite spawns matches the certify
//! library every other suite links — `cargo build -p` cannot enable a
//! non-member's `bin` feature (measured: "cannot specify features for
//! packages outside of workspace"), and an unpinned install would
//! certify a CLI the lockfile never resolved.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use rdlt_testkit::spawn::built_connector_bin;

/// The workspace root: two levels above this crate's manifest.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("a workspace member's manifest sits two levels below the workspace root")
        .to_path_buf()
}

/// The announced refusal when NO revision is resolvable — the same
/// posture as the suite's RDLT_BUILD_CONNECTOR_BINS discipline: loud,
/// with instructions, never a bare expect-failure.
const NO_LOCKED_REV: &str = "the rdlt-certify lock entry carries no git source — the \
     two-checkout [patch] dev loop is active (an uncommitted .cargo/config.toml \
     redirects the rdlt git dependencies to a local checkout, and cargo strips the \
     source line from patched packages) and the workspace manifest pins no `rev`, \
     so there is no locked revision to install the certifier from. The gate of \
     record never runs patched: delete the patch, let cargo restore Cargo.lock, \
     and run the suite again.";

/// The rdlt revision the lockfile resolved — the certifier must be
/// built from the SAME tree as the certify library the suites link.
/// Under the README's two-checkout [patch] dev loop the lock entry
/// loses its `source =` line; the fallback is a `rev` pinned on the
/// workspace manifest's own git spec, and where neither names a
/// revision the suite refuses with [`NO_LOCKED_REV`].
fn locked_rdlt_rev(root: &Path) -> String {
    let lock = std::fs::read_to_string(root.join("Cargo.lock")).expect("Cargo.lock reads");
    let manifest = std::fs::read_to_string(root.join("Cargo.toml")).expect("Cargo.toml reads");
    match resolve_locked_rev(&lock, &manifest) {
        Ok(rev) => rev,
        Err(message) => panic!("{message}"),
    }
}

/// Pure resolution over the two documents' text, so the patched-lock
/// arm is pinnable without patching this workspace.
fn resolve_locked_rev(lock: &str, manifest: &str) -> Result<String, String> {
    if let Some(source) =
        lines_after_package(lock, "rdlt-certify").find(|line| line.starts_with("source = "))
    {
        return Ok(source
            .rsplit_once('#')
            .expect("a git source pins its revision after `#`")
            .1
            .trim_end_matches('"')
            .to_owned());
    }
    let manifest_rev = manifest
        .lines()
        .find(|line| line.trim_start().starts_with("rdlt-certify"))
        .and_then(|line| {
            let (_, rest) = line.split_once("rev = \"")?;
            rest.split_once('"').map(|(rev, _)| rev.to_owned())
        });
    manifest_rev.ok_or_else(|| NO_LOCKED_REV.to_owned())
}

/// The lines of the `[[package]]` block naming `name` — bounded at the
/// next block so a later package's source can never answer.
fn lines_after_package<'a>(lock: &'a str, name: &str) -> impl Iterator<Item = &'a str> + use<'a> {
    let header = format!("name = \"{name}\"");
    lock.lines()
        .skip_while(move |line| *line != header)
        .take_while(|line| !line.starts_with("[[package]]"))
}

/// The installed certifier bin, installing it first (at the locked
/// revision) when `RDLT_BUILD_CONNECTOR_BINS` is set — the Makefile
/// line sets it, and a repeat install at an unchanged revision is a
/// sub-second no-op. Without the env var a missing bin fails with
/// instructions, never silently.
fn built_certify_bin() -> PathBuf {
    let root = workspace_root();
    let bin = root.join("target/certify-install/bin/rdlt-certify");
    if std::env::var_os("RDLT_BUILD_CONNECTOR_BINS").is_none() {
        eprintln!(
            "note: RDLT_BUILD_CONNECTOR_BINS is unset — spawning the rdlt-certify \
             binary already on disk WITHOUT reinstalling. Whatever this suite \
             certifies is that binary, not necessarily the locked revision. The \
             Makefile's spawn-bins line sets the var."
        );
    } else {
        let rev = locked_rdlt_rev(&root);
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let status = Command::new(&cargo)
            .current_dir(&root)
            .args([
                "install",
                "--git",
                "https://github.com/rapidbyte-io/rdlt",
                "--rev",
                &rev,
                "rdlt-certify",
                "--features",
                "bin",
                "--debug",
                "--locked",
                "--root",
                "target/certify-install",
            ])
            .status()
            .unwrap_or_else(|error| panic!("cargo install rdlt-certify did not spawn: {error}"));
        assert!(status.success(), "cargo install rdlt-certify failed");
    }
    assert!(
        bin.is_file(),
        "no certifier bin at {} — run the Makefile's file-connector certify-wire \
         line (it sets RDLT_BUILD_CONNECTOR_BINS=1 so this suite installs the \
         locked revision itself)",
        bin.display()
    );
    bin
}

/// Run the certifier with `args`, capturing everything.
fn certify(certify_bin: &Path, args: &[&str]) -> Output {
    Command::new(certify_bin)
        .args(args)
        .output()
        .expect("the certifier bin spawns")
}

fn stdout_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// A source-suite skip is NOT certified evidence by default (the
/// certifier's round-3 rule): a stream that never checkpoints and
/// declares no cursor field earns an honest S2 skip — but a source
/// that merely FORGOT resume looks identical, so the bin refuses
/// (exit 1), the report naming the skipped clause and the
/// acknowledgment. The acknowledgment takes STREAM NAMES (round-12 —
/// a blanket flag accepted for one genuine snapshot stream also
/// folded a regressed co-stream green): naming the wrong stream still
/// refuses; naming the skipping stream is the operator owning the
/// trade — exit 0, the skip still rendered.
#[test]
fn a_source_suite_skip_refuses_unless_acknowledged() {
    let dir = tempfile::tempdir().expect("tempdir");
    // A glob matching NOTHING: the stream reads no files, never
    // checkpoints, and declares no cursor field — the snapshot shape.
    let config = serde_json::json!({
        "streams": [{
            "name": "events",
            "format": "jsonl",
            "path": format!("{}/*.jsonl", dir.path().display()),
        }]
    });
    let config_path = dir.path().join("config.json");
    std::fs::write(&config_path, config.to_string()).expect("the config file writes");
    let certify_bin = built_certify_bin();
    let bin = built_connector_bin(env!("CARGO_MANIFEST_DIR"), "rdlt-connector-file");

    let refused = certify(
        &certify_bin,
        &[
            "--role",
            "source",
            "--config",
            config_path.to_str().expect("utf-8 path"),
            bin.to_str().expect("utf-8 path"),
        ],
    );
    let stdout = stdout_of(&refused);
    assert_eq!(
        refused.status.code(),
        Some(1),
        "an unacknowledged source skip refuses\nstdout:\n{stdout}\nstderr:\n{}",
        stderr_of(&refused)
    );
    // The refusal is the LIBRARY's (the certifier's round-4 rule): the
    // unacknowledged skip folds as a FAIL entry naming the flag, so
    // embedders gating on Report::passed share the exact guard this
    // exit code speaks.
    assert!(
        stdout.contains("FAIL S2") && stdout.contains("--accept-skips events"),
        "the unacknowledged skip fails S2 naming the stream's own acknowledgment: {stdout}"
    );

    // Naming a DIFFERENT stream acknowledges nothing: still exit 1.
    let wrong_name = certify(
        &certify_bin,
        &[
            "--role",
            "source",
            "--config",
            config_path.to_str().expect("utf-8 path"),
            "--accept-skips",
            "not-events",
            bin.to_str().expect("utf-8 path"),
        ],
    );
    assert_eq!(
        wrong_name.status.code(),
        Some(1),
        "a wrong-name acknowledgment must not certify\nstdout:\n{}",
        stdout_of(&wrong_name)
    );

    let accepted = certify(
        &certify_bin,
        &[
            "--role",
            "source",
            "--config",
            config_path.to_str().expect("utf-8 path"),
            "--accept-skips",
            "events",
            bin.to_str().expect("utf-8 path"),
        ],
    );
    let stdout = stdout_of(&accepted);
    assert_eq!(
        accepted.status.code(),
        Some(0),
        "the acknowledged skip passes\nstdout:\n{stdout}\nstderr:\n{}",
        stderr_of(&accepted)
    );
    assert!(
        stdout.contains("SKIP S2"),
        "the skip still renders: {stdout}"
    );
}

/// The three resolution arms of the locked-revision helper, pinned on
/// document text: a locked git source wins; a [patch]-stripped lock
/// falls back to a manifest `rev`; and neither refuses with the FULL
/// announced message — the README's dev loop must meet instructions,
/// not a bare expect-failure.
#[test]
fn the_locked_rev_resolves_or_refuses_with_the_announced_posture() {
    let locked = "[[package]]\n\
         name = \"rdlt-certify\"\n\
         version = \"0.3.0\"\n\
         source = \"git+https://github.com/rapidbyte-io/rdlt#0123456789abcdef\"\n\
         [[package]]\n\
         name = \"other\"\n\
         source = \"registry+https://github.com/rust-lang/crates.io-index\"\n";
    let bare_manifest = "[workspace.dependencies]\n\
         rdlt-certify = { git = \"https://github.com/rapidbyte-io/rdlt\" }\n";
    assert_eq!(
        resolve_locked_rev(locked, bare_manifest).expect("a git source resolves"),
        "0123456789abcdef"
    );

    // The [patch] dev loop: cargo rewrote the lock entry without a
    // source line. A manifest that pins `rev` still answers…
    let patched = "[[package]]\n\
         name = \"rdlt-certify\"\n\
         version = \"0.3.0\"\n\
         [[package]]\n";
    let pinned_manifest = "[workspace.dependencies]\n\
         rdlt-certify = { git = \"https://github.com/rapidbyte-io/rdlt\", rev = \"feedc0de\" }\n";
    assert_eq!(
        resolve_locked_rev(patched, pinned_manifest).expect("a manifest rev resolves"),
        "feedc0de"
    );

    // …and one that does not refuses with the full announced message.
    assert_eq!(
        resolve_locked_rev(patched, bare_manifest).expect_err("nothing to resolve"),
        NO_LOCKED_REV
    );
}
