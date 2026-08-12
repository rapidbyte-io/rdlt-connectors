//! A table ≥ 10× the enforced memory ceiling snapshots successfully —
//! memory is bounded by configuration, not table size. The release CLI runs
//! as a subprocess under `prlimit --data` (heap ceiling); the test verifies
//! the 10× ratio from pg_total_relation_size, so the claim is measured, not
//! asserted.
//!
//! Destination = the streaming parquet writer: the point under test is the
//! SOURCE/engine path (never materializes the table); DuckDB's buffer-pool
//! reservations scale with its own config, not table size, and belong to its
//! own memory story. Measured on the reference machine (recorded on the
//! pre-swap in-process shape, under its old 256 MiB single-process
//! ceiling): 40 M rows / 6.86 GB source table → 39 MB peak RSS.
//!
//! WHICH CONNECTORS THIS EXERCISES: since the 043 D1 swap the CLI spawns
//! the connectors a document names, so the `postgres:` source and `file:`
//! destination below resolve to THIS crate's release bin and the file
//! crate's, discovered off `<target>/release` prepended to the spawned
//! CLI's PATH. And prlimit's rlimits INHERIT across fork and exec, so the
//! `--data` ceiling binds the CLI AND each spawned connector process
//! individually — the per-process claim is now made of every process in
//! the pipeline, which strengthens what this test proves; that is
//! deliberate, not incidental.
//!
//! THE CEILING IS 512 MiB AND CANNOT BE THE OLD 256, measured not
//! assumed: RLIMIT_DATA counts private writable ADDRESS SPACE, and the
//! post-swap CLI's steady state sits at ~257 MiB of VmData on a
//! 16-core machine while its resident heap is far smaller — two 64 MiB
//! glibc arena reservations (the CLI's own mallopt), ~30 tokio thread
//! stacks at 2 MiB each (a floor that grows with core count), and
//! ~70 MiB of engine/wire heap. A 256 MiB ceiling leaves the CLI zero
//! headroom and aborts mid-run on machine-shaped virtual reservations,
//! not on data. 512 MiB keeps the claim honest: the table is still
//! ≥ 10× the ceiling (asserted below from pg_total_relation_size), and
//! boundedness itself was verified directly — every process plateaus
//! flat while 6.4 GB streams through.
//!
//! Self-skips (visibly) without `prlimit`, a container runtime, a built
//! release CLI (`make release`), or the two built release connector bins
//! (`make connector-bins`) — UNLESS `RDLT_HEAVY=1` (the sweep/deep
//! targets), where a missing prerequisite is a hard FAIL with instructions:
//! the deep job must never green-wash this claim by silently skipping.

use rdlt_connector_postgres::fixtures::PostgresContainer;

/// RLIMIT_DATA ceiling for every process in the pipeline (heap + data
/// mmaps + thread stacks — address space, not residency; see header).
const CEILING_BYTES: u64 = 512 * 1024 * 1024;
/// Seeded rows: ~170 B/row on-disk ⇒ ~6.9 GB, ≥ 12× the ceiling.
const ROWS: u64 = 40_000_000;

/// The release artifacts directory — `CARGO_TARGET_DIR` honored when
/// absolute and REFUSED when relative, the same rule as
/// `rdlt_testkit::spawn` (which owns the debug half): cargo resolves a
/// relative value against its own invocation cwd, so any single
/// resolution here would be a guess that can disagree with where the
/// binaries actually landed.
fn release_dir() -> std::path::PathBuf {
    match std::env::var_os("CARGO_TARGET_DIR") {
        None => std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("target/release"),
        Some(target) => {
            let target = std::path::PathBuf::from(target);
            assert!(
                target.is_absolute(),
                "CARGO_TARGET_DIR `{}` is relative — cargo resolves it against each \
                 invocation's OWN cwd, so this suite cannot know where the release \
                 binaries landed; set an absolute CARGO_TARGET_DIR",
                target.display()
            );
            target.join("release")
        }
    }
}

fn release_bin(name: &str) -> Option<std::path::PathBuf> {
    let path = release_dir().join(name);
    path.exists().then_some(path)
}

/// SIGKILL the pipeline's orphans — and ONLY them. After `wait()` has
/// reaped the group leader, the PGID number is free for reuse the
/// moment every member exits, so a blanket `kill -9 -- -PGID` could
/// land on an unrelated process group that recycled it (the deep gate
/// runs many suites concurrently and PIDs wrap at kernel.pid_max).
/// Each candidate must therefore match TWICE before it is signalled:
/// it sits in the pipeline's process group (/proc stat pgrp) AND its
/// cmdline names an rdlt artifact this test spawned.
fn reap_pipeline_orphans(pgid: u32) {
    let Ok(entries) = std::fs::read_dir("/proc") else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some(pid) = name.to_str().and_then(|s| s.parse::<u32>().ok()) else {
            continue;
        };
        let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat")) else {
            continue;
        };
        // `pid (comm) state ppid pgrp …` — comm may hold spaces and
        // parens, so the fields are taken after the LAST ')'.
        let Some((_, tail)) = stat.rsplit_once(')') else {
            continue;
        };
        if tail.split_whitespace().nth(2) != Some(pgid.to_string().as_str()) {
            continue;
        }
        let Ok(cmdline) = std::fs::read_to_string(format!("/proc/{pid}/cmdline")) else {
            continue;
        };
        if !cmdline.contains("rdlt") {
            continue;
        }
        let _ = std::process::Command::new("kill")
            .args(["-9", &pid.to_string()])
            .stderr(std::process::Stdio::null())
            .status();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_ten_times_the_memory_ceiling_still_snapshots_within_it() {
    let heavy = std::env::var("RDLT_HEAVY").is_ok_and(|value| value == "1");
    if std::process::Command::new("prlimit")
        .arg("--version")
        .output()
        .is_err()
    {
        assert!(
            !heavy,
            "RDLT_HEAVY=1 but prlimit is missing — install util-linux; \
             this test must RUN in the deep job, not skip"
        );
        eprintln!("SKIP memory_bound: prlimit not available (util-linux)");
        return;
    }
    let Some(cli) = release_bin("rdlt") else {
        assert!(
            !heavy,
            "RDLT_HEAVY=1 but the release CLI is not built — run `make release` \
             first; this test must RUN in the deep job, not skip"
        );
        eprintln!("SKIP memory_bound: release CLI not built (run `make release` first)");
        return;
    };
    // Since the D1 swap the CLI spawns the connectors its document names,
    // so the two release bins are prerequisites exactly like the CLI —
    // same announced skip, same RDLT_HEAVY refusal, never a silent pass.
    for bin in ["rdlt-connector-postgres", "rdlt-connector-file"] {
        if release_bin(bin).is_none() {
            assert!(
                !heavy,
                "RDLT_HEAVY=1 but the release connector bin `{bin}` is not built — \
                 run `make connector-bins` first; this test must RUN in the deep \
                 job, not skip"
            );
            eprintln!(
                "SKIP memory_bound: release connector bin `{bin}` not built \
                 (run `make connector-bins` first)"
            );
            return;
        }
    }

    let Some(container) = PostgresContainer::start().await else {
        assert!(
            !heavy,
            "RDLT_HEAVY=1 but no container runtime is available — install and start \
             Docker or Podman; this test must RUN in the deep job, not skip"
        );
        return;
    };
    container
        .seed(
            "CREATE TABLE big (id int8 PRIMARY KEY, bucket int4, payload text); \
             INSERT INTO big SELECT i, (i % 1000)::int4, repeat('x', 100) \
             FROM generate_series(1, 40000000) i;",
        )
        .await;

    // The 10× claim is MEASURED: on-disk table size vs the ceiling.
    let client = container.client().await;
    let table_bytes: i64 = client
        .query_one("SELECT pg_total_relation_size('big')", &[])
        .await
        .expect("size")
        .get(0);
    assert!(
        table_bytes as u64 >= 10 * CEILING_BYTES,
        "seeded table ({table_bytes} B) must be ≥ 10× the ceiling ({CEILING_BYTES} B)"
    );

    let directory = tempfile::tempdir().expect("tempdir");
    let source_yaml = directory.path().join("pg.yaml");
    // The SOURCE's own batch knobs are the third sizing rule (the other
    // two are on the pipeline document below), and they are load-bearing:
    // the sdk's serve loop buffers up to 16 already-encoded frames, so
    // in-flight memory in the source process is 16× the frame size. At
    // the defaults (8 MiB / 65,536 rows ⇒ ~10 MiB frames) that is
    // ~160 MiB before allocator churn, measured at a ~500 MiB plateau;
    // at 1 MiB frames the source plateaus at ~38 MiB resident. A source
    // under a tight per-process ceiling must size its batches to fit.
    std::fs::write(
        &source_yaml,
        format!(
            "conn: \"{}\"\ntables:\n  - name: big\n\
             batch_target_bytes: 1048576\nbatch_max_rows: 8192\n",
            container.connection_string
        ),
    )
    .expect("yaml");
    let spec = directory.path().join("pipeline.yaml");
    let report_path = directory.path().join("report.json");
    std::fs::write(
        &spec,
        format!(
            // `parts` is pinned SMALL on purpose. The destination
            // accumulates an output part in memory before writing it,
            // so the shipping 128 MiB default would put half the
            // ceiling into the writer and make this measure the
            // DESTINATION's file sizing rather than the source path
            // it claims to measure. 8 MiB keeps the destination's
            // contribution a known constant. The consequence is worth
            // stating plainly: a file-destination pipeline under a
            // tight memory limit must size `parts` to fit it.
            // `batch_policy.every_bytes` is the ENGINE-side half: it
            // bounds how many batch bytes the CLI process holds in
            // flight. It does NOT reach the spawned connectors'
            // buffering — measured directly: quartering it left the
            // source's plateau byte-identical. What bounds a connector
            // is its OWN batch sizing (the pg.yaml knobs above), so a
            // pipeline under a tight per-process ceiling sizes all
            // THREE: the source's batches, the engine's every_bytes,
            // and the destination's parts.
            // The source's config rides the path form (a bare string
            // is a path), keeping the credential out of the pipeline
            // document exactly as before.
            "pipeline: membound\nworkdir: {}\nbatch_policy:\n  every_bytes: 8388608\n\
             source:\n  postgres: {}\n\
             destination:\n  file:\n    path: {}\n    format: parquet\n    parts:\n      \
             target_bytes: 8388608\n      max_open_bytes: 8388608\n",
            directory.path().join(".rdlt").display(),
            source_yaml.display(),
            directory.path().join("pq-out").display()
        ),
    )
    .expect("spec");

    // The CLI discovers the connector bins over ITS OWN PATH, so the
    // release directory is prepended for the child alone. The rlimit set
    // by prlimit inherits to those spawned connectors too — see the
    // header: every process in the pipeline is individually bound.
    let mut paths = vec![release_dir()];
    paths.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    let path_with_bins = std::env::join_paths(paths).expect("PATH entries join");
    // stdout/stderr go to FILES and the pipeline gets its own process
    // group, both for the same reason: if the CLI aborts under the
    // ceiling, spawned connectors survive it holding its inherited
    // stdio — with pipes, `.output()` would then block on the orphans'
    // write ends forever, converting a FAILURE into a HANG (measured:
    // one abort ticked nextest's SLOW marker past 2,940 s). Files make
    // `wait` return the moment the CLI exits; the group kill below
    // reaps whatever the pipeline left behind.
    let stdout_path = directory.path().join("cli-stdout.log");
    let stderr_path = directory.path().join("cli-stderr.log");
    use std::os::unix::process::CommandExt;
    let mut child = std::process::Command::new("prlimit")
        .env("PATH", path_with_bins)
        // RLIMIT_DATA counts VIRTUAL reservations, and each glibc malloc
        // arena reserves 64 MiB of address space up front — a handful of
        // concurrently-allocating runtime threads reserve their way
        // toward the ceiling before any real data does (measured: an
        // unbounded source connector died on a 15.8 MB allocation with
        // almost nothing resident). The CLI bounds its own arenas via
        // mallopt, deliberately CLI-only — library embedders and spawned
        // connectors own their allocator policy — so the SPAWNED
        // processes get the same bound the same way an operator would
        // give it: the process-tree env knob.
        .env("MALLOC_ARENA_MAX", "2")
        .arg(format!("--data={CEILING_BYTES}"))
        .arg("--")
        .arg(&cli)
        .arg("run")
        .arg(&spec)
        .arg("--report")
        .arg(&report_path)
        .process_group(0)
        .stdout(std::fs::File::create(&stdout_path).expect("stdout log"))
        .stderr(std::fs::File::create(&stderr_path).expect("stderr log"))
        .spawn()
        .expect("spawn CLI under prlimit");
    let status = child.wait().expect("wait for CLI under prlimit");
    // The child was its own group leader, so its id is the pipeline's
    // PGID; on success this reaps nothing.
    reap_pipeline_orphans(child.id());
    assert!(
        status.success(),
        "CLI under a {CEILING_BYTES}-byte data ceiling failed:\nstdout: {}\nstderr: {}",
        std::fs::read_to_string(&stdout_path).unwrap_or_default(),
        std::fs::read_to_string(&stderr_path).unwrap_or_default()
    );

    // Row-count equality proves the stream completed, not just survived.
    let report: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&report_path).expect("report"))
            .expect("report json");
    let rows = report["tables"]["big"]["rows"]
        .as_u64()
        .expect("rows in report");
    assert_eq!(rows, ROWS);
}
