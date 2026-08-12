# rdlt-connectors

The first-party connectors for [rdlt](https://github.com/rapidbyte-io/rdlt),
each one a separate binary the rdlt CLI (or any embedder's runtime)
spawns and drives over the frozen connector wire:

| crate | role |
|---|---|
| `rdlt-connector-postgres` | source (incl. CDC) + destination |
| `rdlt-connector-file` | source + destination (JSONL/Parquet/CSV, local or S3) |
| `rdlt-connector-rest` | source (declarative REST) |
| `rdlt-connector-oracle` | source |
| `rdlt-connector-duckdb` | destination |
| `rdlt-connector-iceberg` | destination (REST catalog) |
| `rdlt-connector-snowflake` | destination |
| `rdlt-connector-sqlcore` | shared SQL merge core (substrate, not a connector) |

Runnable pipelines for every connector live in [`examples/`](examples/README.md).

## Provenance

Seeded from `rapidbyte-io/rdlt` @ `5bd54c1e` (feature 044): the crates
moved here byte-identical, and their full history remains in rdlt's
log. The engine, SPI, sdk, testkit, runtime, and certifier stayed
behind; this workspace consumes them as git dependencies until their
registry publish wave runs (`tools/allowed-git-deps.toml` records the
arrangement and its exit — until then no crate here can publish, and
`make lint`'s git-dep gate verifies that record against the resolved
graph on every run).

## The boundary

A connector lives here when `rdlt-certify` passes it — the same bar
any third-party connector answers to. The gate enforces it: every
connector's own certify-wire suite spawns the REAL built binary and
drives it through the full clause vocabulary (and, where the shape
exists, the SIGKILL kill matrix) on every `make check`. Nothing about
being first-party earns a connector a private door into the engine.

## Building and installing

```sh
make connector-bins        # every connector binary, release, target/release
export PATH="$PWD/target/release:$PATH"
```

The rdlt CLI's discovery convention is the binary named
`rdlt-connector-<name>` on PATH; the CLI itself is built in an rdlt
checkout (`cargo build --release -p rdlt-cli`). See
[`examples/README.md`](examples/README.md) for the full install story.

## Gates

```sh
make lint            # fmt + git-dep gate + clippy (warnings are errors)
make docs            # rustdoc, warnings as errors
make test            # workspace nextest + the spawn/certify matrix + doc-tests
make check           # everything a PR must pass (adds e2e + sweep)
make reclaim         # remove leaked test containers/volumes (label rdlt-test=1)
```

Container-fixture suites (postgres, oracle, iceberg cells) self-skip
announced without a container runtime; the oracle live cells
additionally need an Oracle Instant Client on the machine and announce
their own skip without one. The snowflake suites read live credentials
from `~/.config/rdlt/snowflake/` and run against the real service where
present; `make certify-snowflake` is the by-hand live certification.

## The two-checkout dev loop

Committed builds resolve the engine crates from rdlt's `main` by git
(pinned in `Cargo.lock`). To develop against a LOCAL rdlt checkout,
drop an UNCOMMITTED `.cargo/config.toml` at this repo's root — it is
gitignored, and it must stay out of commits because it redirects every
build at a path that exists on one machine:

```toml
[patch.'https://github.com/rapidbyte-io/rdlt']
rdlt-connector = { path = "../rdlt/crates/rdlt-connector" }
rdlt-connector-sdk = { path = "../rdlt/crates/rdlt-connector-sdk" }
rdlt-engine = { path = "../rdlt/crates/rdlt-engine" }
rdlt-testkit = { path = "../rdlt/crates/rdlt-testkit" }
rdlt-runtime = { path = "../rdlt/crates/rdlt-runtime" }
rdlt-certify = { path = "../rdlt/crates/rdlt-certify" }
rdlt = { path = "../rdlt/crates/rdlt" }

# For a private rdlt clone: let cargo fetch with the system git, which
# carries your credentials.
[net]
git-fetch-with-cli = true
```

Delete the file (or move it aside) before reading a gate as the gate
of record: the record is the standalone repo against the real git
dependencies, and the patch changes what a run proves. Note the
certifier CLI that the file connector's certify-wire suite and
`make certify-snowflake` spawn is installed from the LOCKED git
revision regardless of the patch — the patch redirects libraries, not
that install.

## Releases

Connector crates version together at the workspace version (0.3.0
today) and release per connector: a release is a tag
`<crate-name>-v<version>` on the commit whose gate certified that
binary, and the shipped artifact is the crate's release-built
`bin-serve` binary. Publishing to crates.io is blocked until the rdlt
engine crates publish (the git-dep record above); the release
convention and its governance live with the program's ADRs in rdlt
(ADR 0001).
