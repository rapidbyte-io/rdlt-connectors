# rdlt-connectors — canonical entry points for contributors AND CI.
# CI invokes these verbs; never duplicate their commands inline anywhere else.
#
#   make build                 debug build, whole workspace
#   make connector-bins        every connector binary, release — what `rdlt run`
#                                spawns from PATH (the CLI itself lives in the
#                                rdlt repository)
#   make lint                  format check + git-dep gate + clippy (warnings
#                                are errors)
#   make docs                  public documentation with warnings as errors
#   make test                  fast suite (nextest + spawn/certify matrix +
#                                doc-tests)
#     TARGET=unit make test      nextest + spawn/certify matrix only
#     TARGET=e2e  make test      end-to-end integration tests only
#     TARGET=sweep make test     crash-point sweeps (failpoints feature)
#   make check                 everything a PR must pass (lint + docs + test
#                                + e2e + sweep)
#   make reclaim               remove every container AND volume this
#                                workspace started (label rdlt-test=1)
#   make certify-snowflake     live snowflake destination certification, BY
#                                HAND only (talks to a real account)
#
# Suites are selected by TARGET; the tools behind them are implementation
# details. Container-fixture suites (postgres/oracle/iceberg cells) self-skip
# announced without a runtime; the snowflake suites read live credentials from
# ~/.config/rdlt/snowflake/ and run against the real service where present.

TARGET ?=

.PHONY: build connector-bins lint docs test check reclaim certify-snowflake

build:
	cargo build --workspace

# The seven connector BINARIES, release — the artifacts this repository
# ships. `rdlt run`'s discovery convention is the binary named
# `rdlt-connector-<name>` on PATH; these land in target/release.
connector-bins:
	cargo build --release \
	  -p rdlt-connector-file -p rdlt-connector-snowflake -p rdlt-connector-postgres \
	  -p rdlt-connector-rest -p rdlt-connector-duckdb -p rdlt-connector-iceberg \
	  -p rdlt-connector-oracle \
	  --features bin-serve \
	  --bin rdlt-connector-file --bin rdlt-connector-snowflake --bin rdlt-connector-postgres \
	  --bin rdlt-connector-rest --bin rdlt-connector-duckdb --bin rdlt-connector-iceberg \
	  --bin rdlt-connector-oracle

# check-git-deps.sh is the distribution gate. It runs AHEAD of clippy
# deliberately: it costs under a second, builds nothing, and answers a question
# no other check here asks — whether this workspace can still be published. A
# manifest that cannot ship should not wait behind a multi-minute compile to
# say so. Needs python3 3.11+ (stdlib tomllib).
lint:
	cargo fmt --all --check
	tools/check-git-deps.sh
	cargo clippy --workspace --all-targets -- -D warnings
	# The snowflake crash sweep is `#![cfg(feature = "failpoints")]` and no gate
	# command enables that feature for this crate (the sweep itself costs
	# 101.5 min and needs live credentials, so it is run BY HAND). Type-checked
	# here so the file cannot rot against deleted APIs with every gate green.
	# The feature is enabled for this crate ALONE, because turning it on
	# workspace-wide changes what compiles in the six others.
	cargo clippy -p rdlt-connector-snowflake --all-targets --features failpoints -- -D warnings

# `-D warnings` promotes rustdoc's lints to errors: a dead intra-doc link is a
# defect in what consumers read. --all-features so cfg-gated public items are
# documented too.
docs:
	RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features

# THE SPAWN-SUITE MATRIX, stated once and expanded in BOTH gate blocks (the
# rdlt 024 both-blocks discipline). One module per invocation throughout:
# nextest fails only a FULLY empty selection, so an OR filter with a renamed
# module beside a live one passes green (measured in rdlt) — separate lines
# make each module fail its own line.
define spawn-suite-matrix
# The connector BINARIES: behind `bin-serve` + `required-features`, so NO
# workspace command ever compiles them — built here explicitly so a bin that
# stops compiling fails the gate rather than rotting unseen. ONE batched
# cargo invocation (the rdlt round-10 shape: sequential invocations paid
# resolution, the target-dir lock and process startup once per bin per gate
# block). The BARE `--features bin-serve` spelling is deliberate: cargo
# applies it to every selected package (each defines the feature), while the
# package-prefixed form does NOT register for the one crate whose workspace
# dependency entry pins `default-features = false` (postgres). A build
# failure still names its package.
cargo build \
  -p rdlt-connector-file -p rdlt-connector-snowflake -p rdlt-connector-postgres \
  -p rdlt-connector-rest -p rdlt-connector-duckdb -p rdlt-connector-iceberg \
  -p rdlt-connector-oracle \
  --features bin-serve \
  --bin rdlt-connector-file --bin rdlt-connector-snowflake --bin rdlt-connector-postgres \
  --bin rdlt-connector-rest --bin rdlt-connector-duckdb --bin rdlt-connector-iceberg \
  --bin rdlt-connector-oracle
# The postgres bin's OWN spawn suite (041) — the crate's gated cases
# drive the built bin through the provider's Spec RPC (identity,
# version, exit codes), same env-var discipline as rdlt's runtime lines.
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-postgres --features fixtures,spawn-bins -E 'test(test_spawned_bin)'
# CDC over the wire (041 Task 1): the spawned pg bin against a live
# logical-replication container — snapshot, cursor JSON round-trip,
# resumed change pass across two processes, slot persistence parity.
# Skip-not-fail without a container runtime, own line per the
# one-module-per-invocation rule.
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-postgres --features fixtures,spawn-bins -E 'test(test_cdc_wire)'
# The certification cells (041 Task 3): the spawned pg bin faces the
# FULL clause suite over the wire against a live container, both
# roles — S1/S2/S4 + P1-P7 (source, certified twice in a row) and
# D1-D6 + D8 LIVE + P1-P10 (destination). Skip-not-fail without a
# container runtime, own line per the one-module-per-invocation rule.
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-postgres --features fixtures,spawn-bins -E 'test(test_certify_wire)'
# The kill matrix (041 Task 4): the spawned pg bin SIGKILLed at
# every K boundary against a live container. Skip-not-fail without a
# container runtime, own line per the one-module-per-invocation rule.
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-postgres --features fixtures,spawn-bins -E 'test(test_kill_wire)'
# The rest bin's OWN spawn suite (042 Task 5), the first SOURCE-ONLY
# port: spawn smoke (identity, --version, exit 2 — including
# --role=destination), then certification (S1/S2/S4 + P1-P7, twice
# in a row) and the source kill matrix (K-S1..K-S3) over the real
# wire against a LOCAL wiremock stub — NEVER the live PokeAPI (that
# cell stays behind RDLT_NET and is never a kill subject). No
# container runtime involved, so these cells never skip; own line
# per the one-module-per-invocation rule.
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-rest --features spawn-bins -E 'test(test_spawned_bin)'
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-rest --features spawn-bins -E 'test(test_certify_wire)'
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-rest --features spawn-bins -E 'test(test_kill_wire)'
# The duckdb bin's OWN spawn suite (042 Task 6), the first
# SINGLE-WRITER destination port: spawn smoke (identity, --version,
# exit 2 — including --role=source on a destination-only crate, plus
# the cross-process lock-conflict FATAL refusal, D-042-2), then
# certification (D1-D6 + D8 live + ALL TEN P-clauses incl. P11/P12)
# and the destination kill matrix (K-D1..K-D6), all hermetic on
# tempdir database files — no container runtime, so these cells never
# skip; own line per the one-module-per-invocation rule.
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-duckdb --features spawn-bins -E 'test(test_spawned_bin)'
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-duckdb --features spawn-bins -E 'test(test_certify_wire)'
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-duckdb --features spawn-bins -E 'test(test_kill_wire)'
# The support module's OWN pin (the shared count_at probe helper's
# absence-vs-broken-read rule) lives at cases::support::probe, which
# none of the three case-module filters above matches — without this
# line it is compiled behind `spawn-bins` yet selected by NOTHING
# (the 024 zero-coverage class). Own line per the
# one-module-per-invocation rule; an empty selection (module renamed)
# fails the line.
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-duckdb --features spawn-bins -E 'test(support::probe)'
# The iceberg bin's OWN spawn suite (042 Task 7), the first CATALOG
# destination port: spawn smoke (identity, --version, exit 2 —
# including --role=source on a destination-only crate; offline, never
# skips), then certification and the destination kill matrix
# (K-D1..K-D6, all six arms run live — D-042-3) against the
# Polaris/RUSTFS fixture. The two live cells are skip-not-fail
# without a container runtime and ride the `iceberg-live` nextest
# group by package filter; own line per the
# one-module-per-invocation rule.
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-iceberg --features spawn-bins -E 'test(test_spawned_bin)'
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-iceberg --features spawn-bins -E 'test(test_certify_wire)'
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-iceberg --features spawn-bins -E 'test(test_kill_wire)'
# The oracle bin's OWN spawn suite (042 Task 8), the port with the
# PRE-SPAWN CLIENT PROBE: the driver dlopens an Oracle Client at
# RUNTIME, so the bin probes BEFORE the handshake line and a missing
# client is a typed stderr refusal + exit 1 with stdout EMPTY —
# never an opaque spawn death. The spawn smoke pins BOTH probe arms
# (each skips, announced, where the other has the subject) plus
# identity/--version/exit 2; certification (S1/S2/S4 + P1-P7, twice
# in a row) and the source kill matrix (K-S1..K-S3) run against the
# live Oracle Free container with DOUBLE skip-not-fail — no
# container runtime AND no client each announce their own reason.
# The whole package rides the `oracle-live` nextest group (the ~75 s
# boots, bounded at 3); own line per the one-module-per-invocation
# rule.
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-oracle --features spawn-bins -E 'test(test_spawned_bin)'
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-oracle --features spawn-bins -E 'test(test_certify_wire)'
RDLT_BUILD_CONNECTOR_BINS=1 cargo nextest run -p rdlt-connector-oracle --features spawn-bins -E 'test(test_kill_wire)'
endef

test:
ifeq ($(TARGET),)
	cargo nextest run --workspace
	$(spawn-suite-matrix)
	cargo test --doc --workspace
else ifeq ($(TARGET),unit)
	cargo nextest run --workspace
	$(spawn-suite-matrix)
else ifeq ($(TARGET),e2e)
	# ONE e2e binary answers this name here: the file crate's (default
	# features). An empty selection — a renamed binary — fails,
	# deliberately: the 024 empty-selection discipline.
	cargo nextest run --workspace -E 'binary(/e2e/)'
else ifeq ($(TARGET),sweep)
	# No `--no-tests=pass` on any line below, and that distinction is the point:
	# `--no-tests` governs which tests the runner SELECTS, not whether they then
	# skip. These binaries are always selected and self-skip internally when a
	# container runtime or credentials are absent, so an empty SELECTION only
	# ever means a binary was renamed, deleted, or misspelled here — which must
	# fail. nextest's default is already `fail`; relying on it is deliberate.
	# Postgres sweeps self-skip without a container runtime.
	cargo nextest run -p rdlt-connector-postgres --features failpoints -E 'binary(source_crash_sweep) or binary(destination_crash_sweep) or binary(cdc_crash_sweep)'
	cargo nextest run -p rdlt-connector-duckdb --features failpoints -E 'binary(crash_sweep)'
	cargo nextest run -p rdlt-connector-rest --features failpoints -E 'binary(sweep)'
	cargo nextest run -p rdlt-connector-file --features failpoints -E 'binary(crash_sweep)'
	cargo nextest run -p rdlt-connector-iceberg --features failpoints -E 'binary(crash_sweep)'
	# Oracle self-skips without a container runtime; ~15 s fixture boot.
	cargo nextest run -p rdlt-connector-oracle --features failpoints -E 'binary(crash_sweep)'
	# The snowflake crash sweep is DELIBERATELY absent: it talks to a real
	# account for 101.5 min and is run BY HAND (its suite is type-checked
	# by lint's failpoints clippy line).
else
	$(error unknown test TARGET '$(TARGET)' — see header comment)
endif

check: lint
	$(MAKE) docs
	$(MAKE) test
	$(MAKE) test TARGET=e2e
	$(MAKE) test TARGET=sweep

# Live snowflake destination certification — BY HAND only, the same
# discipline as the snowflake crash sweep: it talks to a real account, so no
# check/test block ever invokes it. The config file may hold real credentials;
# this recipe passes its PATH to --config and never echoes its contents.
# Without the file it announces the skip and exits 0, so an uncredentialed
# machine can run it harmlessly. The certifier bin is installed from the
# LOCKED rdlt revision (Cargo.lock's), so the CLI the certification spawns
# matches the certify library the suites link.
certify-snowflake:
	@set -e; \
	config="$$HOME/.config/rdlt/snowflake/certify.json"; \
	if [ ! -f "$$config" ]; then \
		echo "SKIP: no snowflake credentials (~/.config/rdlt/snowflake/certify.json)"; \
		exit 0; \
	fi; \
	rev=$$(grep -A3 'name = "rdlt-certify"' Cargo.lock | grep '^source' | sed 's/.*rdlt.//; s/"//'); \
	cargo install --git https://github.com/rapidbyte-io/rdlt --rev "$$rev" rdlt-certify \
		--features bin --debug --locked --root target/certify-install; \
	cargo build -p rdlt-connector-snowflake --features bin-serve --bin rdlt-connector-snowflake; \
	target/certify-install/bin/rdlt-certify --role destination --config "$$config" \
		$${CARGO_TARGET_DIR:-target}/debug/rdlt-connector-snowflake

# Reclaim leaked test containers and their volumes.
#
# Scoped by the `rdlt-test=1` label that every start site in the testkit
# applies. Volumes are removed SEPARATELY because an anonymous volume outlives
# the container that created it — reaping containers alone is what let the
# disk fill twice during rdlt's 017.
#
# Whichever engine is present wins; `docker` here may itself be podman's
# compat CLI, which is why the socket-probing order matches the testkit's.
reclaim:
	@engine=""; \
	for candidate in podman docker; do \
	  if $$candidate ps >/dev/null 2>&1; then engine=$$candidate; break; fi; \
	done; \
	if [ -z "$$engine" ]; then \
	  echo "reclaim: no working container engine (podman or docker) — nothing to do"; \
	  exit 0; \
	fi; \
	echo "reclaim: using $$engine"; \
	containers=$$($$engine ps -aq --filter label=rdlt-test=1); \
	if [ -n "$$containers" ]; then \
	  echo "$$containers" | xargs $$engine rm -f -v; \
	else \
	  echo "reclaim: no labelled containers"; \
	fi; \
	volumes=$$($$engine volume ls -q --filter label=rdlt-test=1); \
	if [ -n "$$volumes" ]; then \
	  echo "$$volumes" | xargs $$engine volume rm -f; \
	else \
	  echo "reclaim: no labelled volumes"; \
	fi; \
	echo "reclaim: done"
