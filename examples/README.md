# Examples

Runnable pipelines covering EVERY connector, each executed exactly as
written before being committed — the row counts below are what they
actually produced. Where a database is involved, a `compose.yaml` in
the example's directory starts it seeded; the images are the exact
pins rdlt's own test suites run against.

| example | flow | needs | verified result |
|---|---|---|---|
| [`pokemon-to-jsonl`](pokemon-to-jsonl/) | REST → jsonl files | network | 1,351 rows, 2 files (147 KB + 104 KB) |
| [`csv-to-duckdb`](csv-to-duckdb/) | CSV → DuckDB | nothing | 60 rows; re-run adds 0 |
| [`postgres-to-parquet`](postgres-to-parquet/) | PostgreSQL → parquet | compose | 5,000 + 200 + 3 rows, idempotent |
| [`jsonl-to-postgres`](jsonl-to-postgres/) | jsonl → PostgreSQL | compose | 40 rows merged; re-run leaves 40 |
| [`oracle-to-jsonl`](oracle-to-jsonl/) | Oracle → jsonl files | compose + Instant Client | 250 rows, then 0 (incremental) |
| [`postgres-to-iceberg`](postgres-to-iceberg/) | PostgreSQL → Iceberg (Polaris + S3) | compose | 5,200 rows, 1 snapshot/table, partitioned |
| [`jsonl-to-snowflake`](jsonl-to-snowflake/) | jsonl → Snowflake | an account | 40 rows merged; re-run leaves 40 |

Run one with:

```sh
# if it has a compose.yaml (docker compose and podman-compose both work):
docker compose -f examples/<name>/compose.yaml up -d
rdlt run examples/<name>/pipeline.yaml
```

Podman users: `podman compose` is only a delegator and finds its
provider through podman's own lookup, not your shell's PATH. Either
call `podman-compose` directly (`pip install podman-compose`), or
name it once in `~/.config/containers/containers.conf` so
`podman compose` works everywhere:

```toml
[engine]
compose_providers = ["/home/you/.local/bin/podman-compose"]
```

`workdir:` and a path-form `config:` resolve beside the pipeline file
itself, wherever you run from. Paths INSIDE a connector's config —
sample data, output directories, database files — resolve against the
directory you run from, so run the examples from THIS repository's
root.

## Installing the binaries

Connectors are separate binaries the CLI spawns per run. The CLI lives
in the engine repository ([rapidbyte-io/rdlt](https://github.com/rapidbyte-io/rdlt));
the connector binaries are this repository's `make connector-bins`
verb — build both once, then each build directory goes on PATH:

```sh
# in an rdlt checkout: the CLI, target/release/rdlt
cargo build --release -p rdlt-cli
# in THIS repository: every rdlt-connector-* binary
make connector-bins
export PATH="$PWD/target/release:$PATH"   # in each repository
```

A pipeline arm names its connector by reverse-DNS id, and the id's
last segment names the binary: `id: io.rapidbyte.postgres` resolves to
`rdlt-connector-postgres` on PATH — that convention is the whole
discovery mechanism. A single connector can also be built by crate
(`cargo build --release -p rdlt-connector-postgres --features
bin-serve`), and a binary that lives off PATH is named explicitly with
`path:` beside the id:

```yaml
source:
  connector:
    id: io.rapidbyte.postgres
    path: /opt/rdlt/bin/rdlt-connector-postgres
    config: { conn: "host=..." }
```

## The full configuration, enforced

Each connector has ONE example that is the reference for its complete
vocabulary — every field appears there, active where the example uses
it, commented with a real value where it does not:

| connector | its reference example |
|---|---|
| rest source | pokemon-to-jsonl |
| postgres source | postgres-to-parquet |
| oracle source | oracle-to-jsonl |
| file source | jsonl-to-postgres |
| file destination | postgres-to-parquet |
| postgres destination | jsonl-to-postgres |
| duckdb destination | csv-to-duckdb |
| snowflake destination | jsonl-to-snowflake |
| iceberg destination | postgres-to-iceberg |

This is a GATE PROPERTY, not a promise: `crates/examples-gate/tests/examples.rs`
parses every pipeline through the real Spec gate, checks every arm's
`connector:` id names a shipped first-party connector, validates every
config against that connector's own document gate, and fails if a
reference example omits any field of its connector's config schema. A config
field added without a home in the examples fails the suite.

Every arm's `config:` is the connector's own document, written inline
(as these do) or as a bare string path — `config: postgres.yaml` —
pointing at a separate YAML/JSON file with the identical shape,
resolved relative to the pipeline file's own directory. The value is
either the document or the path, so half a document can never be
silently ignored.

Fixed host ports, chosen high to avoid dev services: postgres 15432
(parquet example) / 15433 (postgres-destination example) / 15434
(iceberg example), oracle 11521, RUSTFS 19000, Polaris 18181.

---

## `pokemon-to-jsonl` — REST → newline-delimited JSON

Reads every Pokémon from [PokéAPI](https://pokeapi.co); no
credentials, no setup — the one to try first.

**Verified:** 1,351 rows, matching the `count` PokéAPI reports for the
same endpoint — so pagination followed every page rather than stopping
at the first, landing in **two files of 147 KB and 104 KB**. Running
it a second time leaves 1,351, not 2,702: `write_mode: replace`
truncates rather than appends.

What the pipeline says:

- The rest source's config shows the full vocabulary: all six auth
  forms, all seven pagination families, incremental windows, response
  actions, a parent-child stream (commented — it makes 1,351 polite
  requests), and type hints.
- The active parts: PokéAPI's `next` link is followed
  (`pagination: next_url`) because arithmetic on offset/limit is how
  off-by-one bugs are born; `min_request_interval_ms` keeps rdlt a
  good citizen of someone else's free API.
- The destination's `parts` block is a deliberately tiny 128 KiB so
  the file-sizing mechanics are visible: the first file spans TWO of
  the engine's 400-row writes and closes just after crossing the
  target (147 KB — a part overshoots, a write is never split), and
  the second is smaller (104 KB) because the commit closed it — no
  part ever spans a commit. Delete the block and all 1,351 rows land
  in one file: the real default is 128 MiB.

Output rows carry two engine columns beside your data: `_rdlt_id` is
the row identity used for deduplication and merges; `_rdlt_load_id`
says which load wrote the row.

---

## `csv-to-duckdb` — CSV → DuckDB

No containers, no credentials. The sample data is `;`-delimited with
a quoted note column, so the `csv:` options block is load-bearing.

**Verified:** 60 rows, `sum(amount) = 23660.51`, and `placed_on` is a
real DATE column — the `type_hints` did the parsing, not luck. A
second run adds 0 rows: the file source's own cursor knows a
fully-read file and re-reads only what grew.

The duckdb destination's config is the reference for that destination:
`memory_limit` (worth pinning on shared machines — DuckDB's own
default is a fraction of SYSTEM memory), bare-identifier-only
`settings:` passthrough, `extensions:`, and the shared SQL merge
options.

---

## `postgres-to-parquet` — PostgreSQL → parquet

```sh
docker compose -f examples/postgres-to-parquet/compose.yaml up -d
rdlt run examples/postgres-to-parquet/pipeline.yaml
```

**Verified:** 5,000 orders + 200 customers + 3 rows from a join QUERY
stream, as zstd parquet; a second run produces the same counts —
`replace` is a mirror.

This is the reference for BOTH the postgres source (tables, query
streams, the full cursor vocabulary, CDC, TLS, batch shaping) and the
file destination (parquet tuning, partitioning, `parts`, S3). One
interaction stated in the file because it surprises people: under
`replace` a cursor is deliberately NOT applied — a mirror rebuilt
from only-the-new-rows would lose every old one. Cursors pair with
`append` and `merge`.

---

## `jsonl-to-postgres` — files → PostgreSQL, merged

```sh
docker compose -f examples/jsonl-to-postgres/compose.yaml up -d
rdlt run examples/jsonl-to-postgres/pipeline.yaml
```

**Verified:** 40 rows land in `raw.events`; running it again leaves
40, twice over — the file source re-reads nothing (its cursor knows
the file), and `merge` upserts by key rather than appending.

The reference for the file SOURCE (formats, globs, per-extension
compression, CSV shape, S3 reading, hints, validation) and the
postgres DESTINATION — including the whole shared SQL option
vocabulary in prose: `merge_strategy` (delete_insert | upsert |
scd2), `hard_delete`, `dedup_sort`, `merge_scope`, and the full
`scd2` block. Those options read identically on duckdb and snowflake;
this file is where each is explained.

---

## `oracle-to-jsonl` — Oracle → jsonl, INCREMENTALLY

```sh
docker compose -f examples/oracle-to-jsonl/compose.yaml up -d
rdlt run examples/oracle-to-jsonl/pipeline.yaml
```

The compose file starts an Oracle Free seeded with 250 employees
(first start takes a minute or two). One more thing is needed on the
machine that RUNS rdlt: **Oracle Instant Client**, loaded at runtime
(nothing is needed to build rdlt; its absence reports as `DPI-1047`).
It is a free download under Oracle's OTN licence — we cannot
redistribute it:

```sh
curl -LO https://download.oracle.com/otn_software/linux/instantclient/instantclient-basiclite-linuxx64.zip
unzip instantclient-basiclite-linuxx64.zip
export LD_LIBRARY_PATH=$PWD/instantclient_23_26:$LD_LIBRARY_PATH
# Fedora/RHEL: sudo dnf install libaio    Debian/Ubuntu: libaio1
```

**Verified:** run 1 reads 250 rows; run 2 reads **0** — the
`cursor: updated_at` + `write_mode: append` pairing reads only what
changed since the checkpoint. (`merge` is refused by a FILE
destination — files cannot update a row in place — which is why the
upsert demo lives in jsonl-to-postgres.) A row shows three deliberate
choices: `salary: 79704.41` is an exact decimal, not a rounded float;
`hired` keeps its TIME because Oracle's DATE carries one; and
`updated_at` arrives as the UTC instant. The cursor column must be
NOT NULL — Oracle sorts NULLs last, so a nullable cursor would
deliver those rows once and then silently skip them forever; it is
refused up front instead.

---

## `postgres-to-iceberg` — PostgreSQL → Apache Iceberg

```sh
docker compose -f examples/postgres-to-iceberg/compose.yaml up -d
# wait for: docker compose -f ... logs bootstrap -> "polaris bootstrap complete"
rdlt run examples/postgres-to-iceberg/pipeline.yaml
```

The compose file is the whole stack: seeded PostgreSQL, RUSTFS (S3),
Apache Polaris (REST catalog, pinned by digest — upstream publishes
no stable tag), and a one-shot bootstrap that creates the bucket,
catalog and grants.

**Verified, read back from the CATALOG rather than from rdlt's own
report:** orders = 1 snapshot, 5,000 rows, partitioned
`(status, identity)`; customers = 1 snapshot, 200 rows. A second run
reads 0 rows — cursors make append incremental, so re-running does
not duplicate.

The reference for the iceberg destination: catalog auth (oauth2 and
bearer), vended credentials vs an explicit `storage.s3` override,
per-stream `tables` with all seven partition transforms, `parquet`
tuning and `parts` sizing (rdlt's 128 MiB default becomes the table's
`write.target-file-size-bytes`).

---

## `jsonl-to-snowflake` — files → Snowflake, merged

The one example with no container: Snowflake is a service, so the
connection facts in the pipeline are placeholders to edit. Everything
else runs as written.

**Verified against a real account** (with the placeholders swapped
for credentials): 40 rows merged into `EVENTS`; a second run is a
clean no-op; `SELECT COUNT(*)` through Snowflake's own SQL API
answers 40.

The reference for the snowflake destination: all four auth methods
(key-pair, password + MFA passcode, OAuth token, PAT), the
account-identifier refusals (a URL or a full host is refused with a
pointer), warehouse/role/table_type/session_parameters/query_tag,
PrivateLink `host` override, staged-part sizing, and the shared SQL
merge options.

---

## Controlling how rows are grouped

THREE different things decide the shape of the output, and it is worth
keeping them apart. You normally touch one.

| you want | set |
|---|---|
| ~128 MB output files | `parts.target_bytes` on the destination |
| less memory, fewer writes | `batch_policy` |
| less work lost to a crash | `commit_policy` |

### Output file size: `parts`

Only the destination knows how big a file is, because only it has
encoded one. So this is a destination setting, not an engine one:

```yaml
destination:
  connector:
    id: io.rapidbyte.file
    config:
      path: out
      format: parquet
      parts:
        target_bytes: 134217728    # ~128 MB files (the default)
        roll_after_seconds: 900    # … or every 15 min, whichever first
```

The default is 128 MiB, so a source paging a few hundred rows at a
time still produces data-lake-sized files rather than one file per
page. Measured on 1.5M rows in a single commit: parts of 141.5 MB,
141.3 MB and 126.4 MB against that target. The pokemon example ships
with a scaled-down live demonstration (128 KiB target, two files of
147 KB and 104 KB).

Two honest caveats. Parts OVERSHOOT — a batch is never split, so a
part closes just after crossing its target, not at it; and with a
target BELOW one write's size, the floor on file size is simply the
write (the batch is delivered whole). `roll_after_seconds` fires only
when data arrives; there is no background timer, so a quiet stream
rolls at its next write or at its next commit.

`max_open_bytes` (default 512 MiB) is a safety valve rather than a
tuning knob. An open part lives in memory until it closes, and a
`partition_by` destination holds one per partition — without a ceiling
the footprint would be partitions × target. When the ceiling is
reached the largest open part is closed early, which costs file size,
not correctness. A ceiling below `target_bytes` is refused: the target
could never be reached, and you would never be told.

### Which destinations take `parts`

The ones that write files. `file`, `iceberg` and `snowflake` all
accept the block and honour it; `postgres` and `duckdb` REFUSE it,
because rows going into a table have no file whose size it could
describe. A refusal is deliberate — a setting quietly accepted and
never applied is worse than one that is rejected.

Two per-destination notes:

- **iceberg** hands `target_bytes` to the Iceberg library's own
  rolling file writer, i.e. the table property
  `write.target-file-size-bytes`. rdlt's 128 MiB applies rather than
  the library's 512 MiB default, so every destination writes the same
  size. `max_open_bytes` has nothing to bound there: the library
  streams each file out instead of accumulating it in memory.
- **snowflake** stages parts and then loads them with one `COPY`. The
  service's own guidance is 100-250 MB compressed per file for load
  parallelism, which the default sits inside.

### Write granularity: `batch_policy`

**How many rows the engine accumulates before each destination write.**
It is destination-agnostic: the engine does the accumulating, so the
same setting means the same thing to a file, a table or a warehouse.

```yaml
batch_policy:
  every_rows: 50000
  every_bytes: 134217728      # … or 128 MB in memory, whichever first
```

`every_bytes` counts the ARROW IN-MEMORY footprint, not the bytes
written. **It is a memory bound, not an output-size one** — if you
came here wanting 128 MB files, `parts.target_bytes` above is the
setting, not this one. Arrow reports allocated capacity — buffers grow
geometrically, and per-value offsets and validity bitmaps count too.
Measured on the pokemon stream: `every_bytes: 100000` gave 400-row
writes of ~73 KB.

Both thresholds are FLOORS, not targets: a source batch is never
split, so accumulation stops at the first batch to cross the line.
With 100-row pages you get multiples of 100.

Measured on the pokemon example, whose source still pages 100 rows at
a time: without it, 14 destination writes of 100 rows; with
`every_rows: 600`, **3 writes of 600/600/151**. Read granularity and
write granularity are separate decisions — and neither is FILE
granularity, which `parts` owns: the shipped example batches 400 rows
per write and still produces two files, each spanning writes.
Omitting `batch_policy` hands each source batch straight through.

### Durability: `commit_policy`

**How often work is committed** — a durability decision, not a
file-size one. A commit is the unit a crash can cost you and the point
a resume restarts from.

```yaml
commit_policy:
  every_bytes: 104857600      # 100 MB …
  every_seconds: 900          # … or every 15 minutes, whichever first
```

Both take any combination of thresholds and fire on whichever is
reached first. A `commit_policy` naming NO threshold is refused — it
would hold everything uncommitted until the run ended. An empty
`batch_policy` is fine and means "write straight through".

### The one interaction worth knowing

**Neither a batch nor a part spans a commit, so the commit cadence is
an upper bound on both.** Measured: the same 1.5M-row run that gave
141 MB parts under one commit gave **9.5 MB parts across 44 commits**
under the default cadence — the 128 MiB target never came close to
binding. If your files are smaller than you asked for, this is why.
The same holds one level down: set `batch_policy: {every_rows: 50000}`
while committing at every checkpoint, and if checkpoints arrive every
100 rows you will still get 100-row writes — the commit flushes what
has accumulated before it closes. To get large writes, and large
files, commits must be at least as coarse:

```yaml
batch_policy:  {every_rows: 50000}
commit_policy: {every_checkpoints: 500}
```

The cost is what a crash loses: a coarser commit policy means more
work replayed on resume. That is the trade the knob exists to let you
make.

A source also only checkpoints where it has a resumable position. The
pokemon stream declares no cursor, so it checkpoints once at the end —
which is exactly why its batching and part sizing work there with the
default commit policy: everything is one commit.

## Where to go next

- `rdlt run <pipeline>` ends with a summary; the full JSON report
  (rows, commits, retries, where it resumed from) lands on stdout
  when stdout is redirected, or wherever `--report <path>` points.
- Delete a `workdir` to force a fresh run; keep it to let a crashed
  run resume where it stopped.
- File-destination `out/` directories also hold `_rdlt_state.*.json`,
  `_rdlt_commits.*.json` and `_rdlt_manifest.*.json`. Those are
  rdlt's bookkeeping — what makes a re-run idempotent instead of
  duplicating. Leave them beside the data.
- `docker compose -f examples/<name>/compose.yaml down -v` resets an
  example's database entirely (the seed runs again on next start).
