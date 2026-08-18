# rdlt-connector-postgres

PostgreSQL **source** and **destination** for rdlt in one crate. Both
directions are feature-gated (`source` / `destination`, both on by default)
over three shared substrate modules — `tls` holds the connection-security
vocabulary, `session` turns a connection string into a live connection, and
one type rulebook serves every decode, parse, literal, and encode — so the
two halves cannot drift apart.

A pipeline's `postgres:` blocks resolve to this crate's binary —
`rdlt-connector-postgres`, built with `--features bin-serve`
(`make connector-bins` in-tree) and spawned per run by the host.
It is the second-generation rewrite of the original
`rdlt-connector-postgres`, which it replaced wholesale: behavior,
configuration vocabulary, type mapping, and operational semantics are
identical to the original; what changed is the Rust API — module paths, type
names, and internal structure — plus a destination that can be built from a
configuration document.

What the two directions do, in one paragraph: the source streams rows as
typed Arrow batches decoded **straight out of the binary COPY wire format**
— no JSON hop, no shredding, no type inference — and the destination writes
them back through binary COPY with encoders proven round-trip-equal against
those decoders. Incremental loads checkpoint **inside a table** and resume
there; CDC picks up inserts, updates, and deletes with exactly-once
outcomes; nothing fails quietly, because every failure is a typed error that
says which field, table, or column broke it. Each claim below is backed by a
conformance test, a crash sweep, or a recorded measurement.

The behavioral contracts are in `specs/`: `005-postgres-source`
(source-config, type-mapping), `006-postgres-completeness` (tls-policy,
type-hints, query-streams, merge-structured), `007` (cursor-lag,
connstring-portability, tls-client-auth), `008-postgres-dest-completion`
(dest-types, merge-strategies, scd2), `009-postgres-cdc` (cdc-protocol,
cdc-config, cdc-operability), `010-merge-refinements` (merge-refinements).
This crate's own design record — the seam split, the naming rules, and what
was deliberately frozen — is `specs/025-postgres-v2/plan.md`.

---

## Quick start

A pipeline is ONE YAML document: pipeline-wide settings, a source block, a
destination block. Run it with `rdlt run pipeline.yaml`.

```yaml
# pipeline.yaml — mirror two tables, incremental on one
pipeline: app-mirror
write_mode: {merge: {key: [id]}}

source:
  connector:
    id: io.rapidbyte.postgres
    config:
      conn: "postgresql://etl@db.internal/app?sslmode=verify-full&sslrootcert=/etc/ca.pem"
      tables:
        - name: orders
          cursor: {column: updated_at}
        - name: customers

destination:
  connector:
    id: io.rapidbyte.postgres
    config:
      conn: "host=warehouse user=loader password=… dbname=analytics"
      dataset: mirror
      merge_strategy: upsert
```

The arm's `config` can instead point at its own reusable file —
`config: source.yaml` — carrying **the same fields under the same
validation**; the value is either the inline document or the path, never
a mix. Every source example in this README works in either position.

Embedders build the same objects from Rust. Both directions take a document
or a struct, and every entry point runs the one validation path:

```rust
use rdlt_connector_postgres::{destination, source};

fn build() -> Result<(), Box<dyn std::error::Error>> {
    // Source: document in, validated connector or the exact mistake out.
    let _source = source::Postgres::from_yaml(
        r#"
conn: "postgresql://etl@db.internal/app"
tables:
  - name: orders
    cursor: {column: updated_at}
"#,
    )?;

    // Destination: the same document vocabulary the pipeline YAML carries…
    let _from_document = destination::Postgres::from_yaml(
        r#"
conn: "host=warehouse user=loader dbname=analytics"
dataset: mirror
merge_strategy: upsert
"#,
    )?;

    // …or the builder, when the values are already in hand.
    let _from_builder =
        destination::Postgres::new("host=warehouse user=loader dbname=analytics")
            .schema("mirror")
            .options(destination::DestinationOptions {
                merge_strategy: Some(destination::MergeStrategy::Upsert),
                ..Default::default()
            })?;

    Ok(())
}
```

`source::Postgres::{from_yaml, from_json, from_value, new}` and
`destination::Postgres::{from_yaml, from_json, from_value, from_config,
new}` are the full set. Both sides publish a JSON Schema
(`source::config_schema()`, `destination::config_schema()`) **generated from
the config structs** by schemars, so a platform's validation and this
crate's parser cannot disagree — and since the destination now has a
document form, a platform can render a destination form, not only a source
one.

Every public item has exactly one spelling, reached through its module:
there are no crate-root re-exports. The source connector is
`source::Postgres`, its configuration `source::Config`, its type-hint
vocabulary `source::config::TypeHint`, the TLS posture `tls::Policy` with
`tls::Mode`.

---

## Pipeline spec (CLI)

One pipeline per YAML file. Top-level fields:

| Field | Type / values | Default | Description |
|---|---|---|---|
| `pipeline` | string | required | Pipeline id — names engine state; keep it stable across runs (cursors and resume state key on it). |
| `workdir` | path | `.rdlt` | Engine working directory (WAL, state). |
| `write_mode` | `append` \| `replace` \| `{merge: {key: [...]}}` | `append` | Write disposition for every stream. `append` adds rows; `replace` truncates once per load then loads; `merge` converges to one row per key — required for the upsert/scd2 strategies, cursor-lag exact totals, and the CDC composition. |
| `source.connector` | `{id: io.rapidbyte.postgres, config: …}` | required | The source document, inline or a path — see the full reference below. |
| `destination.connector` | `{id: io.rapidbyte.postgres, config: …}` | required | Connection + options — see the full reference below. |

(Other connectors take the same arm under their own ids —
`io.rapidbyte.rest`, `io.rapidbyte.file`, `io.rapidbyte.duckdb`, … —
this README covers postgres.)

---

## Source configuration — full reference

One document, two carriers with identical fields and identical validation:
inline under the arm's `config:`, or a standalone YAML/JSON file referenced
by `config: path`. Unknown fields are rejected by both
the schema and the parser. A validation failure names the field, table, or
column that caused it.

### Top level

| Field | Type / values | Default | Description |
|---|---|---|---|
| `conn` | string | required | libpq-style connection string or URL (`host=… user=…` or `postgresql://…`). Parse failures are typed config errors up front, never retried. See **Connection strings** below for which parameters are honored. |
| `schema` | string | `public` | Reflection scope. All bare table names below resolve inside it; schema-qualified names in `tables` are rejected. |
| `include_views` | bool | `false` | Include views and materialized views in schema-wide discovery. A view listed by name under `tables:` is always included regardless. |
| `tables` | list of table entries | absent | Omit to discover **every** table in `schema`. Discovery excludes partition leaves and `INHERITS` children (rows arrive once through the parent — list a child explicitly to read it alone) and never discovers foreign tables. |
| `queries` | list of query streams | `[]` | Custom SQL as streams; see **Query streams**. |
| `tls` | TLS block | absent (= `prefer`) | TLS posture; see **TLS**. `verify_ca`/`verify_full` are expressible only here or via conn-string `sslmode`. Contradicting an explicit conn `sslmode` is a typed error naming both. |
| `cdc` | CDC block | absent | Log-based capture for every configured table; see **CDC**. Mutually exclusive with any table's `cursor` (typed, names the table). |
| `batch_target_bytes` | int > 0 | `8388608` (8 MiB) | The decoder cuts an Arrow batch once this many bytes are buffered. |
| `batch_max_rows` | int > 0 | `65536` | Secondary cut: maximum rows per batch. |

`tables` is three-valued and the third value matters: **present** means
exactly this list and nothing else, **absent** means discover everything in
`schema`. A pipeline that declares only `queries:` and leaves `tables` out
therefore also receives every discovered table alongside them; spell
`tables: []` to get the queries alone. The empty list is rejected in the two
cases where it would select nothing at all — with no `queries` declared, and
under a `cdc` block, since capture has no tables to capture.

### Table entry (`tables[]`)

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Bare table name (no schema qualifier). Listed twice = error. |
| `cursor` | cursor block | absent | Incremental loading; absent = snapshot stream (full re-read every run — pair it with the pipeline's `replace` write mode for mirrors). |
| `primary_key` | [string] | reflected PK | Overrides the reflected primary key — the dedup/merge identity. Present-but-empty is an error. Under CDC with `REPLICA IDENTITY FULL` the override also wins as the merge key; under default/index identity a mismatching override is a typed error (delete records carry only the identity columns). |
| `included_columns` | [string] | all | Load only these columns. Mutually exclusive with `excluded_columns`; empty list is an error. |
| `excluded_columns` | [string] | none | Load all but these columns. |
| `type_hints` | map column → hint | `{}` | Per-column overrides from a **closed** conversion table; see **Type hints**. Unknown columns or undefined (source → hint) pairs fail typed at open. |

### Cursor block (`tables[].cursor`)

Incremental loading with dlt-parity boundary semantics, plus checkpointed
resume from inside a table: a crash, a cancel, or a transient failure picks
up at the last **committed mid-table checkpoint** rather than at the top of
the table. The saved watermark is never lowered.

| Field | Type / values | Default | Description |
|---|---|---|---|
| `column` | string | required | Must exist in the selection and map to a cursor-capable type (ints, decimals, text, uuid, timestamps, date, time). Validated at open, before any data moves. |
| `initial_value` | typed literal string | absent | First-run lower bound (e.g. `"2026-01-01T00:00:00Z"`, `"1000"`). Absent = full initial load. |
| `boundary` | `inclusive` \| `exclusive` | `inclusive` | Resume semantics. `inclusive` (`>=`) re-fetches watermark-equal rows and dedups them source-side by primary key — or by whole-row hash on a table with no PK — which is what makes a non-unique cursor such as a timestamp safe. `exclusive` (`>`) skips the dedup entirely: correct only for strictly monotonic cursors like sequences. |
| `direction` | `max` \| `min` | `max` | Ascending (watermark = max seen) or descending. |
| `end_value` | typed literal string | absent | Optional upper bound — a read filter only, never resume state. |
| `end_bound` | `exclusive` \| `inclusive` | `exclusive` | Upper-bound semantics: `inclusive` makes `[start, end]` directly expressible. |
| `nulls` | `exclude` \| `include` \| `error` | `exclude` | NULL-cursor rows: filtered out, included on **every** run, or a typed data-contract failure naming stream + column (for pipelines where a NULL `updated_at` is a bug). |
| `lag` | duration or magnitude string | absent | Attribution window: every **resumed** run reaches this far back behind the watermark, catching rows committed late. Durations (`"90s"`, `"5m"`, `"2h"`, `"1d"`) for time cursors (whole days for `date`); plain magnitudes (`"1000"`, `"0.5"`) for numeric cursors. Requires an `inclusive` boundary and a primary key; pair it with the merge write mode for exact totals — under append the window re-delivers its rows each run, which is documented at-least-once. |

### Query streams (`queries[]`)

One stream per SQL statement. The server **describes** the schema (nothing
is inferred), and the statement always runs as `SELECT * FROM (sql) AS q`,
so subquery rules do the read-only enforcement. Incremental support is the
same as a table's.

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Stream name; must be unique across tables AND queries. |
| `sql` | string | required | The SELECT/CTE text. |
| `cursor` | cursor block | absent | Same semantics as table cursors, applied to the wrapped query. |
| `primary_key` | [string] | none | Declared key (there is nothing to reflect): drives dedup and merge. |
| `type_hints` | map | `{}` | Same closed vocabulary as tables. |

### Type hints (`type_hints`)

The string vocabulary, also enforced by the generated schema:
`bool`, `int64`, `float64`, `decimal(p,s)`, `utf8`, `binary`,
`timestamp_tz`, `timestamp_naive`, `date`, `time`, `uuid`, `json`.

The conversion table is **closed**: only documented (source type → hint)
pairs are defined — an unconstrained `numeric` → `decimal(12,4)` to recover
real decimality, say, or a text column → `timestamp_tz` through a strict
server-side cast. Anything outside the table is a typed error at open. A
hint can change whether a column is cursor-capable, and capability is
checked after hints apply.

### CDC block (`cdc`)

Log-based capture over logical replication with the built-in `pgoutput`
plugin — no third-party server extensions. Once this block is present
**every configured table** is captured through the slot, and a `cursor` on
any of them is a typed error. Query streams are untouched.

| Field | Type / values | Default | Description |
|---|---|---|---|
| `slot` | string | required | Replication slot name. Single consumer: a slot actively held elsewhere is a typed error naming the pid. |
| `publication` | string | required | Must cover every CDC table (preflighted; gaps are typed errors naming the missing tables — rdlt creates publications but never alters them). |
| `create_if_missing` | bool | `false` | Create slot and publication idempotently when absent. rdlt **never drops** either. |
| `mode` | `catchup` \| `tail` | `catchup` | `catchup`: consume the backlog up to the run-start WAL position, then finish (cron-able). `tail`: chunked catch-up loop until cancelled, checkpointing per chunk. |
| `idle_wait` | duration string | `"1s"` | Tail-mode quiet wait between chunks (`"1s"`, `"5m"`, `"2h"`, `"1d"` — durations only, no bare magnitudes). |
| `flag_column` | string | `_rdlt_deleted` | Deletion-flag column added to every CDC stream: NULL on insert/update rows, TRUE on deletes. Colliding with an existing column is a typed error. |
| `ack` | `auto` \| `off` | `auto` | `off` never advances the slot (debugging / fan-in staging) — the server then retains WAL indefinitely. |

What that buys, and what it asks for:

- **First run**: the slot is created first, then ONE `REPEATABLE READ`
  snapshot covers all CDC tables at once, so they are consistent with each
  other. Changes landing in the slot-to-snapshot window are applied twice
  and converge — a row changed inside it ends up present exactly once, in
  its final state.
- **Later runs**: per-table passes over a peeked WAL range. Checkpoints land
  only at transaction-commit positions. An update that changes the primary
  key emits delete(old) then insert(new), in that order.
- **Acks are conservative**: the slot's confirmed position only ever moves
  up to a destination-committed cursor, so it trails by one run. That costs
  hygiene, never correctness — but a long-lived tail accumulates WAL
  retention (a warning fires past 256 MiB). Cycle tail runs, or put
  catch-up on a cron, for pipelines meant to run for weeks.
- **Requirements**: `wal_level = logical`, and every table needs a usable
  replica identity — a primary key (the default identity), `REPLICA
  IDENTITY FULL`, or `USING INDEX`. Without one, a typed error names the
  table and the fix. Unchanged TOAST values are substituted under `FULL`;
  without it they fail typed, naming the column and the `ALTER` that
  resolves it. `TRUNCATE` on a published table is a typed error that spells
  out the recovery.
- **Recommended composition** (the CLI warns when it is missing):
  `write_mode: {merge: {key: […]}}` + destination `merge_strategy: upsert` +
  `hard_delete: <flag_column>`. With those three, a deleted source row
  actually disappears from the destination. Without hard-delete support the
  flag simply arrives as data — a soft delete, documented as such.
- **Observability**: replication lag (`lag_bytes`, and `lag_seconds` too
  when the server has `track_commit_timestamp = on`) is emitted once per
  completed run as a structured event on the `rdlt::cdc` tracing target.

### TLS (`tls`)

```yaml
tls:
  mode: verify_full          # disable | prefer | require | verify_ca | verify_full
  root_cert: /etc/ca.pem     # path or inline PEM; omit = platform trust store
  client_cert: /etc/c.pem    # mutual TLS: both-or-neither with…
  client_key: /etc/c.key     # …an unencrypted PKCS#8/RSA/SEC1 private key
```

| Mode | Encrypted | Chain verified | Hostname verified |
|---|---|---|---|
| `disable` | no | — | — |
| `prefer` (default) | when the server offers | no | no |
| `require` | always | **no** (libpq semantics) | no |
| `verify_ca` | always | yes | no |
| `verify_full` | always | yes | yes — **the production recommendation** |

A server rejecting the client credential is its own `ClientCert` failure,
kept distinct from our verification of the server. The destination takes the
identical policy type — `tls::Policy`, via `destination::Postgres::tls(…)`
or the CLI's `tls = {…}` — down the same connect path. In Rust the ladder is
`tls::Mode::{Disable, Prefer, Require, VerifyCa, VerifyFull}`; a `tls:`
block may keep or strengthen a connection string's `sslmode`, never quietly
weaken it.

### Connection strings

Existing libpq URLs work as they are: `sslmode=verify-ca|verify-full`,
`sslrootcert=` (`system` selects the platform store), and
`sslcert=`/`sslkey=` all translate into the TLS policy. When a conn
parameter and a `tls:` field disagree, the error names both. An unsupported
parameter is rejected **by name**, never as a bare parse failure. libpq's
implicit `~/.postgresql/*` file defaults are deliberately not emulated.
Every connection carries `application_name = rdlt` unless the string sets
its own.

### Type mapping (source → engine)

Lossless for the scalars that matter: `bool`, `int2/4/8` → int64, `float4/8`
→ float64, `numeric(p≤38,s)` → decimal(p,s), the text family → utf8, `bytea`
→ binary, `timestamp`/`timestamptz` at microsecond resolution with
`±infinity` saturating visibly, `date`, `time`, `uuid` → canonical text,
`json`/`jsonb` → JSON text. A numeric that is unconstrained or wider than 38
digits arrives as **text**, in full — never truncated to fit. Everything
else (arrays, enums, composites, ranges, `interval`, `inet`, `money`, `xml`,
…) comes through a documented-lossy textual or JSON conversion, and every
such column announces itself once per read as a structured `tracing::warn!`
on the `rdlt::lossy` target, so a representation change is visible without
scraping logs.

---

## Destination configuration — full reference

Two ways in, one validation path. As a document —
`destination::Postgres::from_yaml / from_json / from_value` over
`destination::Config` — or as a builder:
`destination::Postgres::new(conn).schema(dataset).tls(policy).options(opts)`.
The CLI's `destination: postgres:` block carries exactly the document's
fields. Options are validated where they are supplied (`options()` hands
back the error; the document constructors fold the same check into parsing)
and again at open against the live stream schema.

> **One vocabulary, every SQL destination**: `merge_strategy`,
> `hard_delete`, `dedup_sort`, `merge_scope`, and the `scd2` block are the
> SHARED merge core (`rdlt-connector-sqlcore`), re-exported here under their
> bare sqlcore names. They behave identically under `destination: duckdb:` —
> same YAML shape, same validation, same typed errors. DuckDB-specific
> notes: `crates/rdlt-connector-duckdb/README.md`.

### Connection

| Field | Type | Default | Description |
|---|---|---|---|
| `conn` | string | required | libpq-style connection string or URL — same parsing, portability rules, and typed rejections as the source (`application_name = rdlt` unless the string sets its own). |
| `dataset` | string | `public` | Target schema; created if missing. Engine bookkeeping tables (`_rdlt_state`, `_rdlt_commits`, `_rdlt_cleared`) and per-pipeline staging tables live here too. |
| `tls` | TLS block | absent (= `prefer`) | The SAME policy type and connect path as the source — see **TLS** above. `tls: {mode: verify_full, root_cert: /ca.pem}`. |

The builder spells `dataset` as `.schema(…)`, which is what Postgres calls
it; the document keeps `dataset`, which is what the pipeline vocabulary has
always called it.

Native types need **no configuration at all**: decimals land as
`numeric(p,s)`, JSON as `jsonb`, UUIDs as `uuid`, required columns as `NOT
NULL`. Values ride binary COPY the whole way. Schema migrations are additive
— new columns through `ADD COLUMN`, widenings through `USING` casts. Every
destination error carries the server's message and its SQLSTATE, and a COPY
data error names the column that failed.

### Destination-wide options (`DestinationOptions`)

| Field | Type / values | Default | Description |
|---|---|---|---|
| `merge_strategy` | `delete_insert` \| `upsert` \| `scd2` | `delete_insert` | How the merge write mode executes, for every table unless overridden. EXPLICITLY configuring it — destination-wide or per-table — under an append/replace write mode is a typed error; the unconfigured default never rejects. |
| `tables` | map table → per-table options | `{}` | Per-table overrides, below. |

The three strategies apply only under the pipeline's **merge** write mode;
append and replace are engine dispositions, not strategies.

- **`delete_insert`** — delete-then-insert by the merge identity, atomic
  inside one transaction. The default, and the only strategy valid for
  shredded (JSON) streams, where it replaces whole subtrees by root id.
- **`upsert`** — `INSERT … ON CONFLICT DO UPDATE`: a matched key updates in
  place, with no window in which the row is missing. Needs a keyed
  structured stream, and says so typed otherwise — a shredded stream's
  identity is a content hash, so the conflict would never fire. The unique
  index it depends on is created automatically; pre-existing duplicate keys
  fail typed, naming the columns.
- **`scd2`** — full version history: validity columns on the target, change
  detection by `IS DISTINCT FROM` (bookkeeping columns excluded), one
  boundary timestamp per commit unit, stable under redelivery.

### Per-table options (`tables.<name>`)

| Field | Type | Default | Description |
|---|---|---|---|
| `merge_strategy` | strategy | destination-wide value | Per-table override. |
| `hard_delete` | column name | absent | CDC-style deletion flag: a row whose flag fires **deletes its key** instead of merging (boolean columns compare `IS TRUE`, other types `IS NOT NULL`). The in-load survivor's flag is the one that decides. Root tables only (typed error on children); the column must exist; not valid with scd2. |
| `dedup_sort` | `{ column, order: asc\|desc }` | absent (= last-wins) | **Ordered in-load survivor selection**: when a single load carries several versions of one key, the version this column ranks first survives — `desc` = greatest wins, `asc` = least wins — instead of whichever arrived last. Values beat NULL; ties, and groups that are all NULL, fall back to the deterministic arrival-order last-wins. The survivor drives every downstream decision: the hard-delete flag, the upserted content, SCD2 change detection. `order` is required. Typed errors: a nonexistent column, the hard_delete flag, a merge-key column (constant within a group, so it could never order anything), shredded streams, non-merge write modes. |
| `merge_scope` | [column] | absent | **Scope replacement**: a non-unique column set, independent of row identity. A merge load deletes every target row whose scope appears among the delivered rows, then applies the batch — so undelivered rows in a delivered scope disappear, while untouched scopes stay exactly as they were. NULL is not a scope and matches nothing on either side. Scope columns are indexed automatically. The scoped **table's** feed must arrive in one commit unit; the rule is per-table, so another stream's checkpoint never trips it, and a split feed is a typed error pointing at the engine commit thresholds (a re-run converges). One recorded caveat: a scoped stream should checkpoint only at feed end, because a mid-feed checkpoint plus a crash inside that window resumes as a partial feed the destination cannot tell from a fresh load. Typed errors: nonexistent columns, the hard_delete flag, shredded streams, scd2-without-retire, non-merge write modes. |
| `scd2` | scd2 block | defaults | Below; valid only with `merge_strategy: scd2`, typed in both directions. |

Worked example:

```yaml
destination:
  connector:
    id: io.rapidbyte.postgres
    config:
      conn: "host=warehouse user=loader password=… dbname=analytics"
      dataset: mirror
      merge_strategy: upsert
      tables:
        orders:
          hard_delete: _rdlt_deleted
          dedup_sort: {column: seq, order: desc}
          merge_scope: [day]
        customers:
          merge_strategy: scd2
          scd2: {absent: retire}
```

### SCD2 block (`tables.<name>.scd2`)

| Field | Type / values | Default | Description |
|---|---|---|---|
| `valid_from` | column name | `_rdlt_valid_from` | Validity-start column added to the target (`TIMESTAMPTZ NOT NULL`). |
| `valid_to` | column name | `_rdlt_valid_to` | Validity-end column; `NULL` marks the active version. Must differ from `valid_from`; neither may collide with a stream column. |
| `absent` | `keep` \| `retire` | `keep` | What happens to active keys **absent** from a load: `keep` leaves them active (an incremental feed is partial by nature); `retire` closes them at the boundary (full-feed semantics). Retire needs the table's whole feed in a single commit unit — the same per-table rule as `merge_scope`, the same typed error, the same thresholds remedy. When the table also has a `merge_scope`, retirement is **scoped**: absent keys retire only inside delivered scopes. That combination requires `retire`; under `keep` the merge_scope would do nothing, so it is a typed error. |
| `active_record_timestamp` | RFC3339 timestamp | absent (= NULL marker) | The OPEN-version marker written into `valid_to` in place of NULL (e.g. `9999-12-31T00:00:00Z` — some BI tools cannot range-query NULLs). Must be zone-explicit RFC3339, since a zone-less literal would resolve against the session TimeZone (typed error), and must differ from `boundary_timestamp` (typed error). Active-version predicates treat NULL **and** the marker as open, so a table whose history predates the option keeps working. |
| `boundary_timestamp` | RFC3339 timestamp | absent (= transaction timestamp) | A caller-supplied boundary used for close/open/retire instead of the transaction timestamp. Same zone-explicit validation; never interpolated unvalidated. |

### Indexes

Merge identities get their supporting indexes automatically, under
deterministic names (`rdlt_ix_*`, and `rdlt_ux_*` for unique ones): the
identity index per strategy (unique for upsert), `(key…, valid_to)` for
SCD2's active-version lookups, and the scope columns for `merge_scope`.
Measured where it counts: 20.4× on the incremental-regime merge DELETE
(`benches/RESULTS.md`).

---

## Operational semantics worth knowing

- **Crash discipline**: exactly-once outcomes under kill or panic at every
  registered fail point — source, destination, and CDC — on both occurrence
  passes, verified by sweeps with armed-fire pins. Commits are idempotent by
  `(load_id, commit_seq)`, and state travels in the same transaction as the
  data it describes.
- **Error posture**: the source separates transient failures
  (connection-shaped — the engine backs off, retries, and resumes from
  committed state) from fatal ones (config-, auth-, or data-shaped, where
  retrying cannot help); the destination draws the same line and always
  carries SQLSTATE. Misconfiguration fails at parse or at open, before any
  data moves.
- **Memory is bounded** by the batch knobs whatever the table or transaction
  size — a 6.9 GB table streams through a 256 MiB process ceiling in the
  test suite.
- **Merge write mode** needs a declared `primary_key` for structured streams
  (a keyless structured stream is rejected at plan time); shredded streams
  merge by content identity, replacing subtrees.
- **What an open unit transaction costs.** Append and Replace rows COPY
  straight into their target inside one transaction per commit unit rather
  than landing in a stage table and being moved at publish, so every row is
  written once instead of twice. (Merge still stages: its arms join
  delivered rows against the target.) Three consequences are worth knowing
  before running rdlt against a busy database — none blocks a load; all are
  about what else the database can do meanwhile. A Replace target is locked
  for the whole load, not just the publish, because `TRUNCATE` takes ACCESS
  EXCLUSIVE and holds it to commit. Vacuum falls behind while a unit is
  open, since the transaction pins the oldest reclaimable row version across
  the whole database. And a stalled load holds both at once — commit cadence
  is the control, as shorter units mean shorter locks. Constraint violations
  surface during the COPY rather than at publish, so a bad row fails in the
  batch that carried it and is named there.

## Verification

`cargo nextest run -p rdlt-connector-postgres`. The destination side
covers native-type fidelity, strategy conformance, SCD2 history, and the
merge-refinement matrices; the source side covers conformance (the full type
matrix round-tripped against a real Postgres), incremental boundary
semantics, a differential property test pitting the decoder against an
independent driver reference, the drift matrix, the TLS matrix (five sslmode
levels × certificate scenarios, in both directions), query streams, config
schema round-trips, and CDC (equality cycle, boundary overlap, ack pin,
tail, the TOAST + identity matrix, lag capture). Every suite compiles
through one integration root, `tests/integration.rs`, as a
`cases/test_<noun>` module.

`--features failpoints` adds the crash sweeps and the memory ceiling test,
which the gate selects by name as their own binaries: `source_crash_sweep`,
`destination_crash_sweep`, `cdc_crash_sweep`, and `memory_bound`
(`RDLT_HEAVY=1` turns a missing prerequisite there into a hard failure
instead of a skip). Container-backed suites skip visibly rather than failing
when no container runtime is present — `fixtures::PostgresContainer::start()`
and `::start_for_cdc()` return `None` after printing a `SKIP` line.
Scoreboards and gated bars: `benches/RESULTS.md`.
