# rdlt-connector-rest

Declarative REST source: one YAML/JSON document describes an API — auth,
pagination, record selection, incremental cursors, response handling,
parent-child stream composition — and the connector reads it into the
engine with typed errors for everything that can go wrong. It is also a
LIBRARY: named-API connectors (a Google-Search-Console-style wrapper) are
built from the same public pieces, inheriting the client, the paginators,
and the whole test discipline instead of re-implementing HTTP.

A pipeline's `rest:` blocks resolve to this crate's binary —
`rdlt-connector-rest`, built with `--features bin-serve`
(`make connector-bins` in-tree) and spawned per run by the host.
It is the second-generation rewrite of the original
`rdlt-connector-rest`, which it replaced wholesale: behavior, the config
document vocabulary, error classification, and operational semantics are
identical to the original; what changed is the Rust API — module paths,
type names, and internal structure. The design record is
`specs/026-rest-v2/plan.md`.

The perf posture: a stream without `records_path` passes response bytes
to the engine **byte-identical** (no parse/reserialize) — the flagship
REST→Postgres benchmark rides this path.

```yaml
source:
  rest:
    base_url: "https://api.example.com"
    auth:
      oauth2_client_credentials:
        token_url: "https://auth.example.com/token"
        client_id: my-client
        client_secret: "${SECRET}"
        scopes: [read]
    headers: {user-agent: rdlt}
    min_request_interval_ms: 100
    streams:
      - name: repos
        path: /repos
        pagination: {type: page}
        incremental: {cursor_field: updated_at, start_param: since}
      - name: issues
        path: /repos/{owner}/{repo}/issues
        records_path: data.items[*]
        pagination: {type: link_header}
        parent:
          stream: repos
          placeholders: {owner: owner, repo: name}
          include: [name]
        response_actions:
          - {status: 404, action: end_stream}
```

Entry points: `source::Rest::from_yaml(…)`, `source::Config::from_yaml` /
`from_json` / `from_value` (the embedder seam — platforms holding configs
as JSON documents pass `serde_json::Value` directly) + `source::Rest::new`.
The generated JSON schema (`source::config_schema()`) validates exactly
what the parser accepts. All validation runs eagerly at parse — selector
syntax, alias exclusivity, parent linkage — a bad document never reaches
the network.

## Source-level options

| Field | Type | Default | Description |
|---|---|---|---|
| `base_url` | URL | required | Joined with each stream's `path`. Relative `next_url` pages resolve against it. |
| `auth` | scheme block | `none` | See Auth schemes. |
| `headers` | map | `{}` | Sent with every request, merged UNDER stream headers (same name → the stream's value wins, sent exactly once). |
| `params` | map | `{}` | Query params on every request, merged UNDER stream params the same way. |
| `max_concurrency` | int ≥ 1 | `1` | Concurrent child sequences during parent-child fan-out. `1` = strictly sequential. `0` is a typed error. Plain (parentless) streams always read sequentially. |
| `min_request_interval_ms` | ms | `0` | Politeness floor: at least this long between request sends, across ALL streams and children of the source. |
| `retry_after_cap_secs` | secs | `300` | A 429/503 carrying `Retry-After ≤ cap` is honored in-source — **one** wait, one retry per request. A 429 beyond the cap (or a second rate-limit) surfaces as a typed `RateLimited` error to the engine's retry budget; a beyond-cap 503 surfaces as its classification, `Transient`. The source never free-loops. |
| `request_timeout_secs` | secs ≥ 1 | `300` | Read deadline per request: the longest wait for the server to produce MORE bytes (resets on progress, so a large transfer that keeps moving never dies). `0` is refused — no configuration produces an unbounded wait. |
| `max_pages` | int | `10000` | Per-sequence page guard; exceeding it is a typed error naming the stream (raise it for genuinely long streams). |
| `streams` | list | required, non-empty | See Stream options. |

## Auth schemes

Externally tagged: `auth: {bearer: {token: …}}` (YAML singleton-map and
JSON alike; the legacy tagged spelling `auth: !bearer` also still
parses — frozen). Every credential field is a `Secret` — `Debug`/`Display`
render `***`, and the test suite grep-proves that no config/source/error
rendering ever contains a secret substring.

| Scheme | Fields | Behavior |
|---|---|---|
| `none` | — | Default; requests go bare. |
| `bearer` | `token` | `Authorization: Bearer <token>` on every request. |
| `header` | `name`, `value` | Arbitrary header credential. |
| `basic` | `username`, `password` | HTTP Basic. |
| `api_key` | `name`, `key`, `location: header\|query` (default `header`) | Named credential as a header or query param. |
| `oauth2_client_credentials` | `token_url`, `client_id`, `client_secret`, `scopes` (`[]`), `audience` (optional), `expiry_margin_secs` (`60`) | Client-credentials grant: token fetched lazily on first use, cached, refreshed `expiry_margin_secs` before expiry, single-flight (one fetch even under concurrent children). A 401 on a data request drops the cache and re-fetches **once**; a second 401 is fatal (wrong credentials, never a loop). Token-endpoint 5xx is transient (engine budget), 429 is `RateLimited` with the server's Retry-After attached, other 4xx fatal. |

## Stream options

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Stream/table name. |
| `path` | string | required | Joined onto `base_url`. May carry `{placeholder}` tokens **only** when a `parent` block declares them (typed error otherwise). |
| `method` | `get` \| `post` | `get` | POST is for search-style endpoints; pagination params go INTO the body. |
| `body` | JSON value | absent | POST body template (`body` without `method: post` is a typed error). `{placeholder}` substitution applies inside strings under a `parent`. |
| `params` | map | `{}` | Per-stream query params (merged OVER source `params`); `{placeholder}` substitution applies. |
| `headers` | map | `{}` | Per-stream headers (merged OVER source `headers`). |
| `records_path` | selector | absent | Where the records array lives: dot paths + `[*]` wildcards + `[N]` indices (`data.items[*].payload`). **Absent = the body IS the records array, streamed byte-identical (the perf path).** Unsupported syntax is a typed error at parse naming the subset; a non-matching path is a typed error naming the path and the response's top-level keys — except a wildcard over an existing EMPTY array, which is a legitimately empty page (the standard terminal-page shape). |
| `pagination` | family block | `none` | See Pagination families. |
| `incremental` | block | absent | See Incremental. |
| `cursor_field`, `cursor_param` | strings | absent | FROZEN legacy aliases for `incremental.cursor_field`/`start_param` — old documents parse unchanged. Set together; mixing them with the block is a typed error. |
| `response_actions` | list | `[]` | See Response actions. |
| `parent` | block | absent | See Parent-child. |
| `primary_key` | [column] | absent | Declared key for merge identity downstream. |
| `type_hints` | map column → type | `{}` | Logical-type overrides: `bool`, `int64`, `float64`, `utf8`, `timestamp_tz`, `date`, `time`, `uuid`, `json`. |

## Pagination families

`pagination: {type: <family>, …}`. Two guards protect **every** family: a
page that would repeat ANY earlier request of the sequence byte-for-byte
is a typed error (the API is not advancing — adjacent repeats and A→B→A
cycles alike), and `max_pages` bounds runaways. There
is deliberately **no auto-detection** — a wrong family fails typed, never
guesses.

| Family | Fields (defaults) | Termination |
|---|---|---|
| `none` | — | Single request. |
| `page` | `page_param` (`page`), `start` (`1`), `total_pages_path` / `total_count_path` (optional stop, mutually exclusive) | Empty page — or the declared total, which stops WITHOUT the extra empty-page request. |
| `offset` | `offset_param` (`offset`), `limit_param` (`limit`), `page_size` (required), `total_count_path` (optional) | Short page, or the declared count. |
| `cursor` | `cursor_path` (selector into the body), `cursor_param` | Cursor absent/null. The value rides the query for GET (and body-less POST) and the body for POST. |
| `header_cursor` | `header`, `cursor_param` | Response header absent. |
| `next_url` | `next_url_path` (selector) | Absent/null. Absolute URLs followed verbatim; relative resolved against `base_url`. |
| `link_header` | — | RFC5988 `Link: <…>; rel="next"` absent. |

Custom schemes: implement the public `read::paginate::Paginator` trait
(`initial_params()` → the first request's params,
`decide(&paginate::Context)` → `Done` / `NextParams` / `NextUrl`) — the
same seam the built-in families compile to.

## Incremental

```yaml
incremental:
  cursor_field: updated_at   # field observed in records (required)
  start_param: since         # resume value sent as ?since=<cursor>
  end_param: until           # optional closed window …
  end_value: "2026-07-01"    # … end_param requires end_value (typed)
  initial_value: "2020-01-01" # first-run lower bound
```

Mechanics: the max observed `cursor_field` value is checkpointed AFTER
the rows it covers (a crash replays only the tail); resume sends the
committed cursor as `start_param`. Parentless streams checkpoint per
page; child streams checkpoint once, at feed end. Cursor ordering is
lexicographic — ISO-8601 timestamps and equal-width numerics order
correctly; other shapes are the operator's responsibility.

## Response actions

A declared allow-list, first match wins; anything undeclared keeps the
typed-error posture:

```yaml
response_actions:
  - {status: 404, action: end_stream}          # end cleanly, keep rows so far
  - {content_contains: quota_warn, action: ignore}  # page contributes nothing
  - {status: 403, content_contains: expired, action: error}  # both must hold
```

Matching is TYPED: `status` compares against the actual response status
(success and error responses alike), `content_contains` searches the
first 64KiB of the body — including error bodies — and an entry
declaring both requires both. Each entry needs at least one matcher, and
`status` must be a real HTTP status (100–599); both are typed at parse.
`action: error` is a fatal typed failure; `ignore` treats the page as
empty while body-driven paginators (`cursor`/`next_url`) still read the
ignored body's cursor — a mid-chain ignored page continues the chain, a
final one ends it.

## Parent-child

```yaml
parent:
  stream: repos                       # a declared, non-child stream
  placeholders: {owner: owner, repo: name}  # {token} → parent field (dot paths OK)
  include: [name]                     # embedded as _parent_name on child records
```

The parent stream is read first (a fresh pass, values-only buffering);
each parent record yields one child sequence with `{token}`s substituted
into path/params/body. Placeholder fields must be scalars (typed
otherwise); `include` fields land as `_parent_<field>` (collision with an
existing child field is typed). Child failures name the parent's
resolved values (`owner=acme, repo=ghost`). Fan-out runs up to
`max_concurrency` children at once. One level of nesting; validation
rejects undeclared parents, self-parents, deeper chains, and unused
placeholders.

## Error classification & politeness

Network/5xx → transient (the ENGINE retries within its budget); 429 →
`RateLimited` carrying the server's `Retry-After`; other 4xx → fatal.
In-source waits are bounded by construction: at most one Retry-After
wait and one auth re-fetch per request. `min_request_interval_ms` paces
all data requests of the source; the OAuth2 token endpoint shares the
read deadline but is deliberately not paced.

## Using it as a library

The composition surface is public: `source::Config`/`source::Rest`
(config generators — see the MiniHub example in
`tests/cases/test_children.rs`), the `read::paginate::Paginator` trait
for API quirks, `rdlt_connector::Secret` for credentials, and
`source::config_schema()` for embedders. A wrapper connector writes **no** HTTP.

## Verification records

`specs/026-rest-v2/plan.md` — this crate's design record: the frozen
surfaces, the rename ledger, and the review rounds.
`specs/014-rest-completeness/matrix.md` — parameter traceability, zero
uncited rows. `specs/014-rest-completeness/dlt-parity.md` — paginator/
auth/config mapping against dlt 1.29.0 with deliberate deviations named.
Crash points `rest.request` / `rest.decode` / `rest.checkpoint` swept
crash/rerun with exactly-once totals (`tests/sweep.rs`). Live cells
against PokeAPI run under `RDLT_NET=1` (skipped, never failed, without
it).
