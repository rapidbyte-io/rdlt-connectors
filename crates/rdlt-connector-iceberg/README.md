# rdlt-connector-iceberg

The Iceberg REST-catalog destination, second generation — born on the
rdlt connector sdk. Rows land as parquet data files committed through
Iceberg snapshots; exactly-once is snapshot-native (every commit
stamps its identity into the snapshot summary and replays converge on
the table's own history); pipeline state rides as properties of a
`_rdlt_state` marker table in the destination namespace.

## Quickstart

```yaml
catalog:
  uri: https://polaris.example.com/api/catalog
  warehouse: analytics
  auth:
    oauth2_client_credentials:
      client_id: loader
      client_secret: "…"
      scopes: [PRINCIPAL_ROLE:ALL]
namespace: raw.events
create_namespace: true
tables:
  events:
    partition_by:
      - {column: ts, transform: day}
```

`destination::Shell::from_yaml(text)` turns that document into a
running destination. Auth is exactly one of `oauth2_client_credentials`
or `bearer`. Without a `storage` block the catalog VENDS scoped object-
store credentials per table (the default and recommended path); an
explicit `storage.s3` block overrides it. `catalog.props` passes
properties through verbatim and wins over generated ones — the
escape hatch for non-secret keys (e.g. `uri`). The four
credential-bearing keys (`credential`, `token`, `s3.access-key-id`,
`s3.secret-access-key`) are refused at validate: `catalog.props` may
not be used to smuggle a secret past the typed auth fields.

## Semantics worth knowing

- **Append only.** Merge is refused by capability; Replace is a typed
  refusal (the underlying library exposes no overwrite transaction,
  and emulating one would not be atomic).
- **Additive evolution.** New nullable columns are added; contradictory
  drift (type conflicts, nested reshapes, new NOT-NULL demands) is a
  typed refusal. Partition specs are FIXED at table creation.
- **Replay durability = snapshot retention.** Redelivery detection
  reads the table's snapshot history; retention must outlive the
  redelivery window.
- Engine types map onto a CLOSED Iceberg table; `json` columns land as
  strings (Iceberg v2 has no JSON type).
- **A `workdir:` is required.** `publish` commits one table at a time
  (029 N2) — a mid-publish transient restart with no durable load id
  would mint a fresh one and re-append rows the first attempt already
  committed to the tables it reached. The destination declares
  `requires_durable_identity`, and the engine refuses to start a run
  against it without a workdir. With the workdir guaranteed, WAL
  replay under the ORIGINAL load id already converges (redelivery
  detection reads the snapshot history keyed on that identity), so no
  publish-internal retry was added on top of the refusal.

## Testing

Offline cells (the document corpus, classification, drift, retry) run
anywhere. Container cells boot their own Polaris + RUSTFS pair
(pinned images — Polaris by digest, RUSTFS by version tag — with `rdlt-test=1` labels) and SKIP visibly without
a runtime; the sdk conformance kit certifies the shell against that
fixture. The crash sweep is its own failpoints-gated binary in the
`make test TARGET=sweep` gate.
