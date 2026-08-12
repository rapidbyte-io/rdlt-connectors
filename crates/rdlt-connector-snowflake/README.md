# rdlt-connector-snowflake

The Snowflake destination, second generation — born on the rdlt
connector sdk. Rows travel as parquet parts through the service's own
internal stage (`PUT` + `COPY INTO`), publishes run as sqlcore-planned
merges inside one transaction per commit unit, and exactly-once rests on
the unit and its receipt.

## Quickstart

```yaml
account: "MYORG-MYACCT"
user: LOADER
auth:
  key_pair:
    private_key: /etc/rdlt/keys/loader.p8
database: ANALYTICS
schema: RAW
warehouse: LOAD_WH
merge_strategy: upsert
```

`destination::Shell::from_yaml(text)` turns that document into a running
destination. Auth is exactly one of `key_pair` (recommended for
unattended use), `password`, `oauth_token`, or `pat`. The merge options
(`merge_strategy`, `tables`, hard delete, dedup sort, scd2) are the same
flattened vocabulary every rdlt SQL destination reads — and they are
validated at parse, naming the field to fix.

## The three service facts the design bends around

Measured, not assumed (022/023's research holds the probes):

1. **DDL auto-commits the open transaction** — so all schema work runs
   before a unit opens, and the unit's executor refuses DDL in code.
2. **Nothing enforces uniqueness** — merge correctness is the planned
   SQL's, and the live suites read every result back.
3. **Unquoted identifiers fold upper; quoted lower-case is a different
   object** — every identifier is emitted quoted upper case, which is
   what an unquoted user query resolves to.

## Testing

Offline cells (config vocabulary, DDL pins, statement economy, secret
hygiene) run anywhere. Live cells gate on credentials — environment
first, then `~/.config/rdlt/snowflake/` — and SKIP visibly without
them; the count-discipline record is the net. The crash sweep is its
own binary, run by hand with `--features failpoints -E
'binary(crash_sweep)'`, because it spends real account time.
