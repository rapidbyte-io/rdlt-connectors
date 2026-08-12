# rdlt-connector-sqlcore

The shared merge-planning core for rdlt's SQL destinations. ONE core, so the
PostgreSQL and DuckDB destinations cannot drift apart in merge semantics.

## What it owns

| Surface | Responsibility |
|---|---|
| `options` | the destination options vocabulary and its validation — one YAML shape across every SQL destination |
| `plan` | the plan shapes: dedup and survivor ordering, scope replacement, strategy arms, hard-delete decisions, index plans |
| `MergeDialect` (trait, at the crate root) | the seam through which a destination owns SQL **text** and nothing else |

The division is the point. Deciding *which rows survive a merge* is
semantics and lives here; spelling `ON CONFLICT` versus `INSERT OR REPLACE`
is dialect and lives in the destination.

## Merge strategies

`delete_insert`, `upsert`, and `scd2` (slowly-changing dimensions with
validity columns), selectable destination-wide or per table, alongside
`hard_delete`, `dedup_sort`, and `merge_scope`.

## Why a change here is safe to make

The plan shapes are shared, and the PostgreSQL crate's golden-SQL suite pins
the emitted statements **byte for byte**. Any change here that alters
generated SQL fails there — so a refactor that was meant to be behaviour
preserving is either provably so, or it is caught immediately.
