//! The read-before-write catalog image: a FRESH session's only way to
//! see what a PRIOR session already ensured. Feeds ONLY the widen
//! planner's `previous` parameter (031's S3 record; 028's
//! read-before-write catalog-image answer, re-derived here for the
//! schema shape rather than snapshot properties) — never the
//! drop/reorder guards in `load.rs::ensure_table`, which stay
//! session-scoped on purpose: cross-run column drift-BY-NAME is legal
//! (`test_conformance.rs::cross_run_column_drift_publishes_by_name`),
//! only a within-session drop or reorder is a defect.

use duckdb::params;
use rdlt_connector_sdk::spi::core::{
    schema::Column, schema::ColumnType, schema::Provenance, schema::TableSchema, types::LogicalType,
};
use rdlt_connector_sdk::spi::error::DestinationError;
use rdlt_connector_sqlcore::names;

use super::client::classify;
use super::schema::{EnsureStatement, quote, sql_type};

/// The live table's schema, mapped back into [`ColumnType`] — `None`
/// when the table does not exist yet, OR exists but every column's
/// physical type was unmapped by [`column_type_of`] (an unrecognized
/// spelling omits its column rather than guessing, so an all-unmapped
/// table looks empty here too — nothing for the widen planner to
/// compare against either way).
///
/// Column order comes from `ordinal_position`, matching how this
/// destination's own DDL only ever appends — so the image's column
/// order agrees with a session that had ensured the same table.
pub(super) fn live_schema(
    conn: &duckdb::Connection,
    table: &TableSchema,
) -> Result<Option<TableSchema>, DestinationError> {
    // Scoped to THIS database's `main` schema (F3, 2026-08 review round
    // 1): unscoped, `information_schema.columns` also answers for a
    // same-named TEMP table or a same-named table in an ATTACHed
    // database — either would splice a foreign table's columns into
    // this table's image, and a temp table in particular can carry a
    // stale shape from a crashed session's own stage naming collision.
    let mut stmt = conn
        .prepare(
            "SELECT column_name, data_type FROM information_schema.columns \
             WHERE table_catalog = current_database() AND table_schema = 'main' \
             AND table_name = ? ORDER BY ordinal_position",
        )
        .map_err(classify)?;
    let mut rows = stmt
        .query(params![table.table.as_str()])
        .map_err(classify)?;
    let mut columns = Vec::new();
    while let Some(row) = rows.next().map_err(classify)? {
        let name: String = row.get(0).map_err(classify)?;
        let duckdb_type: String = row.get(1).map_err(classify)?;
        // An unmapped type is OMITTED, never guessed: inventing a shape
        // here could plan a spurious widen every run against a column
        // that never changed — exactly what the round-trip test below
        // guards against.
        let Some(column_type) = column_type_of(&duckdb_type) else {
            continue;
        };
        columns.push(Column {
            name,
            column_type,
            // Physically unrecoverable, and unused by the widen
            // planner either way (`ensure::schema_steps` compares
            // `column_type` alone): every column this destination's
            // own DDL creates is nullable — `create_table_sql` never
            // emits NOT NULL — and provenance is metadata the catalog
            // carries no trace of.
            nullable: true,
            provenance: Provenance::Inferred,
        });
    }
    if columns.is_empty() {
        return Ok(None);
    }
    Ok(Some(TableSchema {
        table: table.table.clone(),
        parent: table.parent.clone(),
        columns,
    }))
}

/// The pre-planning FILTER (2026-08 review round 1, F1 CRITICAL + F2
/// MAJOR): run once on every fetched image, BEFORE it ever reaches the
/// widen planner as `previous`. `schema_steps` plans a widen from a
/// bare `column_type != column_type` inequality — it has no notion of
/// DIRECTION, so left unfiltered the raw image turns two different
/// classes of "different" into a real `ALTER … SET DATA TYPE`:
///
/// 1. Two SQL spellings of the SAME physical type. `Uuid` and `Utf8`
///    both render `VARCHAR` (F2) — every "Uuid changed" the raw image
///    reported was really "nothing changed", yet it planned a widen
///    (and, on an upsert-keyed `Uuid` column, a full unique-index
///    rebuild) EVERY SINGLE RUN.
/// 2. A NARROWING or otherwise incompatible cross-run drift (F1): this
///    run's inferred type is not reachable by widening the catalog's
///    existing type (e.g. a prior run committed text, this run's batch
///    happens to be all-numeric). Planning `SET DATA TYPE` here is
///    actively dangerous — DuckDB CASTS existing rows, so a value like
///    `"01234"` silently and irreversibly becomes `1234`, and the run
///    REPORTS SUCCESS while corrupting already-committed data. Where
///    the cast is outright impossible, the ALTER instead FAILS a run
///    that used to succeed (pre-image, no widen was ever planned
///    cross-run, so the mismatch was resolved at PUBLISH by an
///    implicit widening cast on the STAGE leg alone — never against a
///    committed row).
///
/// Filtering by CONSTRUCTION — compare the RENDERED sql type, then
/// consult a small explicit widening lattice — kills both classes
/// entirely rather than enumerating every known-lossy pair one at a
/// time: a class of bug enumeration cannot find by definition.
///
/// Session-memory `previous` (this session's OWN prior `ensure_table`
/// call on this table) is NEVER passed through this filter — the
/// engine's evolution contract guarantees within-run changes are
/// widen-only already, so there is nothing to filter there. Only a
/// CROSS-run image can disagree with the engine's own contract, because
/// it comes from the catalog rather than from the engine, and that
/// asymmetry is exactly why this filter exists at all.
pub(super) fn prefilter(mut image: TableSchema, def: &TableSchema) -> TableSchema {
    for d in &def.columns {
        let Some(i) = image.columns.iter_mut().find(|c| c.name == d.name) else {
            continue; // not ensured yet — schema_steps adds it, never a widen
        };
        let physically_same = sql_type(&i.column_type, false) == sql_type(&d.column_type, false);
        let genuine_widen = !physically_same && is_widening(&i.column_type, &d.column_type);
        if !genuine_widen {
            // Either physically identical already (adopt `d`'s type so
            // the planner's equality check sees no difference at all —
            // this is what kills the Uuid/Utf8 rebuild), or a
            // narrowing/incompatible drift the engine's evolution
            // contract never intended for the destination to enact.
            // SKIP, don't refuse: refusing would turn a pipeline that
            // published successfully yesterday into a hard failure
            // today, and silently rewriting committed rows is worse
            // than either. Adopting `d`'s type here makes the planner
            // see no difference, so `ensure` emits no ALTER — a real
            // mismatch (if any) surfaces exactly where it always did
            // before this feature existed: an implicit cast at PUBLISH
            // time, on the STAGE leg, never against a committed row.
            i.column_type = d.column_type.clone();
        }
        // else: a genuine widen — leave `i` at the catalog's OLD type
        // so `schema_steps` sees the mismatch and plans the ALTER.
    }
    image
}

/// Does `Binary` appear ANYWHERE in `t` — as the scalar itself, as a
/// `ScalarList` item, or inside any field of a `Struct`, at any depth?
/// (N3, review round 3: a scalar-only check let `ScalarList { item:
/// Binary }` — a real `BLOB[]` column `column_type_of` genuinely
/// produces — fall through [`is_widening`]'s `anything → Utf8/Json`
/// absorption rule, reopening N3's class one type-shape down.)
///
/// STRUCTURAL, not enumerated: the match is exhaustive over every
/// [`ColumnType`] variant (no wildcard arm), so a future variant this
/// enum gains forces a compile error here rather than silently
/// defaulting to "no Binary inside" and reopening the same class a
/// third time.
fn has_binary(t: &ColumnType) -> bool {
    match t {
        ColumnType::Scalar { scalar } => matches!(scalar, LogicalType::Binary),
        ColumnType::ScalarList { item } => matches!(item, LogicalType::Binary),
        ColumnType::Struct { fields } => fields.iter().any(|f| has_binary(&f.column_type)),
    }
}

/// THE widening policy this destination honors across a run boundary —
/// a minimal, DELIBERATE lattice, not the engine's general one
/// (`rdlt_core::types::widen`): the direction here is catalog type →
/// incoming type, and getting it wrong either corrupts committed data
/// or fails a run that used to succeed (see [`prefilter`]), so this
/// stays small and explicit rather than reusing a lattice designed for
/// a different question (inference's own type-of-a-value join) — and,
/// per review round 2, narrower than that lattice on THREE edges where
/// the engine's own version is either value-conditional or measures a
/// different quantity than a physical ALTER can honor.
///
/// - `Binary` ANYWHERE in the source type ([`has_binary`]) → NOTHING,
///   not even `Utf8`/`Json` (N3, MINOR, review round 2; widened to
///   STRUCTURAL in review round 3 after `ScalarList { item: Binary }`
///   — a real `BLOB[]` column `column_type_of` genuinely produces —
///   was proven live to fall through the original scalar-only guard
///   and absorb into `Utf8`/`Json` anyway). The engine's own rule is
///   "Binary is textable only via encodings we refuse to invent
///   silently" (`rdlt_core::types`, the `(Binary, _) => Json` join
///   arm) — its escalation to `Json` goes through the library's OWN
///   chosen encoding at shred time. An ALTER's `CAST` is a different,
///   unchosen encoding, so ANY source type carrying `Binary` —
///   scalar, inside a `ScalarList`, or nested inside a `Struct` field,
///   at any depth — is excluded; a cross-run mismatch involving it
///   SKIPS (see [`prefilter`]) rather than casting through some
///   encoding this function never decided on.
/// - anything ELSE → `Utf8`: text is always a safe destination for any
///   value this connector can already render (same simplification
///   `sql_type` already makes for `Uuid`).
/// - anything else → `Json`: the top of the engine's own lattice.
/// - `Int64` → `Float64` is DELETED (N1, CRITICAL, review round 2).
///   rdlt-core's own lattice only takes this edge after a per-VALUE
///   check (`int64_fits_in_f64`, exact only within ±2^53) — the engine
///   value-checks this edge at shred time; a destination widening
///   committed rows cannot, so the edge is absent here — the diff
///   skips and publish-time casting keeps yesterday's behavior. Proven
///   live: `9007199254740993` (2^53 + 1) committed as `BIGINT`,
///   `ALTER … SET DATA TYPE DOUBLE` casts it to `…992` and the run
///   still reports success — silent, irreversible corruption.
/// - `Int64` → `Decimal { precision, scale }` (N2, IMPORTANT, review
///   round 2) iff its INTEGER CAPACITY (`precision` minus `scale`) is
///   at least 19: an `i64` needs 19 INTEGER digits (up to
///   `9223372036854775807`), so a decimal with plenty of total
///   precision but too little of it left of the point does not
///   actually hold every `i64` value.
/// - `Decimal { p1, s1 }` → `Decimal { p2, s2 }` (N2) iff `s2` is at
///   least `s1` AND the target's integer capacity (`p2` minus `s2`) is
///   at least the source's (`p1` minus `s1`) — INTEGER CAPACITY, not
///   raw precision. This is `rdlt-core`'s own `join_decimals`
///   semantics (max integer digits, max scale, never shrinking
///   either): a decimal with MORE total digits but FEWER integer
///   digits is not actually wider. Violating either N2 rule is LOUD (a
///   probed Conversion Error, data survives) but turns a working
///   pipeline into a deterministic hard failure — the skip arm is the
///   behavior-preserving one.
///
/// Everything else is `false` — including the reverse of every rule
/// above, and anything not named here at all.
fn is_widening(from: &ColumnType, to: &ColumnType) -> bool {
    use LogicalType::{Decimal, Json, Utf8};

    if has_binary(from) {
        return false;
    }
    if matches!(
        to,
        ColumnType::Scalar {
            scalar: Utf8 | Json
        }
    ) {
        return true;
    }
    match (from, to) {
        (
            ColumnType::Scalar {
                scalar: LogicalType::Int64,
            },
            ColumnType::Scalar {
                scalar: Decimal { precision, scale },
            },
        ) => precision.saturating_sub(*scale) >= 19,
        (
            ColumnType::Scalar {
                scalar:
                    Decimal {
                        precision: from_p,
                        scale: from_s,
                    },
            },
            ColumnType::Scalar {
                scalar:
                    Decimal {
                        precision: to_p,
                        scale: to_s,
                    },
            },
        ) => to_s >= from_s && to_p.saturating_sub(*to_s) >= from_p.saturating_sub(*from_s),
        _ => false,
    }
}

/// Which `DROP INDEX` statements must run BEFORE `table_ddl`'s ALTER,
/// so a widen landing on the upsert arbiter's merge-key column does not
/// hit DuckDB's index-dependency refusal (Task 15 probe). Both name
/// spellings are dropped (2026-08 review round 1, F5): the CURRENT
/// unique-prefixed name, and the LEGACY plain-formula spelling
/// `merge_ddl` itself still carries a drop-then-recreate for — an old
/// database's arbiter index may be named either way, and `merge_ddl`'s
/// own legacy drop runs too LATE (after `table_ddl`) to clear the way
/// for this ALTER.
///
/// Pure and independently testable: this is the ECONOMY GUARANTEE an
/// unchanged table pays nothing for — steady state (`planning_previous`
/// already equal to `def`, which is exactly what [`prefilter`]
/// produces for an unchanged live table) returns an EMPTY list.
pub(super) fn pre_alter_index_drops(
    planning_previous: Option<&TableSchema>,
    def: &TableSchema,
    merge_statements: &[EnsureStatement],
) -> Vec<String> {
    let Some(prev) = planning_previous else {
        return Vec::new();
    };
    let widened: Vec<&str> = def
        .columns
        .iter()
        .filter(|d| {
            prev.column(&d.name)
                .is_some_and(|old| old.column_type != d.column_type)
        })
        .map(|d| d.name.as_str())
        .collect();
    if widened.is_empty() {
        return Vec::new();
    }
    let mut drops = Vec::new();
    for (_, unique_index) in merge_statements {
        if let Some(cols) = unique_index
            && cols.iter().any(|c| widened.contains(&c.as_str()))
        {
            drops.push(names::drop_index_sql(true, def.table.as_str(), cols, quote));
            drops.push(names::drop_index_sql(
                false,
                def.table.as_str(),
                cols,
                quote,
            ));
        }
    }
    drops
}

/// The inverse of [`super::schema::sql_type`]'s target-leg rendering
/// (`is_stage: false` — a fresh session only ever reads the target
/// back). TOTAL: an unrecognized spelling returns `None` rather than
/// guessing, so a future logical type this function doesn't know yet
/// never plans a widen against a column it can't actually interpret.
///
/// `Uuid` is deliberately never RETURNED here: `sql_type` already lowers
/// it to the same `VARCHAR` as `Utf8` ("text for portability with the
/// hex `_rdlt_id` convention"), so the physical catalog cannot tell the
/// two apart. `VARCHAR` maps back to `Utf8`, the far more common shape —
/// [`prefilter`] is what makes that harmless: it adopts the INCOMING
/// schema's `Uuid` type back onto the image the moment it sees the two
/// render the same physical SQL, so a genuinely `Uuid`-hinted column
/// pays no widen, no index rebuild, at steady state.
fn column_type_of(duckdb_type: &str) -> Option<ColumnType> {
    if let Some(item) = duckdb_type.strip_suffix("[]") {
        return scalar_type_of(item).map(|item| ColumnType::ScalarList { item });
    }
    if let Some(body) = duckdb_type
        .strip_prefix("STRUCT(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let mut fields = Vec::new();
        for field in split_top_level(body) {
            let (name, rest) = split_identifier(field.trim())?;
            let column_type = column_type_of(rest.trim())?;
            fields.push(Column {
                name,
                column_type,
                nullable: true,
                provenance: Provenance::Inferred,
            });
        }
        return Some(ColumnType::Struct { fields });
    }
    scalar_type_of(duckdb_type)
        .map(ColumnType::scalar)
        .or_else(|| decimal_type_of(duckdb_type).map(ColumnType::scalar))
}

fn scalar_type_of(duckdb_type: &str) -> Option<LogicalType> {
    Some(match duckdb_type {
        "BOOLEAN" => LogicalType::Bool,
        "BIGINT" => LogicalType::Int64,
        "DOUBLE" => LogicalType::Float64,
        "VARCHAR" => LogicalType::Utf8,
        "BLOB" => LogicalType::Binary,
        "TIMESTAMP WITH TIME ZONE" => LogicalType::TimestampTz,
        "TIMESTAMP" => LogicalType::TimestampNaive,
        "DATE" => LogicalType::Date,
        "TIME" => LogicalType::Time,
        "JSON" => LogicalType::Json,
        _ => return None,
    })
}

fn decimal_type_of(duckdb_type: &str) -> Option<LogicalType> {
    let body = duckdb_type
        .strip_prefix("DECIMAL(")
        .and_then(|s| s.strip_suffix(')'))?;
    let (precision, scale) = body.split_once(',')?;
    Some(LogicalType::Decimal {
        precision: precision.trim().parse().ok()?,
        scale: scale.trim().parse().ok()?,
    })
}

/// Split a `STRUCT(...)` body on its top-level commas — depth- and
/// quote-aware, so a nested `STRUCT(...)` or a `DECIMAL(p,s)` field
/// type never gets cut in the middle.
fn split_top_level(body: &str) -> Vec<&str> {
    let mut depth = 0i32;
    let mut in_quotes = false;
    let mut start = 0;
    let mut parts = Vec::new();
    for (i, c) in body.char_indices() {
        match c {
            '"' => in_quotes = !in_quotes,
            '(' if !in_quotes => depth += 1,
            ')' if !in_quotes => depth -= 1,
            ',' if !in_quotes && depth == 0 => {
                parts.push(&body[start..i]);
                start = i + c.len_utf8();
            }
            _ => {}
        }
    }
    parts.push(&body[start..]);
    parts
}

/// One `"name" TYPE` (or bare `name TYPE` when the name needs no
/// quoting) field spec's leading identifier — the mirror of
/// [`rdlt_connector_sqlcore::quote_identifier`]'s `"` doubling — and
/// the type text that follows it.
fn split_identifier(field: &str) -> Option<(String, &str)> {
    if let Some(rest) = field.strip_prefix('"') {
        let mut name = String::new();
        let mut chars = rest.char_indices();
        while let Some((i, c)) = chars.next() {
            if c != '"' {
                name.push(c);
                continue;
            }
            if rest[i + 1..].starts_with('"') {
                name.push('"');
                chars.next();
            } else {
                return Some((name, rest[i + 1..].trim_start()));
            }
        }
        None
    } else {
        let idx = field.find(char::is_whitespace)?;
        Some((field[..idx].to_owned(), field[idx..].trim_start()))
    }
}

#[cfg(test)]
mod tests {
    use rdlt_connector_sdk::spi::core::{commit::WriteMode, id::TableName};
    use rdlt_connector_sqlcore::{DestinationOptions, MergeStrategy};

    use super::super::schema::{create_table_sql, merge_ddl, table_ddl};
    use super::*;

    fn memdb() -> duckdb::Connection {
        duckdb::Connection::open_in_memory().expect("bundled in-memory duckdb")
    }

    fn col(name: &str, column_type: ColumnType) -> Column {
        Column {
            name: name.to_owned(),
            column_type,
            nullable: true,
            provenance: Provenance::Inferred,
        }
    }

    /// One of every [`ColumnType`] shape this connector's own DDL can
    /// produce — every [`LogicalType`] except `Uuid` (see
    /// `column_type_of`'s doc comment: it is physically indistinguishable
    /// from `Utf8` and is EXPECTED not to round-trip), plus a struct and
    /// a scalar list.
    fn schema_with_every_column_type(table: &str) -> TableSchema {
        TableSchema {
            table: TableName::new(table),
            parent: None,
            columns: vec![
                col("c_bool", ColumnType::scalar(LogicalType::Bool)),
                col("c_int64", ColumnType::scalar(LogicalType::Int64)),
                col("c_float64", ColumnType::scalar(LogicalType::Float64)),
                col(
                    "c_decimal",
                    ColumnType::scalar(LogicalType::Decimal {
                        precision: 18,
                        scale: 4,
                    }),
                ),
                col("c_utf8", ColumnType::scalar(LogicalType::Utf8)),
                col("c_binary", ColumnType::scalar(LogicalType::Binary)),
                col(
                    "c_timestamptz",
                    ColumnType::scalar(LogicalType::TimestampTz),
                ),
                col(
                    "c_timestampnaive",
                    ColumnType::scalar(LogicalType::TimestampNaive),
                ),
                col("c_date", ColumnType::scalar(LogicalType::Date)),
                col("c_time", ColumnType::scalar(LogicalType::Time)),
                col("c_json", ColumnType::scalar(LogicalType::Json)),
                col(
                    "c_list",
                    ColumnType::ScalarList {
                        item: LogicalType::Int64,
                    },
                ),
                col(
                    "c_struct",
                    ColumnType::Struct {
                        fields: vec![
                            col("a b", ColumnType::scalar(LogicalType::Int64)),
                            col("s", ColumnType::scalar(LogicalType::Utf8)),
                            col(
                                "d",
                                ColumnType::scalar(LogicalType::Decimal {
                                    precision: 5,
                                    scale: 1,
                                }),
                            ),
                        ],
                    },
                ),
            ],
        }
    }

    /// The fidelity guard: any mismatch here would plan a widen EVERY
    /// RUN against a table that never actually changed.
    #[test]
    fn every_column_type_round_trips_through_the_live_image() {
        let conn = memdb();
        let schema = schema_with_every_column_type("t");
        conn.execute_batch(&create_table_sql("t", &schema, false))
            .expect("ddl");
        let image = live_schema(&conn, &schema)
            .expect("image")
            .expect("table exists");
        assert_eq!(image.columns, schema.columns);
    }

    /// A table that was never created images to `None` — there is
    /// nothing for the widen planner to compare against, so the ensure
    /// path's own `Table { leg }` step (not a phantom widen) is what
    /// creates it.
    #[test]
    fn a_table_that_does_not_exist_images_to_none() {
        let conn = memdb();
        let schema = schema_with_every_column_type("absent");
        assert!(live_schema(&conn, &schema).expect("no error").is_none());
    }

    /// The documented Uuid/Utf8 collapse: a Uuid column images back as
    /// Utf8 (VARCHAR is all the catalog can report), never as Uuid.
    #[test]
    fn a_uuid_column_images_as_utf8() {
        let conn = memdb();
        let schema = TableSchema {
            table: TableName::new("u"),
            parent: None,
            columns: vec![col("id", ColumnType::scalar(LogicalType::Uuid))],
        };
        conn.execute_batch(&create_table_sql("u", &schema, false))
            .expect("ddl");
        let image = live_schema(&conn, &schema).expect("image").expect("exists");
        assert_eq!(
            image.columns[0].column_type,
            ColumnType::scalar(LogicalType::Utf8)
        );
    }

    /// An unmapped type spelling omits the column rather than guessing —
    /// a hostile probe on the parser, not a shape this connector's own
    /// DDL ever produces.
    #[test]
    fn an_unmapped_type_spelling_is_none() {
        assert_eq!(column_type_of("HUGEINT"), None);
        assert_eq!(column_type_of("MAP(VARCHAR, INTEGER)"), None);
    }

    /// F3 (2026-08 review round 1): a same-named TEMP table must never
    /// splice its columns into the image. Verified live: a TEMP table
    /// reports `table_catalog = "temp"` (the real database reports its
    /// own name, e.g. `"memory"`) while BOTH share `table_schema =
    /// "main"` — so the catalog clause is what actually excludes it,
    /// confirming the scope belongs on `table_catalog`, not just
    /// `table_schema`.
    #[test]
    fn a_same_named_temp_table_never_splices_into_the_image() {
        let conn = memdb();
        let real = TableSchema {
            table: TableName::new("t"),
            parent: None,
            columns: vec![col("a", ColumnType::scalar(LogicalType::Int64))],
        };
        conn.execute_batch(&create_table_sql("t", &real, false))
            .expect("real table ddl");
        conn.execute_batch("CREATE TEMP TABLE t (b VARCHAR)")
            .expect("temp table ddl");

        let image = live_schema(&conn, &real).expect("image").expect("exists");
        assert_eq!(
            image
                .columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            vec!["a"],
            "only the REAL table's own column, never the temp table's `b`"
        );
    }

    fn scalar(lt: LogicalType) -> ColumnType {
        ColumnType::scalar(lt)
    }

    fn decimal(precision: u8, scale: u8) -> ColumnType {
        ColumnType::scalar(LogicalType::Decimal { precision, scale })
    }

    /// THE widening policy, pinned rule by rule (2026-08 review rounds
    /// 1 and 2 — F1/F2/N1/N2/N3): every named rule is `true`, its
    /// reverse and everything unnamed is `false`.
    #[test]
    fn is_widening_matches_its_documented_lattice() {
        // anything (except Binary) -> Utf8 / Json.
        assert!(is_widening(
            &scalar(LogicalType::Bool),
            &scalar(LogicalType::Utf8)
        ));
        assert!(is_widening(
            &scalar(LogicalType::Int64),
            &scalar(LogicalType::Json)
        ));
        assert!(is_widening(
            &ColumnType::ScalarList {
                item: LogicalType::Int64
            },
            &scalar(LogicalType::Json)
        ));

        // N1 (CRITICAL, review round 2): Int64 -> Float64 is DELETED —
        // the engine only takes this edge after a per-value check this
        // destination cannot perform on committed rows.
        assert!(
            !is_widening(&scalar(LogicalType::Int64), &scalar(LogicalType::Float64)),
            "Int64 -> Float64 is never a widen here — value-checked at shred time, not here"
        );

        // N2 (IMPORTANT, review round 2): Int64 -> Decimal iff the
        // decimal's INTEGER CAPACITY (precision - scale) covers i64's
        // 19 integer digits.
        assert!(
            is_widening(&scalar(LogicalType::Int64), &decimal(21, 2)),
            "21 - 2 = 19 integer digits: exactly covers i64"
        );
        assert!(
            !is_widening(&scalar(LogicalType::Int64), &decimal(20, 2)),
            "20 - 2 = 18 < 19: does NOT cover every i64 value"
        );

        // N2: Decimal -> Decimal compares INTEGER CAPACITY, not raw
        // precision (rdlt-core's own join_decimals semantics).
        assert!(
            is_widening(&decimal(10, 2), &decimal(12, 4)),
            "10-2=8 -> 12-4=8 integer digits (unchanged) AND scale grew 2 -> 4"
        );
        assert!(
            is_widening(&decimal(10, 2), &decimal(10, 2)),
            "equal counts as widening"
        );
        assert!(
            !is_widening(&decimal(10, 2), &decimal(10, 4)),
            "10-2=8 -> 10-4=6 integer digits SHRANK even though scale grew and \
             raw precision held — capacity, not raw precision, is what matters"
        );
        assert!(
            !is_widening(&decimal(10, 4), &decimal(12, 2)),
            "scale shrank (4 -> 2)"
        );
        assert!(
            !is_widening(&decimal(12, 2), &decimal(10, 4)),
            "10-4=6 integer digits < 12-2=10 — capacity shrank"
        );

        // N3 (MINOR, review round 2): Binary is excluded as a FROM type
        // entirely — not even Binary -> Utf8/Json count as a widen.
        assert!(
            !is_widening(&scalar(LogicalType::Binary), &scalar(LogicalType::Utf8)),
            "Binary -> Utf8 is never a widen — the engine's own escalation \
             goes through an encoding chosen at shred time, not an ALTER cast"
        );
        assert!(
            !is_widening(&scalar(LogicalType::Binary), &scalar(LogicalType::Json)),
            "Binary -> Json is never a widen either"
        );

        // N3 widened STRUCTURALLY (review round 3): a scalar-only guard
        // let `ScalarList { item: Binary }` — a real `BLOB[]` column —
        // fall through to the anything->Utf8/Json absorption rule.
        // Binary anywhere in the source type is excluded, not just the
        // bare scalar.
        assert!(
            !is_widening(
                &ColumnType::ScalarList {
                    item: LogicalType::Binary
                },
                &scalar(LogicalType::Utf8)
            ),
            "ScalarList{{Binary}} -> Utf8 is never a widen — BLOB[] carries Binary too"
        );
        assert!(
            !is_widening(
                &ColumnType::ScalarList {
                    item: LogicalType::Binary
                },
                &scalar(LogicalType::Json)
            ),
            "ScalarList{{Binary}} -> Json is never a widen either"
        );
        assert!(
            !is_widening(
                &ColumnType::Struct {
                    fields: vec![col("payload", scalar(LogicalType::Binary))]
                },
                &scalar(LogicalType::Utf8)
            ),
            "a Struct carrying a Binary field ANYWHERE inside is excluded too"
        );

        // Reverses and unnamed pairs are false.
        assert!(!is_widening(
            &scalar(LogicalType::Utf8),
            &scalar(LogicalType::Int64)
        ));
        assert!(!is_widening(
            &scalar(LogicalType::Float64),
            &scalar(LogicalType::Int64)
        ));
        assert!(!is_widening(
            &scalar(LogicalType::Bool),
            &scalar(LogicalType::Int64)
        ));
    }

    /// `has_binary` directly, at every depth the exhaustive match
    /// covers (review round 3): the bare scalar, a `ScalarList` item,
    /// a `Struct` field, and a `Struct` field nested inside ANOTHER
    /// `Struct` — plus the negative cases proving it doesn't over-fire.
    #[test]
    fn has_binary_finds_binary_at_any_depth() {
        assert!(has_binary(&scalar(LogicalType::Binary)));
        assert!(!has_binary(&scalar(LogicalType::Utf8)));

        assert!(has_binary(&ColumnType::ScalarList {
            item: LogicalType::Binary
        }));
        assert!(!has_binary(&ColumnType::ScalarList {
            item: LogicalType::Int64
        }));

        assert!(has_binary(&ColumnType::Struct {
            fields: vec![
                col("a", scalar(LogicalType::Utf8)),
                col("b", scalar(LogicalType::Binary)),
            ]
        }));
        assert!(!has_binary(&ColumnType::Struct {
            fields: vec![col("a", scalar(LogicalType::Utf8))]
        }));

        // Nested two levels deep.
        assert!(has_binary(&ColumnType::Struct {
            fields: vec![col(
                "outer",
                ColumnType::Struct {
                    fields: vec![col("inner", scalar(LogicalType::Binary))]
                }
            )]
        }));
    }

    /// F1 (CRITICAL): a narrowing cross-run drift (text image, int
    /// incoming) is not a widen — `prefilter` adopts the incoming type
    /// so the planner sees NO difference, and the physical column is
    /// left untouched (no ALTER is ever planned against it).
    #[test]
    fn prefilter_skips_a_narrowing_drift_leaving_no_widen() {
        let image = TableSchema {
            table: TableName::new("t"),
            parent: None,
            columns: vec![col("n", scalar(LogicalType::Utf8))],
        };
        let def = TableSchema {
            table: TableName::new("t"),
            parent: None,
            columns: vec![col("n", scalar(LogicalType::Int64))],
        };
        let filtered = prefilter(image, &def);
        assert_eq!(
            filtered.columns[0].column_type,
            scalar(LogicalType::Int64),
            "adopted the incoming type — the planner's equality check now agrees"
        );
        // The planner comparison `schema_steps` actually runs:
        assert_eq!(
            filtered.column("n").unwrap().column_type,
            def.column("n").unwrap().column_type,
            "no widen: old(filtered) == new(def)"
        );
    }

    /// F1's genuine-widen arm: Int64 image -> Decimal incoming IS a
    /// widen, so `prefilter` leaves the image at its OLD type and the
    /// mismatch survives for the planner to act on.
    #[test]
    fn prefilter_keeps_a_genuine_widen_as_a_real_mismatch() {
        let image = TableSchema {
            table: TableName::new("t"),
            parent: None,
            columns: vec![col("n", scalar(LogicalType::Int64))],
        };
        let def = TableSchema {
            table: TableName::new("t"),
            parent: None,
            columns: vec![col("n", decimal(19, 0))],
        };
        let filtered = prefilter(image, &def);
        assert_eq!(
            filtered.columns[0].column_type,
            scalar(LogicalType::Int64),
            "OLD type kept — schema_steps must see a difference to plan the ALTER"
        );
        assert_ne!(
            filtered.column("n").unwrap().column_type,
            def.column("n").unwrap().column_type
        );
    }

    /// F2 (MAJOR): a `Uuid` image column (physically `VARCHAR`, same as
    /// `Utf8`) against a `Uuid` incoming schema is adopted STRAIGHT
    /// THROUGH — `prefilter` sees the two render the identical SQL type
    /// and rewrites, so the planner never sees a difference at all.
    #[test]
    fn prefilter_adopts_uuid_straight_through_when_physically_identical() {
        let image = TableSchema {
            table: TableName::new("u"),
            parent: None,
            columns: vec![col("id", scalar(LogicalType::Utf8))], // what live_schema reports
        };
        let def = TableSchema {
            table: TableName::new("u"),
            parent: None,
            columns: vec![col("id", scalar(LogicalType::Uuid))],
        };
        let filtered = prefilter(image, &def);
        assert_eq!(
            filtered.column("id").unwrap().column_type,
            def.column("id").unwrap().column_type,
            "no difference at all — no widen, no index rebuild"
        );
    }

    /// The economy guarantee (`table_ddl` half): with the image already
    /// equal to `def`, `table_ddl` emits no `SET DATA TYPE` — an
    /// unchanged table pays nothing for the catalog image existing.
    #[test]
    fn table_ddl_emits_no_alter_when_the_image_already_matches() {
        let schema = schema_with_every_column_type("t");
        let image = schema.clone();
        let statements = table_ddl(&schema, Some(&image));
        assert!(
            statements.iter().all(|sql| !sql.contains("SET DATA TYPE")),
            "steady state must render no ALTER: {statements:?}"
        );
    }

    /// The economy guarantee's other half, extended to a Uuid-keyed
    /// merge schema (the SIMPLEST behavioral proof for F2's steady
    /// state — 2026-08 review round 1 red-proof #3): a real
    /// `live_schema` + `prefilter` round trip through the catalog
    /// leaves NEITHER a `SET DATA TYPE` NOR a `DROP INDEX` in steady
    /// state, so a Uuid-keyed upsert table never rebuilds its unique
    /// index on an unchanged run.
    #[test]
    fn a_uuid_keyed_merge_table_pays_nothing_at_steady_state() {
        let conn = memdb();
        let schema = TableSchema {
            table: TableName::new("items"),
            parent: None,
            columns: vec![
                col("id", scalar(LogicalType::Uuid)),
                col("v", scalar(LogicalType::Utf8)),
            ],
        };
        let mode = WriteMode::Merge {
            key: vec!["id".to_owned()],
        };
        let options = DestinationOptions {
            merge_strategy: Some(MergeStrategy::Upsert),
            ..DestinationOptions::default()
        };

        // Ensure it once, for real, so the catalog carries the SAME
        // physical shape a live session would leave behind.
        for sql in table_ddl(&schema, None) {
            conn.execute_batch(&sql).expect("phase 1 ddl");
        }
        let merge_statements = merge_ddl(&options, &schema, &mode).expect("valid options");
        for (sql, _) in &merge_statements {
            conn.execute_batch(sql).expect("phase 2 ddl");
        }

        // A FRESH session's view: fetch the image, run it through the
        // SAME prefilter `ensure_table` runs.
        let image = live_schema(&conn, &schema).expect("image").expect("exists");
        let filtered = prefilter(image, &schema);

        let alters = table_ddl(&schema, Some(&filtered));
        assert!(
            alters.iter().all(|sql| !sql.contains("SET DATA TYPE")),
            "no ALTER against an unchanged Uuid-keyed table: {alters:?}"
        );
        let drops = pre_alter_index_drops(Some(&filtered), &schema, &merge_statements);
        assert!(
            drops.is_empty(),
            "no DROP INDEX against an unchanged unique-Uuid-key table: {drops:?}"
        );
    }
}
