//! SQL assembly for the COPY subselect. Injection-safe by construction:
//! identifiers come ONLY from reflection/config validated against
//! reflection, and are strictly double-quoted; COPY accepts no bind
//! parameters, so cursor values render as typed literals with explicit
//! casts — never raw user strings.

use rdlt_connector_sqlcore::quote_identifier;

use crate::source::reflect::Column;
use crate::types::Scalar;
use crate::types::literal;
use crate::types::map::Projection;

/// One projection item per the column's mapping; policy conversions run
/// server-side so the wire only carries the lossless decode set.
fn projection(column: &Column) -> String {
    let identifier = quote_identifier(&column.name);
    match &column.mapping.projection {
        Projection::Direct => identifier,
        Projection::CastText => format!("({identifier})::text AS {identifier}"),
        Projection::CastJsonbText => format!("to_jsonb({identifier})::text AS {identifier}"),
        // Per-column type hint: strict server-side cast to the target.
        Projection::Cast(target) => format!("({identifier})::{target} AS {identifier}"),
    }
}

/// The ONE subquery wrapping for a query stream — used by BOTH the describe
/// path and the read path, so the shape a query is described under is the
/// shape it is read under.
pub(crate) fn subquery(sql: &str) -> String {
    format!("( {} ) AS q", sql.trim().trim_end_matches(';'))
}

/// The snapshot SELECT for a table (schema-qualified, selected columns in
/// reflected order). `where_sql`/`order_sql` are already rendered, or empty.
pub(crate) fn select(
    schema: &str,
    table: &str,
    columns: &[&Column],
    where_sql: &str,
    order_sql: &str,
) -> String {
    select_from(
        &format!("{}.{}", quote_identifier(schema), quote_identifier(table)),
        columns,
        where_sql,
        order_sql,
    )
}

/// A SELECT over an arbitrary FROM clause (query streams use the wrapped
/// subquery); the table form above delegates here.
pub(crate) fn select_from(
    from: &str,
    columns: &[&Column],
    where_sql: &str,
    order_sql: &str,
) -> String {
    let projections: Vec<String> = columns.iter().map(|column| projection(column)).collect();
    let mut sql = format!("SELECT {} FROM {from}", projections.join(", "));
    if !where_sql.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(where_sql);
    }
    if !order_sql.is_empty() {
        sql.push_str(" ORDER BY ");
        sql.push_str(order_sql);
    }
    sql
}

pub(crate) fn copy(select: &str) -> String {
    format!("COPY ({select}) TO STDOUT (FORMAT BINARY)")
}

/// Incremental WHERE/ORDER BY rendering — the full boundary matrix.
pub(crate) struct IncrementalClauses {
    pub(crate) where_sql: String,
    pub(crate) order_sql: String,
}

/// Render the incremental window. `lower` is the RESUME bound
/// (value, closed?): `>=` re-fetches watermark-equal rows for dedup, `>`
/// skips them — independent of direction. `lag_delta` widens the resume
/// window BEHIND the watermark (`- delta` under max, `+ delta` under min) —
/// read-side only. `upper` is (value, inclusive?). Text cursors force
/// `COLLATE "C"`: the tracker compares watermarks in Rust byte order, so the
/// SQL ordering/filtering must use byte order too — a column collation (ICU,
/// en_US…) would diverge.
pub(crate) fn incremental_clauses(
    column: &str,
    direction_max: bool,
    lower: Option<(&Scalar, bool)>,
    lag_delta: Option<&str>,
    upper: Option<(&Scalar, bool)>,
    nulls_include: bool,
    collate_byte_order: bool,
) -> IncrementalClauses {
    let bare = quote_identifier(column);
    let identifier = if collate_byte_order {
        format!("{bare} COLLATE \"C\"")
    } else {
        bare
    };
    let mut predicates: Vec<String> = Vec::new();
    if let Some((value, closed)) = lower {
        let operator = match (direction_max, closed) {
            (true, true) => ">=",
            (true, false) => ">",
            (false, true) => "<=",
            (false, false) => "<",
        };
        let rendered = literal::render(value);
        let bound = match lag_delta {
            Some(delta) => format!(
                "({rendered} {} {delta})",
                if direction_max { "-" } else { "+" }
            ),
            None => rendered,
        };
        predicates.push(format!("{identifier} {operator} {bound}"));
    }
    if let Some((value, inclusive)) = upper {
        let operator = match (direction_max, inclusive) {
            (true, false) => "<",
            (true, true) => "<=",
            (false, false) => ">",
            (false, true) => ">=",
        };
        predicates.push(format!(
            "{identifier} {operator} {}",
            literal::render(value)
        ));
    }
    let where_sql = match (predicates.is_empty(), nulls_include) {
        (true, true) => String::new(),
        (true, false) => format!("{identifier} IS NOT NULL"),
        (false, true) => format!("(({}) OR {identifier} IS NULL)", predicates.join(" AND ")),
        (false, false) => format!("{} AND {identifier} IS NOT NULL", predicates.join(" AND ")),
    };
    // NULLS FIRST keeps the watermark tail at the stream end in BOTH
    // directions — checkpoint tracking depends on it.
    let order_sql = format!(
        "{identifier} {} NULLS FIRST",
        if direction_max { "ASC" } else { "DESC" }
    );
    IncrementalClauses {
        where_sql,
        order_sql,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::source::reflect::tests::table;
    use crate::types::map::oid;

    #[test]
    fn quotes_hostile_identifiers() {
        assert_eq!(quote_identifier("plain"), "\"plain\"");
        assert_eq!(quote_identifier("Order Items"), "\"Order Items\"");
        // Embedded quotes double; injection attempts stay inert identifiers.
        assert_eq!(
            quote_identifier(r#"x"; DROP TABLE t; --"#),
            r#""x""; DROP TABLE t; --""#
        );
    }

    #[test]
    fn projects_policies_server_side() {
        let reflected = table(&[("id", oid::INT8, false), ("price", 790, false)]);
        let mut columns: Vec<&Column> = reflected.columns.iter().collect();
        let array_table = {
            use crate::types::map::{CatalogType, map};
            crate::source::reflect::Table {
                name: "t".into(),
                columns: vec![{
                    let catalog = CatalogType {
                        oid: 1007,
                        typtype: 'b',
                        typcategory: 'A',
                        typmod: -1,
                    };
                    Column {
                        name: "tags".into(),
                        type_name: "_text".into(),
                        mapping: map(&catalog),
                        catalog,
                        not_null: false,
                        in_primary_key: false,
                    }
                }],
            }
        };
        columns.push(&array_table.columns[0]);
        let sql = select("public", "t", &columns, "", "");
        assert_eq!(
            sql,
            r#"SELECT "id", ("price")::text AS "price", to_jsonb("tags")::text AS "tags" FROM "public"."t""#
        );
    }

    #[test]
    fn incremental_boundary_matrix() {
        let watermark = Scalar::Int(5);
        let end = Scalar::Int(9);
        // (direction_max, closed) × operators
        let clauses = incremental_clauses(
            "ts",
            true,
            Some((&watermark, true)),
            None,
            None,
            false,
            false,
        );
        assert_eq!(clauses.where_sql, r#""ts" >= 5::int8 AND "ts" IS NOT NULL"#);
        assert_eq!(clauses.order_sql, r#""ts" ASC NULLS FIRST"#);
        let clauses = incremental_clauses(
            "ts",
            true,
            Some((&watermark, false)),
            None,
            Some((&end, false)),
            false,
            false,
        );
        assert_eq!(
            clauses.where_sql,
            r#""ts" > 5::int8 AND "ts" < 9::int8 AND "ts" IS NOT NULL"#
        );
        let clauses = incremental_clauses(
            "ts",
            false,
            Some((&watermark, true)),
            None,
            Some((&end, false)),
            false,
            false,
        );
        assert_eq!(
            clauses.where_sql,
            r#""ts" <= 5::int8 AND "ts" > 9::int8 AND "ts" IS NOT NULL"#
        );
        assert_eq!(clauses.order_sql, r#""ts" DESC NULLS FIRST"#);
        // NULL policy include wraps with OR IS NULL; bare include filters
        // nothing.
        let clauses = incremental_clauses(
            "ts",
            true,
            Some((&watermark, true)),
            None,
            None,
            true,
            false,
        );
        assert_eq!(clauses.where_sql, r#"(("ts" >= 5::int8) OR "ts" IS NULL)"#);
        let clauses = incremental_clauses("ts", true, None, None, None, true, false);
        assert_eq!(clauses.where_sql, "");
        let clauses = incremental_clauses("ts", true, None, None, None, false, false);
        assert_eq!(clauses.where_sql, r#""ts" IS NOT NULL"#);
        // Hostile cursor string literal stays inert.
        let hostile = Scalar::Text("x'; DROP TABLE t; --".into());
        let clauses =
            incremental_clauses("v", true, Some((&hostile, true)), None, None, false, true);
        assert!(
            clauses.where_sql.contains("'x''; DROP TABLE t; --'::text"),
            "{}",
            clauses.where_sql
        );
    }

    #[test]
    fn where_and_order_hooks() {
        let reflected = table(&[("id", oid::INT8, false)]);
        let columns: Vec<&Column> = reflected.columns.iter().collect();
        let sql = select("s", "t", &columns, r#""id" > 5"#, r#""id" ASC"#);
        assert_eq!(
            sql,
            r#"SELECT "id" FROM "s"."t" WHERE "id" > 5 ORDER BY "id" ASC"#
        );
        assert_eq!(
            copy("SELECT 1"),
            "COPY (SELECT 1) TO STDOUT (FORMAT BINARY)"
        );
        assert_eq!(subquery("SELECT 1; "), "( SELECT 1 ) AS q");
    }

    #[test]
    fn lag_delta_widens_the_lower_bound_direction_aware() {
        let watermark = Scalar::TimestampTz(1_000_000);
        // max direction: watermark MINUS the window, closed.
        let clauses = incremental_clauses(
            "ts",
            true,
            Some((&watermark, true)),
            Some("INTERVAL '300 seconds'"),
            None,
            false,
            false,
        );
        assert_eq!(
            clauses.where_sql,
            "\"ts\" >= ((TIMESTAMPTZ 'epoch' + 1000000::int8 * INTERVAL '1 microsecond') \
             - INTERVAL '300 seconds') AND \"ts\" IS NOT NULL"
        );
        // min direction: watermark PLUS the window.
        let clauses = incremental_clauses(
            "ts",
            false,
            Some((&watermark, true)),
            Some("INTERVAL '300 seconds'"),
            None,
            false,
            false,
        );
        assert!(
            clauses.where_sql.contains("+ INTERVAL '300 seconds'"),
            "{}",
            clauses.where_sql
        );
        assert!(clauses.where_sql.contains("<="), "{}", clauses.where_sql);
        // No lag: rendering identical to the un-lagged form.
        let clauses = incremental_clauses(
            "ts",
            true,
            Some((&watermark, true)),
            None,
            None,
            false,
            false,
        );
        assert!(
            !clauses.where_sql.contains("INTERVAL '300"),
            "{}",
            clauses.where_sql
        );
    }
}
