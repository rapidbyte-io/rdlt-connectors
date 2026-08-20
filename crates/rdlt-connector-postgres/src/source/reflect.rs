//! Catalog reflection: one pg_catalog round trip per run → a [`Table`] per
//! selected relation. The reflected structure is the authority for the
//! published stream schema, column projection, and cursor-column validation.

use std::collections::BTreeMap;

use rdlt_connector_sdk::spi::error::SourceError;
use tokio_postgres::Client;

use crate::source::config::{Config, TableConfig};
use crate::source::errors::{self, Phase};
use crate::types::map::{CatalogType, Mapping, map};

/// One reflected column: the catalog facts, the type-mapping decision, and
/// the constraints the stream schema carries.
#[derive(Debug, Clone)]
pub struct Column {
    pub(crate) name: String,
    /// Postgres type name, diagnostics only.
    pub(crate) type_name: String,
    /// The reflected shape facts — kept so per-column type hints can consult
    /// the closed conversion table post-reflection.
    pub(crate) catalog: CatalogType,
    pub(crate) mapping: Mapping,
    pub(crate) not_null: bool,
    pub(crate) in_primary_key: bool,
}

impl Column {
    /// The Arrow type this column will carry on the structured path.
    pub fn arrow_type(&self) -> arrow_schema::DataType {
        self.mapping.kind.arrow()
    }

    pub fn is_not_null(&self) -> bool {
        self.not_null
    }
}

/// One reflected relation (or described query) and its columns, in attnum
/// order.
#[derive(Debug, Clone)]
pub struct Table {
    pub(crate) name: String,
    pub(crate) columns: Vec<Column>,
}

impl Table {
    pub fn primary_key(&self) -> Vec<&str> {
        self.columns
            .iter()
            .filter(|column| column.in_primary_key)
            .map(|column| column.name.as_str())
            .collect()
    }

    /// The effective merge/dedup key for a stream: a declared `primary_key`
    /// override, else the reflected primary key. Empty when neither exists.
    pub(crate) fn effective_primary_key(&self, config: Option<&TableConfig>) -> Vec<String> {
        match config.and_then(|table| table.primary_key.clone()) {
            Some(declared) => declared,
            None => self
                .primary_key()
                .iter()
                .map(|name| (*name).to_owned())
                .collect(),
        }
    }

    pub fn column(&self, name: &str) -> Option<&Column> {
        self.columns.iter().find(|column| column.name == name)
    }

    /// Columns after applying the table's include/exclude selection: unknown
    /// names and empty results are typed errors.
    pub fn selected_columns(
        &self,
        config: Option<&TableConfig>,
    ) -> Result<Vec<&Column>, SourceError> {
        let (include, exclude) = match config {
            Some(table) => (
                table.included_columns.as_deref(),
                table.excluded_columns.as_deref(),
            ),
            None => (None, None),
        };
        for requested in include
            .iter()
            .chain(exclude.iter())
            .flat_map(|names| names.iter())
        {
            if self.column(requested).is_none() {
                return Err(errors::fatal(
                    Phase::Reflect,
                    Some(&self.name),
                    format!("selected column `{requested}` does not exist"),
                ));
            }
        }
        let selected: Vec<&Column> = self
            .columns
            .iter()
            .filter(|column| match (include, exclude) {
                (Some(included), _) => included.iter().any(|name| name == &column.name),
                (_, Some(excluded)) => !excluded.iter().any(|name| name == &column.name),
                _ => true,
            })
            .collect();
        if selected.is_empty() {
            return Err(errors::fatal(
                Phase::Reflect,
                Some(&self.name),
                "column selection leaves zero data columns",
            ));
        }
        Ok(selected)
    }
}

/// Describe-based schema for a QUERY stream: prepare the WRAPPED statement —
/// enforcing read-only via the database's own subquery rules BEFORE any data
/// moves — and map the described column types through the standard type
/// mapping. typmod is not described (numerics take the textual policy row
/// unless hinted); nullability is unknowable (all nullable).
pub(crate) async fn describe_query(
    client: &Client,
    name: &str,
    sql: &str,
) -> Result<Table, SourceError> {
    let wrapped = format!("SELECT * FROM {}", crate::source::sql::subquery(sql));
    let statement = client.prepare(&wrapped).await.map_err(|e| {
        errors::fatal(
            Phase::Reflect,
            Some(name),
            format!("query does not describe (read-only SELECT/CTE required): {e}"),
        )
    })?;
    let mut columns = Vec::with_capacity(statement.columns().len());
    for column in statement.columns() {
        let catalog = catalog_type_of(column.type_());
        columns.push(Column {
            name: column.name().to_owned(),
            type_name: column.type_().name().to_owned(),
            mapping: map(&catalog),
            catalog,
            not_null: false,
            in_primary_key: false,
        });
    }
    if columns.is_empty() {
        return Err(errors::fatal(
            Phase::Reflect,
            Some(name),
            "query describes zero columns",
        ));
    }
    Ok(Table {
        name: name.to_owned(),
        columns,
    })
}

/// Map a described `tokio_postgres::types::Type` into the same shape facts
/// reflection extracts from pg_catalog (domains resolve one level, exactly
/// as reflection does).
fn catalog_type_of(described: &tokio_postgres::types::Type) -> CatalogType {
    use tokio_postgres::types::Kind;
    let mut oid = described.oid();
    let mut kind = described.kind();
    if let Kind::Domain(base) = kind {
        oid = base.oid();
        kind = base.kind();
    }
    let (typtype, typcategory) = match kind {
        Kind::Array(_) => ('b', 'A'),
        Kind::Enum(_) => ('e', 'E'),
        Kind::Composite(_) => ('c', 'C'),
        Kind::Range(_) => ('r', 'R'),
        _ => ('b', 'X'),
    };
    CatalogType {
        oid,
        typtype,
        typcategory,
        typmod: -1,
    }
}

/// The selected columns with type hints APPLIED: owned clones whose mapping
/// is replaced per the closed conversion table. Typed errors: a hint naming
/// a non-selected column; an undefined (source type → hint) pair.
pub(crate) fn hinted_columns(
    table: &Table,
    config: Option<&TableConfig>,
) -> Result<Vec<Column>, SourceError> {
    let selected = table.selected_columns(config)?;
    let mut columns: Vec<Column> = selected.into_iter().cloned().collect();
    if let Some(config) = config {
        for (name, hint) in &config.type_hints {
            let column = columns
                .iter_mut()
                .find(|column| column.name == *name)
                .ok_or_else(|| {
                    errors::fatal(
                        Phase::Reflect,
                        Some(&table.name),
                        format!("type hint names `{name}`, which is not a selected column"),
                    )
                })?;
            column.mapping =
                crate::types::map::apply_hint(&column.catalog, *hint).map_err(|detail| {
                    errors::fatal(
                        Phase::Reflect,
                        Some(&table.name),
                        format!(
                            "type hint on `{name}` (source type `{}`): {detail}",
                            column.type_name
                        ),
                    )
                })?;
        }
    }
    Ok(columns)
}

/// One round trip: every column of every relation in `schema` matching the
/// relkind filter, with type shape + primary-key membership, in attnum
/// order. Hierarchy CHILDREN are excluded via `pg_inherits` (declarative
/// partitions AND classic INHERITS children in one predicate): the parent's
/// stream already scans every child — reflecting children too would
/// double-load every row under schema-wide discovery. Explicitly LISTED
/// names override the exclusion ($3) — reading one partition/child alone is
/// a legitimate backfill. Domains resolve one level to their base (nested
/// domains fall to the textual fallback — documented); the domain's own
/// typmod wins when present.
const REFLECT_SQL: &str = r#"
SELECT c.relname::text                       AS table_name,
       a.attname::text                       AS column_name,
       a.atttypmod                           AS typmod,
       a.attnotnull                          AS not_null,
       t.oid::int8                           AS type_oid,
       t.typtype::text                       AS typtype,
       t.typcategory::text                   AS typcategory,
       t.typname::text                       AS type_name,
       t.typtypmod                           AS domain_typmod,
       bt.oid::int8                          AS base_oid,
       bt.typtype::text                      AS base_typtype,
       bt.typcategory::text                  AS base_typcategory,
       (pk.conkey IS NOT NULL AND a.attnum = ANY(pk.conkey)) AS is_pk
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
JOIN pg_type t ON t.oid = a.atttypid
LEFT JOIN pg_type bt ON t.typtype = 'd' AND bt.oid = t.typbasetype
LEFT JOIN (SELECT conrelid, conkey FROM pg_constraint WHERE contype = 'p') pk
       ON pk.conrelid = c.oid
WHERE n.nspname = $1
  AND c.relkind::text = ANY($2)
  AND (NOT EXISTS (SELECT 1 FROM pg_inherits i WHERE i.inhrelid = c.oid)
       OR c.relname = ANY($3))
ORDER BY c.relname, a.attnum
"#;

pub(crate) async fn reflect(
    client: &Client,
    config: &Config,
) -> Result<BTreeMap<String, Table>, SourceError> {
    let relkinds: Vec<&str> = if config.include_views {
        vec!["r", "p", "v", "m"]
    } else {
        vec!["r", "p"]
    };
    // Explicitly listed tables bypass the hierarchy-child exclusion.
    let listed: Vec<String> = config
        .tables
        .iter()
        .flatten()
        .map(|table| table.name.clone())
        .collect();
    let rows = client
        .query(REFLECT_SQL, &[&config.schema, &relkinds, &listed])
        .await
        .map_err(|e| errors::classify(Phase::Reflect, None, &e))?;

    let mut tables: BTreeMap<String, Table> = BTreeMap::new();
    for row in rows {
        let table_name: String = row.get("table_name");
        let column_name: String = row.get("column_name");
        let typmod: i32 = row.get("typmod");
        let not_null: bool = row.get("not_null");
        let type_oid: i64 = row.get("type_oid");
        let typtype: String = row.get("typtype");
        let typcategory: String = row.get("typcategory");
        let type_name: String = row.get("type_name");
        let in_primary_key: bool = row.get("is_pk");

        // Domains resolve one level; a domain's own typmod (typtypmod) wins
        // over the attribute's (-1 for domain attributes).
        let catalog = if typtype == "d" {
            let base_oid: Option<i64> = row.get("base_oid");
            let base_typtype: Option<String> = row.get("base_typtype");
            let base_typcategory: Option<String> = row.get("base_typcategory");
            let domain_typmod: i32 = row.get("domain_typmod");
            match (base_oid, base_typtype, base_typcategory) {
                // A nested domain (base is itself a domain) → textual
                // fallback via an OID no lossless arm matches.
                (Some(_), Some(base), _) if base == "d" => CatalogType {
                    oid: 0,
                    typtype: 'b',
                    typcategory: 'X',
                    typmod: -1,
                },
                (Some(oid), Some(base_typtype), Some(base_typcategory)) => CatalogType {
                    oid: oid as u32,
                    typtype: base_typtype.chars().next().unwrap_or('b'),
                    typcategory: base_typcategory.chars().next().unwrap_or('X'),
                    typmod: if domain_typmod != -1 {
                        domain_typmod
                    } else {
                        typmod
                    },
                },
                _ => CatalogType {
                    oid: 0,
                    typtype: 'b',
                    typcategory: 'X',
                    typmod: -1,
                },
            }
        } else {
            CatalogType {
                oid: type_oid as u32,
                typtype: typtype.chars().next().unwrap_or('b'),
                typcategory: typcategory.chars().next().unwrap_or('X'),
                typmod,
            }
        };

        tables
            .entry(table_name.clone())
            .or_insert_with(|| Table {
                name: table_name,
                columns: Vec::new(),
            })
            .columns
            .push(Column {
                name: column_name,
                type_name,
                mapping: map(&catalog),
                catalog,
                not_null,
                in_primary_key,
            });
    }

    // Every listed table must exist in the reflected set.
    if let Some(listed) = &config.tables {
        for table in listed {
            if !tables.contains_key(&table.name) {
                return Err(errors::fatal(
                    Phase::Reflect,
                    Some(&table.name),
                    format!(
                        "table not found in schema `{}` (include_views={})",
                        config.schema, config.include_views
                    ),
                ));
            }
        }
    }
    Ok(tables)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::types::Kind;
    use crate::types::map::oid;

    pub(crate) fn table(columns: &[(&str, u32, bool)]) -> Table {
        Table {
            name: "t".into(),
            columns: columns
                .iter()
                .map(|(name, type_oid, in_primary_key)| {
                    let catalog = CatalogType {
                        oid: *type_oid,
                        typtype: 'b',
                        typcategory: 'X',
                        typmod: -1,
                    };
                    Column {
                        name: (*name).into(),
                        type_name: "x".into(),
                        mapping: map(&catalog),
                        catalog,
                        not_null: false,
                        in_primary_key: *in_primary_key,
                    }
                })
                .collect(),
        }
    }

    #[test]
    fn selection_include_exclude_and_errors() {
        let reflected = table(&[
            ("a", oid::INT8, true),
            ("b", oid::TEXT, false),
            ("c", oid::BOOL, false),
        ]);
        let all = reflected.selected_columns(None).expect("all");
        assert_eq!(all.len(), 3);

        let include = TableConfig {
            name: "t".into(),
            cursor: None,
            primary_key: None,
            included_columns: Some(vec!["a".into(), "c".into()]),
            excluded_columns: None,
            type_hints: Default::default(),
        };
        let picked = reflected.selected_columns(Some(&include)).expect("include");
        assert_eq!(
            picked
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["a", "c"]
        );

        let missing = TableConfig {
            included_columns: Some(vec!["nope".into()]),
            ..include.clone()
        };
        assert!(reflected.selected_columns(Some(&missing)).is_err());

        let empty = TableConfig {
            included_columns: None,
            excluded_columns: Some(vec!["a".into(), "b".into(), "c".into()]),
            ..include
        };
        assert!(reflected.selected_columns(Some(&empty)).is_err());
    }

    #[test]
    fn hinted_columns_apply_and_reject() {
        use crate::source::config::TypeHint;
        let reflected = table(&[("id", oid::INT8, true), ("v", oid::TEXT, false)]);
        // Hint applies: text column → timestamptz.
        let hinted_config = TableConfig {
            name: "t".into(),
            cursor: None,
            primary_key: None,
            included_columns: None,
            excluded_columns: None,
            type_hints: [("v".to_string(), TypeHint::TimestampTz)]
                .into_iter()
                .collect(),
        };
        let columns = hinted_columns(&reflected, Some(&hinted_config)).expect("hint applies");
        assert_eq!(
            columns
                .iter()
                .find(|column| column.name == "v")
                .unwrap()
                .mapping
                .kind,
            Kind::TimestampTz
        );
        // text → binary: the text is parsed as bytea input server-side.
        let binary = TableConfig {
            type_hints: [("v".to_string(), TypeHint::Binary)].into_iter().collect(),
            ..hinted_config.clone()
        };
        let columns = hinted_columns(&reflected, Some(&binary)).expect("binary hint applies");
        assert_eq!(
            columns
                .iter()
                .find(|column| column.name == "v")
                .unwrap()
                .mapping
                .kind,
            Kind::Bytea
        );
        // Undefined pairs stay closed: int8 → uuid and int8 → binary.
        let bad = TableConfig {
            type_hints: [("id".to_string(), TypeHint::Uuid)].into_iter().collect(),
            ..hinted_config.clone()
        };
        assert!(hinted_columns(&reflected, Some(&bad)).is_err());
        let bad_binary = TableConfig {
            type_hints: [("id".to_string(), TypeHint::Binary)].into_iter().collect(),
            ..hinted_config.clone()
        };
        assert!(hinted_columns(&reflected, Some(&bad_binary)).is_err());
        // Hint on a non-selected column.
        let ghost = TableConfig {
            type_hints: [("ghost".to_string(), TypeHint::Utf8)]
                .into_iter()
                .collect(),
            ..hinted_config
        };
        assert!(hinted_columns(&reflected, Some(&ghost)).is_err());
    }

    #[test]
    fn primary_key_extraction() {
        let reflected = table(&[
            ("id", oid::INT8, true),
            ("ts", oid::TIMESTAMPTZ, true),
            ("v", oid::TEXT, false),
        ]);
        assert_eq!(reflected.primary_key(), ["id", "ts"]);
    }
}
