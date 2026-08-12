//! Open-time (ensure_table) validation: one rule set, identical typed errors
//! across every SQL destination. Each rule group is one `check_*` fn; the
//! rejections are a typed [`ValidateError`] whose `Display` is the frozen
//! message text (the postgres conformance cells pin these strings, and the
//! destinations map the error to a fatal `DestinationError` at their SPI boundary).

use std::fmt;

use rdlt_connector::core::TableSchema;

use crate::options::{AbsentPolicy, DestinationOptions, MergeStrategy};

/// Table facts a destination resolves before validation.
#[derive(Debug)]
pub struct TableFacts<'a> {
    pub schema: &'a TableSchema,
    pub has_identity: bool,
    pub is_child: bool,
}

impl<'a> TableFacts<'a> {
    /// Read the facts off the schema itself.
    ///
    /// Both derivations were copied into every destination: an identity table
    /// is one carrying the shredder's id column, and a child is one naming a
    /// parent. Neither is a per-destination decision, so neither should be a
    /// per-destination line of code.
    pub fn of(schema: &'a TableSchema) -> Self {
        Self {
            schema,
            has_identity: schema
                .columns
                .iter()
                .any(|c| c.name == rdlt_connector::core::schema::system_columns::ID),
            is_child: schema.parent.is_some(),
        }
    }
}

/// A rejected destination-option configuration — every variant names the
/// offender, and `Display` renders the exact frozen text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidateError {
    /// A merge-only option under a non-merge write mode.
    OptionRequiresMerge {
        table: String,
        option: &'static str,
    },
    HardDeleteOnChild {
        table: String,
    },
    HardDeleteColMissing {
        table: String,
        col: String,
    },
    DedupSortNeedsKeyed {
        table: String,
    },
    DedupSortColMissing {
        table: String,
        col: String,
    },
    DedupSortIsHardDelete {
        table: String,
        col: String,
    },
    DedupSortInKey {
        table: String,
        col: String,
    },
    /// A merge-key column absent from the schema — the key could never
    /// identify a row.
    KeyColMissing {
        table: String,
        col: String,
    },
    MergeKeyNeedsKeyed {
        table: String,
    },
    MergeKeyScd2NeedsRetire {
        table: String,
    },
    MergeKeyColMissing {
        table: String,
        col: String,
    },
    MergeKeyColIsHardDelete {
        table: String,
        col: String,
    },
    UpsertNeedsKeyed {
        table: String,
    },
    Scd2NeedsKeyed {
        table: String,
    },
    Scd2ValidityCollides {
        table: String,
        name: String,
    },
}

impl fmt::Display for ValidateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidateError::OptionRequiresMerge { table, option } => write!(
                f,
                "table `{table}`: {option} requires the merge write mode \
                 — under append/replace it would be silently inert"
            ),
            ValidateError::HardDeleteOnChild { table } => write!(
                f,
                "table `{table}`: hard_delete applies to the ROOT table of a \
                 shredded stream — configure it on the root, not the child"
            ),
            ValidateError::HardDeleteColMissing { table, col } => write!(
                f,
                "hard_delete column `{col}` is not a column of table `{table}`"
            ),
            ValidateError::DedupSortNeedsKeyed { table } => write!(
                f,
                "table `{table}`: dedup_sort requires a KEYED structured \
                 stream — a shredded stream's identity is a content hash, \
                 ordered survivors are meaningless there"
            ),
            ValidateError::DedupSortColMissing { table, col } => write!(
                f,
                "dedup_sort column `{col}` is not a column of table `{table}`"
            ),
            ValidateError::DedupSortIsHardDelete { table, col } => write!(
                f,
                "table `{table}`: dedup_sort column `{col}` is the hard_delete \
                 flag — use a distinct ordering column"
            ),
            ValidateError::DedupSortInKey { table, col } => write!(
                f,
                "table `{table}`: dedup_sort column `{col}` is part of the \
                 merge key — constant within each identity group, it can \
                 never order survivors"
            ),
            ValidateError::KeyColMissing { table, col } => write!(
                f,
                "merge key column `{col}` is not a column of table `{table}`"
            ),
            ValidateError::MergeKeyNeedsKeyed { table } => write!(
                f,
                "table `{table}`: merge_scope requires a KEYED structured \
                 stream — shredded streams replace by root subtree"
            ),
            ValidateError::MergeKeyScd2NeedsRetire { table } => write!(
                f,
                "table `{table}`: merge_scope with scd2 scopes RETIREMENT and \
                 requires scd2 {{absent: retire}}"
            ),
            ValidateError::MergeKeyColMissing { table, col } => write!(
                f,
                "merge_scope column `{col}` is not a column of table `{table}`"
            ),
            ValidateError::MergeKeyColIsHardDelete { table, col } => write!(
                f,
                "table `{table}`: merge_scope column `{col}` is the \
                 hard_delete flag — a deletion flag is not a scope"
            ),
            ValidateError::UpsertNeedsKeyed { table } => write!(
                f,
                "table `{table}`: the upsert strategy requires a KEYED \
                 structured stream — shredded streams use delete_insert"
            ),
            ValidateError::Scd2NeedsKeyed { table } => write!(
                f,
                "table `{table}`: scd2 requires a KEYED structured stream \
                 — shredded streams have no declared key"
            ),
            ValidateError::Scd2ValidityCollides { table, name } => write!(
                f,
                "table `{table}`: scd2 validity column `{name}` collides \
                 with a stream column — configure different names"
            ),
        }
    }
}

impl std::error::Error for ValidateError {}

/// Merge-only options (strategy, dedup_sort, merge_scope) under Append or
/// Replace would be silently inert — reject typed instead. Only an EXPLICIT
/// merge_strategy rejects; the unconfigured default does not.
pub fn validate_non_merge(options: &DestinationOptions, table: &str) -> Result<(), ValidateError> {
    for (declared, option) in [
        (
            options.explicit_strategy_for(table).is_some(),
            "merge_strategy",
        ),
        (options.dedup_sort_for(table).is_some(), "dedup_sort"),
        (options.merge_scope_for(table).is_some(), "merge_scope"),
    ] {
        if declared {
            return Err(ValidateError::OptionRequiresMerge {
                table: table.to_owned(),
                option,
            });
        }
    }
    Ok(())
}

/// The merge-mode option checks (existence, collisions, keyed-only rules) —
/// one `check_*` per rule group, run in the order the messages were frozen.
/// Every error names the offender; the postgres conformance cells pin them.
pub fn validate_merge(
    options: &DestinationOptions,
    table: &str,
    key: &[String],
    facts: &TableFacts<'_>,
) -> Result<(), ValidateError> {
    let strategy = options.strategy_for(table);
    check_key(table, key, facts)?;
    check_hard_delete(options, table, facts)?;
    check_dedup_sort(options, table, key, facts)?;
    check_merge_scope(options, table, facts, strategy)?;
    check_upsert(strategy, facts.has_identity, table)?;
    check_scd2(options, table, facts, strategy)?;
    Ok(())
}

/// Every merge-key column must exist on the schema — on the tables
/// that actually merge BY that key: keyed structured roots. Shredded
/// and child tables merge on the engine's lineage ids and never bind
/// the declared key as columns, so the declared key is legitimately
/// absent there. Without this check a ghost key surfaces as whatever
/// statement first binds it — a different error on every destination,
/// and on some paths (the arbiter-index DDL) one that risks borrowing
/// an unrelated diagnosis.
fn check_key(table: &str, key: &[String], facts: &TableFacts<'_>) -> Result<(), ValidateError> {
    if facts.has_identity || facts.is_child {
        return Ok(());
    }
    for col in key {
        if facts.schema.column(col).is_none() {
            return Err(ValidateError::KeyColMissing {
                table: table.to_owned(),
                col: col.clone(),
            });
        }
    }
    Ok(())
}

fn check_hard_delete(
    options: &DestinationOptions,
    table: &str,
    facts: &TableFacts<'_>,
) -> Result<(), ValidateError> {
    if let Some(col) = options.hard_delete_for(table) {
        // Configuring hard_delete on a CHILD table would be silently inert —
        // reject typed instead: the flag lives on the ROOT row of a shredded
        // stream.
        if facts.is_child {
            return Err(ValidateError::HardDeleteOnChild {
                table: table.to_owned(),
            });
        }
        // The flag column must exist on THIS table's schema.
        if facts.schema.column(col).is_none() {
            return Err(ValidateError::HardDeleteColMissing {
                table: table.to_owned(),
                col: col.to_owned(),
            });
        }
    }
    Ok(())
}

/// dedup_sort is keyed-structured only, its column must exist, it may not
/// repurpose the hard_delete flag, and it may not be a merge-key column
/// (constant within each identity group).
fn check_dedup_sort(
    options: &DestinationOptions,
    table: &str,
    key: &[String],
    facts: &TableFacts<'_>,
) -> Result<(), ValidateError> {
    let Some(dedup) = options.dedup_sort_for(table) else {
        return Ok(());
    };
    if facts.has_identity {
        return Err(ValidateError::DedupSortNeedsKeyed {
            table: table.to_owned(),
        });
    }
    if facts.schema.column(&dedup.column).is_none() {
        return Err(ValidateError::DedupSortColMissing {
            table: table.to_owned(),
            col: dedup.column.clone(),
        });
    }
    if options.hard_delete_for(table) == Some(dedup.column.as_str()) {
        return Err(ValidateError::DedupSortIsHardDelete {
            table: table.to_owned(),
            col: dedup.column.clone(),
        });
    }
    // A merge-key column is CONSTANT within each identity group — the ordering
    // could never choose a survivor, silently reverting to arrival order.
    if key.contains(&dedup.column) {
        return Err(ValidateError::DedupSortInKey {
            table: table.to_owned(),
            col: dedup.column.clone(),
        });
    }
    Ok(())
}

/// merge_scope (scope replacement) is keyed-structured only; under scd2 it scopes
/// retirement and requires `absent: retire`; its columns must exist and may not
/// be the hard_delete flag.
fn check_merge_scope(
    options: &DestinationOptions,
    table: &str,
    facts: &TableFacts<'_>,
    strategy: MergeStrategy,
) -> Result<(), ValidateError> {
    let Some(scope) = options.merge_scope_for(table) else {
        return Ok(());
    };
    if facts.has_identity {
        return Err(ValidateError::MergeKeyNeedsKeyed {
            table: table.to_owned(),
        });
    }
    if strategy == MergeStrategy::Scd2 && options.scd2_for(table).absent != AbsentPolicy::Retire {
        // Belt: parse-time validation already rejects this; direct struct
        // construction must not slip past it — merge_scope with scd2 is scoped
        // retirement and requires `absent: retire`.
        return Err(ValidateError::MergeKeyScd2NeedsRetire {
            table: table.to_owned(),
        });
    }
    for col in scope {
        if facts.schema.column(col).is_none() {
            return Err(ValidateError::MergeKeyColMissing {
                table: table.to_owned(),
                col: col.clone(),
            });
        }
        if options.hard_delete_for(table) == Some(col.as_str()) {
            return Err(ValidateError::MergeKeyColIsHardDelete {
                table: table.to_owned(),
                col: col.clone(),
            });
        }
    }
    Ok(())
}

fn check_upsert(
    strategy: MergeStrategy,
    has_identity: bool,
    table: &str,
) -> Result<(), ValidateError> {
    if strategy == MergeStrategy::Upsert && has_identity {
        // A shredded stream's _rdlt_id is a CONTENT hash for keyless streams —
        // updates mint new ids and ON CONFLICT never fires, silently
        // duplicating. The destination cannot distinguish keyed from keyless
        // shredded streams, so upsert is keyed-structured only; shredded
        // streams keep delete_insert (subtree replacement).
        return Err(ValidateError::UpsertNeedsKeyed {
            table: table.to_owned(),
        });
    }
    Ok(())
}

fn check_scd2(
    options: &DestinationOptions,
    table: &str,
    facts: &TableFacts<'_>,
    strategy: MergeStrategy,
) -> Result<(), ValidateError> {
    if strategy != MergeStrategy::Scd2 {
        return Ok(());
    }
    if facts.has_identity {
        return Err(ValidateError::Scd2NeedsKeyed {
            table: table.to_owned(),
        });
    }
    let scd2 = options.scd2_for(table);
    for name in [&scd2.valid_from, &scd2.valid_to] {
        if facts.schema.column(name).is_some() {
            return Err(ValidateError::Scd2ValidityCollides {
                table: table.to_owned(),
                name: name.clone(),
            });
        }
    }
    Ok(())
}
