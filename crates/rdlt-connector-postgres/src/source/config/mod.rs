//! The source configuration: `vocabulary` spells every noun a document can
//! contain; `validate` holds the entry points and the local (database-free)
//! rules. Catalog-dependent rules run at open, in `plan`.

mod validate;
mod vocabulary;

pub use vocabulary::{
    AckMode, Bound, CdcConfig, CdcMode, Config, ConfigError, CursorConfig, Direction, Lag,
    NullPolicy, QueryConfig, TableConfig, TypeHint, Wait, config_schema,
};
