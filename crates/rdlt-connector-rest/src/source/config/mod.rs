//! The REST source document: vocabulary + the one validation gate.
//!
//! `vocabulary` holds every config type with its frozen serde spelling;
//! `validate` is the single gate every entry point (`from_yaml`,
//! `from_json`, `from_value`, `Rest::new`) runs, so an invariant the read
//! path relies on is checked exactly once, at parse time.

pub(crate) mod validate;
pub(crate) mod vocabulary;

pub use vocabulary::{
    ActionKind, Auth, Config, ConfigError, Incremental, KeyLocation, Method, Pagination, Parent,
    ResponseAction, Stream, TypeHint, config_schema,
};
