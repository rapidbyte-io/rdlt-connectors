//! # rdlt-connector-postgres — the PostgreSQL connectors, second generation
//!
//! `source` (binary-COPY→Arrow extraction, reflection, cursor incremental,
//! CDC over logical replication) and `destination` (binary-COPY writes, one
//! transaction per commit unit, sqlcore-planned publishes) share three
//! substrate modules so the two directions cannot drift: [`tls`] holds the
//! connection-security vocabulary, `session` owns the whole path from a
//! connection string to a prepared live connection, and `types` is the one
//! Postgres type rulebook — every wire decode, text parse, SQL literal, and
//! wire encode dispatches over the same closed kind set, so a new kind is a
//! compiler-forced edit in every face.
//!
//! ONE SPELLING PER ITEM: this crate has NO crate-root re-exports — every
//! public item is reached through its module path (`source::…`,
//! `destination::…`, `tls::…`, `fixtures::…`), and that path is the
//! canonical spelling. Types do not repeat their module's noun: the policy
//! is [`tls::Policy`], the source connector is `source::Postgres`, the
//! source configuration is `source::Config`.
//!
//! The primary workflow starts from configuration, and everything up to the
//! database round-trip runs anywhere — parse the pipeline YAML, get back
//! either a validated connector or the exact configuration mistake:
//!
//! ```
//! use rdlt_connector_postgres::source;
//! use rdlt_connector_sdk::config::Document;
//!
//! let config = source::Config::from_yaml(
//!     r#"
//! conn: "postgresql://app@db.internal:5432/app"
//! tables:
//!   - name: orders
//!     cursor:
//!       column: updated_at
//! "#,
//! )
//! .expect("a listed table with a cursor column validates");
//! let _source = source::Shell::new(config).expect("validated");
//!
//! // Validation is the same gate for every entry point: a conn string whose
//! // sslmode contradicts the tls block is refused at parse time, not at
//! // connect time.
//! let err = source::Config::from_yaml(
//!     "conn: \"postgresql://u:p@localhost/db?sslmode=require\"\ntls:\n  mode: disable\n",
//! )
//! .unwrap_err();
//! assert!(err.to_string().contains("contradicts"));
//!
//! // The destination is describable the same way — the SAME document
//! // vocabulary the pipeline YAML's `destination: postgres:` block carries.
//! let _destination = rdlt_connector_postgres::destination::Shell::from_yaml(
//!     "conn: \"host=warehouse user=loader\"\ndataset: raw\n",
//! )
//! .expect("a destination document validates");
//! ```
//!
//! Connecting and moving rows needs a database; the container-backed
//! integration suites under `tests/` are the executable reference for that
//! half.

pub(crate) mod session;
pub mod tls;
pub(crate) mod types;

#[cfg(feature = "destination")]
pub mod destination;
#[cfg(feature = "fixtures")]
pub mod fixtures;
#[cfg(feature = "source")]
pub mod source;
#[cfg(any(feature = "source", feature = "destination"))]
#[doc(hidden)]
pub mod testsupport;
