//! # rdlt-connector-rest — the declarative REST source, second generation
//!
//! One YAML/JSON document describes an API — pagination (seven families),
//! auth (including OAuth2 client credentials), JSONPath-subset record
//! selection, response actions, incremental cursors, and parent-child
//! endpoint composition — and [`source::Rest`] (through the sdk shell, [`source::Shell`]) reads it. Configs are DATA:
//! no callbacks, ever, so a platform can render, validate, and store them.
//!
//! Error classification is the source's whole retry story: 429 becomes
//! `RateLimited` (the server's Retry-After passed through), 5xx and network
//! failures are `Transient`, other 4xx are `Fatal` — unless a DECLARED
//! response action says otherwise. The ENGINE retries; every in-source wait
//! is bounded by construction.
//!
//! ONE SPELLING PER ITEM: no crate-root re-exports — every public item is
//! reached through its module path (`source::…`), and that path is the
//! canonical spelling.
//!
//! The primary workflow starts from configuration; parsing returns either a
//! validated connector or the exact configuration mistake:
//!
//! ```
//! use rdlt_connector_rest::source;
//!
//! let rest = source::Shell::from_yaml(
//!     r#"
//! base_url: "https://api.example.com"
//! streams:
//!   - name: issues
//!     path: /issues
//!     records_path: data.items[*]
//!     pagination: {type: page, page_param: page}
//! "#,
//! )
//! .expect("a declared stream with a valid selector parses");
//! drop(rest);
//!
//! // Validation is the same gate for every entry point: an undeclared
//! // placeholder is refused at parse time, not at request time.
//! let err = source::Shell::from_yaml(
//!     "base_url: \"https://x\"\nstreams:\n  - name: s\n    path: /a/{id}\n",
//! )
//! .unwrap_err();
//! assert!(err.to_string().contains("no `parent` block declares them"));
//! ```

pub mod source;
