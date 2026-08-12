//! # rdlt-connector-snowflake — Snowflake connector, second generation
//!
//! Destination-only, and the first connector born on the sdk: the
//! framework session drives the protocol, this crate supplies the
//! system IO. The root stays a façade — the destination lives in
//! [`destination`], and a future source would slot in beside it.
//!
//! ## The three service facts everything here bends around
//!
//! Measured against the real service (022/023's research holds the
//! probes), not assumed:
//!
//! 1. **A DDL statement commits whatever transaction is open.** There
//!    is no error and no visible sign — a half-written commit unit
//!    simply becomes durable. Schema work therefore runs strictly
//!    before a unit opens, and the executor a unit runs on refuses DDL
//!    in code ([`destination`]'s unit module) instead of trusting call
//!    sites.
//! 2. **Nothing enforces uniqueness.** Primary keys are metadata, so
//!    merge correctness comes entirely from the SQL the shared planner
//!    emits — and every merge suite reads its results back.
//! 3. **Identifier case is destiny.** Unquoted names fold upper; a
//!    quoted lower-case name is a DIFFERENT object. This crate emits
//!    every identifier quoted upper case — the spelling an unquoted
//!    user query resolves to.

pub mod destination;
