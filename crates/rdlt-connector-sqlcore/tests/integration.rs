//! One compile root for the sqlcore pins, mirroring the house layout: topic
//! files under `cases/`. These suites are this crate's own half of its net —
//! the other half is the four golden-SQL files in the postgres and duckdb
//! crates, which pin the emitted text byte-for-byte from outside.

mod cases;
