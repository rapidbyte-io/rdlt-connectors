//! The Oracle source: streaming reads into Arrow, and the watermark
//! cursor rulebook, born on the rdlt connector sdk.
//!
//! ONE QUERY PER STREAM, streamed from a live cursor into Arrow
//! batches. The batch size is a buffering choice, not a protocol
//! limit — an earlier generation on a pure-Rust driver had to page
//! by ROWID keyset because that driver could not continue a cursor,
//! and every consequence of that (a server cursor per page, a rescan
//! per page) died with it. specs/032-oracle/plan.md T005 records the
//! measurement that decided it.
//!
//! ROWS CROSS AS ARROW, so the schema travels with the data: exact
//! decimals stay exact, binary stays binary, and NaN and Infinity
//! survive. Streams are declared `structured` for that reason.
//!
//! CONSISTENCY IS PER STATEMENT. Oracle's default is
//! statement-level, and this connector opens no transaction — but
//! the read is now ONE statement per batch rather than one per page,
//! and a cursor-paged stream converges by watermark. A full-refresh
//! stream under concurrent DML can still miss or repeat rows.
//!
//! THE DRIVER IS SYNCHRONOUS (`oracle`, wrapping ODPI-C), so the
//! connection lives on its own thread behind `source::client`, and
//! Oracle Client libraries must be present AT RUNTIME.

pub mod source;
