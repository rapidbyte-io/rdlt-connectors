//! The paginated read loop. `delivery` owns the per-stream entry
//! (sequence-or-fan-out dispatch, cursor tracking, checkpoint cadence);
//! the request sequence itself lives in `sequence`; the fan-out in
//! `fanout`; placeholder resolution in `resolve`; the paginator families
//! in [`paginate`]; record extraction in `extract`; cursor tracking in
//! `cursor`.

pub(crate) mod cursor;
mod delivery;
pub(crate) mod extract;
pub(crate) mod fanout;
pub mod paginate;
pub(crate) mod resolve;
pub(crate) mod sequence;

pub(crate) use delivery::{deliver, parse_selector};
