//! Source-side test access: reflection without a full pipeline, the decoder
//! bench body, and the fuzz entries.

use std::collections::BTreeMap;

use rdlt_connector_sdk::spi::error::SourceError;

pub use crate::source::reflect::{Column, Table};

/// CDC lifecycle surface for the integration suites.
pub use crate::source::cdc::slot as cdc_slot;

/// Reflect a config's schema exactly as a run would, without the pipeline.
pub async fn reflect_for_tests(
    config: &crate::source::Config,
) -> Result<BTreeMap<String, Table>, SourceError> {
    let connection = crate::source::connect(config).await?;
    crate::source::reflect::reflect(&connection, config).await
}

/// Canned binary-COPY stream for the gated decoder bench: `rows` tuples over
/// the representative column mix in [`crate::testsupport::data`].
/// Deterministic bytes.
pub fn bench_wire(rows: usize) -> Vec<u8> {
    let mut wire = b"PGCOPY\n\xff\r\n\0".to_vec();
    wire.extend_from_slice(&0i32.to_be_bytes());
    wire.extend_from_slice(&0i32.to_be_bytes());
    let field = |wire: &mut Vec<u8>, bytes: &[u8]| {
        wire.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
        wire.extend_from_slice(bytes);
    };
    for row in 0..rows as i64 {
        wire.extend_from_slice(&8i16.to_be_bytes());
        field(&mut wire, &row.to_be_bytes());
        field(&mut wire, &((row % 100_000) as i32).to_be_bytes());
        field(&mut wire, &(row as f64 * 0.5).to_be_bytes());
        field(&mut wire, format!("user-{row}").as_bytes());
        field(&mut wire, &(row * 1_000_000).to_be_bytes()); // µs since PG epoch
        field(&mut wire, &[(row % 2) as u8]);
        let mut uuid = [0u8; 16];
        uuid[8..].copy_from_slice(&row.to_be_bytes());
        field(&mut wire, &uuid);
        let mut jsonb = vec![1u8];
        jsonb.extend_from_slice(
            format!(r#"{{"city":"NYC","zip":{}}}"#, 10_001 + row % 100).as_bytes(),
        );
        field(&mut wire, &jsonb);
    }
    wire.extend_from_slice(&(-1i16).to_be_bytes());
    wire
}

/// The gated decoder hot path (bench body): full stream → Arrow batches;
/// returns decoded rows so the work cannot be optimized away.
pub fn bench_decode(wire: &[u8]) -> u64 {
    let mut decoder = crate::types::binary::Decoder::new(
        crate::testsupport::data::bench_columns(),
        8 << 20,
        65_536,
    )
    .expect("bench columns are valid");
    // Feed in 64 KiB chunks — socket-realistic boundaries.
    let mut rows = 0u64;
    for chunk in wire.chunks(64 << 10) {
        let batches = decoder.feed(chunk).expect("bench wire is valid");
        rows += batches
            .iter()
            .map(|batch| batch.num_rows() as u64)
            .sum::<u64>();
    }
    if let Some(tail) = decoder.finish().expect("trailer") {
        rows += tail.num_rows() as u64;
    }
    rows
}

/// Fuzz entry (`copy_decode` target): arbitrary bytes through the decoder
/// over the representative multi-type plan — typed errors only, never a
/// panic. The first fuzz byte splits the input into two feeds so
/// chunk-boundary states get fuzzed too.
pub fn fuzz_copy_decode(data: &[u8]) {
    let Ok(mut decoder) =
        crate::types::binary::Decoder::new(crate::testsupport::data::fuzz_columns(), 4096, 64)
    else {
        return; // fixed fuzz columns are valid; a build failure is not the target
    };
    let Some((&split, rest)) = data.split_first() else {
        return;
    };
    let cut = (split as usize).min(rest.len());
    let (first, second) = rest.split_at(cut);
    if decoder.feed(first).is_err() {
        return;
    }
    if decoder.feed(second).is_err() {
        return;
    }
    let _ = decoder.finish();
}

/// Fuzz entry (`pgoutput_decode` target): arbitrary bytes through the
/// logical-replication message parser — typed errors only, never a panic.
pub fn fuzz_pgoutput_decode(data: &[u8]) {
    let _ = crate::source::cdc::parse_pgoutput(data);
}
