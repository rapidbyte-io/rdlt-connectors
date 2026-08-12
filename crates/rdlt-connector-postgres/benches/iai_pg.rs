//! Gated instruction-count benches for the binary-COPY hot paths — the
//! decoder (source) and the encoder (destination). Instruction counts are
//! load-insensitive: the wall-clock cells cannot resolve changes this size
//! against machine noise, but callgrind can. Wired into the workspace perf
//! gate: the recorded baselines in `benches/perf-baselines.json` key on the
//! benchmark function names, so those names are part of the gate's surface —
//! rename one and its baseline silently stops binding.

use std::hint::black_box;

use arrow_array::RecordBatch;
use iai_callgrind::{library_benchmark, library_benchmark_group, main};

fn wire_10k() -> Vec<u8> {
    rdlt_connector_postgres::testsupport::source::bench_wire(10_000)
}

fn batch_10k() -> RecordBatch {
    rdlt_connector_postgres::testsupport::destination::bench_batch(10_000)
}

#[library_benchmark]
#[bench::rows_10k(wire_10k())]
fn pg_copy_decode_10k(wire: Vec<u8>) -> u64 {
    black_box(rdlt_connector_postgres::testsupport::source::bench_decode(
        &wire,
    ))
}

#[library_benchmark]
#[bench::rows_10k(batch_10k())]
fn pg_copy_encode_10k(batch: RecordBatch) -> u64 {
    black_box(rdlt_connector_postgres::testsupport::destination::bench_encode(&batch))
}

library_benchmark_group!(
    name = hotpath;
    benchmarks = pg_copy_decode_10k, pg_copy_encode_10k
);
main!(library_benchmark_groups = hotpath);
