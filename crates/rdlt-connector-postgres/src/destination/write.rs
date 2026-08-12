//! Landing bytes: the binary-COPY streaming loop that writes one batch into
//! one relation. Nothing here decides WHEN or WHERE — the load session picks
//! the relation and owns the transaction; this file owns the wire.

use arrow_array::RecordBatch;
use bytes::{BufMut, Bytes, BytesMut};
use futures::SinkExt;
use rdlt_connector_sdk::spi::DestinationError;
use rdlt_connector_sdk::spi::core::TableSchema;
use rdlt_connector_sqlcore::quote_identifier;

use super::errors::{Phase, classify_statement, fatal};
use crate::session::Connection;
use crate::types::encode;

/// The 11-byte binary-COPY file signature. The `\r\n` / `\n` pair and the
/// high 0xff byte are a deliberate corruption detector: a stream mangled by
/// a text-mode or non-8-bit-clean transport no longer matches, so the
/// server rejects it up front instead of misreading the first tuple.
const COPY_BINARY_SIGNATURE: &[u8] = b"PGCOPY\n\xff\r\n\0";

/// Stream one batch into `relation` via `COPY … FROM STDIN BINARY`.
///
/// Wire decisions come from the ENSURED schema's logical types; the Arrow
/// representation only fills in where the schema is silent. On any error
/// the sink is dropped WITHOUT `finish()`, which is tokio-postgres' abort
/// protocol: the server sees CopyFail and the written rows never become
/// visible.
pub(super) async fn copy_batch(
    connection: &Connection,
    relation: &str,
    table_schema: Option<&TableSchema>,
    batch: &RecordBatch,
) -> Result<(), DestinationError> {
    let arrow_schema = batch.schema();
    let column_names = arrow_schema
        .fields()
        .iter()
        .map(|field| quote_identifier(field.name()))
        .collect::<Vec<_>>()
        .join(", ");
    let wires: Vec<encode::Wire> = arrow_schema
        .fields()
        .iter()
        .map(|field| {
            let logical = table_schema
                .and_then(|schema| schema.column(field.name()))
                .map(|column| &column.column_type);
            encode::wire_of(logical, field.data_type())
                .map_err(|detail| fatal(Phase::Write, detail))
        })
        .collect::<Result<_, _>>()?;

    // Downcast ONCE per column, not once per cell.
    let encoders: Vec<encode::Encoder<'_>> = arrow_schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            encode::Encoder::new(wires[index], batch.column(index).as_ref()).map_err(|detail| {
                fatal(Phase::Write, format!("column `{}`: {detail}", field.name()))
            })
        })
        .collect::<Result<_, _>>()?;
    let field_count = i16::try_from(batch.num_columns()).map_err(|_| {
        fatal(
            Phase::Write,
            "a COPY tuple cannot carry more than 32767 columns",
        )
    })?;

    let sink = connection
        .copy_in::<_, Bytes>(&format!(
            "COPY {} ({column_names}) FROM STDIN BINARY",
            quote_identifier(relation)
        ))
        .await
        .map_err(|e| classify_statement(Phase::Write, e))?;
    futures::pin_mut!(sink);

    // One reused buffer for the whole batch, handed to the sink in ~64 KiB
    // slices. The driver's own `BinaryCopyInWriter` flushes every 4096
    // bytes into an `mpsc::channel(1)`, so each flush is a wakeup and a
    // task handoff — the mechanism behind the six-figure voluntary
    // context-switch count on a 1M-row load.
    //
    // 64 KiB is the measured knee, not a guess (swept in generation one on
    // the 1M-row cell, medians of 5): CPU is flat from 64 KiB on, so
    // everything past it buys only context switches, and buys them badly —
    // 4x the buffer for 17% fewer, then 4x again for 3% fewer.
    const FLUSH_AT: usize = 64 * 1024;
    let mut buffer = BytesMut::with_capacity(FLUSH_AT * 2);
    buffer.put_slice(COPY_BINARY_SIGNATURE);
    buffer.put_i32(0); // flags: no OIDs
    buffer.put_i32(0); // header extension area: empty

    for row in 0..batch.num_rows() {
        buffer.put_i16(field_count);
        for (column, encoder) in encoders.iter().enumerate() {
            encoder.append_field(row, &mut buffer).map_err(|detail| {
                fatal(
                    Phase::Write,
                    format!(
                        "column `{}`: {detail}",
                        arrow_schema.fields()[column].name()
                    ),
                )
            })?;
        }
        if buffer.len() >= FLUSH_AT {
            let chunk = buffer.split().freeze();
            sink.as_mut()
                .feed(chunk)
                .await
                .map_err(|e| classify_statement(Phase::Write, e))?;
            // `split` hands the used capacity to the chunk, so without this
            // the buffer is near-empty from the third flush on and every
            // later chunk is rebuilt by repeated growth inside the per-cell
            // loop — the opposite of reusing one buffer.
            buffer.reserve(FLUSH_AT);
        }
    }
    buffer.put_i16(-1); // file trailer
    sink.as_mut()
        .feed(buffer.split().freeze())
        .await
        .map_err(|e| classify_statement(Phase::Write, e))?;
    sink.as_mut()
        .flush()
        .await
        .map_err(|e| classify_statement(Phase::Write, e))?;
    sink.finish()
        .await
        .map_err(|e| classify_statement(Phase::Write, e))?;
    Ok(())
}
