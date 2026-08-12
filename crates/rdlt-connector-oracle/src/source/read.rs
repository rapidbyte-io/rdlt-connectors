//! The read path: ONE query per stream, streamed into Arrow batches.
//!
//! This is the shape the previous driver could not support. It could
//! not continue a cursor, so every page had to be a fresh bounded
//! query — which opened a server cursor per page (dying at ~297) and
//! re-walked the table below the watermark every time (super-linear).
//! With a driver that streams, the whole apparatus — ROWID keyset
//! paging, session-data-unit page sizing, connection recycling — is
//! gone. plan.md T005 records what each driver measured.
//!
//! WHAT SURVIVES is the cursor rulebook, because it is about
//! correctness across RUNS rather than within one: rows are ordered
//! by the watermark with ROWID as the tie-break, and the resume
//! predicate is `c > w OR (c = w AND ROWID > tie)` so a run that
//! stopped mid-watermark neither repeats nor skips.

use std::ops::ControlFlow;

use arrow::array::RecordBatch;
use rdlt_connector_sdk::source::Feed;
use rdlt_connector_sdk::spi::SourceError;

use super::batch::BatchBuilder;
use super::client::{Client, quote_table, quote_upper};
use super::config::{Config, Stream};
use super::cursor::OracleCursor;
use super::schema::{cursor_capable, is_numeric, schema_of};

/// Read one stream in full, or from its watermark.
///
/// Returns `false` when the consumer asked to stop.
pub(crate) async fn read_stream(
    client: &Client,
    config: &Config,
    stream: &Stream,
    cursor: &mut OracleCursor,
    feed: &mut Feed,
) -> Result<bool, SourceError> {
    let table = quote_table(&stream.table);
    let name = stream.name.clone();

    // Learn the shape first: the Arrow schema, and whether the
    // configured cursor column can actually carry a watermark.
    let described = describe(client, &name, &table).await?;
    let schema =
        std::sync::Arc::new(schema_of(&described).map_err(|e| {
            SourceError::fatal(format!("stream `{name}`: `{}`: {e}", stream.table))
        })?);

    let cursor_at = match &stream.cursor {
        Some(column) => Some(validate_cursor(&name, &stream.table, &described, column)?),
        None => None,
    };

    let resume = cursor.streams.get(&name).cloned();
    // The position ADVANCES with every batch, and the statement is
    // rebuilt from it each time. The driver's cursor cannot outlive
    // the closure that owns the connection, so each batch re-queries
    // — and re-querying from a STALE position returns the same rows
    // forever. Measured: the crash sweep spun on rows 1-25 until the
    // run was killed.
    let mut position = resume.clone();
    let prefetch = config.tuning.page_rows;
    let batch_rows = config.tuning.batch_rows as usize;

    // The driver hands rows over one at a time from a live cursor, so
    // the batch is assembled here and the connection thread only ever
    // holds one batch's worth.
    let mut sent_any = false;
    loop {
        let sql = select_sql(&table, stream, position.as_ref())?;
        let chunk = fetch_chunk(
            client,
            &sql,
            prefetch,
            schema.clone(),
            cursor_at,
            batch_rows,
        )
        .await?;
        let Some(chunk) = chunk else { break };
        sent_any = true;

        if feed.arrow(chunk.batch).await == ControlFlow::Break(()) {
            return Ok(false);
        }
        if let Some((watermark, tie)) = chunk.last_seen.clone() {
            position = Some(super::cursor::StreamCursor {
                // A cursorless stream pages by ROWID alone, so an
                // absent watermark is expected here, not a defect.
                watermark: watermark.unwrap_or_default(),
                tie: Some(tie),
            });
        }
        if let (Some(_), Some((Some(watermark), tie))) = (cursor_at, chunk.last_seen) {
            cursor.advance(&name, &watermark, Some(&tie));
            rdlt_connector_sdk::spi::core::crash_point!(
                "ora.checkpoint",
                Err(SourceError::fatal("injected crash at ora.checkpoint"))
            );
            if feed.checkpoint(cursor.encode()).await == ControlFlow::Break(()) {
                return Ok(false);
            }
        }
        if chunk.exhausted {
            break;
        }
    }

    // An empty stream still declares its shape, so a destination can
    // create the table rather than skipping it.
    if !sent_any && feed.arrow(RecordBatch::new_empty(schema)).await == ControlFlow::Break(()) {
        return Ok(false);
    }
    Ok(true)
}

/// Describe the projection without reading a row.
pub(crate) async fn describe(
    client: &Client,
    stream: &str,
    table: &str,
) -> Result<Vec<oracle::ColumnInfo>, SourceError> {
    let sql = format!("SELECT t.* FROM {table} t WHERE 1 = 0");
    let context = format!("describing `{stream}`");
    client
        .with(move |conn| {
            let rows = conn
                .query(&sql, &[])
                .map_err(|e| super::client::classify(&context, &e))?;
            Ok(rows.column_info().to_vec())
        })
        .await
}

/// A cursor column must exist, must order, and must NOT admit NULLs.
///
/// The nullability check is the one that has to happen HERE, before a
/// row moves. Oracle sorts NULLs last, so refusing at the row landed
/// on the final page of the first run — after most of the table was
/// already delivered and checkpointed — and every later run resumed
/// with `c > w`, which never matches NULL. The stream went green
/// forever with those rows silently absent.
fn validate_cursor(
    stream: &str,
    table: &str,
    described: &[oracle::ColumnInfo],
    column: &str,
) -> Result<usize, SourceError> {
    let at = described
        .iter()
        .position(|c| c.name().eq_ignore_ascii_case(column))
        .ok_or_else(|| {
            SourceError::fatal(format!(
                "stream `{stream}`: cursor column `{column}` is not a column of `{table}` — \
                 incremental reads would silently re-read everything"
            ))
        })?;
    let info = &described[at];
    if info.nullable() {
        return Err(SourceError::fatal(format!(
            "stream `{stream}`: cursor column `{column}` admits NULLs, which a watermark \
             cannot order — rows holding one would be delivered once and then skipped by \
             every resume. Make the column NOT NULL, choose another cursor, or drop \
             `cursor` to read the stream in full"
        )));
    }
    let kind = super::schema::arrow_type(info.name(), info.oracle_type())
        .map_err(|e| SourceError::fatal(format!("stream `{stream}`: {e}")))?;
    // A bare NUMBER lands in Utf8 for magnitude reasons but still
    // orders numerically — and it is how estates spell a
    // sequence-backed surrogate key, so refusing it rejected the
    // commonest cursor there is.
    if !cursor_capable(&kind) && !is_numeric(info.oracle_type()) {
        return Err(SourceError::fatal(format!(
            "stream `{stream}`: cursor column `{column}` is {}, which has no usable order \
             for a watermark — choose a numeric or timestamp column",
            info.oracle_type()
        )));
    }
    Ok(at)
}

/// The one statement this stream runs.
fn select_sql(
    table: &str,
    stream: &Stream,
    resume: Option<&super::cursor::StreamCursor>,
) -> Result<String, SourceError> {
    let projection = "t.*, ROWIDTOCHAR(t.ROWID) AS RDLT_ROWID";
    let Some(column) = &stream.cursor else {
        // A full read orders by ROWID and resumes PAST the last one
        // seen: without the ORDER BY the server may return rows in
        // any order, and without the predicate the next batch would
        // re-read the same rows.
        let predicate = match resume.and_then(|state| state.tie.as_deref()) {
            Some(tie) => format!(" WHERE t.ROWID > CHARTOROWID({})", rowid_literal(tie)?),
            None => String::new(),
        };
        return Ok(format!(
            "SELECT {projection} FROM {table} t{predicate} ORDER BY t.ROWID"
        ));
    };
    let column = quote_upper(column);
    let predicate = match resume {
        Some(state) => {
            let watermark = super::cursor::checked_watermark_literal(&state.watermark)?;
            match &state.tie {
                // The tie-break is what makes a mid-watermark stop
                // safe: rows AT the watermark resume after the exact
                // row already delivered, rather than all repeating or
                // all being skipped.
                Some(tie) => format!(
                    " WHERE {column} > {watermark} OR ({column} = {watermark} \
                     AND t.ROWID > CHARTOROWID({}))",
                    rowid_literal(tie)?
                ),
                None => format!(" WHERE {column} > {watermark}"),
            }
        }
        None => String::new(),
    };
    Ok(format!(
        "SELECT {projection} FROM {table} t{predicate} ORDER BY {column}, t.ROWID"
    ))
}

/// One assembled batch, plus where it left off.
struct Chunk {
    batch: RecordBatch,
    /// The last row's `(watermark, rowid)`. The WATERMARK is optional
    /// — a cursorless stream has none — but the ROWID always exists,
    /// and it is what advances the position. Tying the pair's
    /// existence to the watermark is how a cursorless read came to
    /// re-issue the same statement forever.
    last_seen: Option<(Option<String>, String)>,
    exhausted: bool,
}

/// Pull up to [`BATCH_ROWS`] rows from the stream's cursor.
///
/// The cursor cannot outlive the closure (the driver's handles are
/// not `Send`), so each call re-runs the statement and skips what was
/// already delivered via the resume predicate — except that with a
/// streaming driver the common case is a SINGLE call that drains
/// everything, because the loop exits as soon as the cursor is
/// exhausted.
async fn fetch_chunk(
    client: &Client,
    sql: &str,
    prefetch: Option<u32>,
    schema: std::sync::Arc<arrow::datatypes::Schema>,
    cursor_at: Option<usize>,
    batch_rows: usize,
) -> Result<Option<Chunk>, SourceError> {
    let sql = sql.to_owned();
    client
        .with(move |conn| {
            rdlt_connector_sdk::spi::core::crash_point!(
                "ora.query",
                Err(SourceError::fatal("injected crash at ora.query"))
            );
            let mut builder = conn.statement(&sql);
            if let Some(rows) = prefetch {
                builder.prefetch_rows(rows);
            }
            let mut stmt = builder
                .build()
                .map_err(|e| super::client::classify("preparing the read", &e))?;
            let rows = stmt
                .query(&[])
                .map_err(|e| super::client::classify("reading", &e))?;

            let mut batch = BatchBuilder::new(schema.clone()).with_cursor_at(cursor_at);
            let mut last_seen = None;
            let mut exhausted = true;
            for row in rows {
                let row = row.map_err(|e| super::client::classify("reading a row", &e))?;
                let rowid: String = row
                    .get(schema.fields().len())
                    .map_err(|e| super::client::classify("reading the rowid", &e))?;
                let watermark = batch.append(&row)?;
                last_seen = Some((watermark, rowid));
                if batch.len() >= batch_rows {
                    exhausted = false;
                    break;
                }
            }
            if batch.len() == 0 {
                return Ok(None);
            }
            Ok(Some(Chunk {
                batch: batch.finish()?,
                last_seen,
                exhausted,
            }))
        })
        .await
}

/// A ROWID travelling back into SQL as a literal. The shape check is
/// the injection gate: the persisted cursor is an operator-editable
/// document.
fn rowid_literal(rowid: &str) -> Result<String, SourceError> {
    if rowid.is_empty()
        || !rowid
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '/')
    {
        return Err(SourceError::fatal(format!(
            "resume rowid `{rowid}` has an unexpected shape — refusing to interpolate it \
             into SQL; clear the pipeline state"
        )));
    }
    Ok(format!("'{rowid}'"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_rowid_must_look_like_one() {
        assert!(rowid_literal("AAAR1sAABAAAa1PAAA").is_ok());
        assert!(rowid_literal("").is_err());
        assert!(rowid_literal("' OR '1'='1").is_err());
        assert!(rowid_literal("AAAR1s'--").is_err());
    }
}
