//! Row counting over the OWNERSHIP listing — verification's view of
//! what a table holds. Partitions included, staging excluded (the
//! listing never enters `.rdlt-staging`), and only OWNED shapes are
//! counted: a user's own files under the table directory are not this
//! destination's rows to report (030 review, docket S12 — gen 1
//! counted any `.parquet`/`.jsonl`, so a foreign file inflated
//! verification counts or broke them outright when unreadable).

use parquet::file::reader::{FileReader, SerializedFileReader};
use rdlt_connector_sdk::spi::error::DestinationError;

use crate::location::Location;

/// Count the rows published under `table` — both protocols.
pub(crate) async fn count_rows_async(
    location: &Location,
    table: &str,
) -> Result<u64, DestinationError> {
    let mut rows = 0;
    for tail in location.keys_of_table(table).await? {
        if !super::truncate::owns(&tail) {
            continue;
        }
        let count: u64 = if tail.ends_with(".parquet") {
            let bytes = location.read_table_file(table, &tail).await?;
            let reader = SerializedFileReader::new(bytes::Bytes::from(bytes)).map_err(|e| {
                DestinationError::fatal(format!("unreadable parquet `{tail}`: {e}"))
            })?;
            u64::try_from(reader.metadata().file_metadata().num_rows()).unwrap_or(0)
        } else if tail.ends_with(".jsonl") {
            let bytes = location.read_table_file(table, &tail).await?;
            memchr::memchr_iter(b'\n', &bytes).count() as u64
        } else {
            0
        };
        rows += count;
    }
    Ok(rows)
}

/// The synchronous form, local only — the frozen refusal names the
/// async alternative.
pub(crate) fn count_rows(location: &Location, table: &str) -> Result<u64, DestinationError> {
    if !location.is_local() {
        return Err(DestinationError::fatal(
            "count_rows is synchronous; use count_rows_async for object stores",
        ));
    }
    futures::executor::block_on(count_rows_async(location, table))
}
