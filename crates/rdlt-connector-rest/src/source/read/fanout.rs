//! Parent-child fan-out: collect each parent record's resolved values (a
//! fresh cursor-less pass over the parent stream), then drive one child
//! sequence per parent record — up to `max_concurrency` in flight, records
//! forwarded through one bounded channel. Buffering is BOUNDED:
//! placeholder values and declared include fields only, never whole parent
//! records.

use bytes::Bytes;
use rdlt_connector_sdk::spi::error::SourceError;

use crate::source::config::{Config, Incremental, Parent, Stream};
use crate::source::http::Client;
use crate::source::select::Selector;

use super::cursor;
use super::paginate;
use super::parse_selector;
use super::resolve::{self, ParentValues};
use super::sequence::Sequence;

/// Read the parent stream (all pages) collecting resolved values.
pub(crate) async fn collect_parents(
    config: &Config,
    client: &Client,
    parent_stream: &Stream,
    parent_link: &Parent,
    into: &mut Vec<ParentValues>,
) -> Result<(), SourceError> {
    let selector = parse_selector(parent_stream)?;
    let mut paginator = paginate::build(&parent_stream.pagination).map_err(SourceError::fatal)?;
    let mut sequence = Sequence::new(config, parent_stream, client, &mut *paginator, true, None);
    loop {
        let Some(page) = sequence.fetch_page(&selector).await? else {
            break;
        };
        into.extend(resolve::parent_values(
            page.values.as_deref().unwrap_or_default(),
            parent_link,
        )?);
        if !sequence.advance(&page)? {
            break;
        }
    }
    Ok(())
}

/// Child fan-out: up to `max_concurrency` child sequences in flight,
/// records forwarded to `out` through one bounded channel (order across
/// children is not meaningful — each child is an independent sequence and
/// checkpointing happens once, at feed end). `max_concurrency: 1` (the
/// default) reads children strictly one at a time.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn read_children(
    config: &Config,
    client: &Client,
    stream: &Stream,
    incremental: &Option<Incremental>,
    start_value: &Option<String>,
    max_cursor: &mut Option<String>,
    parents: Vec<ParentValues>,
    out: &mut rdlt_connector_sdk::source::Feed,
) -> Result<(), SourceError> {
    use futures::StreamExt;
    let selector = parse_selector(stream)?;
    let limit = config.max_concurrency as usize;
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Bytes>(limit * 2);

    let forward = async {
        while let Some(records) = rx.recv().await {
            if out.raw_json(records).await.is_break() {
                break; // closed channel = cancellation
            }
        }
    };
    let drive = async {
        // Collected EAGERLY: the map closure runs here, not lazily inside
        // the stream (a lazy map trips higher-ranked lifetime inference).
        let child_futures: Vec<_> = parents
            .iter()
            .map(|parent_values| {
                let tx = tx.clone();
                let selector = &selector;
                async move {
                    read_child_pages(
                        config,
                        client,
                        stream,
                        selector,
                        incremental,
                        start_value,
                        parent_values,
                        &tx,
                    )
                    .await
                    .map_err(|e| with_parent_context(e, &stream.name, parent_values))
                }
            })
            .collect();
        let mut children = futures::stream::iter(child_futures).buffer_unordered(limit);
        let mut maxes: Vec<Option<String>> = Vec::new();
        while let Some(result) = children.next().await {
            maxes.push(result?);
        }
        drop(children);
        drop(tx); // last sender gone → the forwarder drains and ends
        Ok::<_, SourceError>(maxes)
    };
    let (maxes, ()) = tokio::join!(drive, forward);
    cursor::merge_maxes(max_cursor, maxes?);
    Ok(())
}

/// One child's paginated sequence, pushing record pages into the fan-out
/// channel and returning the child's own max-observed cursor.
#[allow(clippy::too_many_arguments)]
async fn read_child_pages(
    config: &Config,
    client: &Client,
    stream: &Stream,
    selector: &Option<Selector>,
    incremental: &Option<Incremental>,
    start_value: &Option<String>,
    parent_values: &ParentValues,
    tx: &tokio::sync::mpsc::Sender<Bytes>,
) -> Result<Option<String>, SourceError> {
    let needs_values = incremental.is_some() || !parent_values.include.is_empty();
    let mut paginator = paginate::build(&stream.pagination).map_err(SourceError::fatal)?;
    let mut sequence = Sequence::new(
        config,
        stream,
        client,
        &mut *paginator,
        needs_values,
        Some(parent_values),
    );
    sequence.bind_incremental(incremental, start_value);
    let mut local_max = start_value.clone();
    loop {
        let Some(mut page) = sequence.fetch_page(selector).await? else {
            break;
        };
        if page.count > 0 {
            if let Some(incremental) = incremental {
                let page_values = page.values.as_deref().unwrap_or_default();
                cursor::observe(page_values, &incremental.cursor_field, &mut local_max);
            }
            // Parent-field embedding reuses the already-parsed values — no
            // reparse; without `include` the page bytes pass through.
            let records = if parent_values.include.is_empty() {
                page.records.clone()
            } else {
                let page_values = page.values.take().unwrap_or_default();
                resolve::embed(page_values, &parent_values.include)?
            };
            if tx.send(records).await.is_err() {
                break; // forwarder ended = cancellation
            }
        }
        if !sequence.advance(&page)? {
            break;
        }
    }
    Ok(local_max)
}

/// Attach parent fan-out context to a child failure WITHOUT changing how
/// the engine will treat it: a transient or rate-limited child error keeps
/// its variant (and any `retry_after`) so the run still spends its retry
/// budget rather than aborting; only an already-fatal error stays fatal.
///
/// Context wraps the variant's INNER cause, not its rendered Display —
/// rebuilding the same variant around its own rendering would print the
/// classification frame twice ("transient source error: …: transient
/// source error: …").
fn with_parent_context(
    error: SourceError,
    stream: &str,
    parent_values: &ParentValues,
) -> SourceError {
    let context = format!(
        "child stream `{stream}` failed for parent ({})",
        resolve::describe(&parent_values.placeholders)
    );
    match error {
        SourceError::Transient(inner) => SourceError::transient(format!("{context}: {inner}")),
        SourceError::RateLimited {
            retry_after,
            source,
        } => SourceError::RateLimited {
            retry_after,
            source: format!("{context}: {source}").into(),
        },
        SourceError::Fatal(inner) => SourceError::fatal(format!("{context}: {inner}")),
        // `SourceError` is non_exhaustive: an unknown future variant keeps
        // its whole rendering (frame included) rather than losing its
        // classification — fatal is the safe direction for the unknown.
        other => SourceError::fatal(format!("{context}: {other}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parent_values() -> ParentValues {
        ParentValues {
            placeholders: [("id".to_owned(), "7".to_owned())].into_iter().collect(),
            include: Vec::new(),
        }
    }

    /// Context wraps the variant's INNER cause: the classification frame
    /// ("transient source error: …") must render exactly once. Re-wrapping
    /// the variant around its own rendering would print it twice.
    #[test]
    fn parent_context_renders_the_classification_frame_once() {
        let wrapped = with_parent_context(
            SourceError::transient("HTTP 502"),
            "issues",
            &parent_values(),
        );
        let rendered = wrapped.to_string();
        assert_eq!(
            rendered.matches("transient source error:").count(),
            1,
            "{rendered}"
        );
        assert!(
            rendered.contains("child stream `issues` failed for parent (id=7): HTTP 502"),
            "{rendered}"
        );

        let wrapped = with_parent_context(
            SourceError::RateLimited {
                retry_after: Some(std::time::Duration::from_secs(3)),
                source: "HTTP 429 from API".into(),
            },
            "issues",
            &parent_values(),
        );
        let rendered = wrapped.to_string();
        assert_eq!(
            rendered.matches("source rate limited:").count(),
            1,
            "{rendered}"
        );
        match wrapped {
            SourceError::RateLimited { retry_after, .. } => {
                assert_eq!(retry_after, Some(std::time::Duration::from_secs(3)));
            }
            other => panic!("classification must survive wrapping: {other:?}"),
        }

        let wrapped =
            with_parent_context(SourceError::fatal("HTTP 404"), "issues", &parent_values());
        let rendered = wrapped.to_string();
        assert_eq!(
            rendered.matches("fatal source error:").count(),
            1,
            "{rendered}"
        );
    }
}
