//! Records extraction. The no-selector passthrough — body bytes stream
//! through untouched — is the flagship perf path; selector extraction is
//! one parse + reserialize. (A body-driven paginator parses the body once
//! more for its cursor — that parse belongs to pagination, not to record
//! extraction.)

use bytes::Bytes;
use rdlt_connector_sdk::spi::SourceError;
use serde_json::Value;

use crate::source::select::Selector;

/// One page's extraction result: the records-array bytes, the record
/// count, and — when the caller declared it needs them (incremental
/// cursors, parent-child) — the parsed record values, so the cursor and
/// embedding consumers never reparse the records. The passthrough branch
/// keeps the values regardless: its parse already happened for the count.
#[derive(Debug)]
pub(crate) struct Page {
    pub(crate) records: Bytes,
    pub(crate) count: usize,
    pub(crate) values: Option<Vec<Value>>,
}

impl Page {
    /// An empty page (the `ignore` response action).
    pub(crate) fn empty() -> Self {
        Self {
            records: Bytes::from_static(b"[]"),
            count: 0,
            values: Some(Vec::new()),
        }
    }
}

/// Extract records per the stream's selector. Without one, the body must
/// BE a JSON array and streams through untouched (the perf path); with
/// one, one parse + reserialize. No-match is a typed error naming the path
/// and the response's top-level keys — EXCEPT when a wildcard traversed an
/// existing empty array: that's a legitimately empty page (the standard
/// terminal-page shape), never an error. `needs_values` keeps the parsed
/// records on the result for downstream consumers.
pub(crate) fn page(
    body: &Bytes,
    selector: Option<&Selector>,
    needs_values: bool,
) -> Result<Page, SourceError> {
    match selector {
        None => {
            let value: Value = parse_body(body)?;
            match value {
                // The parse already happened for the count — keep the
                // values for free; the forwarded BYTES stay untouched.
                Value::Array(items) => Ok(Page {
                    records: body.clone(),
                    count: items.len(),
                    values: Some(items),
                }),
                _ => Err(SourceError::fatal(
                    "response body is not a JSON array; set records_path",
                )),
            }
        }
        Some(selector) => {
            let value: Value = parse_body(body)?;
            let (matches, wildcard_saw_empty) = selector.select_tracking(&value);
            if matches.is_empty() {
                if wildcard_saw_empty {
                    return Ok(Page::empty());
                }
                let top = top_level_keys(&value);
                return Err(SourceError::fatal(format!(
                    "records_path `{}` matched nothing in the response (top-level: {top})",
                    selector.raw()
                )));
            }
            // One match that is an array = the records array; otherwise the
            // matches themselves are the records (wildcard selection).
            let items: Vec<&Value> = if matches.len() == 1 {
                match matches[0] {
                    Value::Array(items) => items.iter().collect(),
                    _ => {
                        return Err(SourceError::fatal(format!(
                            "records_path `{}` selected a non-array; append [*] to \
                             select elements",
                            selector.raw()
                        )));
                    }
                }
            } else {
                matches
            };
            let count = items.len();
            let records =
                serde_json::to_vec(&items).map_err(|e| SourceError::fatal(e.to_string()))?;
            let values = needs_values.then(|| items.into_iter().cloned().collect());
            Ok(Page {
                records: records.into(),
                count,
                values,
            })
        }
    }
}

/// Parse a response body into a JSON value, mapping a parse failure to the
/// canonical typed "not valid JSON" fatal — one message, one policy,
/// shared by every JSON-decoding site on the read path.
pub(crate) fn parse_body(body: &[u8]) -> Result<Value, SourceError> {
    serde_json::from_slice(body)
        .map_err(|e| SourceError::fatal(format!("response is not valid JSON: {e}")))
}

/// The terse top-level summary for no-match errors: names an object's keys
/// (the usual "you meant one of these" case) or the body's shape.
fn top_level_keys(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let keys: Vec<&str> = map.keys().map(String::as_str).take(8).collect();
            format!("object with keys [{}]", keys.join(", "))
        }
        Value::Array(items) => format!("array of {} items", items.len()),
        Value::Null => "null".to_owned(),
        Value::Bool(_) => "bool".to_owned(),
        Value::Number(_) => "number".to_owned(),
        Value::String(_) => "string".to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn passthrough_is_byte_identical() {
        let body = Bytes::from_static(b"[{\"a\":1},{\"a\":2}]");
        let out = page(&body, None, false).unwrap();
        assert_eq!(out.count, 2);
        assert!(std::ptr::eq(
            out.records.as_ref().as_ptr(),
            body.as_ref().as_ptr()
        ));
    }

    #[test]
    fn non_array_shapes_are_typed() {
        // No selector + non-array body: typed with the records_path hint.
        let body = Bytes::from(serde_json::to_vec(&json!({"a": 1})).unwrap());
        let error = page(&body, None, false).unwrap_err().to_string();
        assert!(error.contains("records_path"), "{error}");
        // Selector landing on a scalar: typed with the [*] hint.
        let selector = Selector::parse("a").unwrap();
        let error = page(&body, Some(&selector), false).unwrap_err().to_string();
        assert!(
            error.contains("non-array") && error.contains("[*]"),
            "{error}"
        );
    }

    #[test]
    fn no_match_names_path_and_shape() {
        let body = Bytes::from(serde_json::to_vec(&json!({"meta": 1, "rows": []})).unwrap());
        let selector = Selector::parse("data.items").unwrap();
        let error = page(&body, Some(&selector), false).unwrap_err().to_string();
        assert!(
            error.contains("data.items") && error.contains("meta"),
            "{error}"
        );
    }

    #[test]
    fn wildcard_over_an_empty_array_is_an_empty_page() {
        let body = Bytes::from(serde_json::to_vec(&json!({"items": []})).unwrap());
        let selector = Selector::parse("items[*]").unwrap();
        let out = page(&body, Some(&selector), false).unwrap();
        assert_eq!(out.count, 0);
    }
}
