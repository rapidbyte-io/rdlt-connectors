//! The JSONPath SUBSET selector: dot paths + `[*]` wildcards + `[N]`
//! indices (`data.items[*].payload`). Hand-rolled over `serde_json::Value`
//! rather than pulling in a JSONPath crate; parsed eagerly at config
//! validation so a bad path fails at parse time, not mid-read.
//!
//! Substrate on purpose: the config gate, the paginator families, record
//! extraction, and parent placeholder resolution all select with THIS one
//! type — config validation never reaches into the read path for it.

use serde_json::Value;

/// One parsed selector segment.
#[derive(Debug, Clone, PartialEq)]
enum Segment {
    Field(String),
    Index(usize),
    Wildcard,
}

/// A validated selector. `data.items[*].payload` → each payload under every
/// item; selection results flatten across wildcards.
#[derive(Debug, Clone, PartialEq)]
pub struct Selector {
    segments: Vec<Segment>,
    raw: String,
}

impl Selector {
    /// Parse or fail with a message naming the supported subset.
    pub fn parse(raw: &str) -> Result<Self, String> {
        if raw.is_empty() {
            return Err("empty selector".into());
        }
        let mut segments = Vec::new();
        for part in raw.split('.') {
            if part.is_empty() {
                return Err(format!(
                    "selector `{raw}`: empty segment (supported: dot paths, [*], [N])"
                ));
            }
            // field, field[*], field[N], or bare [*]/[N]
            let (field, brackets) = match part.find('[') {
                Some(at) => (&part[..at], &part[at..]),
                None => (part, ""),
            };
            if !field.is_empty() {
                segments.push(Segment::Field(field.to_owned()));
            }
            let mut rest = brackets;
            while !rest.is_empty() {
                let Some(end) = rest.find(']') else {
                    return Err(format!(
                        "selector `{raw}`: unclosed `[` (supported: dot paths, [*], [N])"
                    ));
                };
                let inner = &rest[1..end];
                match inner {
                    "*" => segments.push(Segment::Wildcard),
                    n => {
                        let index: usize = n.parse().map_err(|_| {
                            format!(
                                "selector `{raw}`: `[{n}]` is neither `[*]` nor an index \
                                 (supported: dot paths, [*], [N])"
                            )
                        })?;
                        segments.push(Segment::Index(index));
                    }
                }
                rest = &rest[end + 1..];
                if !rest.is_empty() && !rest.starts_with('[') {
                    return Err(format!("selector `{raw}`: unexpected `{rest}` after `]`"));
                }
            }
        }
        Ok(Self {
            segments,
            raw: raw.to_owned(),
        })
    }

    /// The spelling this selector was parsed from, for error messages.
    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// Select all matches (wildcards flatten).
    pub fn select<'v>(&self, root: &'v Value) -> Vec<&'v Value> {
        self.select_tracking(root).0
    }

    /// Select, also reporting whether a wildcard traversed an EXISTING but
    /// empty array — zero matches then means "the page is legitimately
    /// empty", not "the path is wrong" (the terminal-page case).
    pub(crate) fn select_tracking<'v>(&self, root: &'v Value) -> (Vec<&'v Value>, bool) {
        let mut current = vec![root];
        let mut wildcard_saw_empty = false;
        for segment in &self.segments {
            let mut next = Vec::new();
            for node in current {
                match segment {
                    Segment::Field(name) => {
                        if let Some(value) = node.get(name) {
                            next.push(value);
                        }
                    }
                    Segment::Index(index) => {
                        if let Some(value) = node.get(index) {
                            next.push(value);
                        }
                    }
                    Segment::Wildcard => {
                        if let Value::Array(items) = node {
                            if items.is_empty() {
                                wildcard_saw_empty = true;
                            }
                            next.extend(items.iter());
                        }
                    }
                }
            }
            current = next;
        }
        (current, wildcard_saw_empty)
    }

    /// Select exactly one scalar-ish value (for cursors/next-urls/totals).
    pub fn select_one<'v>(&self, root: &'v Value) -> Option<&'v Value> {
        let matches = self.select(root);
        matches.first().copied()
    }
}

/// The kind of a JSON value, article included ("an array", "a bool",
/// "null"), for error messages that read "field X is {kind}".
pub(crate) fn value_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a bool",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn selector_subset_parses_and_rejects() {
        assert!(Selector::parse("data").is_ok());
        assert!(Selector::parse("data.items").is_ok());
        assert!(Selector::parse("data.items[*]").is_ok());
        assert!(Selector::parse("data.items[0].payload").is_ok());
        assert!(Selector::parse("[*]").is_ok());
        for bad in ["", "a..b", "a[", "a[x]", "a[*]b", "a[1]2"] {
            let error = Selector::parse(bad).unwrap_err();
            assert!(
                error.contains("selector") || error.contains("empty"),
                "{bad}: {error}"
            );
        }
    }

    #[test]
    fn selection_flattens_wildcards() {
        let document = json!({"data": {"items": [
            {"payload": {"id": 1}}, {"payload": {"id": 2}}
        ]}});
        let selector = Selector::parse("data.items[*].payload").unwrap();
        let matches = selector.select(&document);
        assert_eq!(matches.len(), 2);
        assert_eq!(matches[0]["id"], 1);
    }

    #[test]
    fn index_segments_select_one() {
        let document = json!({"items": [{"id": 1}, {"id": 2}]});
        let selector = Selector::parse("items[1].id").unwrap();
        assert_eq!(selector.select_one(&document), Some(&json!(2)));
    }

    #[test]
    fn wildcard_over_an_existing_empty_array_is_tracked() {
        let document = json!({"items": []});
        let selector = Selector::parse("items[*]").unwrap();
        let (matches, saw_empty) = selector.select_tracking(&document);
        assert!(matches.is_empty());
        assert!(saw_empty, "an existing empty array is a legitimate page");

        let missing = json!({"other": 1});
        let (matches, saw_empty) = selector.select_tracking(&missing);
        assert!(matches.is_empty());
        assert!(!saw_empty, "a missing path is not an empty page");
    }
}
