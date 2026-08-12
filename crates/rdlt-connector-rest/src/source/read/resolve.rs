//! Parent-child placeholder resolution: `{token}` substitution into
//! path/params/body from a parent record's fields, plus the
//! `_parent_<field>` embedding. Buffering is BOUNDED — only the referenced
//! placeholder values and declared include fields, never whole parent
//! records.

use std::collections::BTreeMap;

use rdlt_connector_sdk::spi::SourceError;
use serde_json::Value;

use crate::source::config::Parent;
use crate::source::select::{Selector, value_kind};

/// One parent record's contribution: resolved placeholder values + the
/// fields to embed into child records.
#[derive(Debug, Clone)]
pub(crate) struct ParentValues {
    pub(crate) placeholders: BTreeMap<String, String>,
    pub(crate) include: Vec<(String, Value)>,
}

/// Extract every parent record's values from one parent PAGE (the parsed
/// records the read loop already holds — never a reparse).
pub(crate) fn parent_values(
    items: &[Value],
    parent: &Parent,
) -> Result<Vec<ParentValues>, SourceError> {
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let mut placeholders = BTreeMap::new();
        for (token, field_path) in &parent.placeholders {
            let selector = Selector::parse(field_path)
                .map_err(|e| SourceError::fatal(format!("parent placeholder `{token}`: {e}")))?;
            let value = selector.select_one(item).ok_or_else(|| {
                SourceError::fatal(format!(
                    "parent record lacks field `{field_path}` for placeholder `{{{token}}}`"
                ))
            })?;
            // Placeholders deliberately accept Bool (a `{active}` path
            // segment renders `true`/`false`) as well as string and number;
            // only container/null values are rejected — a distinct policy
            // from the cursor scalar render, which is string-or-number only.
            let rendered = match value {
                Value::String(s) => s.clone(),
                Value::Number(n) => n.to_string(),
                Value::Bool(b) => b.to_string(),
                other => {
                    return Err(SourceError::fatal(format!(
                        "parent field `{field_path}` for placeholder `{{{token}}}` is {} — \
                         placeholders take scalars",
                        value_kind(other)
                    )));
                }
            };
            placeholders.insert(token.clone(), rendered);
        }
        let mut include = Vec::with_capacity(parent.include.len());
        for field in &parent.include {
            let value = item.get(field).cloned().unwrap_or(Value::Null);
            include.push((field.clone(), value));
        }
        out.push(ParentValues {
            placeholders,
            include,
        });
    }
    Ok(out)
}

/// Substitute `{token}` occurrences into a URL PATH, percent-encoding each
/// value so data cannot alter the request's structure.
///
/// The values come from a parent stream's records — remote data.
/// Substituted raw, a value containing `/`, `?`, `#` or `%` re-points the
/// child request: an id of `../admin` walks up a path segment, `x?y=1`
/// starts a query string. Only the substituted VALUE is encoded; the
/// template's own separators are structure the operator wrote and must
/// survive.
///
/// The escape set is RFC 3986 unreserved (`A-Z a-z 0-9 - . _ ~`), i.e.
/// RFC 6570 simple string expansion — the standard rule for a URI-template
/// variable. Hand-rolled rather than pulling in a crate: it is nine lines
/// and one table.
///
/// With ONE addition the standard rule does not cover: a value that
/// encodes to exactly `.` or `..` is a DOT SEGMENT, and the URL parser
/// removes it — so `..` still walks up a path segment even though every
/// character in it is unreserved. Those two values have their dots escaped
/// as well, which keeps them a literal segment. Escaping `.` everywhere is
/// not the answer: it is legitimate and common inside an ordinary id.
pub(crate) fn substitute_path(template: &str, values: &BTreeMap<String, String>) -> String {
    let mut encoded_values = BTreeMap::new();
    for (token, value) in values {
        let mut encoded = String::with_capacity(value.len());
        for byte in value.bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                    encoded.push(byte as char);
                }
                _ => encoded.push_str(&format!("%{byte:02X}")),
            }
        }
        if encoded == "." || encoded == ".." {
            encoded = encoded.replace('.', "%2E");
        }
        encoded_values.insert(token.clone(), encoded);
    }
    replace_tokens(template, &encoded_values)
}

/// Substitute `{token}` occurrences in a template string, VERBATIM.
///
/// For query-parameter values and body strings only: `reqwest` and `serde`
/// encode those themselves, so encoding here would double-encode them.
/// Never use this for a path — see [`substitute_path`].
pub(crate) fn substitute(template: &str, values: &BTreeMap<String, String>) -> String {
    replace_tokens(template, values)
}

/// ONE left-to-right pass over the TEMPLATE: each declared `{token}` is
/// replaced with its value, an undeclared brace stays literal, and
/// substituted text is never rescanned. Sequential per-token `.replace()`
/// calls would rescan earlier substitutions — a parent value containing
/// literal text shaped like another declared `{token}` (remote data) would
/// then have that other field's value injected into it.
fn replace_tokens(template: &str, values: &BTreeMap<String, String>) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        out.push_str(&rest[..open]);
        let after_brace = &rest[open + 1..];
        let replaced = after_brace.find('}').and_then(|close| {
            values
                .get(&after_brace[..close])
                .map(|value| (value, &after_brace[close + 1..]))
        });
        match replaced {
            Some((value, remainder)) => {
                out.push_str(value);
                rest = remainder;
            }
            None => {
                out.push('{');
                rest = after_brace;
            }
        }
    }
    out.push_str(rest);
    out
}

/// Substitute `{token}` occurrences throughout a JSON body template (POST
/// bodies carry placeholders in string leaves, at any depth).
pub(crate) fn substitute_body(body: &Value, values: &BTreeMap<String, String>) -> Value {
    match body {
        Value::String(s) => Value::String(substitute(s, values)),
        Value::Array(items) => {
            Value::Array(items.iter().map(|v| substitute_body(v, values)).collect())
        }
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), substitute_body(v, values)))
                .collect(),
        ),
        other => other.clone(),
    }
}

/// Human-readable resolved-values summary for failure messages, so a child
/// failure NAMES the parent's resolved values.
pub(crate) fn describe(values: &BTreeMap<String, String>) -> String {
    values
        .iter()
        .map(|(token, value)| format!("{token}={value}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Embed `_parent_<field>` values into each child record of a page (the
/// parsed values, consumed — no reparse) and serialize once. Collision
/// with an existing child field is a typed error.
pub(crate) fn embed(
    mut items: Vec<Value>,
    include: &[(String, Value)],
) -> Result<bytes::Bytes, SourceError> {
    for item in &mut items {
        let Value::Object(map) = item else {
            return Err(SourceError::fatal(
                "child records must be objects to embed parent fields",
            ));
        };
        for (field, value) in include {
            let key = format!("_parent_{field}");
            if map.contains_key(&key) {
                return Err(SourceError::fatal(format!(
                    "child record already has a `{key}` field — parent include collides"
                )));
            }
            map.insert(key, value.clone());
        }
    }
    let bytes = serde_json::to_vec(&items).map_err(|e| SourceError::fatal(e.to_string()))?;
    Ok(bytes.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parent(placeholders: &[(&str, &str)], include: &[&str]) -> Parent {
        Parent {
            stream: "parents".into(),
            placeholders: placeholders
                .iter()
                .map(|(token, path)| (token.to_string(), path.to_string()))
                .collect(),
            include: include.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn collects_and_substitutes() {
        let records = [json!({"id": 7, "name": "ada", "org": {"slug": "acme"}})];
        let values = parent_values(
            &records,
            &parent(&[("id", "id"), ("org", "org.slug")], &["name"]),
        )
        .unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(
            substitute("/orgs/{org}/users/{id}", &values[0].placeholders),
            "/orgs/acme/users/7"
        );
        assert_eq!(describe(&values[0].placeholders), "id=7, org=acme");
    }

    #[test]
    fn missing_field_and_non_scalar_are_typed() {
        let error = parent_values(&[json!({"id": [1, 2]})], &parent(&[("id", "id")], &[]))
            .unwrap_err()
            .to_string();
        assert!(error.contains("an array"), "{error}");
        let error = parent_values(&[json!({"x": 1})], &parent(&[("id", "id")], &[]))
            .unwrap_err()
            .to_string();
        assert!(error.contains("lacks field `id`"), "{error}");
    }

    #[test]
    fn embeds_parent_fields_and_detects_collisions() {
        let out = embed(vec![json!({"a": 1})], &[("name".into(), json!("ada"))]).unwrap();
        let items: Vec<Value> = serde_json::from_slice(&out).unwrap();
        assert_eq!(items[0]["_parent_name"], "ada");

        let error = embed(
            vec![json!({"_parent_name": 1})],
            &[("name".into(), json!("x"))],
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("collides"), "{error}");
    }

    /// Parent values are REMOTE DATA. Substituted raw into a path they can
    /// re-point the request: `../admin` walks up a segment, `x?y=1` starts
    /// a query string. Only the value is encoded — the template's own
    /// separators are structure the operator wrote.
    #[test]
    fn a_path_value_cannot_restructure_the_request() {
        let mut values = BTreeMap::new();
        values.insert("id".to_owned(), "../admin".to_owned());
        assert_eq!(
            substitute_path("/orgs/acme/users/{id}", &values),
            "/orgs/acme/users/..%2Fadmin"
        );

        // `.` and `..` are unreserved, so a naive RFC 3986 encoder passes
        // them through and the URL parser then REMOVES the segment — `..`
        // walks up.
        for dots in [".", ".."] {
            let mut values = BTreeMap::new();
            values.insert("id".to_owned(), dots.to_owned());
            let out = substitute_path("/orgs/acme/users/{id}", &values);
            assert!(
                !out.ends_with("/.") && !out.ends_with("/.."),
                "`{dots}` must not survive as a dot segment: {out}"
            );
        }

        let mut values = BTreeMap::new();
        values.insert("id".to_owned(), "x?y=1#frag".to_owned());
        assert_eq!(
            substitute_path("/users/{id}/events", &values),
            "/users/x%3Fy%3D1%23frag/events"
        );

        // Unreserved characters pass through, so ordinary ids stay
        // readable.
        let mut values = BTreeMap::new();
        values.insert("id".to_owned(), "abc-123_x.y~z".to_owned());
        assert_eq!(substitute_path("/u/{id}", &values), "/u/abc-123_x.y~z");
    }

    /// Query and body values keep going through verbatim — reqwest and
    /// serde encode those, and encoding here would double-encode them.
    #[test]
    fn query_and_body_substitution_stays_verbatim() {
        let mut values = BTreeMap::new();
        values.insert("id".to_owned(), "a b&c".to_owned());
        assert_eq!(substitute("{id}", &values), "a b&c");
    }

    /// Substitution is ONE pass over the TEMPLATE: a parent value whose
    /// text happens to look like another declared placeholder arrives
    /// verbatim — the other field's value must not be injected into it
    /// (values are remote data; sequential per-token replacement rescans
    /// earlier substitutions and corrupts exactly this case).
    #[test]
    fn a_value_resembling_another_token_is_not_resubstituted() {
        let mut values = BTreeMap::new();
        values.insert("a".to_owned(), "prefix-{z}-suffix".to_owned());
        values.insert("z".to_owned(), "999".to_owned());
        assert_eq!(substitute("{a}", &values), "prefix-{z}-suffix");
        assert_eq!(
            substitute("q={a}&r={z}", &values),
            "q=prefix-{z}-suffix&r=999"
        );
        // Undeclared braces in the template stay literal.
        assert_eq!(substitute("{missing}-{z}", &values), "{missing}-999");
    }
}
