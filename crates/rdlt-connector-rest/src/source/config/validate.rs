//! The ONE validation gate. Every entry point runs it, so the read path's
//! invariants (selectors parse, `max_concurrency >= 1`, parent membership,
//! bounded request deadline) are real by the time a connector exists —
//! nothing downstream re-checks or papers over them.
//!
//! Refusal messages are FROZEN: the suites pin these strings, and an
//! operator's muscle memory is part of the surface.

use std::collections::BTreeMap;

use rdlt_connector_sdk::config::Document;

use super::vocabulary::{Config, ConfigError, Method, Pagination, Stream};
use crate::source::select::Selector;

/// The [`Document`] gate: the sdk's provided `from_yaml`/`from_json`/
/// `from_value` parse and then run THIS — the one validation gate, with
/// the crate's own frozen refusal spellings.
impl Document for Config {
    type Error = ConfigError;

    fn validate(&self) -> Result<(), ConfigError> {
        if self.streams.is_empty() {
            return invalid("at least one stream is required".into());
        }
        // The base_url is the PIN every outbound credential-bearing
        // request answers to (pagination next-urls and redirect hops are
        // refused past its origin), so it must parse as an absolute URL
        // with a host — a relative spelling or a bare host would leave
        // the pin undefined. Plain http stays legal: local stubs and
        // intranet APIs are real deployments.
        match reqwest::Url::parse(&self.base_url) {
            Ok(parsed) if matches!(parsed.scheme(), "http" | "https") && parsed.host_str().is_some() => {}
            _ => {
                return invalid(
                    "base_url must be an absolute http(s) URL with a host, e.g. \
                     https://api.example.com"
                        .into(),
                )
            }
        }
        if self.max_concurrency == 0 {
            return invalid("max_concurrency must be at least 1".into());
        }
        if self.request_timeout_secs == 0 {
            return invalid(
                "request_timeout_secs must be at least 1 — a request without a deadline can \
                 stall a stream forever; raise it rather than disabling it"
                    .into(),
            );
        }
        validate_headers(&self.headers, None)?;
        let names: Vec<&str> = self.streams.iter().map(|s| s.name.as_str()).collect();
        for stream in &self.streams {
            validate_post_body_pagination(stream)?;
            validate_headers(&stream.headers, Some(&stream.name))?;
            validate_cursor_spellings(stream)?;
            validate_selectors(stream)?;
            validate_response_actions(stream)?;
            validate_parent(self, &names, stream)?;
        }
        Ok(())
    }
}

fn invalid(message: String) -> Result<(), ConfigError> {
    Err(ConfigError::Invalid(message))
}

/// Header names that almost always mean a credential was hard-coded into a
/// plain (Debug-printable) `headers:` map instead of the `Secret`-wrapped
/// `auth:` block. Returns the rejection message when `name` is one of them,
/// case-insensitively.
fn credential_header_refusal(name: &str) -> Option<String> {
    // A slice, so the length is not hand-maintained. EXACT names only,
    // matched case-insensitively — a substring or suffix rule would reject
    // legitimate headers like `x-request-token-count`, and a guard that
    // fires on innocent configuration gets disabled rather than heeded.
    const RESERVED: &[&str] = &[
        "authorization",
        "proxy-authorization",
        "cookie",
        "x-api-key",
        "api-key",
        "apikey",
        "x-api-token",
        "x-auth-token",
        "x-access-token",
        "x-amz-security-token",
        "x-goog-api-key",
        "private-token",
        "x-functions-key",
    ];
    let lowercase = name.to_ascii_lowercase();
    RESERVED.contains(&lowercase.as_str()).then(|| {
        format!(
            "header `{name}` carries a credential — put it under `auth:` \
             (Secret-wrapped and masked), not in a plain `headers:` map that \
             renders verbatim in Debug output"
        )
    })
}

/// Reject credential-bearing header names toward `auth:`. Source-level
/// headers (`stream` is `None`) additionally get their name/value
/// parse-checked, since they are parsed once at client construction;
/// per-stream headers are attached per request, where reqwest surfaces a
/// malformed one at build time.
fn validate_headers(
    headers: &BTreeMap<String, String>,
    stream: Option<&str>,
) -> Result<(), ConfigError> {
    for (name, value) in headers {
        if stream.is_none() {
            if name.parse::<reqwest::header::HeaderName>().is_err() {
                return invalid(format!("header `{name}`: not a valid HTTP header name"));
            }
            if value.parse::<reqwest::header::HeaderValue>().is_err() {
                return invalid(format!(
                    "header `{name}`: value is not a valid HTTP header value"
                ));
            }
        }
        if let Some(message) = credential_header_refusal(name) {
            return match stream {
                Some(name) => invalid(format!("stream `{name}`: {message}")),
                None => invalid(message),
            };
        }
    }
    Ok(())
}

/// Page parameters reach a POST request through its BODY, and only a keyed
/// document has anywhere to put them. With any other body the paginator's
/// parameters are silently dropped and every page sends the byte-identical
/// request — which the duplicate-request guard cannot see, because it
/// hashes the page parameters and those do change. The run then ingests the
/// first page `max_pages` times and fails citing a page limit that was
/// never the problem.
///
/// The four families listed here are the ones that produce parameters.
/// `None` produces a single request, and `NextUrl`/`LinkHeader` carry the
/// next page in the response rather than in the request — none of them
/// needs the body.
fn validate_post_body_pagination(stream: &Stream) -> Result<(), ConfigError> {
    let carries_params = matches!(
        stream.pagination,
        Pagination::Page { .. }
            | Pagination::Offset { .. }
            | Pagination::BodyCursor { .. }
            | Pagination::HeaderCursor { .. }
    );
    if stream.method == Method::Post
        && carries_params
        && let Some(body) = &stream.body
        && !body.is_object()
    {
        return invalid(format!(
            "stream `{}`: a paginated POST needs an object `body` — page parameters are \
             merged into it, and a {} body has nowhere to carry them, so every page would \
             repeat the first request",
            stream.name,
            match body {
                serde_json::Value::Array(_) => "list",
                serde_json::Value::String(_) => "string",
                serde_json::Value::Null => "null",
                _ => "scalar",
            }
        ));
    }
    Ok(())
}

/// Legacy cursor aliases (set together, never mixed with the block), the
/// incremental block's own consistency, and the POST-only `body`.
fn validate_cursor_spellings(stream: &Stream) -> Result<(), ConfigError> {
    let name = &stream.name;
    if stream.cursor_field.is_some() != stream.cursor_param.is_some() {
        return invalid(format!(
            "stream `{name}`: cursor_field and cursor_param must be set together"
        ));
    }
    if stream.incremental.is_some() && stream.cursor_field.is_some() {
        return invalid(format!(
            "stream `{name}`: use either the `incremental` block or the \
             legacy cursor_field/cursor_param aliases, not both"
        ));
    }
    if let Some(incremental) = &stream.incremental {
        if incremental.end_param.is_some() != incremental.end_value.is_some() {
            return invalid(format!(
                "stream `{name}`: incremental.end_param and end_value must be \
                 set together (closed windows take explicit values)"
            ));
        }
        if incremental.cursor_field.trim().is_empty() {
            return invalid(format!(
                "stream `{name}`: incremental.cursor_field must not be empty"
            ));
        }
    }
    if stream.body.is_some() && stream.method != Method::Post {
        return invalid(format!("stream `{name}`: `body` requires `method: post`"));
    }
    Ok(())
}

/// Selector syntax, validated eagerly at parse time: the stream's
/// `records_path` and every selector its pagination family carries, plus
/// the Page family's mutually exclusive total stops.
fn validate_selectors(stream: &Stream) -> Result<(), ConfigError> {
    let name = &stream.name;
    let records_path = stream.records_path.as_deref().map(|p| ("records_path", p));
    for (label, selector) in records_path
        .into_iter()
        .chain(stream.pagination.selector_paths())
    {
        if let Err(e) = Selector::parse(selector) {
            return invalid(format!("stream `{name}`: {label}: {e}"));
        }
    }
    if let Pagination::Page {
        total_pages_path: Some(_),
        total_count_path: Some(_),
        ..
    } = &stream.pagination
    {
        return invalid(format!(
            "stream `{name}`: pagination declares both total_pages_path and \
             total_count_path — pick one stop condition"
        ));
    }
    Ok(())
}

/// Response actions: each needs at least one matcher, and a declared status
/// must be a real HTTP status (a typo like `42` would otherwise be silently
/// dead).
fn validate_response_actions(stream: &Stream) -> Result<(), ConfigError> {
    let name = &stream.name;
    for (index, action) in stream.response_actions.iter().enumerate() {
        if action.status.is_none() && action.content_contains.is_none() {
            return invalid(format!(
                "stream `{name}`: response_actions[{index}] needs `status` and/or \
                 `content_contains` — an unconditional action would swallow \
                 every response"
            ));
        }
        if let Some(status) = action.status
            && !(100..=599).contains(&status)
        {
            return invalid(format!(
                "stream `{name}`: response_actions[{index}]: status {status} is not \
                 an HTTP status (100–599)"
            ));
        }
    }
    Ok(())
}

/// Parent-child linkage: a declared, non-self, non-nested parent stream,
/// with every placeholder actually referenced; and, absent a parent, no
/// stray `{placeholder}` tokens in the path.
fn validate_parent(config: &Config, names: &[&str], stream: &Stream) -> Result<(), ConfigError> {
    let name = &stream.name;
    let Some(parent) = &stream.parent else {
        // Placeholders in the path REQUIRE a parent block.
        if stream.path.contains('{') && stream.path.contains('}') {
            return invalid(format!(
                "stream `{name}`: path contains `{{placeholder}}` tokens but no \
                 `parent` block declares them"
            ));
        }
        return Ok(());
    };
    if parent.stream == *name {
        return invalid(format!("stream `{name}`: parent.stream is itself"));
    }
    if !names.contains(&parent.stream.as_str()) {
        return invalid(format!(
            "stream `{name}`: parent.stream `{}` is not a declared stream",
            parent.stream
        ));
    }
    let parent_declaration = config
        .streams
        .iter()
        .find(|s| s.name == parent.stream)
        .expect("parent.stream membership just checked against declared names");
    if parent_declaration.parent.is_some() {
        return invalid(format!(
            "stream `{name}`: parent.stream `{}` is itself a child — \
             one level of nesting is supported",
            parent.stream
        ));
    }
    if parent.placeholders.is_empty() {
        return invalid(format!(
            "stream `{name}`: parent.placeholders must not be empty"
        ));
    }
    for token in parent.placeholders.keys() {
        let referenced = stream.path.contains(&format!("{{{token}}}"))
            || stream
                .params
                .values()
                .any(|v| v.contains(&format!("{{{token}}}")))
            || stream
                .body
                .as_ref()
                .is_some_and(|b| b.to_string().contains(&format!("{{{token}}}")));
        if !referenced {
            return invalid(format!(
                "stream `{name}`: placeholder `{{{token}}}` is declared but \
                 never used in path, params, or body"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rdlt_connector_sdk::config::Document;

    use super::super::vocabulary::Config;

    fn err(config: serde_json::Value) -> String {
        Config::from_value(config)
            .expect_err("must reject")
            .to_string()
    }

    #[test]
    fn credential_header_names_are_rejected_toward_auth() {
        // Source-level, per-stream, and case-insensitively — each steered
        // to `auth:`.
        for header in ["Authorization", "authorization", "X-API-Key", "x-api-key"] {
            let source_level = serde_json::json!({
                "base_url": "https://x",
                "headers": {header: "Bearer leaked"},
                "streams": [{"name": "s", "path": "/s"}],
            });
            let message = err(source_level);
            assert!(
                message.contains("auth:"),
                "{header} source-level: {message}"
            );

            let per_stream = serde_json::json!({
                "base_url": "https://x",
                "streams": [{"name": "s", "path": "/s", "headers": {header: "leaked"}}],
            });
            let message = err(per_stream);
            assert!(message.contains("auth:"), "{header} per-stream: {message}");
        }
    }

    #[test]
    fn ordinary_headers_still_accepted() {
        Config::from_value(serde_json::json!({
            "base_url": "https://x",
            "headers": {"user-agent": "rdlt", "x-shared": "1"},
            "streams": [{"name": "s", "path": "/s", "headers": {"x-stream": "2"}}],
        }))
        .expect("non-credential headers pass validation");
    }
}
