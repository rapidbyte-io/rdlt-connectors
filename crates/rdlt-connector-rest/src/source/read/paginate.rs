//! The paginator vocabulary: seven config-backed families behind the
//! public [`Paginator`] trait — the composition seam wrappers may implement
//! for API quirks. Every family runs under the same loop guards
//! (same-request detection + `max_pages`), owned by the read loop.

use serde_json::Value;

use crate::source::config::Pagination;
use crate::source::select::{Selector, value_kind};

/// What the paginator saw of one response — bounded on purpose: never the
/// whole body a second time.
#[derive(Debug)]
pub struct Context<'a> {
    /// Parsed response body (present for body-driven paginators).
    pub body: Option<&'a Value>,
    /// Response headers the paginator may need.
    pub headers: &'a reqwest::header::HeaderMap,
    /// Records extracted from this page.
    pub record_count: usize,
    /// Cumulative records across the stream so far.
    pub total_records: u64,
}

/// Where the next request goes.
#[derive(Debug, Clone, PartialEq)]
pub enum Decision {
    /// Stop: the stream is complete.
    Done,
    /// Fetch again with these query params merged in.
    NextParams(Vec<(String, String)>),
    /// Fetch this URL next (absolute, or relative — a relative URL
    /// resolves against the source `base_url`).
    NextUrl(String),
}

/// Why a paginator could not decide the next page. Both variants are
/// fatal: a wrong-typed cursor or next-url is an API-contract violation
/// retrying cannot fix; the single call site (the read loop's request
/// sequence) maps them to `SourceError::fatal`.
#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    /// A body cursor value was neither a string nor a number.
    CursorNotScalar { path: String, kind: &'static str },
    /// A next-url value was not a string.
    NextUrlNotString { path: String },
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CursorNotScalar { path, kind } => {
                write!(
                    formatter,
                    "cursor at `{path}` is {kind} — expected string or number"
                )
            }
            Self::NextUrlNotString { path } => {
                write!(formatter, "next url at `{path}` is not a string")
            }
        }
    }
}

impl std::error::Error for Error {}

/// The pagination contract (a public, stable seam). Implementations are
/// per-stream-read state machines: [`Paginator::initial_params`] yields the
/// extra params for the first request; [`Paginator::decide`] decides after
/// each page.
pub trait Paginator: Send + Sync {
    /// Extra query params for the FIRST request (e.g. `page=1`).
    fn initial_params(&self) -> Vec<(String, String)>;
    /// Decide after a page. `context.body` is `Some` iff
    /// [`Paginator::needs_body`].
    fn decide(&mut self, context: &Context<'_>) -> Result<Decision, Error>;
    /// Whether this paginator needs the parsed response body.
    fn needs_body(&self) -> bool {
        false
    }
}

/// Build the config-backed paginator for a stream. Selector parsing is
/// re-run here (config validation already proved these paths parse); a
/// parse error surfaces honestly rather than through a panic.
pub fn build(pagination: &Pagination) -> Result<Box<dyn Paginator>, String> {
    let paginator: Box<dyn Paginator> = match pagination {
        Pagination::None => Box::new(SinglePage),
        Pagination::Page {
            page_param,
            start,
            total_pages_path,
            total_count_path,
        } => Box::new(PageNumber {
            param: page_param.clone(),
            next: *start,
            start: *start,
            total_pages: parse_optional_selector(total_pages_path)?,
            total_count: parse_optional_selector(total_count_path)?,
        }),
        Pagination::Offset {
            offset_param,
            limit_param,
            page_size,
            total_count_path,
        } => Box::new(OffsetLimit {
            offset_param: offset_param.clone(),
            limit_param: limit_param.clone(),
            page_size: *page_size,
            offset: 0,
            total_count: parse_optional_selector(total_count_path)?,
        }),
        Pagination::BodyCursor {
            cursor_path,
            cursor_param,
        } => Box::new(BodyCursor {
            path: Selector::parse(cursor_path)?,
            param: cursor_param.clone(),
        }),
        Pagination::HeaderCursor {
            header,
            cursor_param,
        } => Box::new(HeaderCursor {
            header: header.clone(),
            param: cursor_param.clone(),
        }),
        Pagination::NextUrl { next_url_path } => Box::new(NextUrl {
            path: Selector::parse(next_url_path)?,
        }),
        Pagination::LinkHeader => Box::new(LinkHeader),
    };
    Ok(paginator)
}

/// Parse an optional selector path (a total-count/total-pages stop).
fn parse_optional_selector(path: &Option<String>) -> Result<Option<Selector>, String> {
    path.as_deref().map(Selector::parse).transpose()
}

/// A declared total-count stop: true once the cumulative record count
/// reaches the total the response advertises at `selector`.
fn total_count_reached(selector: &Option<Selector>, context: &Context<'_>) -> bool {
    let (Some(selector), Some(body)) = (selector, context.body) else {
        return false;
    };
    selector
        .select_one(body)
        .and_then(Value::as_u64)
        .is_some_and(|total| context.total_records >= total)
}

// ---- the seven families ---------------------------------------------------

struct SinglePage;

impl Paginator for SinglePage {
    fn initial_params(&self) -> Vec<(String, String)> {
        vec![]
    }
    fn decide(&mut self, _context: &Context<'_>) -> Result<Decision, Error> {
        Ok(Decision::Done)
    }
}

struct PageNumber {
    param: String,
    next: u64,
    start: u64,
    total_pages: Option<Selector>,
    total_count: Option<Selector>,
}

impl Paginator for PageNumber {
    fn initial_params(&self) -> Vec<(String, String)> {
        vec![(self.param.clone(), self.next.to_string())]
    }
    fn decide(&mut self, context: &Context<'_>) -> Result<Decision, Error> {
        if context.record_count == 0 {
            return Ok(Decision::Done);
        }
        let current = self.next;
        // Declared totals stop BEFORE an extra empty-page request.
        if let (Some(selector), Some(body)) = (&self.total_pages, context.body)
            && let Some(total) = selector.select_one(body).and_then(Value::as_u64)
        {
            let pages_done = current - self.start + 1;
            if pages_done >= total {
                return Ok(Decision::Done);
            }
        }
        if total_count_reached(&self.total_count, context) {
            return Ok(Decision::Done);
        }
        self.next = current + 1;
        Ok(Decision::NextParams(vec![(
            self.param.clone(),
            self.next.to_string(),
        )]))
    }
    fn needs_body(&self) -> bool {
        self.total_pages.is_some() || self.total_count.is_some()
    }
}

struct OffsetLimit {
    offset_param: String,
    limit_param: String,
    page_size: u64,
    offset: u64,
    total_count: Option<Selector>,
}

impl OffsetLimit {
    fn params(&self) -> Vec<(String, String)> {
        vec![
            (self.offset_param.clone(), self.offset.to_string()),
            (self.limit_param.clone(), self.page_size.to_string()),
        ]
    }
}

impl Paginator for OffsetLimit {
    fn initial_params(&self) -> Vec<(String, String)> {
        self.params()
    }
    fn decide(&mut self, context: &Context<'_>) -> Result<Decision, Error> {
        if context.record_count < self.page_size as usize {
            return Ok(Decision::Done); // short page = last page
        }
        if total_count_reached(&self.total_count, context) {
            return Ok(Decision::Done);
        }
        self.offset += self.page_size;
        Ok(Decision::NextParams(self.params()))
    }
    fn needs_body(&self) -> bool {
        self.total_count.is_some()
    }
}

struct BodyCursor {
    path: Selector,
    param: String,
}

impl Paginator for BodyCursor {
    fn initial_params(&self) -> Vec<(String, String)> {
        vec![]
    }
    fn decide(&mut self, context: &Context<'_>) -> Result<Decision, Error> {
        // No parsed body (an `ignore`d page whose body wasn't JSON): no
        // cursor to follow — the sequence ends cleanly.
        let Some(body) = context.body else {
            return Ok(Decision::Done);
        };
        match self.path.select_one(body) {
            None | Some(Value::Null) => Ok(Decision::Done),
            Some(Value::String(s)) if s.is_empty() => Ok(Decision::Done),
            Some(Value::String(s)) => {
                Ok(Decision::NextParams(vec![(self.param.clone(), s.clone())]))
            }
            Some(Value::Number(n)) => Ok(Decision::NextParams(vec![(
                self.param.clone(),
                n.to_string(),
            )])),
            Some(other) => Err(Error::CursorNotScalar {
                path: self.path.raw().to_owned(),
                kind: value_kind(other),
            }),
        }
    }
    fn needs_body(&self) -> bool {
        true
    }
}

struct HeaderCursor {
    header: String,
    param: String,
}

impl Paginator for HeaderCursor {
    fn initial_params(&self) -> Vec<(String, String)> {
        vec![]
    }
    fn decide(&mut self, context: &Context<'_>) -> Result<Decision, Error> {
        match context
            .headers
            .get(&self.header)
            .and_then(|v| v.to_str().ok())
        {
            None => Ok(Decision::Done),
            Some("") => Ok(Decision::Done),
            Some(value) => Ok(Decision::NextParams(vec![(
                self.param.clone(),
                value.to_owned(),
            )])),
        }
    }
}

struct NextUrl {
    path: Selector,
}

impl Paginator for NextUrl {
    fn initial_params(&self) -> Vec<(String, String)> {
        vec![]
    }
    fn decide(&mut self, context: &Context<'_>) -> Result<Decision, Error> {
        let Some(body) = context.body else {
            return Ok(Decision::Done); // ignored non-JSON page: no next URL
        };
        match self.path.select_one(body) {
            None | Some(Value::Null) => Ok(Decision::Done),
            Some(Value::String(url)) if url.is_empty() => Ok(Decision::Done),
            Some(Value::String(url)) => Ok(Decision::NextUrl(url.clone())),
            Some(_) => Err(Error::NextUrlNotString {
                path: self.path.raw().to_owned(),
            }),
        }
    }
    fn needs_body(&self) -> bool {
        true
    }
}

struct LinkHeader;

impl Paginator for LinkHeader {
    fn initial_params(&self) -> Vec<(String, String)> {
        vec![]
    }
    fn decide(&mut self, context: &Context<'_>) -> Result<Decision, Error> {
        let Some(link) = context
            .headers
            .get(reqwest::header::LINK)
            .and_then(|v| v.to_str().ok())
        else {
            return Ok(Decision::Done);
        };
        match parse_link_next(link) {
            Some(url) => Ok(Decision::NextUrl(url)),
            None => Ok(Decision::Done),
        }
    }
}

/// RFC5988 subset: find the `<url>; rel="next"` member. Members are walked
/// by locating each `<...>` PAIR first (URLs may legally contain commas,
/// so naive `split(',')` breaks them), and a malformed member never aborts
/// the scan of later ones.
pub(crate) fn parse_link_next(header: &str) -> Option<String> {
    let mut rest = header;
    loop {
        let start = rest.find('<')?;
        let end = start + rest[start..].find('>')?;
        let url = &rest[start + 1..end];
        let after = &rest[end + 1..];
        // Params run to the comma that separates this member from the next.
        let (params, next) = match after.find(',') {
            Some(comma) => (&after[..comma], &after[comma + 1..]),
            None => (after, ""),
        };
        let is_next = params.split(';').any(|param| {
            let param = param.trim();
            param.eq_ignore_ascii_case("rel=next") || param.eq_ignore_ascii_case("rel=\"next\"")
        });
        if is_next {
            return Some(url.to_owned());
        }
        if next.is_empty() {
            return None;
        }
        rest = next;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn link_header_subset() {
        let header = "<https://api.example.com/x?page=2>; rel=\"next\", <https://api.example.com/x?page=9>; rel=\"last\"";
        assert_eq!(
            parse_link_next(header).as_deref(),
            Some("https://api.example.com/x?page=2")
        );
        assert_eq!(parse_link_next("<https://x>; rel=\"prev\""), None);
        assert_eq!(parse_link_next("junk"), None);
    }

    /// Commas INSIDE a member's URL, and non-`<...>` members before the
    /// next link, must not truncate the scan (the silent-stop bug).
    #[test]
    fn link_header_survives_commas_and_junk_members() {
        let comma_url = "<https://api.example.com/x?ids=1,2&page=2>; rel=\"next\"";
        assert_eq!(
            parse_link_next(comma_url).as_deref(),
            Some("https://api.example.com/x?ids=1,2&page=2")
        );
        let junk_first = "malformed, <https://api.example.com/x?page=3>; rel=next";
        assert_eq!(
            parse_link_next(junk_first).as_deref(),
            Some("https://api.example.com/x?page=3")
        );
        let prev_with_comma = "<https://x/a?ids=1,2>; rel=\"prev\", <https://x/b>; rel=\"next\"";
        assert_eq!(
            parse_link_next(prev_with_comma).as_deref(),
            Some("https://x/b")
        );
    }
}
