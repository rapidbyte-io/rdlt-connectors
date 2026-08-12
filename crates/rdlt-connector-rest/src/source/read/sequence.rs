//! One paginated request sequence: request build, response actions, loop
//! guards (same-request detection + `max_pages`), and the crash points
//! (`rest.request`, `rest.decode`). The state machine every read path
//! drives, parentless or fanned-out alike.

use std::collections::{BTreeMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};

use bytes::Bytes;
use rdlt_connector_sdk::spi::SourceError;
use rdlt_connector_sdk::spi::core::crash_point;
use serde_json::Value;

use crate::source::config::{ActionKind, Config, Incremental, Method, ResponseAction, Stream};
use crate::source::http::{Client, classify};
use crate::source::select::Selector;

use super::extract::{self, Page};
use super::paginate::{Context, Decision, Paginator};
use super::resolve::{self, ParentValues};

/// One paginated request sequence: request build, actions, guards.
pub(crate) struct Sequence<'a> {
    config: &'a Config,
    stream: &'a Stream,
    client: &'a Client,
    paginator: &'a mut dyn Paginator,
    /// Set when this sequence is one child of a parent fan-out: its path,
    /// params, and body resolve against these placeholder values.
    parent: Option<&'a ParentValues>,
    /// Whether extraction should keep parsed record values on the page,
    /// so the incremental-cursor and parent-child-embedding consumers
    /// never reparse the records.
    needs_values: bool,
    /// Params merged into every request (incremental bindings).
    extra_params: Vec<(String, String)>,
    /// Current page params from the paginator (or a full URL override).
    page_params: Vec<(String, String)>,
    url_override: Option<String>,
    /// Loop guards: request fingerprints, stored as u64 hashes.
    seen_requests: HashSet<u64>,
    pages: u64,
    total_records: u64,
    /// Body of the LAST response (for body-driven paginators).
    last_body: Option<Value>,
    last_headers: reqwest::header::HeaderMap,
    ended: bool,
}

impl<'a> Sequence<'a> {
    pub(crate) fn new(
        config: &'a Config,
        stream: &'a Stream,
        client: &'a Client,
        paginator: &'a mut dyn Paginator,
        needs_values: bool,
        parent: Option<&'a ParentValues>,
    ) -> Self {
        let page_params = paginator.initial_params();
        Self {
            config,
            stream,
            client,
            paginator,
            parent,
            needs_values,
            extra_params: Vec::new(),
            page_params,
            url_override: None,
            seen_requests: HashSet::new(),
            pages: 0,
            total_records: 0,
            last_body: None,
            last_headers: reqwest::header::HeaderMap::new(),
            ended: false,
        }
    }

    /// Merge a stream's incremental bindings into every request of this
    /// sequence: the resume ("since") param and any closed-window upper
    /// bound.
    pub(crate) fn bind_incremental(
        &mut self,
        incremental: &Option<Incremental>,
        start_value: &Option<String>,
    ) {
        let Some(incremental) = incremental else {
            return;
        };
        if let (Some(param), Some(value)) = (&incremental.start_param, start_value.as_ref()) {
            self.extra_params.push((param.clone(), value.clone()));
        }
        if let (Some(param), Some(value)) = (&incremental.end_param, &incremental.end_value) {
            self.extra_params.push((param.clone(), value.clone()));
        }
    }

    fn base_url(&self) -> String {
        let path = match self.parent {
            Some(values) => resolve::substitute_path(&self.stream.path, &values.placeholders),
            None => self.stream.path.clone(),
        };
        join_base_url(&self.config.base_url, &path)
    }

    /// Issue the next request; returns None when the sequence ended before
    /// this page (single page done / paginator said stop last round).
    pub(crate) async fn fetch_page(
        &mut self,
        selector: &Option<Selector>,
    ) -> Result<Option<Page>, SourceError> {
        if self.ended {
            return Ok(None);
        }
        let url = self.url_override.clone().unwrap_or_else(|| self.base_url());

        // The same-request guard: a u64 fingerprint of the URL override,
        // the paginator's page parameters, and the incremental bindings.
        // Only the first two vary between the pages of one sequence; the
        // bindings are fixed but cheap, and folding a fixed component into
        // every fingerprint cannot change an outcome. The body template
        // and the parent's placeholder values are likewise fixed and are
        // left out — rendering the body per page to hash it only costs
        // work.
        let fingerprint = {
            let mut hasher = DefaultHasher::new();
            url.hash(&mut hasher);
            self.page_params.hash(&mut hasher);
            self.extra_params.hash(&mut hasher);
            hasher.finish()
        };
        if !self.seen_requests.insert(fingerprint) {
            return Err(SourceError::fatal(format!(
                "stream `{}`: paginator produced the SAME request twice (page {}, \
                 params {:?}) — the API is not advancing; check the pagination config",
                self.stream.name, self.pages, self.page_params
            )));
        }
        if self.pages >= self.config.max_pages {
            return Err(SourceError::fatal(format!(
                "stream `{}`: exceeded max_pages ({}) — raise `max_pages` if the \
                 stream is genuinely this long",
                self.stream.name, self.config.max_pages
            )));
        }
        self.pages += 1;

        crash_point!(
            "rest.request",
            Err(SourceError::fatal("injected crash at rest.request"))
        );
        let stream = self.stream;
        let page_params = self.page_params.clone();
        let extra_params = self.extra_params.clone();
        let parent_values = self.parent.map(|v| v.placeholders.clone());
        let response = self
            .client
            .send(move |http| {
                build_page_request(
                    http,
                    stream,
                    &url,
                    &page_params,
                    &extra_params,
                    parent_values.as_ref(),
                )
            })
            .await;

        // Err carries anything WITHOUT a data-response status to match:
        // transport failures (transient), token-endpoint failures (already
        // classified), request-build failures (fatal).
        let response = response?;
        let status = response.status();
        // Reported to the engine UNCLAMPED, deliberately. `retry_after_cap`
        // bounds how long this source will wait in-process; it says nothing
        // about how long the SERVER's window is. The engine sleeps on this
        // number verbatim, so clamping it here would send the next attempt
        // back inside a window the server has already said is closed — and
        // with a small cap it would spend the whole retry budget in
        // milliseconds.
        let retry_after = classify::server_retry_after(&response);
        let headers = response.headers().clone();
        let body = response.bytes().await.map_err(classify::transport)?;
        crash_point!(
            "rest.decode",
            Err(SourceError::fatal("injected crash at rest.decode"))
        );
        // Declared response actions match the TYPED status and the body,
        // success and error responses alike — first match wins. This is an
        // allow-list posture: anything undeclared stays classified.
        if let Some(action) = match_action(&self.stream.response_actions, status.as_u16(), &body) {
            let kind = action.kind;
            self.last_headers = headers;
            return self.apply_action(kind, status, &body);
        }
        if !status.is_success() {
            return Err(classify::status(status, retry_after));
        }

        let page = extract::page(&body, selector.as_ref(), self.needs_values)?;
        self.total_records += page.count as u64;
        self.last_headers = headers;
        self.last_body = if self.paginator.needs_body() {
            Some(extract::parse_body(&body)?)
        } else {
            None
        };
        Ok(Some(page))
    }

    fn apply_action(
        &mut self,
        kind: ActionKind,
        status: reqwest::StatusCode,
        body: &Bytes,
    ) -> Result<Option<Page>, SourceError> {
        match kind {
            ActionKind::Error => Err(SourceError::fatal(format!(
                "stream `{}`: declared error action matched (HTTP {status})",
                self.stream.name
            ))),
            ActionKind::EndStream => {
                self.ended = true;
                Ok(None)
            }
            ActionKind::Ignore => {
                // An empty page — but a body-driven paginator still gets the
                // body: an ignored page may carry the cursor that continues
                // the chain; an unparseable one ends it cleanly.
                self.last_body = if self.paginator.needs_body() {
                    serde_json::from_slice(body).ok()
                } else {
                    None
                };
                Ok(Some(Page::empty()))
            }
        }
    }

    /// Ask the paginator for the next move. Returns false when done.
    pub(crate) fn advance(&mut self, page: &Page) -> Result<bool, SourceError> {
        if self.ended {
            return Ok(false);
        }
        let context = Context {
            body: self.last_body.as_ref(),
            headers: &self.last_headers,
            record_count: page.count,
            total_records: self.total_records,
        };
        match self
            .paginator
            .decide(&context)
            .map_err(|e| SourceError::fatal(format!("stream `{}`: {e}", self.stream.name)))?
        {
            Decision::Done => {
                self.ended = true;
                Ok(false)
            }
            Decision::NextParams(params) => {
                self.page_params = params;
                self.url_override = None;
                Ok(true)
            }
            Decision::NextUrl(url) => {
                let absolute = if url.starts_with("http://") || url.starts_with("https://") {
                    url
                } else {
                    join_base_url(&self.config.base_url, &url)
                };
                self.url_override = Some(absolute);
                self.page_params = Vec::new();
                Ok(true)
            }
        }
    }
}

/// Join a stream path (or a relative next-url) onto the source base URL,
/// collapsing the boundary slash so neither a trailing nor a leading one
/// doubles up.
fn join_base_url(base: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

/// Build one page request. A single `match (method, body)` decides how the
/// paginator's page params ride: query for GET (and body-less POST), or
/// into the JSON body for a POST-cursor API. Source/stream params and
/// incremental bindings ride as query in every case; parent placeholders
/// are resolved against `parent_values` when this is a child sequence.
fn build_page_request(
    http: &reqwest::Client,
    stream: &Stream,
    url: &str,
    page_params: &[(String, String)],
    extra_params: &[(String, String)],
    parent_values: Option<&BTreeMap<String, String>>,
) -> reqwest::RequestBuilder {
    let mut request = match stream.method {
        Method::Get => http.get(url),
        Method::Post => http.post(url),
    };
    for (name, value) in &stream.headers {
        request = request.header(name, value);
    }
    let substituted: Vec<(String, String)> = stream
        .params
        .iter()
        .map(|(name, value)| {
            let value = match parent_values {
                Some(values) => resolve::substitute(value, values),
                None => value.clone(),
            };
            (name.clone(), value)
        })
        .collect();
    request = request.query(&substituted);
    if !extra_params.is_empty() {
        request = request.query(extra_params);
    }
    match (stream.method, &stream.body) {
        (Method::Post, Some(body)) => {
            // Pagination params go INTO the body for POST-cursor APIs.
            let mut body = match parent_values {
                Some(values) => resolve::substitute_body(body, values),
                None => body.clone(),
            };
            if let Value::Object(map) = &mut body {
                for (name, value) in page_params {
                    map.insert(name.clone(), Value::String(value.clone()));
                }
            }
            request.json(&body)
        }
        // GET, or a POST with no body: page params ride as query.
        _ if page_params.is_empty() => request,
        _ => request.query(page_params),
    }
}

/// First action whose declared conditions ALL hold: `status` (typed
/// equality against the actual response status) and/or `content_contains`
/// (within the first 64KiB of the body — success and error bodies alike).
fn match_action<'a>(
    actions: &'a [ResponseAction],
    status: u16,
    body: &Bytes,
) -> Option<&'a ResponseAction> {
    const BOUND: usize = 64 * 1024;
    actions.iter().find(|action| {
        let status_matches = action.status.is_none_or(|declared| declared == status);
        let content_matches = action.content_contains.as_deref().is_none_or(|needle| {
            let window = &body[..body.len().min(BOUND)];
            String::from_utf8_lossy(window).contains(needle)
        });
        status_matches && content_matches
    })
}
