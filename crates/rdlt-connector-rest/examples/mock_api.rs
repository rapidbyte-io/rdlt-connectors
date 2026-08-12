//! Standalone mock API for the REST→Postgres benchmark cell.
//!
//! Serves the flagship nested-record shape over page-number pagination:
//! `GET /events?page=N` (1-based) → a bare JSON array; past the last page → `[]`.
//! Pages are pre-rendered at startup so the server adds ~zero per-request work —
//! the benchmark measures the CLIENTS, not this process.
//!
//! Usage: `mock_api [rows] [pages] [port]` (defaults 100000, 100, 8642).

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;

fn render_pages(rows: usize, pages: usize) -> Vec<String> {
    let per_page = rows.div_ceil(pages);
    (0..pages)
        .map(|p| {
            let mut body = String::with_capacity(per_page * 96);
            body.push('[');
            for i in 0..per_page {
                let id = p * per_page + i;
                if id >= rows {
                    break;
                }
                if i > 0 {
                    body.push(',');
                }
                body.push_str(&format!(
                    r#"{{"id":{id},"name":"user-{id}","score":{},"profile":{{"city":"NYC","zip":{}}},"tags":[{{"label":"a"}},{{"label":"b"}}]}}"#,
                    id as f64 * 0.5,
                    10001 + id % 100,
                ));
            }
            body.push(']');
            body
        })
        .collect()
}

fn handle(mut stream: TcpStream, pages: &[String]) {
    let mut buf = [0u8; 2048];
    let n = match stream.read(&mut buf) {
        Ok(n) if n > 0 => n,
        _ => return,
    };
    let request = String::from_utf8_lossy(&buf[..n]);
    // `GET /events?page=N` — anything unparsable serves page 1; past-end → [].
    let page = request
        .split_whitespace()
        .nth(1)
        .and_then(|path| path.split("page=").nth(1))
        .and_then(|v| v.split('&').next())
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(1);
    let body = pages
        .get(page.saturating_sub(1))
        .map_or("[]", |p| p.as_str());
    let _ = write!(
        stream,
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
}

fn main() {
    let mut args = std::env::args().skip(1);
    let rows: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(100_000);
    let pages: usize = args.next().and_then(|a| a.parse().ok()).unwrap_or(100);
    let port: u16 = args.next().and_then(|a| a.parse().ok()).unwrap_or(8642);

    let rendered = Arc::new(render_pages(rows, pages));
    let listener = TcpListener::bind(("127.0.0.1", port)).expect("bind");
    println!(
        "mock_api: {rows} rows over {pages} pages at http://127.0.0.1:{}/events?page=N",
        listener.local_addr().expect("addr").port()
    );
    for stream in listener.incoming().flatten() {
        let pages = Arc::clone(&rendered);
        std::thread::spawn(move || handle(stream, &pages));
    }
}
