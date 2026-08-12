//! The cross-destination differential oracle, re-derived for v2: one
//! keyed structured feed drives BOTH the postgres destination (a live
//! container) and this crate's Shell, and the two must agree on
//! canonical per-table rows AND on typed-error classes.
//!
//! Each destination's own cells only prove it self-consistent; running
//! two dialects of the shared merge core against one feed is the
//! strongest divergence check the suite has. The ONLY sanctioned
//! representation differences are normalized by the canonicalizers and
//! nothing else: NULL becomes `∅`, booleans become `true`/`false`
//! (postgres renders `t`/`f`), everything else is the column's own
//! text form (Int64 is a decimal string on both engines, Utf8 is
//! verbatim).
//!
//! Container-gated skip-not-fail: every cell prints the SKIP line and
//! returns without a runtime; with one present it runs live.

use std::path::Path;
use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use rdlt_connector_duckdb::destination::{self, Config, DestinationOptions};
use rdlt_connector_postgres::destination::Postgres;
use rdlt_connector_postgres::fixtures::PostgresContainer;
use rdlt_connector_sdk::config::Document as _;
use rdlt_connector_sdk::spi::{
    ConnectorSpec, Cursor, ReadRequest, Source, SourceError, StreamSpec, WriteMode,
};
use rdlt_engine::{Engine, EngineConfig};

/// Every cell speaks through one keyed stream.
const STREAM: &str = "kv";
const KEY: &str = "k";

/// The visible skip: `true` means no runtime, and the caller returns.
fn skipped() -> bool {
    if rdlt_testkit::gate::runtime_available() {
        return false;
    }
    eprintln!("SKIP: no container runtime — differential cell not run");
    true
}

// ---- the shared feed --------------------------------------------------------

/// A keyed structured source: each unit is one batch followed by its
/// own checkpoint.
#[derive(Clone)]
struct Feed {
    units: Vec<RecordBatch>,
}

#[async_trait]
impl Source for Feed {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("differential-feed", "0.0.0")
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(vec![
            StreamSpec::new(STREAM)
                .with_structured()
                .with_primary_key([KEY]),
        ])
    }

    async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
        for (i, unit) in self.units.iter().enumerate() {
            let _ = req.out.arrow(unit.clone()).await;
            let _ = req.out.checkpoint(Cursor::new(i as u64 + 1)).await;
        }
        Ok(())
    }
}

/// One nullable column of feed data — the three types the matrix uses.
enum Col {
    I(&'static str, Vec<Option<i64>>),
    S(&'static str, Vec<Option<&'static str>>),
    B(&'static str, Vec<Option<bool>>),
}

/// One feed of one unit built from columns.
fn feed_of(cols: Vec<Col>) -> Feed {
    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for col in cols {
        match col {
            Col::I(name, v) => {
                fields.push(Field::new(name, DataType::Int64, true));
                arrays.push(Arc::new(Int64Array::from(v)));
            }
            Col::S(name, v) => {
                fields.push(Field::new(name, DataType::Utf8, true));
                arrays.push(Arc::new(StringArray::from(v)));
            }
            Col::B(name, v) => {
                fields.push(Field::new(name, DataType::Boolean, true));
                arrays.push(Arc::new(BooleanArray::from(v)));
            }
        }
    }
    Feed {
        units: vec![
            RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("feed unit"),
        ],
    }
}

// ---- canonicalization -------------------------------------------------------

/// Compared columns are bare identifiers, quoted per engine — never
/// interpolated expression text.
fn assert_bare(column: &str) {
    assert!(
        !column.is_empty()
            && column
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_'),
        "differential columns must be bare identifiers: {column}"
    );
}

/// Canonical rows from postgres: each column to text with NULL → `∅`
/// and booleans normalized, rows in the total order of their own text.
async fn canon_pg(conn: &str, schema: &str, columns: &[&str]) -> Vec<Vec<String>> {
    let (client, connection) = tokio_postgres::connect(conn, tokio_postgres::NoTls)
        .await
        .expect("pg connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    let select = columns
        .iter()
        .map(|c| {
            assert_bare(c);
            format!(
                "CASE WHEN \"{c}\" IS NULL THEN '∅' \
                 WHEN pg_typeof(\"{c}\")::text = 'boolean' \
                 THEN (CASE WHEN \"{c}\"::text = 't' THEN 'true' ELSE 'false' END) \
                 ELSE \"{c}\"::text END"
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let rows = client
        .query(
            &format!("SELECT {select} FROM \"{schema}\".\"{STREAM}\""),
            &[],
        )
        .await
        .expect("pg canon query");
    let mut out: Vec<Vec<String>> = rows
        .iter()
        .map(|row| {
            (0..columns.len())
                .map(|i| row.get::<_, String>(i))
                .collect()
        })
        .collect();
    out.sort();
    out
}

/// Canonical rows from DuckDB through the testhook, which returns ONE
/// scalar — so the rows travel as one string, columns joined on \u{1}
/// and rows on \u{2} (neither occurs in the data). `CAST(bool AS
/// VARCHAR)` is already `true`/`false` here.
fn canon_duck(config: &Config, columns: &[&str]) -> Vec<Vec<String>> {
    let row_expr = columns
        .iter()
        .map(|c| {
            assert_bare(c);
            format!("coalesce(CAST(\"{c}\" AS VARCHAR), '∅')")
        })
        .collect::<Vec<_>>()
        .join(" || '\u{1}' || ");
    let joined = destination::testhook::query_string(
        config,
        &format!(
            "SELECT coalesce(string_agg(row, '\u{2}' ORDER BY row), '') \
             FROM (SELECT {row_expr} AS row FROM \"{STREAM}\") rows"
        ),
    )
    .expect("duck canon query");
    let mut out: Vec<Vec<String>> = joined
        .split('\u{2}')
        .filter(|row| !row.is_empty())
        .map(|row| row.split('\u{1}').map(str::to_owned).collect())
        .collect();
    out.sort();
    out
}

// ---- the both-destinations driver -------------------------------------------

/// One side's outcome: canonical rows, or the run's typed error text.
type Outcome = Result<Vec<Vec<String>>, String>;

/// The duckdb-v2 document: the cell's options laid over a fresh path
/// (the option vocabulary IS the config's top level).
fn duck_config(dir: &Path, options: &serde_json::Value) -> Config {
    let mut doc = serde_json::json!({ "path": dir.join("diff.duckdb") });
    let fields = doc.as_object_mut().expect("object");
    for (key, value) in options.as_object().into_iter().flatten() {
        fields.insert(key.clone(), value.clone());
    }
    Config::from_value(doc).expect("valid duckdb document")
}

/// Run the same feeds + options + write mode through both destinations.
async fn run_both(
    name: &str,
    feeds: &[Feed],
    options: serde_json::Value,
    mode: WriteMode,
    columns: &[&str],
) -> (Outcome, Outcome) {
    // The caller probed the runtime and skipped; a missing one here is
    // a harness defect, so fail loudly.
    let pg = PostgresContainer::start()
        .await
        .expect("postgres runtime (probed by the caller)");
    let schema = format!("diff_{name}");
    let dir = tempfile::tempdir().expect("tempdir");

    let mut pg_run: Result<(), String> = Ok(());
    for feed in feeds {
        let dest = Postgres::new(&pg.connection_string)
            .schema(&schema)
            .options(DestinationOptions::from_value(options.clone()).expect("options"))
            .expect("pg options")
            .into_shell();
        let engine = EngineConfig::new(format!("diff-{name}"))
            .with_write_mode(mode.clone())
            .with_workdir(dir.path().join("pg-work"));
        if let Err(e) = Engine::new(engine, feed.clone(), dest).run().await {
            pg_run = Err(e.to_string());
            break;
        }
    }

    let config = duck_config(dir.path(), &options);
    let shell = destination::Shell::new(config.clone()).expect("valid duckdb config");
    let mut duck_run: Result<(), String> = Ok(());
    for feed in feeds {
        let engine = EngineConfig::new(format!("diff-{name}-duck"))
            .with_write_mode(mode.clone())
            .with_workdir(dir.path().join("duck-work"));
        if let Err(e) = Engine::new(engine, feed.clone(), shell.clone()).run().await {
            duck_run = Err(e.to_string());
            break;
        }
    }
    // Sequential re-open is the documented-safe pattern: the canon read
    // opens its own instance, so the runs' instance goes first.
    drop(shell);

    let pg_out = match pg_run {
        Ok(()) => Ok(canon_pg(&pg.connection_string, &schema, columns).await),
        Err(e) => Err(e),
    };
    let duck_out = match duck_run {
        Ok(()) => Ok(canon_duck(&config, columns)),
        Err(e) => Err(e),
    };
    (pg_out, duck_out)
}

/// The typed-error-class key: the text after ``table ` `` — the shared
/// sqlcore wording minus each destination's own framing prefix.
fn error_class(text: &str) -> &str {
    text.split("table `").nth(1).unwrap_or(text)
}

fn assert_agree(name: &str, pg: Outcome, duck: Outcome) {
    match (pg, duck) {
        (Ok(p), Ok(d)) => assert_eq!(p, d, "{name}: canonical rows diverge"),
        (Err(p), Err(d)) => assert_eq!(
            error_class(&p),
            error_class(&d),
            "{name}: typed error classes diverge\npg:   {p}\nduck: {d}"
        ),
        (Ok(_), Err(e)) => panic!("{name}: duckdb errored where postgres succeeded: {e}"),
        (Err(e), Ok(_)) => panic!("{name}: postgres errored where duckdb succeeded: {e}"),
    }
}

fn merge_on_k() -> WriteMode {
    WriteMode::Merge {
        key: vec![KEY.into()],
    }
}

// ---- the six cells ----------------------------------------------------------

/// A key–value feed: the simple two-column shape.
fn kv(rows: &[(Option<i64>, Option<&'static str>)]) -> Feed {
    let (ks, vs): (Vec<_>, Vec<_>) = rows.iter().copied().unzip();
    feed_of(vec![Col::I("k", ks), Col::S("v", vs)])
}

/// A key–scope–value feed for the scoped cells.
fn kdv(rows: &[(Option<i64>, Option<i64>, Option<&'static str>)]) -> Feed {
    feed_of(vec![
        Col::I("k", rows.iter().map(|r| r.0).collect()),
        Col::I("day", rows.iter().map(|r| r.1).collect()),
        Col::S("v", rows.iter().map(|r| r.2).collect()),
    ])
}

/// Default strategy: a redelivered key is replaced, not duplicated.
#[tokio::test(flavor = "multi_thread")]
async fn differential_delete_insert_redelivery() {
    if skipped() {
        return;
    }
    let (pg, duck) = run_both(
        "di",
        &[
            kv(&[(Some(1), Some("one")), (Some(2), Some("two"))]),
            kv(&[(Some(2), Some("two2")), (Some(3), Some("three"))]),
        ],
        serde_json::json!({}),
        merge_on_k(),
        &["k", "v"],
    )
    .await;
    assert_agree("delete_insert", pg, duck);
}

/// Upsert with hard_delete and dedup_sort composed: in-load duplicates
/// resolve by `seq` desc on BOTH engines, and the flagged key leaves.
#[tokio::test(flavor = "multi_thread")]
async fn differential_upsert_with_hard_delete_and_dedup() {
    if skipped() {
        return;
    }
    type Row = (Option<i64>, Option<i64>, Option<&'static str>, Option<bool>);
    let feed = |rows: &[Row]| {
        feed_of(vec![
            Col::I("k", rows.iter().map(|r| r.0).collect()),
            Col::I("seq", rows.iter().map(|r| r.1).collect()),
            Col::S("v", rows.iter().map(|r| r.2).collect()),
            Col::B("gone", rows.iter().map(|r| r.3).collect()),
        ])
    };
    let (pg, duck) = run_both(
        "ups",
        &[
            feed(&[
                (Some(1), Some(1), Some("one"), Some(false)),
                (Some(2), Some(1), Some("two"), Some(false)),
            ]),
            feed(&[
                // In-load dupes: seq desc picks the survivor on BOTH sides.
                (Some(1), Some(5), Some("one-v5"), Some(false)),
                (Some(1), Some(9), Some("one-v9"), Some(false)),
                (Some(2), Some(1), Some("two"), Some(true)), // hard delete
            ]),
        ],
        serde_json::json!({
            "merge_strategy": "upsert",
            "tables": {"kv": {"hard_delete": "gone",
                               "dedup_sort": {"column": "seq", "order": "desc"}}}
        }),
        merge_on_k(),
        &["k", "v"],
    )
    .await;
    assert_agree("upsert+hard_delete+dedup", pg, duck);
}

/// scd2 keep: a changed key closes and reopens, an untouched key does
/// not churn. Validity INSTANTS differ across engines by nature, so the
/// compared shape is (key, value) over the whole history.
#[tokio::test(flavor = "multi_thread")]
async fn differential_scd2_history() {
    if skipped() {
        return;
    }
    let (pg, duck) = run_both(
        "scd2",
        &[
            kv(&[(Some(1), Some("one")), (Some(2), Some("two"))]),
            kv(&[(Some(1), Some("one-v2"))]), // change 1; 2 untouched (keep)
        ],
        serde_json::json!({
            "merge_strategy": "scd2",
            "tables": {"kv": {"scd2": {}}}
        }),
        merge_on_k(),
        &["k", "v"],
    )
    .await;
    assert_agree("scd2", pg, duck);
}

/// merge_scope: a delivered scope is replaced wholesale, other scopes
/// keep their rows — and a NULL scope value is NOT a scope, so its row
/// survives untouched on both engines.
#[tokio::test(flavor = "multi_thread")]
async fn differential_merge_scope_includes_null_scope() {
    if skipped() {
        return;
    }
    let (pg, duck) = run_both(
        "scope",
        &[
            kdv(&[
                (Some(1), Some(1), Some("d1-a")),
                (Some(2), Some(1), Some("d1-b")),
                (Some(3), Some(2), Some("d2-a")),
                (Some(4), None, Some("null-scope")),
            ]),
            kdv(&[(Some(1), Some(1), Some("d1-a2"))]),
        ],
        serde_json::json!({"tables": {"kv": {"merge_scope": ["day"]}}}),
        merge_on_k(),
        &["k", "day", "v"],
    )
    .await;
    assert_agree("merge_scope", pg, duck);
}

/// An EXPLICIT strategy under append is a typed refusal on both sides,
/// with the SAME shared wording after each destination's own prefix.
#[tokio::test(flavor = "multi_thread")]
async fn differential_rejections_are_class_identical() {
    if skipped() {
        return;
    }
    let (pg, duck) = run_both(
        "reject",
        &[kv(&[(Some(1), Some("x"))])],
        serde_json::json!({"merge_strategy": "upsert"}),
        WriteMode::Append,
        &["k", "v"],
    )
    .await;
    let p = pg.expect_err("append + explicit strategy is typed on postgres");
    let d = duck.expect_err("append + explicit strategy is typed on duckdb");
    for text in [&p, &d] {
        assert!(
            text.contains("merge_strategy requires the merge write mode"),
            "{text}"
        );
    }
    assert_eq!(
        error_class(&p),
        error_class(&d),
        "identical typed text after the destination prefix"
    );
}

/// Scoped scd2 retirement: a key absent from a DELIVERED scope retires,
/// keys in undelivered scopes do not — identical history shape on both.
/// The open marker makes "open" comparable without wall-clock instants.
#[tokio::test(flavor = "multi_thread")]
async fn differential_scd2_scoped_retirement() {
    if skipped() {
        return;
    }
    let (pg, duck) = run_both(
        "scd2scope",
        &[
            kdv(&[
                (Some(1), Some(1), Some("k1")),
                (Some(2), Some(1), Some("k2")),
                (Some(3), Some(2), Some("k3")),
            ]),
            kdv(&[(Some(1), Some(1), Some("k1"))]), // day 1 only, k2 absent
        ],
        serde_json::json!({
            "merge_strategy": "scd2",
            "tables": {"kv": {"scd2": {"absent": "retire",
                                        "active_record_timestamp": "9999-12-31T00:00:00Z"},
                               "merge_scope": ["day"]}}
        }),
        merge_on_k(),
        &["k", "v"],
    )
    .await;
    assert_agree("scd2_scoped_retirement", pg, duck);
}
