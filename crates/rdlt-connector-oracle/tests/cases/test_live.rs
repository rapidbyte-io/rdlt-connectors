//! The live cells: one Oracle Free container, the whole read path.

use rdlt_connector_oracle::source::cursor::OracleCursor;
use rdlt_connector_sdk::spi::core::id::StreamName;
use rdlt_connector_sdk::spi::{source::Source, source::StreamSpec};
use rdlt_testkit::conformance::{assert_conformant, source::verify as verify_source};

use super::common;
use super::common::{Flavor, OracleFixture, incremental, stream};

/// The first Arrow batch the connector pushes, for assertions that
/// must see the ARRAY rather than a JSON rendering of it.
async fn read_batch(
    shell: &rdlt_connector_oracle::source::Shell,
    name: &str,
) -> arrow::array::RecordBatch {
    use rdlt_connector_sdk::spi::channel::PushPayload;

    let (out, mut incoming) = rdlt_connector_sdk::spi::channel::records(64 << 20);
    let reader = shell.read(rdlt_connector_sdk::spi::source::ReadRequest::new(
        StreamSpec::new(name),
        None,
        out,
    ));
    let collect = async {
        let mut first = None;
        while let Some(push) = incoming.recv().await {
            if let PushPayload::Arrow(batch) = push.payload
                && batch.num_rows() > 0
            {
                first.get_or_insert(batch);
            }
        }
        first
    };
    let (res, batch) = tokio::join!(reader, collect);
    res.expect("the read settles");
    batch.expect("a batch was pushed")
}

/// The Arrow schema the connector actually pushes.
async fn read_schema(
    shell: &rdlt_connector_oracle::source::Shell,
    name: &str,
) -> std::sync::Arc<arrow::datatypes::Schema> {
    use rdlt_connector_sdk::spi::channel::PushPayload;

    let (out, mut incoming) = rdlt_connector_sdk::spi::channel::records(64 << 20);
    let reader = shell.read(rdlt_connector_sdk::spi::source::ReadRequest::new(
        StreamSpec::new(name),
        None,
        out,
    ));
    let collect = async {
        let mut schema = None;
        while let Some(push) = incoming.recv().await {
            if let PushPayload::Arrow(batch) = push.payload {
                schema.get_or_insert_with(|| batch.schema());
            }
        }
        schema
    };
    let (res, schema) = tokio::join!(reader, collect);
    res.expect("the read settles");
    schema.expect("a batch was pushed")
}

/// Collect one stream's rows through the SPI, returning the JSON
/// objects and the final cursor.
async fn read_all(
    shell: &rdlt_connector_oracle::source::Shell,
    name: &str,
    since: Option<rdlt_connector_sdk::spi::core::cursor::Cursor>,
) -> (
    Vec<serde_json::Value>,
    Option<rdlt_connector_sdk::spi::core::cursor::Cursor>,
) {
    use rdlt_connector_sdk::spi::channel::PushPayload;

    let (out, mut incoming) = rdlt_connector_sdk::spi::channel::records(64 << 20);
    let spec = StreamSpec::new(name);
    let reader = shell.read(rdlt_connector_sdk::spi::source::ReadRequest::new(
        spec, since, out,
    ));
    let collect = async {
        let mut rows = Vec::new();
        let mut cursor = None;
        while let Some(push) = incoming.recv().await {
            match push.payload {
                PushPayload::Arrow(batch) => rows.extend(super::common::batch_to_json(&batch)),
                PushPayload::Checkpoint(c) => cursor = Some(c),
                _ => {}
            }
        }
        (rows, cursor)
    };
    let (result, collected) = tokio::join!(reader, collect);
    result.expect("the read settles");
    collected
}

/// The whole shape at once: types survive the round trip, a large
/// row count streams past the driver's old 100-row ceiling, LOBs
/// arrive whole at megabyte scale, and the watermark resumes.
#[tokio::test(flavor = "multi_thread")]
async fn the_read_path_holds_against_a_live_database() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };

    // A table exercising the type rulebook's interesting rows.
    fixture
        .seed(&[
            "CREATE TABLE TYPES_T (
                ID NUMBER(10) PRIMARY KEY,
                SMALL_N NUMBER(8,2),
                BIG_N NUMBER,
                TXT VARCHAR2(100),
                D DATE,
                TS TIMESTAMP,
                TSTZ TIMESTAMP WITH TIME ZONE,
                B RAW(16)
            )",
            "INSERT INTO TYPES_T VALUES (1, 12.34, \
             12345678901234567890123456789012345678, 'hello', \
             DATE '2026-01-02', TIMESTAMP '2026-01-02 03:04:05.678', \
             TIMESTAMP '2026-01-02 03:04:05.678 +02:00', HEXTORAW('DEADBEEF'))",
        ])
        .await;

    let shell = fixture.shell(&[stream("types", "TYPES_T")]);
    let (rows, _) = read_all(&shell, "types", None).await;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row["id"], serde_json::json!(1));
    assert_eq!(row["txt"], serde_json::json!("hello"));
    assert_eq!(
        row["big_n"],
        serde_json::json!("12345678901234567890123456789012345678"),
        "bare NUMBER keeps all 38 digits as exact text"
    );
    assert!(
        row["d"]
            .as_str()
            .expect("date")
            .starts_with("2026-01-02T00:00:00"),
        "Oracle DATE carries time-of-day: {}",
        row["d"]
    );
    // TIMESTAMP WITH TIME ZONE is normalized to the UTC INSTANT.
    // Arrow stamps the array `UTC` and stores micros since epoch, so
    // `03:04:05.678 +02:00` must land at 01:04:05.678 — keeping the
    // wall-clock and dropping the offset would move every value by
    // its zone.
    assert_eq!(
        row["tstz"].as_str().expect("tstz"),
        "2026-01-02T01:04:05.678000",
        "the offset must be applied, not discarded"
    );
    assert_eq!(row["b"], serde_json::json!("deadbeef"));

    // Volume: far past the old 100-row ceiling, exact count.
    fixture
        .seed(&[
            "CREATE TABLE BULK_T (ID NUMBER(8) PRIMARY KEY, V VARCHAR2(40))",
            "INSERT INTO BULK_T SELECT LEVEL, 'row-'||LEVEL FROM DUAL CONNECT BY LEVEL <= 5000",
        ])
        .await;
    let shell = fixture.shell(&[stream("bulk", "BULK_T")]);
    let (rows, _) = read_all(&shell, "bulk", None).await;
    assert_eq!(
        rows.len(),
        5000,
        "every row crosses, not just the first page"
    );

    // LOBs at megabyte scale, whole.
    fixture
        .seed(&[
            "CREATE TABLE LOB_T (ID NUMBER(4) PRIMARY KEY, DOC CLOB, BIN BLOB)",
            "DECLARE c CLOB; b BLOB; chunk VARCHAR2(1024) := RPAD('x', 1024, 'x'); \
             braw RAW(1024) := UTL_RAW.CAST_TO_RAW(RPAD('y', 1024, 'y')); \
             BEGIN INSERT INTO LOB_T VALUES (1, EMPTY_CLOB(), EMPTY_BLOB()) RETURNING DOC, BIN INTO c, b; \
             FOR i IN 1..2048 LOOP DBMS_LOB.WRITEAPPEND(c, 1024, chunk); \
             DBMS_LOB.WRITEAPPEND(b, 1024, braw); END LOOP; COMMIT; END;",
        ])
        .await;
    let shell = fixture.shell(&[stream("lobs", "LOB_T")]);
    let (rows, _) = read_all(&shell, "lobs", None).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]["doc"].as_str().expect("clob").len(),
        2 * 1024 * 1024,
        "a 2 MiB CLOB arrives whole"
    );
    assert_eq!(
        rows[0]["bin"].as_str().expect("blob").len(),
        2 * 1024 * 1024 * 2,
        "a 2 MiB BLOB arrives whole (hex-rendered)"
    );

    // Incremental: the watermark resumes and never re-reads.
    fixture
        .seed(&[
            "CREATE TABLE INC_T (ID NUMBER(8) PRIMARY KEY, V VARCHAR2(20))",
            "INSERT INTO INC_T SELECT LEVEL, 'v' FROM DUAL CONNECT BY LEVEL <= 150",
        ])
        .await;
    let shell = fixture.shell(&[incremental("inc", "INC_T", "ID")]);
    let (first, cursor) = read_all(&shell, "inc", None).await;
    assert_eq!(first.len(), 150);
    let cursor = cursor.expect("a checkpoint landed");
    let decoded = OracleCursor::decode(Some(&cursor)).expect("decodes");
    assert_eq!(decoded.streams["inc"].watermark, "150");

    fixture
        .seed(&["INSERT INTO INC_T VALUES (151, 'new')"])
        .await;
    let (second, _) = read_all(&shell, "inc", Some(cursor)).await;
    assert_eq!(second.len(), 1, "only the new row");
    assert_eq!(second[0]["id"], serde_json::json!(151));
}

/// Resume must hold when PHYSICAL order disagrees with cursor order.
///
/// Rows are paged by `(cursor, ROWID)` precisely so a checkpoint is a
/// true low-water boundary. Seeding rows whose ROWID order is the
/// REVERSE of their ID order makes a ROWID-ordered read checkpoint a
/// watermark it has not reached — the defect this shape pins.
#[tokio::test(flavor = "multi_thread")]
async fn resume_holds_when_physical_order_disagrees_with_the_cursor() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE SHUFFLE_T (ID NUMBER(8) PRIMARY KEY, V VARCHAR2(20))",
            // Descending inserts: physical order is the reverse of ID order.
            "INSERT INTO SHUFFLE_T SELECT 400 - LEVEL, 'v' FROM DUAL CONNECT BY LEVEL <= 300",
        ])
        .await;
    let shell = fixture.shell(&[incremental("shuffled", "SHUFFLE_T", "ID")]);
    let (rows, cursor) = read_all(&shell, "shuffled", None).await;
    assert_eq!(rows.len(), 300);
    let cursor = cursor.expect("a checkpoint landed");

    // Everything at or below the watermark was delivered, so a resume
    // reads nothing — and adding a row above it reads exactly that one.
    let (again, _) = read_all(&shell, "shuffled", Some(cursor.clone())).await;
    assert!(
        again.is_empty(),
        "a completed stream re-reads nothing: {}",
        again.len()
    );
    fixture
        .seed(&["INSERT INTO SHUFFLE_T VALUES (9999, 'new')"])
        .await;
    let (delta, _) = read_all(&shell, "shuffled", Some(cursor)).await;
    assert_eq!(delta.len(), 1);
    assert_eq!(delta[0]["id"], serde_json::json!(9999));
}

/// A cursor column that does not exist is refused — silently
/// re-reading the whole table on every run is not an option.
#[tokio::test(flavor = "multi_thread")]
async fn a_missing_cursor_column_is_refused() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&["CREATE TABLE GHOST_T (ID NUMBER(8) PRIMARY KEY)"])
        .await;
    let shell = fixture.shell(&[incremental("ghost", "GHOST_T", "CREATED_ON")]);
    let (out, _keep) = rdlt_connector_sdk::spi::channel::records(1 << 20);
    let err = shell
        .read(rdlt_connector_sdk::spi::source::ReadRequest::new(
            StreamSpec::new("ghost"),
            None,
            out,
        ))
        .await
        .expect_err("refused")
        .to_string();
    assert!(
        err.contains("cursor column `CREATED_ON` is not a column")
            && err.contains("silently re-read everything"),
        "{err}"
    );
}

/// The sdk conformance kit certifies the Shell against the live
/// database — "certified = passes conformance".
#[tokio::test(flavor = "multi_thread")]
async fn the_source_is_conformant() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE CONF_T (ID NUMBER(8) PRIMARY KEY, V VARCHAR2(20))",
            "INSERT INTO CONF_T SELECT LEVEL, 'c'||LEVEL FROM DUAL CONNECT BY LEVEL <= 7",
        ])
        .await;
    let shell = fixture.shell(&[incremental("conf_stream", "CONF_T", "ID")]);
    assert_conformant(verify_source(&shell).await.expecting_no_skips());
    let _ = StreamName::new("conf_stream");
}

/// A NULLABLE cursor column is refused BEFORE any row moves.
///
/// The timing is the whole point. Refusing at the row was worse than
/// useless: Oracle sorts NULLs LAST, so the failure landed on the
/// final page of the FIRST run, after most of the table had already
/// been delivered and checkpointed. Every later run resumed with
/// `WHERE c > :watermark`, which never matches NULL, so it went green
/// and those rows were silently absent forever — the loud refusal was
/// unreachable exactly when it mattered.
#[tokio::test(flavor = "multi_thread")]
async fn a_null_cursor_value_is_refused() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE NULLC_T (ID NUMBER(8) PRIMARY KEY, TS DATE)",
            "INSERT INTO NULLC_T SELECT LEVEL, DATE '2026-01-01' FROM DUAL CONNECT BY LEVEL <= 5",
            "INSERT INTO NULLC_T VALUES (99, NULL)",
        ])
        .await;
    let shell = fixture.shell(&[incremental("nullc", "NULLC_T", "TS")]);
    let (out, _keep) = rdlt_connector_sdk::spi::channel::records(1 << 20);
    let err = shell
        .read(rdlt_connector_sdk::spi::source::ReadRequest::new(
            StreamSpec::new("nullc"),
            None,
            out,
        ))
        .await
        .expect_err("refused")
        .to_string();
    assert!(
        err.contains("cursor column `TS` admits NULLs") && err.contains("NOT NULL"),
        "{err}"
    );
    // And it is refused on a table where no row is NULL yet — the
    // COLUMN's nullability is the fact, not today's contents.
    fixture
        .seed(&["CREATE TABLE NULLC_EMPTY (ID NUMBER(8) PRIMARY KEY, TS DATE)"])
        .await;
    let shell = fixture.shell(&[incremental("e", "NULLC_EMPTY", "TS")]);
    let (out, _keep) = rdlt_connector_sdk::spi::channel::records(1 << 20);
    let err = shell
        .read(rdlt_connector_sdk::spi::source::ReadRequest::new(
            StreamSpec::new("e"),
            None,
            out,
        ))
        .await
        .expect_err("refused before a single row")
        .to_string();
    assert!(err.contains("admits NULLs"), "{err}");
}

/// A DATE cursor resumes — the rendering and the SQL format model
/// have to agree exactly, which they did not before this was pinned.
#[tokio::test(flavor = "multi_thread")]
async fn a_date_cursor_resumes_across_runs() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE DATEC_T (ID NUMBER(8) PRIMARY KEY, TS DATE NOT NULL)",
            "INSERT INTO DATEC_T SELECT LEVEL, DATE '2026-01-01' + LEVEL FROM DUAL \
             CONNECT BY LEVEL <= 40",
        ])
        .await;
    let shell = fixture.shell(&[incremental("datec", "DATEC_T", "TS")]);
    let (first, cursor) = read_all(&shell, "datec", None).await;
    assert_eq!(first.len(), 40);
    let cursor = cursor.expect("a checkpoint landed");

    fixture
        .seed(&["INSERT INTO DATEC_T VALUES (999, DATE '2027-06-01')"])
        .await;
    let (delta, _) = read_all(&shell, "datec", Some(cursor)).await;
    assert_eq!(delta.len(), 1, "a DATE watermark resumes exactly");
    assert_eq!(delta[0]["id"], serde_json::json!(999));
}

/// The SAME read path against Oracle XE **21c**.
///
/// The connector claims to work across Oracle generations, and a
/// claim no cell exercises is a wish. What this actually exercises is
/// the EXECUTE path's capability negotiation, the older server's type
/// and ROWID behaviour, and a cursor resume — all on a wire that
/// omits fields 23ai sends.
///
/// It does NOT certify the vendored FETCH patch, whatever an earlier
/// version of this comment claimed: `FetchMessage` is built at one
/// site, `fetch_more`, and this connector never continues a cursor,
/// so that capability read is unreachable from here.
///
/// Deliberately one cell, not the whole suite: 21c costs another
/// ~75 s of gate time, and what differs across generations is the
/// wire and the types, both of which this exercises.
#[tokio::test(flavor = "multi_thread")]
async fn the_read_path_holds_against_oracle_21c() {
    let Some(fixture) = OracleFixture::start_on(Flavor::Xe21).await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE XE_T (\
               ID NUMBER(8) PRIMARY KEY, \
               NAME VARCHAR2(64), \
               AMOUNT NUMBER(12,2), \
               TS TIMESTAMP WITH TIME ZONE NOT NULL, \
               BODY CLOB, \
               RAWS BLOB)",
            "INSERT INTO XE_T VALUES (1, 'first', 10.25, \
             TIMESTAMP '2026-01-01 10:00:00 +00:00', 'hello', \
             UTL_RAW.CAST_TO_RAW('bytes'))",
            "INSERT INTO XE_T VALUES (2, 'sécond', -3.5, \
             TIMESTAMP '2026-01-02 10:00:00 +00:00', NULL, NULL)",
            "INSERT INTO XE_T SELECT LEVEL + 10, 'bulk', LEVEL, \
             TIMESTAMP '2026-02-01 10:00:00 +00:00' + NUMTODSINTERVAL(LEVEL, 'SECOND'), \
             NULL, NULL FROM DUAL CONNECT BY LEVEL <= 600",
        ])
        .await;

    let shell = fixture.shell(&[incremental("xe", "XE_T", "TS")]);
    let (rows, cursor) = read_all(&shell, "xe", None).await;
    assert_eq!(rows.len(), 602, "every row crosses the 21c wire");
    assert_eq!(rows[0]["name"], serde_json::json!("first"));
    // Exact Decimal crosses as TEXT by design — a JSON number is an
    // IEEE double and cannot carry NUMBER(12,2) losslessly.
    assert_eq!(rows[0]["amount"], serde_json::json!("10.25"));
    assert_eq!(
        rows[1]["amount"],
        serde_json::json!("-3.50"),
        "exact at the declared scale"
    );
    assert_eq!(rows[0]["body"], serde_json::json!("hello"));
    assert_eq!(rows[1]["name"], serde_json::json!("sécond"), "utf-8 intact");
    assert!(rows[1]["body"].is_null(), "a NULL LOB stays null");

    // And it resumes: the older server's ROWID and timestamp
    // renderings have to survive a round trip through the cursor.
    let cursor = cursor.expect("a checkpoint landed");
    fixture
        .seed(&["INSERT INTO XE_T VALUES (9999, 'late', 1, \
             TIMESTAMP '2027-01-01 10:00:00 +00:00', NULL, NULL)"])
        .await;
    let (delta, _) = read_all(&shell, "xe", Some(cursor)).await;
    assert_eq!(delta.len(), 1, "21c resumes exactly");
    assert_eq!(delta[0]["id"], serde_json::json!(9999));
}

/// NVARCHAR2 and NCHAR survive the wire.
///
/// They did not. The driver advertises UTF-16 for the national
/// character set at connect but decoded every character column as
/// UTF-8, so `N'zażółć'` arrived as `"\0z\0a\u{1}|..."` — destroyed,
/// unrecoverable, with no error anywhere, while the VARCHAR2 twin in
/// the same row was fine. Both research.md and plan.md claimed these
/// types were lossless and no cell covered them.
#[tokio::test(flavor = "multi_thread")]
async fn national_character_types_survive_the_wire() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE NCHAR_T (ID NUMBER(4) PRIMARY KEY, NV NVARCHAR2(50), \
             NC NCHAR(6), V VARCHAR2(50))",
            "INSERT INTO NCHAR_T VALUES (1, N'zażółć', N'abc', 'zażółć')",
        ])
        .await;
    let shell = fixture.shell(&[stream("n", "NCHAR_T")]);
    let (rows, _) = read_all(&shell, "n", None).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["nv"], serde_json::json!("zażółć"), "NVARCHAR2");
    assert_eq!(
        rows[0]["nc"],
        serde_json::json!("abc   "),
        "NCHAR is padded"
    );
    assert_eq!(rows[0]["v"], serde_json::json!("zażółć"), "VARCHAR2 twin");
}

/// BINARY_FLOAT and BINARY_DOUBLE decode as numbers, and the
/// non-finite values Oracle legally stores do not kill the table.
///
/// The driver had no arm for either type, so the catch-all applied a
/// lossy UTF-8 decode to raw IEEE bytes and the connector kept the
/// mojibake. It surfaced only because Postgres refused an embedded
/// NUL.
#[tokio::test(flavor = "multi_thread")]
async fn binary_float_and_double_decode_as_numbers() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE BINF_T (ID NUMBER(4) PRIMARY KEY, F BINARY_FLOAT, D BINARY_DOUBLE)",
            "INSERT INTO BINF_T VALUES (1, 1.5F, 2.25D)",
            "INSERT INTO BINF_T VALUES (2, -0.5F, -1234.5D)",
            "INSERT INTO BINF_T VALUES (3, BINARY_FLOAT_INFINITY, BINARY_DOUBLE_NAN)",
        ])
        .await;
    let shell = fixture.shell(&[stream("b", "BINF_T")]);
    let (rows, _) = read_all(&shell, "b", None).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["f"], serde_json::json!(1.5));
    assert_eq!(rows[0]["d"], serde_json::json!(2.25));
    assert_eq!(rows[1]["f"], serde_json::json!(-0.5), "negatives invert");
    assert_eq!(rows[1]["d"], serde_json::json!(-1234.5));
    // Non-finite values are held NATIVELY by Arrow — the earlier
    // "crosses as null" assertion was vacuous, because the test's own
    // JSON view renders any non-finite f64 as null and so passed
    // whatever the connector did. Assert the ARRAY instead.
    let batch_f = read_batch(&shell, "b").await;
    let floats = batch_f
        .column_by_name("f")
        .expect("f")
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("Float64");
    let doubles = batch_f
        .column_by_name("d")
        .expect("d")
        .as_any()
        .downcast_ref::<arrow::array::Float64Array>()
        .expect("Float64");
    assert!(
        floats.value(2).is_infinite() && floats.value(2) > 0.0,
        "BINARY_FLOAT_INFINITY survives as an actual infinity"
    );
    assert!(
        doubles.value(2).is_nan(),
        "BINARY_DOUBLE_NAN survives as NaN"
    );
}

/// A table is reachable by its SCHEMA-qualified name, and a
/// case-sensitive name is reachable by quoting it.
///
/// `HR.EMPLOYEES` — an app schema read by a separate user, the
/// ordinary Oracle estate shape — was quoted as ONE identifier,
/// `"HR.EMPLOYEES"`, which no table has ever been called.
#[tokio::test(flavor = "multi_thread")]
async fn qualified_and_case_sensitive_tables_are_reachable() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE QUAL_T (ID NUMBER(4) PRIMARY KEY)",
            "INSERT INTO QUAL_T VALUES (7)",
            "CREATE TABLE \"lower_t\" (ID NUMBER(4) PRIMARY KEY)",
            "INSERT INTO \"lower_t\" VALUES (9)",
        ])
        .await;
    let shell = fixture.shell(&[
        stream("qualified", &format!("{}.QUAL_T", common::APP_USER)),
        stream("lower", "\"lower_t\""),
    ]);
    let (qualified, _) = read_all(&shell, "qualified", None).await;
    assert_eq!(qualified.len(), 1);
    assert_eq!(qualified[0]["id"], serde_json::json!(7));

    let (lower, _) = read_all(&shell, "lower", None).await;
    assert_eq!(lower.len(), 1, "a quoted lower-case name reaches its table");
    assert_eq!(lower[0]["id"], serde_json::json!(9));
}

/// Two columns differing only in case are refused, not silently
/// collapsed — 030's duplicate-CSV-header defect in a new costume.
#[tokio::test(flavor = "multi_thread")]
async fn columns_differing_only_in_case_are_refused() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&["CREATE TABLE DUPC_T (\"id\" NUMBER(4), \"ID\" NUMBER(4))"])
        .await;
    let shell = fixture.shell(&[stream("d", "DUPC_T")]);
    let (out, _keep) = rdlt_connector_sdk::spi::channel::records(1 << 20);
    let err = shell
        .read(rdlt_connector_sdk::spi::source::ReadRequest::new(
            StreamSpec::new("d"),
            None,
            out,
        ))
        .await
        .expect_err("refused")
        .to_string();
    assert!(err.contains("differ only in case"), "{err}");
}

/// A very wide row READS.
///
/// Under the pure-Rust driver this table was unreadable at any page
/// size: one query reply had to fit one network packet, and 8 KB of
/// declared width did not. The limit belonged to that driver, not to
/// Oracle, and it is gone.
#[tokio::test(flavor = "multi_thread")]
async fn a_very_wide_row_reads() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE WIDE_T (ID NUMBER(4), A VARCHAR2(4000), B VARCHAR2(4000))",
            "INSERT INTO WIDE_T VALUES (1, RPAD('a',4000,'a'), RPAD('b',4000,'b'))",
        ])
        .await;
    let shell = fixture.shell(&[stream("w", "WIDE_T")]);
    let (rows, _) = read_all(&shell, "w", None).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["a"].as_str().expect("a").len(), 4000);
    assert_eq!(rows[0]["b"].as_str().expect("b").len(), 4000);
}

/// The batch carries EXACT types, so a decimal is a decimal.
///
/// Under NDJSON the connector derived every column's type from the
/// describe and then threw it away: a `NUMBER(12,2)` crossed as a
/// JSON string and landed as TEXT in the destination, a RAW as hex
/// text, a DATE as text. Arrow carries the schema with the data.
#[tokio::test(flavor = "multi_thread")]
async fn the_batch_carries_exact_types() {
    use arrow::datatypes::{DataType, TimeUnit};

    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE DECL_T (ID NUMBER(8) PRIMARY KEY, AMOUNT NUMBER(12,2), \
             RAWS RAW(16), TS TIMESTAMP WITH TIME ZONE, BIG NUMBER, D BINARY_DOUBLE)",
            "INSERT INTO DECL_T VALUES (1, 10.25, UTL_RAW.CAST_TO_RAW('ab'), \
             TIMESTAMP '2026-01-01 10:00:00 +00:00', 123456789012345678901234567890, 1.5D)",
        ])
        .await;
    let shell = fixture.shell(&[stream("d", "DECL_T")]);

    // STRUCTURED, or the engine refuses the batches outright with
    // "source pushed Arrow batches on a stream not declared
    // `structured`". Every offline and live cell passed without this
    // — only an end-to-end load caught it, so it is pinned here.
    let specs = shell.streams().await.expect("discovery");
    assert!(
        specs.iter().all(|s| s.structured),
        "an Arrow source must declare its streams structured"
    );

    let schema = read_schema(&shell, "d").await;
    let field = |name: &str| {
        schema
            .field_with_name(name)
            .expect(name)
            .data_type()
            .clone()
    };

    assert_eq!(field("id"), DataType::Int64);
    assert_eq!(
        field("amount"),
        DataType::Decimal128(12, 2),
        "an exact decimal, not TEXT"
    );
    assert_eq!(field("raws"), DataType::Binary, "binary, not hex text");
    assert_eq!(
        field("ts"),
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
    );
    assert_eq!(field("d"), DataType::Float64);
    // Bare NUMBER accepts 38 digits, which no binary type holds
    // exactly, so it keeps its digits as text — deliberately.
    assert_eq!(field("big"), DataType::Utf8);
}

/// A read far larger than one batch delivers every row exactly once.
///
/// Its ancestor was called "survives more pages than the server
/// allows cursors" and described four connection recycles — true of
/// the pure-Rust driver, where every page opened a server cursor that
/// was never closed and a read died at ~297 of them. There is no
/// paging and no recycler now, so the name and doc were describing
/// absent machinery. What is still worth asserting is that a
/// many-batch read is exact.
#[tokio::test(flavor = "multi_thread")]
async fn a_many_batch_read_delivers_every_row_once() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE MANYP_T (ID NUMBER(8) PRIMARY KEY, V VARCHAR2(16))",
            "INSERT INTO MANYP_T SELECT LEVEL, 'v' FROM DUAL CONNECT BY LEVEL <= 5000",
        ])
        .await;
    let shell = fixture.shell_tuned(
        &[stream("m", "MANYP_T")],
        serde_json::json!({"page_rows": 5}),
    );
    let (rows, _) = read_all(&shell, "m", None).await;
    assert_eq!(rows.len(), 5000, "every row of a 1,000-batch read");
    let mut ids: Vec<i64> = rows.iter().map(|r| r["id"].as_i64().expect("id")).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), 5000, "and none of them twice");
}

/// A bare `NUMBER` works as a cursor.
///
/// It did not, and that is how Oracle estates spell a
/// sequence-backed surrogate key — the single most common
/// incremental cursor there is. Because Oracle accepts any magnitude
/// in a bare NUMBER it maps to text (exact digits), and the cursor
/// gate refused it with a message saying the column was not numeric,
/// about a column that is numeric and monotonic. The operator's only
/// recourse was redeclaring the table.
#[tokio::test(flavor = "multi_thread")]
async fn a_bare_number_works_as_a_cursor() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE BAREN_T (ID NUMBER NOT NULL PRIMARY KEY, V VARCHAR2(16))",
            "INSERT INTO BAREN_T SELECT LEVEL, 'v' FROM DUAL CONNECT BY LEVEL <= 150",
        ])
        .await;
    let shell = fixture.shell(&[incremental("bare", "BAREN_T", "ID")]);
    let (rows, cursor) = read_all(&shell, "bare", None).await;
    assert_eq!(rows.len(), 150);

    fixture
        .seed(&["INSERT INTO BAREN_T VALUES (99999, 'late')"])
        .await;
    let (delta, _) = read_all(&shell, "bare", Some(cursor.expect("checkpoint"))).await;
    assert_eq!(delta.len(), 1, "and it resumes numerically, not lexically");
    // Bare NUMBER crosses as exact TEXT — Oracle accepts 38 digits
    // in it and a JSON number is an IEEE double.
    assert_eq!(delta[0]["id"], serde_json::json!("99999"));
}

/// A CLOB written by a plain INSERT reads back whole — including at
/// the size that used to be unreadable.
///
/// Recorded as O7 against the previous driver: 3,999 characters
/// raised a buffer underflow inside the locator read while 3,000 was
/// fine and a 2 MiB value written with DBMS_LOB was also fine. The
/// boundary was the driver's, not Oracle's.
#[tokio::test(flavor = "multi_thread")]
async fn a_plain_insert_clob_reads_at_the_old_boundary() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE BIGLOB_T (ID NUMBER(4) PRIMARY KEY, DOC CLOB)",
            "INSERT INTO BIGLOB_T VALUES (1, RPAD('x', 3000, 'x'))",
            "INSERT INTO BIGLOB_T VALUES (2, RPAD('y', 3999, 'y'))",
            // 4,000 is RPAD's own ceiling in SQL (VARCHAR2), not a
            // driver limit — the DBMS_LOB path below goes past it.
            "INSERT INTO BIGLOB_T VALUES (3, RPAD('z', 4000, 'z'))",
        ])
        .await;
    let shell = fixture.shell(&[stream("l", "BIGLOB_T")]);
    let (rows, _) = read_all(&shell, "l", None).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["doc"].as_str().expect("clob").len(), 3_000);
    assert_eq!(
        rows[1]["doc"].as_str().expect("clob").len(),
        3_999,
        "the exact size recorded as unreadable (O7)"
    );
    assert_eq!(rows[2]["doc"].as_str().expect("clob").len(), 4_000);
}

/// A CURSORLESS stream larger than one batch terminates, and
/// delivers each row exactly once.
///
/// It did neither. The position advanced only from the watermark,
/// and a cursorless stream has none — so the same statement was
/// re-issued forever, re-delivering the first batch until something
/// killed the process. Every existing cursorless cell was smaller
/// than one batch (5,000 rows against a default of 8,192), so
/// nothing could see it; the 200k benchmark hit it immediately.
#[tokio::test(flavor = "multi_thread")]
async fn a_cursorless_stream_past_one_batch_terminates() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE NOCUR_T (ID NUMBER(8) PRIMARY KEY, V VARCHAR2(16))",
            "INSERT INTO NOCUR_T SELECT LEVEL, 'v' FROM DUAL CONNECT BY LEVEL <= 500",
        ])
        .await;
    // Batches far smaller than the table: 20 of them.
    let shell = fixture.shell_tuned(
        &[stream("n", "NOCUR_T")],
        serde_json::json!({"batch_rows": 25}),
    );
    let (rows, _) = read_all(&shell, "n", None).await;

    assert_eq!(rows.len(), 500, "every row once, and the read ENDS");
    let mut ids: Vec<i64> = rows.iter().map(|r| r["id"].as_i64().expect("id")).collect();
    ids.sort_unstable();
    ids.dedup();
    assert_eq!(ids.len(), 500, "no row delivered twice");
}
