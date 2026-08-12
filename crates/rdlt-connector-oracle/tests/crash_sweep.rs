#![cfg(feature = "failpoints")]
//! The crash sweep: every fail point × 3 actions through the read
//! path against the live database — armed twice, recovered disarmed,
//! with the resumed run reaching the same rows exactly once. Its own
//! binary, selected by name from `make test TARGET=sweep`;
//! skip-not-fail without a container runtime.

#[path = "cases/common.rs"]
mod common;

use common::{OracleFixture, incremental};
use rdlt_connector_oracle::source::{FAIL_POINTS, Shell};
use rdlt_connector_sdk::spi::core::failpoint::fail;
use rdlt_connector_sdk::spi::{PushPayload, ReadRequest, Source, StreamSpec};

const TOTAL_ROWS: usize = 300;
const ACTIONS: [&str; 3] = ["return", "panic", "1*off->return"];

/// Read one stream, returning the rows delivered and the last cursor.
///
/// The read runs in its own task: the `panic` fail-point action
/// panics inside it, and a panic must be an ATTEMPT failure to
/// observe, not the death of the sweep.
async fn attempt(shell: Shell, since: Option<rdlt_connector_sdk::spi::core::Cursor>) -> Attempt {
    let (out, mut incoming) = rdlt_connector_sdk::spi::records_channel(32 << 20);
    let reader = tokio::spawn(async move {
        shell
            .read(ReadRequest::new(StreamSpec::new("sweep"), since, out))
            .await
            .map_err(|e| e.to_string())
    });
    let collect = async {
        let (mut ids, mut cursor) = (Vec::new(), None);
        while let Some(push) = incoming.recv().await {
            match push.payload {
                PushPayload::Arrow(batch) => {
                    let column = batch.column(0);
                    let column = column
                        .as_any()
                        .downcast_ref::<arrow::array::Int64Array>()
                        .expect("ID is Int64");
                    ids.extend((0..batch.num_rows()).map(|row| column.value(row)));
                }
                PushPayload::Checkpoint(c) => cursor = Some(c),
                _ => {}
            }
        }
        (ids, cursor)
    };
    let (joined, (ids, cursor)) = tokio::join!(reader, collect);
    let failed = match joined {
        Ok(Ok(())) => None,
        Ok(Err(e)) => Some(e),
        Err(join) => Some(format!("panicked: {join}")),
    };
    // WHAT IT DELIVERED AND HOW FAR IT GOT ARE RETURNED EITHER WAY.
    // Discarding them on failure is what made this sweep vacuous:
    // every armed attempt fails by construction, so recovery always
    // restarted from `None` and the assertion was satisfied by a
    // plain uncrashed read.
    Attempt {
        ids,
        cursor,
        failed,
    }
}

/// One read attempt: what it delivered, where it got to, and whether
/// it failed.
struct Attempt {
    ids: Vec<i64>,
    cursor: Option<rdlt_connector_sdk::spi::core::Cursor>,
    failed: Option<String>,
}

/// Every point × action: armed twice (a crash during recovery too),
/// then disarmed — and the resumed read delivers the remaining rows
/// so the run as a whole sees each row exactly once.
#[tokio::test(flavor = "multi_thread")]
async fn every_fail_point_recovers_exactly_once() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE SWEEP_T (ID NUMBER(8) PRIMARY KEY, V VARCHAR2(30))",
            &format!(
                "INSERT INTO SWEEP_T SELECT LEVEL, 'r'||LEVEL FROM DUAL \
                 CONNECT BY LEVEL <= {TOTAL_ROWS}"
            ),
        ])
        .await;
    // Small batches on purpose: the crash points fire PER BATCH, and
    // an action like `1*off->return` needs more than one to reach.
    // With one batch per read the second cell never armed at all.
    let shell = fixture.shell_tuned(
        &[incremental("sweep", "SWEEP_T", "ID")],
        serde_json::json!({"batch_rows": 25}),
    );

    let mut fired = std::collections::BTreeSet::new();
    // Did ANY cell actually resume from a non-empty cursor? The
    // previous sweep passed without ever doing so — recovery always
    // restarted from `None`, so a plain uncrashed read satisfied it.
    // A green sweep that never crossed a checkpoint is worth nothing,
    // and this is what says so.
    let mut ever_crossed = false;
    for &point in FAIL_POINTS {
        for action in ACTIONS {
            fail::cfg(point, action).expect("configure fail point");
            // The armed attempts CARRY THEIR CURSOR forward.
            //
            // Discarding it made the whole sweep vacuous: recovery
            // restarted from `None` with the fail point already
            // removed, so `seen == TOTAL_ROWS` was satisfied by a
            // plain uncrashed full read no matter what the read path
            // did. The property these cells exist to prove — that a
            // crash costs no rows and a resume repeats none — was the
            // one thing untested.
            let mut seen: std::collections::BTreeSet<i64> = std::collections::BTreeSet::new();
            let mut cursor = None;
            let mut any_err = false;
            for _ in 0..2 {
                let a = attempt(shell.clone(), cursor.clone()).await;
                seen.extend(&a.ids);
                if a.cursor.is_some() {
                    cursor = a.cursor;
                }
                any_err |= a.failed.is_some();
            }
            fail::remove(point);
            if any_err {
                fired.insert((point, action));
            }
            let crossed = cursor.is_some();
            ever_crossed |= crossed;

            // Recovery resumes FROM the crashed run's checkpoint.
            for _ in 0..4 {
                let a = attempt(shell.clone(), cursor.clone()).await;
                let fresh = a.ids.len();
                seen.extend(&a.ids);
                if let Some(e) = a.failed {
                    panic!("[{point} / {action}] recovery failed: {e}");
                }
                if a.cursor.is_some() {
                    cursor = a.cursor;
                }
                if fresh == 0 {
                    break;
                }
            }

            // AT-LEAST-ONCE: no row may be LOST. A raw SUM could
            // never express this — a resumed run may legitimately
            // re-deliver the rows after its last checkpoint, so the
            // total can exceed N — which is why the property is over
            // DISTINCT identities.
            assert_eq!(
                seen.len(),
                TOTAL_ROWS,
                "[{point} / {action}] a crash must cost no rows (crossed a checkpoint: {crossed})"
            );
            assert_eq!(
                (*seen.first().expect("rows"), *seen.last().expect("rows")),
                (1, TOTAL_ROWS as i64),
                "[{point} / {action}] the delivered identities must be exactly 1..=N"
            );
        }
    }
    let expected: std::collections::BTreeSet<_> = FAIL_POINTS
        .iter()
        .flat_map(|p| ACTIONS.iter().map(move |a| (*p, *a)))
        .collect();
    assert_eq!(fired, expected, "the armed-fire matrix must be complete");
    assert!(
        ever_crossed,
        "no cell resumed from a non-empty cursor — the sweep proved nothing about \
         recovery, which is the whole reason it exists"
    );
}

/// The registry names exactly the points armed in the sources — the
/// self-check before container minutes are spent (the ungated twin
/// lives in cases/test_gating.rs).
#[test]
fn the_registry_matches_the_sources() {
    rdlt_testkit::assert_registry_matches_sources(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("src")
            .as_path(),
        &[FAIL_POINTS],
    );
}
