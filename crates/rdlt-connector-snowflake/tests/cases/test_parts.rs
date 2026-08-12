//! Output part sizing (034) against the live account: how many parquet
//! files a load stages, and that the answer never changes how many rows
//! land.
//!
//! The oracle is the service's own `COPY_HISTORY`, which reports one
//! row per file it loaded — read back from the account rather than
//! computed by this crate, so the file count is evidence rather than a
//! restatement of the code under test.

use rdlt_connector_sdk::spi::StreamSpec;
use rdlt_connector_snowflake::destination::{Shell, testhook};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemorySource, MemoryStream};
use serde_json::json;

use super::common::{config_for, credentials, scratch_schema};

/// Eight batches of 250 rows, each row distinct so the parts do not
/// dictionary-encode away to nothing.
fn source_of() -> MemorySource {
    MemorySource::new(vec![MemoryStream::new(
        StreamSpec::new("events"),
        (0..8)
            .map(|b: i64| {
                MemoryBatch::new(
                    (0..250)
                        .map(|i| {
                            let id = b * 250 + i;
                            json!({"id": id, "note": format!("note-{id}-{}", "x".repeat(64))})
                        })
                        .collect(),
                )
            })
            .collect(),
    )])
}

/// A small target stages MANY files; the default stages ONE. Same
/// 2,000 rows and the same single commit either way — only
/// `parts.target_bytes` differs, so the file count is attributable to
/// it alone, and the landed rowcount must not move at all.
///
/// MEASURED against the account: **8 files at a 4 KiB target, 1 at the
/// 128 MiB default**, both landing exactly 2,000 rows.
#[tokio::test]
async fn the_target_decides_the_file_count_and_never_the_row_count() {
    let Some(creds) = credentials() else { return };

    let mut staged = Vec::new();
    for (suffix, target) in [("prt_roll", Some(4_096)), ("prt_deflt", None)] {
        let schema = scratch_schema(suffix);
        let mut doc = config_for(&creds, &schema);
        if let Some(bytes) = target {
            doc["parts"] = json!({"target_bytes": bytes});
        }
        let config = {
            use rdlt_connector_sdk::config::Document;
            rdlt_connector_snowflake::destination::Config::from_value(doc.clone()).expect("valid")
        };

        let workdir = tempfile::tempdir().expect("workdir");
        Engine::new(
            EngineConfig::new(format!("sf-{suffix}")).with_workdir(workdir.path().join("wal")),
            source_of(),
            Shell::from_value(doc).expect("valid"),
        )
        .run()
        .await
        .expect("the load settles");

        let qualified = format!(
            "\"{}\".\"{}\".\"EVENTS\"",
            config.database.to_uppercase(),
            schema.to_uppercase()
        );
        let landed = testhook::rows(
            &config,
            &format!("SELECT COUNT(*) AS N FROM {qualified}"),
            &["n"],
        )
        .await
        .expect("read back");
        assert_eq!(landed[0][0], "2000", "{suffix}: every row landed once");

        // One COPY_HISTORY row per FILE the service loaded.
        let files = testhook::rows(
            &config,
            &format!(
                "SELECT COUNT(*) AS N FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(\
                 TABLE_NAME => '{qualified}', START_TIME => DATEADD(hours, -1, CURRENT_TIMESTAMP())))"
            ),
            &["n"],
        )
        .await
        .expect("copy history");
        staged.push((suffix, files[0][0].parse::<u64>().expect("a file count")));
    }

    let rolled = staged[0].1;
    let default = staged[1].1;
    assert_eq!(
        default, 1,
        "2,000 small rows are nowhere near 128 MiB, so the default stages one file: {staged:?}"
    );
    assert!(
        rolled > default,
        "a 4 KiB target must stage more files than the default: {staged:?}"
    );
}
