//! The shared CDC scaffolding every `test_cdc_*` case file builds on: the
//! `cdc:` config block the slot-lifecycle suites hand to `cdc_slot`
//! directly, the two-table seed they share, and the source→destination rig
//! the streaming suites run pipelines through.

use rdlt_connector_postgres::destination::{
    DestinationOptions, MergeStrategy, Postgres, TableOptions,
};
use rdlt_connector_postgres::fixtures::PostgresContainer;
use rdlt_connector_postgres::source;
use rdlt_connector_postgres::source::config::{AckMode, CdcConfig, CdcMode, Wait};
use rdlt_engine::config::Config as EngineConfig;
use rdlt_engine::engine::Engine;

/// A `cdc:` block at its documented defaults, varying only the three things
/// the lifecycle suites vary.
pub fn cdc_config(slot: &str, publication: &str, create_if_missing: bool) -> CdcConfig {
    CdcConfig {
        slot: slot.into(),
        publication: publication.into(),
        create_if_missing,
        mode: CdcMode::Catchup,
        idle_wait: Wait { seconds: 1 },
        flag_column: "_rdlt_deleted".into(),
        ack: AckMode::Auto,
    }
}

/// The seed the lifecycle suites share: one keyed table with two rows.
pub const ORDERS_SEED: &str = r#"
CREATE TABLE public.orders (id int8 PRIMARY KEY, total int4);
INSERT INTO public.orders VALUES (1, 10), (2, 20);
"#;

/// The recommended composition: a CDC source feeding the postgres
/// destination with `merge{key}` + `merge_strategy: upsert` +
/// `hard_delete: <flag>`, into a `mirror` schema of the SAME database — so
/// source/destination equality is a SQL question, not a client-side diff.
pub struct Rig {
    pub container: PostgresContainer,
    pub workdir: std::path::PathBuf,
    pipeline: String,
}

impl Rig {
    /// Skip-not-fail: `None` when no container runtime, so callers return
    /// early.
    pub async fn start(pipeline: &str) -> Option<Self> {
        let container = PostgresContainer::start_for_cdc().await?;
        Some(Self {
            container,
            workdir: leaked_workdir(),
            pipeline: pipeline.to_string(),
        })
    }

    /// A fresh pipeline identity (new workdir + name): the documented
    /// recovery for a wedged stream — cursors gone, so the next run
    /// snapshots.
    pub fn reset_state(&mut self, pipeline: &str) {
        self.workdir = leaked_workdir();
        self.pipeline = pipeline.to_string();
    }

    fn source(&self, tables: &[&str]) -> source::Shell {
        let list = tables
            .iter()
            .map(|table| format!("  - name: {table}\n"))
            .collect::<String>();
        crate::cases::common::source(
            &self.container.connection_string,
            &format!(
                "cdc:\n  slot: s1\n  publication: p1\n  create_if_missing: true\ntables:\n{list}"
            ),
        )
    }

    pub fn destination(&self, tables: &[&str]) -> rdlt_connector_postgres::destination::Shell {
        Postgres::new(self.container.connection_string.clone())
            .schema("mirror")
            .options(DestinationOptions {
                merge_strategy: Some(MergeStrategy::Upsert),
                tables: tables
                    .iter()
                    .map(|table| {
                        (
                            table.to_string(),
                            TableOptions {
                                hard_delete: Some("_rdlt_deleted".into()),
                                ..TableOptions::default()
                            },
                        )
                    })
                    .collect(),
            })
            .expect("valid destination options")
            .into_shell()
    }

    pub async fn run(&self, tables: &[&str], key: &str) -> u64 {
        Engine::new(
            self.engine_config(key),
            self.source(tables),
            self.destination(tables),
        )
        .run()
        .await
        .expect("cdc run")
        .total_rows()
    }

    pub async fn run_expecting_error(&self, tables: &[&str], key: &str) -> String {
        Engine::new(
            self.engine_config(key),
            self.source(tables),
            self.destination(tables),
        )
        .run()
        .await
        .expect_err("run should fail")
        .to_string()
    }

    fn engine_config(&self, key: &str) -> EngineConfig {
        EngineConfig::new(self.pipeline.as_str())
            .with_workdir(self.workdir.clone())
            .with_write_mode(rdlt_connector_sdk::spi::core::commit::WriteMode::Merge {
                key: vec![key.into()],
            })
    }

    /// Row-for-row equality on the projected columns, both directions.
    pub async fn assert_mirror_equals_source(&self, table: &str, columns: &str) {
        let client = self.container.client().await;
        for (left, right) in [("public", "mirror"), ("mirror", "public")] {
            let difference: i64 = client
                .query_one(
                    &format!(
                        "SELECT count(*) FROM (SELECT {columns} FROM {left}.\"{table}\" \
                         EXCEPT SELECT {columns} FROM {right}.\"{table}\") d"
                    ),
                    &[],
                )
                .await
                .expect("equality query")
                .get(0);
            assert_eq!(
                difference, 0,
                "{left} \\ {right} on {table} should be empty"
            );
        }
    }

    pub async fn scalar(&self, sql: &str) -> i64 {
        self.container
            .client()
            .await
            .query_one(sql, &[])
            .await
            .expect("scalar")
            .get(0)
    }

    pub async fn scalar_text(&self, sql: &str) -> String {
        self.container
            .client()
            .await
            .query_one(sql, &[])
            .await
            .expect("scalar text")
            .get(0)
    }
}

/// A workdir whose tempdir is leaked deliberately: engine state must outlive
/// the test body so late writes never race teardown.
fn leaked_workdir() -> std::path::PathBuf {
    let directory = tempfile::tempdir().expect("workdir");
    let workdir = directory.path().to_path_buf();
    std::mem::forget(directory);
    workdir
}
