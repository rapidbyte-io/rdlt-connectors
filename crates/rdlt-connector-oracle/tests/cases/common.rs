//! The Oracle Free container fixture: ONE database shared by this
//! binary's cells (it is the slowest fixture the gate has — ~75 s to
//! readiness), skip-not-fail without a runtime.

#![allow(dead_code)] // shared across cells; not every cell uses every helper

use rdlt_connector_oracle::source::{Config, Shell, Stream};
use rdlt_connector_sdk::config::Document;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

pub const PASSWORD: &str = "rdlt-probe-pw";
pub const APP_USER: &str = "rdlt";

/// Which Oracle a cell runs against.
///
/// The connector is claimed to work across generations, and a claim
/// no cell exercises is a wish. The two flavors differ in more than a
/// tag: the service name, and — the reason this matters — the
/// server's protocol capabilities. 23ai negotiates a ub8 row-count
/// token the vendored driver had to be patched for; 21c does not send
/// it. Running the SAME cells against both is what proves the patch
/// reads the capability rather than assuming the newer wire.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Flavor {
    /// Oracle Free 23ai — the default the gate runs.
    Free23,
    /// Oracle XE 21c — the older generation.
    Xe21,
}

impl Flavor {
    /// Pinned tags: a floating tag re-resolves on upstream's schedule
    /// and would hand their broken day to our gate. Bump
    /// deliberately, with the live cells green on the new tag.
    pub fn image(self) -> (&'static str, &'static str) {
        match self {
            Self::Free23 => ("docker.io/gvenzl/oracle-free", "23.26.2-slim-faststart"),
            Self::Xe21 => ("docker.io/gvenzl/oracle-xe", "21.3.0-slim-faststart"),
        }
    }

    pub fn service(self) -> &'static str {
        match self {
            Self::Free23 => "FREEPDB1",
            Self::Xe21 => "XEPDB1",
        }
    }
}

pub struct OracleFixture {
    _container: ContainerAsync<GenericImage>,
    pub host: String,
    pub port: u16,
    pub flavor: Flavor,
}

impl OracleFixture {
    /// Start Oracle Free 23ai, or skip visibly (None) when a
    /// prerequisite is missing.
    ///
    /// ONE CONTAINER PER CELL, deliberately, and NOT shared through a
    /// `static`. That was tried and reverted on measurement: a static
    /// is never dropped at process exit, so testcontainers' own
    /// cleanup never ran and every cell LEAKED its database — 18 of
    /// them left behind by one run. It bought nothing either, because
    /// nextest gives each test its own process.
    ///
    /// What keeps the machine sane is the `oracle-live` test-group in
    /// .config/nextest.toml: unbounded parallelism was measured
    /// starting SIXTEEN Oracle databases at once, and this is the
    /// heaviest fixture the gate has.
    pub async fn start() -> Option<Self> {
        Self::start_on(Flavor::Free23).await
    }

    /// Start a container of the named flavor, or skip visibly (None)
    /// when either prerequisite is missing.
    pub async fn start_on(flavor: Flavor) -> Option<Self> {
        if !rdlt_testkit::gate::runtime_available() {
            eprintln!("SKIP: no container runtime socket — oracle live cells not run");
            return None;
        }
        if !rdlt_connector_oracle::source::client_available() {
            eprintln!(
                "SKIP: no Oracle Client library — this driver wraps ODPI-C and dlopens \
                 libclntsh at RUNTIME. Install Instant Client and put it on \
                 LD_LIBRARY_PATH; oracle live cells not run"
            );
            return None;
        }
        let (image, tag) = flavor.image();
        let container = GenericImage::new(image, tag)
            .with_exposed_port(1521.tcp())
            // The log line fires BEFORE the PDB registers with the
            // listener; the readiness poll below closes that gap
            // (without it the first connect races ORA-12514).
            .with_wait_for(WaitFor::message_on_stdout("DATABASE IS READY TO USE!"))
            .with_env_var("ORACLE_PASSWORD", PASSWORD)
            .with_env_var("APP_USER", APP_USER)
            .with_env_var("APP_USER_PASSWORD", PASSWORD)
            .with_label(rdlt_testkit::gate::RECLAIM_LABEL, "1")
            .start()
            .await
            .unwrap_or_else(|e| panic!("start {image}:{tag} (runtime socket present): {e}"));
        let port = container
            .get_host_port_ipv4(1521)
            .await
            .expect("mapped port");
        let fixture = Self {
            _container: container,
            host: "127.0.0.1".to_owned(),
            port,
            flavor,
        };
        fixture.await_service().await;
        Some(fixture)
    }

    /// Ready means a real query answers as the APP user — the log
    /// line alone is not enough.
    async fn await_service(&self) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(180);
        let mut last;
        loop {
            use rdlt_connector_sdk::spi::source::Source;
            let shell = Shell::new(self.config(&[])).expect("the fixture document is valid");
            match shell.check().await {
                Ok(()) => return,
                Err(e) => last = e.to_string(),
            }
            let _ = &last;
            assert!(
                std::time::Instant::now() < deadline,
                "oracle never accepted a connection within the deadline; last error: {last}"
            );
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    }

    /// A source document over this fixture with the given streams.
    pub fn config(&self, streams: &[Stream]) -> Config {
        let value = serde_json::json!({
            "host": self.host,
            "port": self.port,
            "service": self.flavor.service(),
            "user": APP_USER,
            "password": PASSWORD,
            "streams": if streams.is_empty() {
                serde_json::json!([{"name": "probe", "table": "DUAL"}])
            } else {
                serde_json::to_value(streams).expect("streams")
            },
        });
        Config::from_value(value).expect("valid fixture document")
    }

    /// A shell whose `tuning` carries the given overrides.
    pub fn shell_tuned(&self, streams: &[Stream], tuning: serde_json::Value) -> Shell {
        let mut value = serde_json::json!({
            "host": self.host,
            "port": self.port,
            "service": self.flavor.service(),
            "user": APP_USER,
            "password": PASSWORD,
            "streams": serde_json::to_value(streams).expect("streams"),
        });
        value["tuning"] = tuning;
        Shell::new(Config::from_value(value).expect("valid tuned document")).expect("valid")
    }

    pub fn shell(&self, streams: &[Stream]) -> Shell {
        Shell::new(self.config(streams)).expect("valid")
    }

    /// Run statements on ONE connection and commit them there.
    ///
    /// Per-statement connections would drop each INSERT's transaction
    /// before any later COMMIT could reach it — probed the hard way:
    /// a seeded table read back empty.
    pub async fn seed(&self, statements: &[&str]) {
        let dsn = format!("//{}:{}/{}", self.host, self.port, self.flavor.service());
        let owned: Vec<String> = statements.iter().map(|s| (*s).to_owned()).collect();
        tokio::task::spawn_blocking(move || {
            let conn =
                oracle::Connection::connect(APP_USER, PASSWORD, &dsn).expect("fixture connect");
            for sql in &owned {
                conn.execute(sql, &[])
                    .unwrap_or_else(|e| panic!("{sql}: {e}"));
            }
            conn.commit().expect("fixture commit");
        })
        .await
        .expect("seed thread");
    }
}

/// A stream document over one table.
pub fn stream(name: &str, table: &str) -> Stream {
    serde_json::from_value(serde_json::json!({"name": name, "table": table}))
        .expect("stream document")
}

/// A stream with a watermark cursor column.
pub fn incremental(name: &str, table: &str, cursor: &str) -> Stream {
    serde_json::from_value(serde_json::json!({
        "name": name, "table": table, "cursor": cursor
    }))
    .expect("stream document")
}

/// One Arrow batch as JSON objects, for assertions.
///
/// This is a TEST convenience, not the transport: the connector
/// pushes Arrow and the engine consumes Arrow. Values are rendered so
/// a reader can see what actually crossed — exact decimals as text,
/// binary as hex, timestamps as ISO — rather than what a JSON
/// encoder would have had to invent.
pub fn batch_to_json(batch: &arrow::array::RecordBatch) -> Vec<serde_json::Value> {
    use arrow::array::{self, Array};

    let mut out = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut object = serde_json::Map::new();
        for (at, field) in batch.schema().fields().iter().enumerate() {
            let column = batch.column(at);
            let value = if column.is_null(row) {
                serde_json::Value::Null
            } else if let Some(a) = column.as_any().downcast_ref::<array::Int64Array>() {
                serde_json::json!(a.value(row))
            } else if let Some(a) = column.as_any().downcast_ref::<array::Float64Array>() {
                serde_json::json!(a.value(row))
            } else if let Some(a) = column.as_any().downcast_ref::<array::StringArray>() {
                serde_json::json!(a.value(row))
            } else if let Some(a) = column.as_any().downcast_ref::<array::BinaryArray>() {
                serde_json::json!(
                    a.value(row)
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect::<String>()
                )
            } else if let Some(a) = column.as_any().downcast_ref::<array::BooleanArray>() {
                serde_json::json!(a.value(row))
            } else if let Some(a) = column.as_any().downcast_ref::<array::Decimal128Array>() {
                serde_json::json!(a.value_as_string(row))
            } else if let Some(a) = column
                .as_any()
                .downcast_ref::<array::TimestampMicrosecondArray>()
            {
                // ISO, so the rendering matches the watermark's own
                // spelling and a reader can compare them by eye.
                serde_json::json!(
                    a.value_as_datetime(row)
                        .map(|d| d.format("%Y-%m-%dT%H:%M:%S%.6f").to_string())
                )
            } else {
                serde_json::json!(format!("<unrendered {}>", field.data_type()))
            };
            object.insert(field.name().clone(), value);
        }
        out.push(serde_json::Value::Object(object));
    }
    out
}
