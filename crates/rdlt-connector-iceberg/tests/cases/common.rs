//! Shared plumbing for the suites. The container fixture lives here
//! once the live cells land; the offline cells share only the
//! document helpers.
#![allow(dead_code)] // shared across many case files; not every file uses every helper

/// A minimal valid document over a bearer token.
pub fn minimal_doc() -> serde_json::Value {
    serde_json::json!({
        "catalog": {
            "uri": "http://localhost:8181/api/catalog",
            "warehouse": "wh",
            "auth": {"bearer": {"token": "t"}},
        },
        "namespace": "raw",
    })
}

// ---- the live fixture ------------------------------------------------------

use std::collections::HashMap;

/// Fixture credentials — the constants the bootstrap script grants.
pub const CLIENT_ID: &str = "root";
pub const CLIENT_SECRET: &str = "s3cr3t";
pub const S3_KEY: &str = "ice-key";
pub const S3_SECRET: &str = "ice-secret";
pub const BUCKET: &str = "ice";
pub const WAREHOUSE: &str = "rdlt";

/// Polaris + RUSTFS, bootstrapped: catalog over the bucket, admin
/// granted. Host networking with randomized ports — the vended S3
/// endpoint must be reachable by Polaris AND the test client, and
/// container-internal DNS would vend unreachable endpoints. SKIPS
/// (None) without a usable runtime, never panics.
pub struct CatalogFixture {
    // Held for Drop: force-removed when the fixture drops.
    _rustfs: ContainerGuard,
    _polaris: ContainerGuard,
    pub catalog_uri: String,
    pub s3_endpoint: String,
    pub admin_token: String,
    http: reqwest::Client,
}

/// Plain podman: testcontainers cannot express host-network mode
/// against podman's compat API (it tries to CREATE a network named
/// "host") — the bench-harness precedent applies.
pub struct ContainerGuard {
    name: String,
}

impl Drop for ContainerGuard {
    fn drop(&mut self) {
        let _ = std::process::Command::new("podman")
            .args(["rm", "-f", &self.name])
            .output();
    }
}

/// Start one container, or say why this machine cannot — `None`,
/// never a panic: the socket probe and the podman BINARY can disagree,
/// and a red gate naming the wrong cause is worse than a visible skip.
fn run_container(prefix: &str, image: &str, envs: &[(String, String)]) -> Option<ContainerGuard> {
    static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let name = format!(
        "rdlt-icev2-{prefix}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    );
    let mut command = std::process::Command::new("podman");
    // The rdlt-test=1 label is the reclaim convention: --rm covers a
    // clean exit, the label is what makes a killed run's leftovers
    // sweepable.
    command.args([
        "run",
        "-d",
        "--rm",
        "--label",
        "rdlt-test=1",
        "--name",
        &name,
        "--network",
        "host",
    ]);
    for (key, value) in envs {
        command.args(["-e", &format!("{key}={value}")]);
    }
    command.arg(image);
    let output = match command.output() {
        Ok(output) => output,
        Err(e) => {
            eprintln!("SKIP: cannot run `podman` ({e}) — iceberg live cell not run");
            return None;
        }
    };
    if !output.status.success() {
        eprintln!(
            "SKIP: starting {image} failed ({}) — iceberg live cell not run",
            String::from_utf8_lossy(&output.stderr).trim()
        );
        return None;
    }
    Some(ContainerGuard { name })
}

/// A free port from a PID-disjoint range BELOW the kernel ephemeral
/// floor. `bind(:0)` alone races across nextest's per-test processes
/// (observed live as create-catalog flakes), and squatting the
/// ephemeral range collides with everything else binding there.
fn free_port() -> u16 {
    use std::sync::atomic::{AtomicU16, Ordering};
    static NEXT: AtomicU16 = AtomicU16::new(0);
    let pid = std::process::id();
    for _ in 0..2000 {
        let slot = NEXT.fetch_add(1, Ordering::Relaxed) as u32;
        let candidate = 21000 + ((pid.wrapping_mul(641) + slot * 7) % 11000) as u16;
        if std::net::TcpListener::bind(("127.0.0.1", candidate)).is_ok() {
            return candidate;
        }
    }
    panic!("no free port found in the PID-derived range");
}

async fn wait_http_answers(url: &str, attempts: u32, require_success: bool) {
    let client = reqwest::Client::new();
    for _ in 0..attempts {
        if let Ok(response) = client
            .get(url)
            .timeout(std::time::Duration::from_secs(2))
            .send()
            .await
            && (!require_success || response.status().is_success())
        {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    panic!(
        "{url} never answered{}",
        if require_success { " 2xx" } else { "" }
    );
}

impl CatalogFixture {
    /// Start both containers and bootstrap, or skip visibly.
    pub async fn start() -> Option<Self> {
        if !rdlt_testkit::gate::runtime_available() {
            eprintln!("SKIP: no container runtime socket — iceberg live cell not run");
            return None;
        }
        let s3_port = free_port();
        let api_port = free_port();
        let health_port = free_port();

        let rustfs = run_container(
            "rustfs",
            "docker.io/rustfs/rustfs:1.0.0-beta.11",
            &[
                ("RUSTFS_ADDRESS".into(), format!("0.0.0.0:{s3_port}")),
                ("RUSTFS_ACCESS_KEY".into(), S3_KEY.into()),
                ("RUSTFS_SECRET_KEY".into(), S3_SECRET.into()),
            ],
        )?;
        let s3_endpoint = format!("http://127.0.0.1:{s3_port}");
        // Any HTTP answer: anonymous requests get S3-style error XML,
        // never 2xx — listening is the readiness signal.
        wait_http_answers(&format!("{s3_endpoint}/"), 100, false).await;

        // Pinned by DIGEST: the upstream publishes no stable version
        // tag yet, `latest` re-resolves whenever upstream pushes (a
        // behavior change would silently mutate the gate — the exact
        // trap the RUSTFS pin above avoids), and this digest is the
        // image every recorded gate ran against.
        let polaris = run_container(
            "polaris",
            "docker.io/apache/polaris@sha256:5b574ce52708e8402af2305e6e64a588af1a33e1cf5f106df4dcbd17852d706c",
            &[
                (
                    "POLARIS_BOOTSTRAP_CREDENTIALS".into(),
                    format!("POLARIS,{CLIENT_ID},{CLIENT_SECRET}"),
                ),
                ("polaris.realm-context.realms".into(), "POLARIS".into()),
                ("QUARKUS_HTTP_PORT".into(), api_port.to_string()),
                ("QUARKUS_MANAGEMENT_PORT".into(), health_port.to_string()),
                ("AWS_REGION".into(), "us-east-1".into()),
                ("AWS_ACCESS_KEY_ID".into(), S3_KEY.into()),
                ("AWS_SECRET_ACCESS_KEY".into(), S3_SECRET.into()),
            ],
        )?;
        let base = format!("http://127.0.0.1:{api_port}");
        // Health must be 2xx: Quarkus answers 503 DOWN while Polaris
        // initializes — any-answer is not readiness here.
        wait_http_answers(
            &format!("http://127.0.0.1:{health_port}/q/health"),
            400,
            true,
        )
        .await;

        bootstrap_catalog(&base, &s3_endpoint);

        let http = reqwest::Client::new();
        let token: serde_json::Value = http
            .post(format!("{base}/api/catalog/v1/oauth/tokens"))
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", CLIENT_ID),
                ("client_secret", CLIENT_SECRET),
                ("scope", "PRINCIPAL_ROLE:ALL"),
            ])
            .send()
            .await
            .expect("oauth reachable")
            .json()
            .await
            .expect("oauth json");
        let admin_token = token["access_token"]
            .as_str()
            .expect("access_token present")
            .to_owned();

        Some(Self {
            _rustfs: rustfs,
            _polaris: polaris,
            catalog_uri: format!("{base}/api/catalog"),
            s3_endpoint,
            admin_token,
            http,
        })
    }

    /// A ready destination DOCUMENT over this fixture: oauth2, no
    /// storage override (the vended-credential default path).
    pub fn doc(&self, namespace: &str) -> serde_json::Value {
        serde_json::json!({
            "catalog": {
                "uri": self.catalog_uri,
                "warehouse": WAREHOUSE,
                "auth": {"oauth2_client_credentials": {
                    "client_id": CLIENT_ID,
                    "client_secret": CLIENT_SECRET,
                    "scopes": ["PRINCIPAL_ROLE:ALL"],
                }},
            },
            "namespace": namespace,
            "create_namespace": true,
        })
    }

    /// Every table in `namespace`, straight off the catalog — the
    /// real-work oracle for re-certification cells: fresh per-run
    /// identities land in fresh tables, so growth here is work a
    /// replay-masked run would not have done.
    pub async fn tables_in(&self, namespace: &str) -> Vec<String> {
        let body: serde_json::Value = self
            .http
            .get(format!(
                "{}/v1/{WAREHOUSE}/namespaces/{namespace}/tables",
                self.catalog_uri
            ))
            .bearer_auth(&self.admin_token)
            .send()
            .await
            .expect("the catalog answers the table listing")
            .json()
            .await
            .expect("the table listing is JSON");
        body["identifiers"]
            .as_array()
            .map(|identifiers| {
                identifiers
                    .iter()
                    .filter_map(|ident| ident["name"].as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Raw table metadata with its HTTP status JUDGED (round-3 fix —
    /// an error-status JSON body parses to "no snapshots" and must
    /// never read as an empty table): `Ok(None)` for 404 (the table
    /// does not exist — honest absence), `Ok(Some(body))` for a
    /// success reply, `Err` naming the status for everything else (an
    /// expired admin token's 401, a 500, ...).
    pub async fn try_table_metadata(
        &self,
        namespace: &str,
        table: &str,
    ) -> Result<Option<serde_json::Value>, String> {
        let response = self
            .http
            .get(format!(
                "{}/v1/{WAREHOUSE}/namespaces/{namespace}/tables/{table}",
                self.catalog_uri
            ))
            .bearer_auth(&self.admin_token)
            .send()
            .await
            .map_err(|e| format!("the catalog metadata request for `{table}` failed: {e}"))?;
        let status = response.status();
        if status.as_u16() == 404 {
            return Ok(None);
        }
        if !status.is_success() {
            return Err(format!(
                "the catalog answered {status} for table `{table}`'s metadata"
            ));
        }
        response
            .json()
            .await
            .map(Some)
            .map_err(|e| format!("the catalog metadata body for `{table}` is not JSON: {e}"))
    }

    /// Raw table metadata straight off the catalog — the independent
    /// oracle for spec/layout assertions. Panics on any failure or
    /// absence: assertion sites want the table to exist.
    pub async fn table_metadata(&self, namespace: &str, table: &str) -> serde_json::Value {
        self.try_table_metadata(namespace, table)
            .await
            .expect("load table")
            .expect("the table exists")
    }

    /// Snapshot summaries oldest-first with failures SURFACED — the
    /// receipt oracle behind [`LiveProbe`]: an absent table is honestly
    /// no snapshots, any error-status reply is `Err`.
    pub async fn try_snapshot_summaries(
        &self,
        namespace: &str,
        table: &str,
    ) -> Result<Vec<HashMap<String, String>>, String> {
        let Some(response) = self.try_table_metadata(namespace, table).await? else {
            return Ok(Vec::new());
        };
        let mut snapshots: Vec<(i64, HashMap<String, String>)> = response["metadata"]["snapshots"]
            .as_array()
            .map(|list| {
                list.iter()
                    .map(|s| {
                        let ts = s["timestamp-ms"].as_i64().unwrap_or(0);
                        let summary = s["summary"]
                            .as_object()
                            .map(|m| {
                                m.iter()
                                    .filter_map(|(k, v)| {
                                        v.as_str().map(|v| (k.clone(), v.to_owned()))
                                    })
                                    .collect()
                            })
                            .unwrap_or_default();
                        (ts, summary)
                    })
                    .collect()
            })
            .unwrap_or_default();
        snapshots.sort_by_key(|(ts, _)| *ts);
        Ok(snapshots.into_iter().map(|(_, s)| s).collect())
    }

    /// [`Self::try_snapshot_summaries`] for assertion sites: a genuine
    /// catalog failure panics loudly instead of comparing as empty.
    pub async fn snapshot_summaries(
        &self,
        namespace: &str,
        table: &str,
    ) -> Vec<HashMap<String, String>> {
        self.try_snapshot_summaries(namespace, table)
            .await
            .expect("snapshot summaries")
    }
}

/// The read-back oracle over the fixture: row counts off the newest
/// snapshot summary — the catalog's own numbers, independent of the
/// crate. Shared by the conformance cell and the wire
/// certification/kill cells (042); the probe OWNS the fixture so the
/// containers outlive every read.
pub struct LiveProbe {
    pub fixture: CatalogFixture,
    pub namespace: String,
}

#[async_trait::async_trait]
impl rdlt_testkit::TableProbe for LiveProbe {
    async fn count(
        &self,
        table: &rdlt_connector_sdk::spi::core::TableName,
    ) -> Result<u64, rdlt_testkit::ProbeError> {
        // Total records off the newest snapshot summary — the
        // catalog's own count, independent of the crate. A table with
        // no snapshots yet (or a 404 — no table at all) reads as 0;
        // that zero is a fact (nothing published), not an oracle
        // failure. Any OTHER catalog reply (an expired token's 401, a
        // 500 — round-3 fix: an error-status JSON body parses to "no
        // snapshots" and used to fold into 0), a missing
        // `total-records` key, or an unparseable value is the oracle
        // failing, and folding it into 0 would certify invisibility
        // clauses vacuously.
        let summaries = self
            .fixture
            .try_snapshot_summaries(&self.namespace, table.as_str())
            .await
            .map_err(|message| rdlt_testkit::ProbeError { message })?;
        let Some(newest) = summaries.last() else {
            return Ok(0);
        };
        let total = newest
            .get("total-records")
            .ok_or_else(|| rdlt_testkit::ProbeError {
                message: format!(
                    "the newest snapshot of `{}` carries no total-records summary key",
                    table.as_str()
                ),
            })?;
        total.parse().map_err(|_| rdlt_testkit::ProbeError {
            message: format!(
                "the newest snapshot of `{}` reports total-records `{total}`, not one u64",
                table.as_str()
            ),
        })
    }
}

/// Bucket + catalog + grants through the ONE bootstrap tool the crate
/// ships — a second Rust copy would drift from it. Host networking
/// makes the client-side and Polaris-side S3 endpoints identical.
fn bootstrap_catalog(polaris_base: &str, s3_endpoint: &str) {
    let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/polaris_bootstrap.py");
    let output = std::process::Command::new("python3")
        .arg(script)
        .args([
            polaris_base,
            s3_endpoint,
            s3_endpoint,
            S3_KEY,
            S3_SECRET,
            CLIENT_ID,
            CLIENT_SECRET,
            WAREHOUSE,
            BUCKET,
        ])
        .output()
        .expect("python3 for the catalog bootstrap");
    assert!(
        output.status.success(),
        "polaris bootstrap failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
