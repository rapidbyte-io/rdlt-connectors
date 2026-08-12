//! The live object-store fixture: one RUSTFS server per test, run
//! through testcontainers. A cell that cannot get a runtime socket
//! gets `None` back and skips VISIBLY — the container-optional gate
//! posture — rather than failing on a machine without podman.

#![allow(dead_code)] // shared across two binaries; neither uses every helper

use object_store::ObjectStore;
use rdlt_connector_file::{LocationOptions, S3Options};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

pub const ACCESS_KEY: &str = "rdlt-test-access";
pub const SECRET_KEY: &str = "rdlt-test-secret";
pub const BUCKET: &str = "raw";
/// The image tag is a PIN, not a preference: `latest` would re-resolve
/// on upstream's schedule and hand their broken day to our gate. Move
/// it on purpose, with the live cells green on the new tag first.
pub const RUSTFS_TAG: &str = "1.0.0-beta.11";

pub struct S3Fixture {
    /// Dropping the fixture drops the container handle, which stops
    /// and removes the server.
    _container: ContainerAsync<GenericImage>,
    pub endpoint: String,
}

impl S3Fixture {
    /// Bring a server up and hand back its endpoint — or `None`, out
    /// loud, when this machine offers no container runtime.
    pub async fn start() -> Option<Self> {
        if !rdlt_testkit::gate::runtime_available() {
            eprintln!("SKIP: no container runtime socket — s3 live cell not run");
            return None;
        }
        let container = GenericImage::new("docker.io/rustfs/rustfs", RUSTFS_TAG)
            .with_exposed_port(9000.tcp())
            .with_wait_for(WaitFor::message_on_stdout("Starting"))
            .with_env_var("RUSTFS_ACCESS_KEY", ACCESS_KEY)
            .with_env_var("RUSTFS_SECRET_KEY", SECRET_KEY)
            .with_label(rdlt_testkit::gate::RECLAIM_LABEL, "1")
            .start()
            .await
            .expect("start rustfs container (runtime socket present)");
        let port = container
            .get_host_port_ipv4(9000)
            .await
            .expect("mapped port");
        let fixture = Self {
            _container: container,
            endpoint: format!("http://127.0.0.1:{port}"),
        };
        fixture.await_signed_answers().await;
        provision_bucket(&fixture.endpoint, BUCKET);
        Some(fixture)
    }

    /// The log line only says the process launched; ready means a
    /// SIGNED request comes back answered. `NotFound` qualifies — the
    /// probe object does not exist, but something authenticated us and
    /// said so.
    async fn await_signed_answers(&self) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        let store = self.client();
        loop {
            match store.head(&object_store::path::Path::from("probe")).await {
                Ok(_) | Err(object_store::Error::NotFound { .. }) => return,
                Err(e) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "rustfs never answered a signed request: {e}"
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(150)).await;
                }
            }
        }
    }

    fn client(&self) -> object_store::aws::AmazonS3 {
        object_store::aws::AmazonS3Builder::new()
            .with_endpoint(&self.endpoint)
            .with_bucket_name(BUCKET)
            .with_region("us-east-1")
            .with_access_key_id(ACCESS_KEY)
            .with_secret_access_key(SECRET_KEY)
            .with_allow_http(true)
            .build()
            .expect("fixture client")
    }

    /// Seed one object. The full suite launches several fixtures at
    /// once, so a single flaky PUT gets a couple of spaced retries
    /// before it is allowed to sink the cell.
    pub async fn put(&self, key: &str, body: &[u8]) {
        let store = self.client();
        let target = object_store::path::Path::from(key);
        for attempt in 1u64..=3 {
            let payload = bytes::Bytes::copy_from_slice(body);
            match store.put(&target, payload.into()).await {
                Ok(_) => return,
                Err(e) if attempt == 3 => panic!("seeding `{key}` kept failing: {e:?}"),
                Err(_) => {
                    tokio::time::sleep(std::time::Duration::from_millis(250 * attempt)).await;
                }
            }
        }
    }

    pub async fn exists(&self, key: &str) -> bool {
        self.client()
            .head(&object_store::path::Path::from(key))
            .await
            .is_ok()
    }

    /// The `location:` block pointing at this server.
    pub fn location_options(&self) -> LocationOptions {
        LocationOptions::s3(S3Options::new(
            self.endpoint.clone(),
            BUCKET,
            ACCESS_KEY,
            SECRET_KEY,
        ))
    }
}

/// Create the bucket with one hand-signed SigV4 PUT. `object_store`
/// deliberately has no bucket-admin surface, and pulling in an AWS SDK
/// for a single fixture call would be absurd — python's stdlib carries
/// hmac/sha256/urllib, so the fixture shells out. Test scaffolding
/// only; product code never signs by hand.
fn provision_bucket(endpoint: &str, bucket: &str) {
    let script = format!(
        r#"
import datetime as dt, hashlib, hmac, urllib.request, urllib.error, sys

aid, sec, reg, svc = "{ACCESS_KEY}", "{SECRET_KEY}", "us-east-1", "s3"
url = "{endpoint}/{bucket}"
host = "{endpoint}".split("://", 1)[1]

now = dt.datetime.now(dt.timezone.utc)
stamp = now.strftime("%Y%m%dT%H%M%SZ")
day = now.strftime("%Y%m%d")
empty = hashlib.sha256(b"").hexdigest()
signed_headers = "host;x-amz-content-sha256;x-amz-date"

canonical = "\n".join([
    "PUT", "/{bucket}", "",
    f"host:{{host}}",
    f"x-amz-content-sha256:{{empty}}",
    f"x-amz-date:{{stamp}}",
    "", signed_headers, empty,
])
scope = f"{{day}}/{{reg}}/{{svc}}/aws4_request"
to_sign = "\n".join([
    "AWS4-HMAC-SHA256", stamp, scope,
    hashlib.sha256(canonical.encode()).hexdigest(),
])

def hkey(key, msg):
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()

signing = hkey(hkey(hkey(hkey(("AWS4" + sec).encode(), day), reg), svc), "aws4_request")
signature = hmac.new(signing, to_sign.encode(), hashlib.sha256).hexdigest()

request = urllib.request.Request(url, method="PUT", headers={{
    "x-amz-date": stamp,
    "x-amz-content-sha256": empty,
    "Authorization": (
        f"AWS4-HMAC-SHA256 Credential={{aid}}/{{scope}}, "
        f"SignedHeaders={{signed_headers}}, Signature={{signature}}"
    ),
}})
try:
    urllib.request.urlopen(request, timeout=10)
except urllib.error.HTTPError as e:
    if e.code != 409:  # an already-existing bucket is fine
        sys.exit(f"bucket PUT answered {{e.code}}: {{e.read()[:200]}}")
"#
    );
    let out = std::process::Command::new("python3")
        .arg("-c")
        .arg(&script)
        .output()
        .expect("python3 for the fixture's bucket PUT");
    assert!(
        out.status.success(),
        "bucket provisioning failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}
