//! Postgres container fixtures for tests, behind the `fixtures` feature.
//! Test support only — no semver guarantees.
//!
//! The runtime probe and the reclaim label live in the testkit
//! (`rdlt_testkit::gate`) — they are container facts, not postgres facts —
//! and the fixture routes through them so every container-gated suite keeps
//! one skip-not-fail posture and one label `make reclaim` can act on.
//!
//! Posture rule: a missing container runtime NEVER panics — `start*()`
//! returns `None` after a visible `SKIP` line, and the caller returns early.
//! Panics are reserved for real startup failures WITH the runtime present.

use rdlt_testkit::gate::{RECLAIM_LABEL, runtime_available};
use testcontainers_modules::postgres::Postgres as PostgresImage;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ContainerRequest, ImageExt};
use tokio_postgres::{Client, NoTls};

/// The postgres image is testcontainers-modules' default repository,
/// `docker.io/library/postgres`; only the tag is ours to pin.
///
/// Pinned: a floating `latest` re-resolves whenever upstream pushes, so a
/// broken upstream build fails our gate without any change on our side.
/// Bump deliberately, with the live cells green, never by drift.
pub const POSTGRES_TAG: &str = "16-alpine";

/// Container-internal postgres port (the module maps it to a random host
/// port, read back at start).
const POSTGRES_PORT: u16 = 5432;

/// A fresh `postgres:16-alpine`, seeded via SQL batches, handing out its
/// connection string and raw clients. One container per fixture; the
/// container stops on Drop. ONE type serves both the plain and the
/// logical-replication (CDC) flavors — they differ only in server flags.
pub struct PostgresContainer {
    // Held for its Drop: the container stops when the fixture drops.
    _container: ContainerAsync<PostgresImage>,
    /// The source-config `conn:` value for this fixture — the ONE spelling.
    pub connection_string: String,
}

// `ContainerAsync` is not `Debug`; expose the useful half by hand.
impl std::fmt::Debug for PostgresContainer {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PostgresContainer")
            .field("connection_string", &self.connection_string)
            .finish()
    }
}

impl PostgresContainer {
    /// Start a fresh postgres, or skip visibly (`None`) without a runtime.
    pub async fn start() -> Option<Self> {
        let request = PostgresImage::default().with_tag(POSTGRES_TAG);
        Self::launch(request, "postgres fixture").await
    }

    /// Start a postgres with logical replication enabled — the CDC fixture.
    /// Same image, three server flags.
    pub async fn start_for_cdc() -> Option<Self> {
        let request = PostgresImage::default().with_tag(POSTGRES_TAG).with_cmd([
            "postgres",
            "-c",
            "wal_level=logical",
            "-c",
            "max_replication_slots=8",
            "-c",
            "max_wal_senders=8",
        ]);
        Self::launch(request, "CDC postgres fixture").await
    }

    /// The one start sequence: probe the runtime (skip visibly without
    /// one), label for `make reclaim`, start, read the mapped port back
    /// into a connection string.
    async fn launch(request: ContainerRequest<PostgresImage>, what: &str) -> Option<Self> {
        if !runtime_available() {
            eprintln!("SKIP: no container runtime — {what} not started");
            return None;
        }
        let container = request
            .with_label(RECLAIM_LABEL, "1")
            .start()
            .await
            .expect("start postgres container (runtime present)");
        let port = container
            .get_host_port_ipv4(POSTGRES_PORT)
            .await
            .expect("mapped port");
        let connection_string =
            format!("host=127.0.0.1 port={port} user=postgres password=postgres dbname=postgres");
        Some(Self {
            _container: container,
            connection_string,
        })
    }

    /// A raw client for seeding/asserting, independent of the connector
    /// under test (NoTls, straight through the driver — deliberately not
    /// the crate's own session path).
    pub async fn client(&self) -> Client {
        let (client, connection) = tokio_postgres::connect(&self.connection_string, NoTls)
            .await
            .expect("connect to fixture postgres");
        tokio::spawn(async move {
            let _ = connection.await;
        });
        client
    }

    /// Run semicolon-separated DDL/DML (simple batch seeding).
    pub async fn seed(&self, sql: &str) {
        self.client()
            .await
            .batch_execute(sql)
            .await
            .expect("seed SQL");
    }
}
