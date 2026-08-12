//! The TLS matrix (contract tls-policy.md) driven against a real TLS-only
//! Postgres built here from a generated PKI: every mode, positive AND
//! distinguished negative, mutual-TLS client credentials, and the libpq
//! connection-string parameters that reach the same policy without a `tls:`
//! block — for BOTH connectors, which share one connect path.

use crate::cases::common;
use rdlt_connector_postgres::destination;
use rdlt_connector_postgres::fixtures::PostgresContainer;
use rdlt_connector_postgres::source;
use rdlt_connector_postgres::testsupport::session;
use rdlt_connector_postgres::tls::{Mode, PemSource, Policy};
use rdlt_connector_sdk::config::Document;
use testcontainers_modules::postgres::Postgres as PostgresImage;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ImageExt};

/// Generated PKI for the TLS matrix: a CA, a server cert signed by it
/// (SAN: localhost ONLY — connecting via 127.0.0.1 is the hostname-mismatch
/// case), and an unrelated CA for the wrong-trust-anchor case.
struct TlsPki {
    ca_pem: String,
    wrong_ca_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
    /// Client credential signed by the REAL CA, CN=postgres (pg `cert` auth
    /// maps the CN to the login role) — the mutual-TLS cells.
    client_cert_pem: String,
    client_key_pem: String,
    /// Client credential signed by the WRONG CA (same CN) — the
    /// server-rejects-our-cert case.
    wrong_client_cert_pem: String,
    wrong_client_key_pem: String,
}

impl TlsPki {
    fn generate() -> Self {
        use rcgen::{BasicConstraints, CertificateParams, IsCa, KeyPair};
        use rcgen::{DistinguishedName, DnType};
        let ca_key = KeyPair::generate().expect("ca key");
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let mut ca_name = DistinguishedName::new();
        ca_name.push(DnType::CommonName, "rdlt test CA");
        ca_params.distinguished_name = ca_name;
        let ca = ca_params.self_signed(&ca_key).expect("ca cert");

        let server_key = KeyPair::generate().expect("server key");
        let server_params =
            CertificateParams::new(vec!["localhost".to_string()]).expect("server params");
        let server = server_params
            .signed_by(&server_key, &ca, &ca_key)
            .expect("server cert");

        let wrong_ca_key = KeyPair::generate().expect("wrong ca key");
        let mut wrong_ca_params = CertificateParams::new(Vec::<String>::new()).expect("params");
        wrong_ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let mut wrong_ca_name = DistinguishedName::new();
        wrong_ca_name.push(DnType::CommonName, "rdlt WRONG CA");
        wrong_ca_params.distinguished_name = wrong_ca_name;
        let wrong_ca = wrong_ca_params
            .self_signed(&wrong_ca_key)
            .expect("wrong ca");

        // Client credentials: CN must be the pg login role (`cert` auth uses
        // the CN as the user); one pair per CA so wrong-CA rejection is a
        // pure trust decision, not a name mismatch.
        let client_credential = |ca_cert: &rcgen::Certificate, ca_key: &KeyPair| {
            let key = KeyPair::generate().expect("client key");
            let mut params = CertificateParams::new(Vec::<String>::new()).expect("client params");
            let mut name = DistinguishedName::new();
            name.push(DnType::CommonName, "postgres");
            params.distinguished_name = name;
            let certificate = params
                .signed_by(&key, ca_cert, ca_key)
                .expect("client cert");
            (certificate.pem(), key.serialize_pem())
        };
        let (client_cert_pem, client_key_pem) = client_credential(&ca, &ca_key);
        let (wrong_client_cert_pem, wrong_client_key_pem) =
            client_credential(&wrong_ca, &wrong_ca_key);

        Self {
            ca_pem: ca.pem(),
            wrong_ca_pem: wrong_ca.pem(),
            server_cert_pem: server.pem(),
            server_key_pem: server_key.serialize_pem(),
            client_cert_pem,
            client_key_pem,
            wrong_client_cert_pem,
            wrong_client_key_pem,
        }
    }
}

/// A TLS-enabled postgres: certs land via /docker-entrypoint-initdb.d
/// (runs AFTER initdb's temp server, as root — fixes key perms and appends
/// ssl config before the FINAL server start; `hostssl`-only pg_hba makes
/// plaintext connections a protocol-level rejection).
struct TlsPostgresContainer {
    _container: ContainerAsync<PostgresImage>,
    port: u16,
    pki: TlsPki,
}

impl TlsPostgresContainer {
    async fn start() -> Option<Self> {
        Self::start_with(false).await
    }

    /// A server that REQUIRES client certificates — the test
    /// CA becomes `ssl_ca_file` and pg_hba uses `cert` auth (the handshake
    /// identity IS the login; CN maps to the role).
    async fn start_cert_auth() -> Option<Self> {
        Self::start_with(true).await
    }

    async fn start_with(cert_auth: bool) -> Option<Self> {
        if !rdlt_testkit::gate::runtime_available() {
            eprintln!("SKIP: no container runtime — TLS postgres fixture not started");
            return None;
        }
        let pki = TlsPki::generate();
        let hba = if cert_auth {
            "hostssl all all 0.0.0.0/0   cert\nhostssl all all ::0/0       cert"
        } else {
            "hostssl all all 0.0.0.0/0   trust\nhostssl all all ::0/0       trust"
        };
        let ca_conf = if cert_auth {
            "ssl_ca_file = 'ca.crt'"
        } else {
            ""
        };
        let init = format!(
            r#"#!/bin/bash
set -e
install -m 600 -o postgres -g postgres /tls-in/server.key "$PGDATA/server.key"
install -m 644 -o postgres -g postgres /tls-in/server.crt "$PGDATA/server.crt"
install -m 644 -o postgres -g postgres /tls-in/ca.crt "$PGDATA/ca.crt"
cat >> "$PGDATA/postgresql.conf" <<CONF
ssl = on
ssl_cert_file = 'server.crt'
ssl_key_file = 'server.key'
{ca_conf}
CONF
cat > "$PGDATA/pg_hba.conf" <<HBA
local   all all             trust
{hba}
HBA
"#
        );
        let container = PostgresImage::default()
            .with_tag("16-alpine")
            .with_label(rdlt_testkit::gate::RECLAIM_LABEL, "1")
            .with_copy_to(
                "/tls-in/server.crt",
                pki.server_cert_pem.clone().into_bytes(),
            )
            .with_copy_to(
                "/tls-in/server.key",
                pki.server_key_pem.clone().into_bytes(),
            )
            .with_copy_to("/tls-in/ca.crt", pki.ca_pem.clone().into_bytes())
            .with_copy_to(
                "/docker-entrypoint-initdb.d/zz-tls.sh",
                init.as_bytes().to_vec(),
            )
            .start()
            .await
            .expect("start TLS postgres (needs docker/podman)");
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("mapped port");
        Some(Self {
            _container: container,
            port,
            pki,
        })
    }

    /// Conn string via `localhost` (matches the cert SAN).
    fn connection_string_via_localhost(&self) -> String {
        format!(
            "host=localhost port={} user=postgres password=postgres dbname=postgres",
            self.port
        )
    }

    /// Conn string via `127.0.0.1` (NOT in the cert SAN — the mismatch case).
    fn connection_string_via_ip(&self) -> String {
        format!(
            "host=127.0.0.1 port={} user=postgres password=postgres dbname=postgres",
            self.port
        )
    }
}

/// Drive a real connection through the source (streams() reflects ⇒ connects).
async fn probe_source(connection_string: &str, tls_yaml: &str) -> Result<(), String> {
    use rdlt_connector_sdk::spi::Source as _;
    common::source(connection_string, tls_yaml)
        .streams()
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn tls_yaml(mode: &str, root: Option<&str>) -> String {
    match root {
        None => format!("tls:\n  mode: {mode}\n"),
        Some(pem) => {
            let indented = pem.trim().replace('\n', "\n    ");
            format!("tls:\n  mode: {mode}\n  root_cert: |\n    {indented}\n")
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn source_matrix_against_tls_only_server() {
    let Some(fixture) = TlsPostgresContainer::start().await else {
        return;
    };
    let ca = fixture.pki.ca_pem.clone();
    let wrong_ca = fixture.pki.wrong_ca_pem.clone();
    let localhost = fixture.connection_string_via_localhost();
    let ip = fixture.connection_string_via_ip();

    // disable → the hostssl-only server rejects plaintext, typed error.
    let error = probe_source(&localhost, "tls:\n  mode: disable\n")
        .await
        .expect_err("plaintext must be rejected by a TLS-only server");
    assert!(error.contains("connect phase"), "{error}");

    // prefer / require → encrypted, no validation, succeed on self-signed.
    probe_source(&localhost, "tls:\n  mode: prefer\n")
        .await
        .expect("prefer connects (encrypted)");
    probe_source(&localhost, "tls:\n  mode: require\n")
        .await
        .expect("require connects without validating (libpq semantics)");

    // verify_ca: our CA passes (even via IP — hostname waived); the wrong
    // CA is a distinguished trust-anchor failure.
    probe_source(&ip, &tls_yaml("verify_ca", Some(&ca)))
        .await
        .expect("verify_ca with the right root (hostname waived)");
    let error = probe_source(&localhost, &tls_yaml("verify_ca", Some(&wrong_ca)))
        .await
        .expect_err("wrong trust anchor must fail");
    assert!(error.contains("TrustAnchor"), "distinguished: {error}");

    // verify_full: succeeds via the SAN'd hostname; the IP is a
    // distinguished hostname failure; missing root is a trust failure.
    probe_source(&localhost, &tls_yaml("verify_full", Some(&ca)))
        .await
        .expect("verify_full via matching hostname");
    let error = probe_source(&ip, &tls_yaml("verify_full", Some(&ca)))
        .await
        .expect_err("IP is not in the cert SAN");
    assert!(error.contains("Hostname"), "distinguished: {error}");
    let error = probe_source(&localhost, &tls_yaml("verify_full", Some(&wrong_ca)))
        .await
        .expect_err("unknown CA under verify_full");
    assert!(error.contains("TrustAnchor"), "distinguished: {error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn prefer_falls_back_on_plaintext_server_and_conn_sslmode_flows() {
    // A plain (non-TLS) postgres: prefer falls back to plaintext — the
    // libpq vocabulary's promise — and conn-string sslmode drives the
    // policy without any tls block.
    let Some(plain) = PostgresContainer::start().await else {
        return;
    };
    probe_source(&plain.connection_string, "")
        .await
        .expect("default prefer falls back to plaintext");
    probe_source(&format!("{} sslmode=disable", plain.connection_string), "")
        .await
        .expect("explicit disable on a plaintext server");
    let error = probe_source(&format!("{} sslmode=require", plain.connection_string), "")
        .await
        .expect_err("require against a server without TLS must fail");
    assert!(error.contains("connect phase"), "{error}");
}

#[tokio::test(flavor = "multi_thread")]
async fn destination_uses_the_same_policy_path() {
    use rdlt_connector_sdk::spi::core::{LoadId, PipelineId};
    use rdlt_connector_sdk::spi::{Destination as _, OpenContext};

    let Some(fixture) = TlsPostgresContainer::start().await else {
        return;
    };
    let pipeline = PipelineId::new("tls");
    let load = LoadId::new("tls-load");

    // verify_full + right root + matching hostname: the destination opens.
    let good = destination::Postgres::new(fixture.connection_string_via_localhost())
        .schema("tls_ok")
        .tls(Policy {
            mode: Mode::VerifyFull,
            root_cert: Some(PemSource(fixture.pki.ca_pem.clone())),
            ..Policy::default()
        })
        .into_shell();
    assert!(
        good.open(OpenContext::new(pipeline.clone(), load.clone()))
            .await
            .is_ok(),
        "destination over verify_full must open"
    );

    // Same policy, wrong trust anchor: typed failure through the SAME path.
    let bad = destination::Postgres::new(fixture.connection_string_via_localhost())
        .schema("tls_bad")
        .tls(Policy {
            mode: Mode::VerifyFull,
            root_cert: Some(PemSource(fixture.pki.wrong_ca_pem.clone())),
            ..Policy::default()
        })
        .into_shell();
    let error = match bad.open(OpenContext::new(pipeline, load)).await {
        Ok(_) => panic!("wrong CA must fail the destination identically"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("TrustAnchor"), "{error}");
}

#[test]
fn config_validation_matrix() {
    // Contradiction typed at validate; refinement allowed; bad roots typed.
    assert!(
        source::Config::from_yaml("conn: \"host=h sslmode=disable\"\ntls:\n  mode: require\n")
            .is_err()
    );
    assert!(
        source::Config::from_yaml("conn: \"host=h sslmode=require\"\ntls:\n  mode: verify_ca\n")
            .is_ok()
    );
}

// ---- mutual TLS (contract tls-client-auth.md) ----

fn pem_block(name: &str, pem: &str) -> String {
    let indented = pem.trim().replace('\n', "\n    ");
    format!("  {name}: |\n    {indented}\n")
}

fn mtls_yaml(mode: &str, root: &str, client: Option<(&str, &str)>) -> String {
    let mut yaml = format!("tls:\n  mode: {mode}\n");
    yaml.push_str(&pem_block("root_cert", root));
    if let Some((certificate, key)) = client {
        yaml.push_str(&pem_block("client_cert", certificate));
        yaml.push_str(&pem_block("client_key", key));
    }
    yaml
}

#[tokio::test(flavor = "multi_thread")]
async fn client_cert_matrix_against_cert_auth_server() {
    use rdlt_connector_sdk::spi::core::{LoadId, PipelineId};
    use rdlt_connector_sdk::spi::{Destination as _, OpenContext};

    let Some(fixture) = TlsPostgresContainer::start_cert_auth().await else {
        return;
    };
    let pki = &fixture.pki;
    let localhost = fixture.connection_string_via_localhost();

    // Valid credential: the SOURCE syncs…
    let good = mtls_yaml(
        "verify_full",
        &pki.ca_pem,
        Some((&pki.client_cert_pem, &pki.client_key_pem)),
    );
    probe_source(&localhost, &good)
        .await
        .expect("valid client cert + key must connect (source)");

    // …and the DESTINATION opens through the same path.
    let postgres_destination =
        destination::Postgres::new(fixture.connection_string_via_localhost())
            .schema("mtls_ok")
            .tls(Policy {
                mode: Mode::VerifyFull,
                root_cert: Some(PemSource(pki.ca_pem.clone())),
                client_cert: Some(PemSource(pki.client_cert_pem.clone())),
                client_key: Some(PemSource(pki.client_key_pem.clone())),
            })
            .into_shell();
    postgres_destination
        .open(OpenContext::new(
            PipelineId::new("mtls"),
            LoadId::new("mtls-load"),
        ))
        .await
        .expect("destination over mTLS must open");

    // No credential: the server demands one — distinguished ClientCert.
    let error = probe_source(&localhost, &mtls_yaml("verify_full", &pki.ca_pem, None))
        .await
        .expect_err("cert-auth server must reject a credential-less client");
    assert!(error.contains("ClientCert"), "distinguished: {error}");

    // Wrong-CA credential: same distinguished class, whichever layer the
    // server rejects at (TLS alert or auth-phase 28000).
    let wrong = mtls_yaml(
        "verify_full",
        &pki.ca_pem,
        Some((&pki.wrong_client_cert_pem, &pki.wrong_client_key_pem)),
    );
    let error = probe_source(&localhost, &wrong)
        .await
        .expect_err("wrong-CA client cert must be rejected");
    assert!(error.contains("ClientCert"), "distinguished: {error}");

    // Mismatched key: a CONFIG error before any connection.
    let mismatched = mtls_yaml(
        "verify_full",
        &pki.ca_pem,
        Some((&pki.client_cert_pem, &pki.wrong_client_key_pem)),
    );
    let error = probe_source(&localhost, &mismatched)
        .await
        .expect_err("mismatched cert/key must fail as config");
    assert!(
        error.contains("client credential"),
        "config-shaped: {error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn credential_offered_but_unused_still_syncs() {
    // C5: against a server that does NOT verify clients, carrying a
    // credential changes nothing.
    let Some(fixture) = TlsPostgresContainer::start().await else {
        return;
    };
    let yaml = mtls_yaml(
        "verify_full",
        &fixture.pki.ca_pem,
        Some((&fixture.pki.client_cert_pem, &fixture.pki.client_key_pem)),
    );
    probe_source(&fixture.connection_string_via_localhost(), &yaml)
        .await
        .expect("credential offered but unused must not break the sync");
}

// ---- conn-string portability (contract connstring-portability.md) ----

#[tokio::test(flavor = "multi_thread")]
async fn sslrootcert_url_syncs_and_application_name_is_set() {
    use rdlt_connector_sdk::spi::core::{LoadId, PipelineId};
    use rdlt_connector_sdk::spi::{Destination as _, OpenContext};

    let Some(fixture) = TlsPostgresContainer::start().await else {
        return;
    };
    // Write the CA where a real deployment would have it: on disk.
    let directory = tempfile::tempdir().expect("tempdir");
    let ca_path = directory.path().join("ca.pem");
    std::fs::write(&ca_path, &fixture.pki.ca_pem).expect("write ca");

    // A production-shaped libpq URL — verify-full + sslrootcert, NO tls block.
    let url = format!(
        "postgresql://postgres:postgres@localhost:{}/postgres?sslmode=verify-full&sslrootcert={}",
        fixture.port,
        ca_path.display()
    );

    // SOURCE: reflect over the URL (connects verified), then check that the
    // live session carries application_name=rdlt.
    use rdlt_connector_sdk::spi::Source as _;
    let postgres_source = source::Shell::from_yaml(&format!("conn: \"{url}\"\n")).expect("config");
    postgres_source
        .streams()
        .await
        .expect("sslrootcert URL syncs (source)");
    let (client, connection) = tokio_postgres::connect(
        &format!(
            "host=localhost port={} user=postgres password=postgres dbname=postgres sslmode=require",
            fixture.port
        ),
        {
            let mut roots = rustls::RootCertStore::empty();
            let mut reader = std::io::Cursor::new(fixture.pki.ca_pem.clone().into_bytes());
            for certificate in rustls_pemfile::certs(&mut reader) {
                roots.add(certificate.expect("ca cert")).expect("add ca");
            }
            tokio_postgres_rustls::MakeRustlsConnect::new(
                rustls::ClientConfig::builder_with_provider(
                    rustls::crypto::ring::default_provider().into(),
                )
                .with_safe_default_protocol_versions()
                .expect("protocol versions")
                .with_root_certificates(roots)
                .with_no_client_auth(),
            )
        },
    )
    .await
    .expect("probe connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    // Hold a source connection open while probing: reflect() connections are
    // short-lived, so probe our OWN default instead — a second rdlt-path
    // connection via the same gate.
    let held = {
        let parsed = session::parse(&url, None).expect("gate");
        session::connect(&parsed)
            .await
            .expect("held rdlt connection")
    };
    let names: Vec<String> = client
        .query(
            "SELECT DISTINCT application_name FROM pg_stat_activity WHERE application_name = 'rdlt'",
            &[],
        )
        .await
        .expect("pg_stat_activity")
        .into_iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(names, vec!["rdlt"], "A1: rdlt identifies itself");
    drop(held);

    // DESTINATION: the same URL through the same gate.
    let postgres_destination = destination::Postgres::new(&url)
        .schema("url_ok")
        .into_shell();
    postgres_destination
        .open(OpenContext::new(
            PipelineId::new("url"),
            LoadId::new("url-load"),
        ))
        .await
        .expect("sslrootcert URL opens (destination)");
}

/// Review F3: EVERY connect-phase db error carries the real server message —
/// not just the cert-28000 shape. Unknown database is the everyday case.
#[tokio::test(flavor = "multi_thread")]
async fn common_connect_failures_carry_the_server_message() {
    let Some(plain) = PostgresContainer::start().await else {
        return;
    };
    let bad_database = plain
        .connection_string
        .clone()
        .replace("dbname=postgres", "dbname=doesnotexist");
    let error = probe_source(&bad_database, "")
        .await
        .expect_err("unknown database must fail");
    assert!(
        error.contains("doesnotexist") && error.contains("SQLSTATE"),
        "server message + SQLSTATE surfaced, not bare 'db error': {error}"
    );
}
