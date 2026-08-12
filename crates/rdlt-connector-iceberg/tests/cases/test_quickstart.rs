//! The README quickstart, kept in step with the vocabulary: the exact
//! block the README shows must parse through the Shell — drift in
//! either direction fails here, not in a user's first five minutes.

use rdlt_connector_iceberg::destination::Shell;

const QUICKSTART: &str = r#"catalog:
  uri: https://polaris.example.com/api/catalog
  warehouse: analytics
  auth:
    oauth2_client_credentials:
      client_id: loader
      client_secret: "…"
      scopes: [PRINCIPAL_ROLE:ALL]
namespace: raw.events
create_namespace: true
tables:
  events:
    partition_by:
      - {column: ts, transform: day}
"#;

#[test]
fn the_readme_quickstart_parses_and_is_the_readme_verbatim() {
    Shell::from_yaml(QUICKSTART).expect("the quickstart is a valid document");
    let readme =
        std::fs::read_to_string(std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("README.md"))
            .expect("README present");
    assert!(
        readme.contains(QUICKSTART.trim_end()),
        "the README's quickstart block drifted from the pinned one"
    );
}
