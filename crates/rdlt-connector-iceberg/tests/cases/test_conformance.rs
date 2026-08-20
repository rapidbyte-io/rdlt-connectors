//! The sdk conformance kit over the live fixture — "certified = passes
//! conformance", through the same Shell every embedder gets. NEW over
//! generation 1, which predated the kit.

use rdlt_connector_iceberg::destination::Shell;
use rdlt_testkit::conformance::{assert_conformant, destination::verify as verify_destination};

use super::common::{CatalogFixture, LiveProbe};

#[tokio::test]
async fn the_destination_is_conformant_against_the_live_fixture() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "conf_v2";
    let shell = Shell::from_value(fixture.doc(namespace)).expect("valid");
    let probe = LiveProbe {
        fixture,
        namespace: namespace.into(),
    };
    assert_conformant(
        verify_destination(&shell, &probe)
            .await
            .expecting_no_skips(),
    );
}
