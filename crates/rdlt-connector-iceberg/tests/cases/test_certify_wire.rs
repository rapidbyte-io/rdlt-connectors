//! THE CERTIFICATION CELL (042 Task 7): iceberg certified over the
//! wire against the live Polaris/RUSTFS fixture. The REAL
//! `rdlt-connector-iceberg` bin is spawned by path, and the certify
//! library judges it: the role-generic protocol clauses (P1–P4), the
//! wire clauses on a raw handshake below the adapters (P3/P7), the
//! testkit's D-clauses reused against the managed adapter (D1–D6, with
//! D8 an honest SKIP — this destination declares `merge = false`, so
//! the merge clause never ran and a Pass would be minted, the file
//! destination's posture exactly), and the session clauses
//! P8/P9/P10/P11/P12 on raw dials of the live socket.
//!
//! Skip-not-fail: without a container runtime the fixture announces
//! the skip and the cell returns — the 015 convention every live
//! iceberg cell rides.

use rdlt_certify::{
    clause::d::NO_MERGE_SKIP, clause::d::certify as certify_destination,
    report::assert_all_pass as assert_certified_all_pass_with_named_skips, target::Target,
};

use super::common::{CatalogFixture, LiveProbe};
use super::support::spawn::built_bin;

/// THE DESTINATION CELL: the built iceberg bin certifies over the wire
/// against the live catalog — every clause a destination can face,
/// asserted present: D1–D6 live, D8 the honest merge-false Skip, and
/// ALL TEN protocol clauses P1/P2/P3/P4/P7/P8/P9/P10/P11/P12.
#[tokio::test(flavor = "multi_thread")]
async fn the_iceberg_destination_certifies_over_the_wire() {
    let Some(fixture) = CatalogFixture::start().await else {
        return;
    };
    let namespace = "certify_wire";
    let config = fixture.doc(namespace);
    let probe = LiveProbe {
        fixture,
        namespace: namespace.into(),
    };

    let report =
        certify_destination(&Target::resolve_path(built_bin(), config), Some(&probe)).await;

    assert_certified_all_pass_with_named_skips(
        &report,
        &[
            "D1", "D2", "D3", "D4", "D5", "D6", "P1", "P2", "P3", "P4", "P7", "P8", "P9", "P10",
            "P11", "P12",
        ],
        &[("D8", NO_MERGE_SKIP)],
    );
}
