//! The destination connector: config in, [`Load`] sessions out.

use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use rdlt_connector_sdk::destination::DestinationConnector;
use rdlt_connector_sdk::spi::core::schema::IdentRules;
use rdlt_connector_sdk::spi::{
    destination::Capabilities as DestinationCapabilities, destination::OpenContext,
    error::DestinationError,
};

use super::config::{Config, ConfigError, config_schema};
use super::layout::scope_of;
use super::load::Load;
use super::stage::writer_props;
use crate::location::Location;

/// A process-lifetime nonce, advanced once per minted owner token — see
/// [`mint_owner`].
static OWNER_NONCE: AtomicU64 = AtomicU64::new(0);

/// Mint a fresh session-lease owner token: `{pid}-{nonce:x}` (037 US2
/// T7 — the ledger's mandatory rule). PROCESS-UNIQUE and nothing else:
/// never derived from `config`, the pipeline name, the output path, or
/// the host, because any of those would make every `File` instance
/// opened against the SAME config mint the SAME token, and the lease's
/// "same owner reacquires" branch would then treat every concurrent
/// `rdlt run` of one pipeline as this session's own retry — silently
/// defeating the whole story (S6). The pid separates processes; the
/// nonce separates instances minted within one process (each `assemble`
/// call advances it), so two `File`s built back-to-back in one test or
/// one embedder never collide. Deliberately NOT stable across a process
/// restart — a genuine new process is a genuinely new session, and the
/// TTL-based takeover path (`lease.rs`) is what lets it eventually
/// claim an abandoned lease, never a coincidental identity match.
fn mint_owner() -> String {
    let nonce = OWNER_NONCE.fetch_add(1, Ordering::Relaxed);
    format!("{}-{nonce:x}", std::process::id())
}

/// The LOCAL-protocol crash points the ENGINE's sweep drives against
/// [`super::ParquetDir`] — exported so the sweep iterates exactly this
/// list; a point in the code but not the sweep is a protocol edge
/// nobody ever crashes at. These spellings are frozen.
pub const FAIL_POINTS: &[&str] = &[
    "pq.replace.truncate",
    "pq.manifest.write",
    "pq.staged.sync",
    "pq.part.rename",
    "pq.dir.fsync",
    "pq.state.write",
    "pq.receipt.write",
];

/// The S3-protocol crash points — they cannot fire on a local store,
/// so the crate's own container-gated sweep owns them.
pub const S3_FAIL_POINTS: &[&str] = &[
    "file.stage.put",
    "file.finalize.copy",
    "file.finalize.delete",
];

/// The session lease's crash points (037 US2 T6) — armed in `lease.rs`
/// on BOTH storage arms (`Lease::acquire`'s create/CAS,
/// `Lease::release`'s delete), unlike the two lists above which each
/// belong to one arm only. Declared as its own registry rather than
/// folded into either: `FAIL_POINTS`'s doc claims local-protocol-only
/// and `S3_FAIL_POINTS`'s claims S3-only, and a lease point would make
/// either claim false. Not yet driven by a sweep — that is T7/T8's
/// job — but the registry-vs-sources scanner
/// (`tests/cases/test_gating.rs`) is ungated and runs on every
/// `cargo nextest run`, and it asserts an armed name is ALWAYS
/// declared somewhere in the union it is given, so this had to exist
/// before `lease.rs` could compile clean through that gate.
pub const LEASE_FAIL_POINTS: &[&str] = &["file.lease.acquire", "file.lease.release"];

/// The file destination.
#[derive(Debug, Clone)]
pub struct File {
    config: Config,
    /// This instance's session-lease identity (037 US2 T7) — minted
    /// ONCE in [`DestinationConnector::assemble`] and stable for the
    /// lifetime of this `File` value, including across every `connect`
    /// call it serves (the engine's own retry of a failed attempt
    /// reopening the SAME connector instance reacquires its own lease
    /// rather than being refused as a foreign session — see
    /// `lease.rs`'s module doc, step 3). `Clone` copies the token
    /// verbatim, which is deliberate: a cloned `File` still IS this
    /// same session's identity, never a new one.
    owner: String,
}

#[async_trait]
impl DestinationConnector for File {
    // Reverse-DNS, not bare `file` — the same id the source half
    // reports, for the same reason: NAME is what the wire handshake
    // reports and the client strictly verifies, and D-039-1 derives the
    // binary name from its last segment (see `source::connector`).
    const NAME: &'static str = "io.rapidbyte.file";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");

    type Config = Config;
    type Backend = Load;

    fn assemble(config: Config) -> Result<Self, ConfigError> {
        Ok(Self {
            config,
            owner: mint_owner(),
        })
    }

    fn config_schema() -> Option<serde_json::Value> {
        Some(config_schema())
    }

    fn capabilities(&self) -> DestinationCapabilities {
        // Files have no key semantics, so merge is honestly false;
        // structs and scalar lists serialize natively in both formats;
        // Json lands as text, so json_type is honestly false.
        DestinationCapabilities::default()
            .with_merge(false)
            .with_structs(true)
            .with_scalar_lists(true)
            .with_json_type(false)
            .with_decimal(true)
            // The publish manifest sweep converges under the ORIGINAL
            // load id (layout.rs's N2-cousin of iceberg's 029 N2): a
            // no-workdir mid-publish transient restart mints a fresh
            // load id, the receipt log's (load_id, seq) dedup misses,
            // and the retry re-publishes rows the crashed attempt
            // already committed (037 US3).
            .with_requires_durable_identity(true)
            .with_ident_rules(IdentRules::default())
    }

    async fn connect(&self, context: &OpenContext) -> Result<Load, DestinationError> {
        // Writer properties resolve FIRST: the translation is pure and
        // can fail (the parquet library bounds level ranges the config
        // gate cannot see), and a refusal must not leave a freshly
        // created output directory behind as its only trace.
        let props = writer_props(&self.config.parquet_options())?;
        let location = Location::for_dest(&self.config.path, self.config.location.as_ref())?;
        Load::open(
            location,
            super::load::WriterWiring {
                format: self.config.format,
                partition_by: self.config.partition_by.clone(),
                props,
            },
            scope_of(context.pipeline.as_str()),
            context.load_id.clone(),
            super::load::PartsWiring {
                options: self.config.part_options(),
                events: context.part_events.clone(),
            },
            super::load::LeaseWiring {
                pipeline: context.pipeline.as_str().to_owned(),
                owner: self.owner.clone(),
            },
        )
        .await
    }
}

/// Seams the tests need and nothing else may use. Not a public API.
#[doc(hidden)]
pub mod testhook {
    use rdlt_connector_sdk::spi::error::DestinationError;

    use crate::location::Location;

    /// Count rows over the ownership listing, both protocols.
    pub async fn count_rows_async(
        config: &super::super::Config,
        table: &str,
    ) -> Result<u64, DestinationError> {
        let location = Location::for_dest(&config.path, config.location.as_ref())?;
        super::super::inspect::count_rows_async(&location, table).await
    }

    /// The synchronous local-only form with its frozen refusal.
    pub fn count_rows(config: &super::super::Config, table: &str) -> Result<u64, DestinationError> {
        let location = Location::for_dest(&config.path, config.location.as_ref())?;
        super::super::inspect::count_rows(&location, table)
    }

    /// Drive the lease's conditional-doc verbs (`create_doc_exclusive`,
    /// `read_doc_versioned`, `replace_doc_if`, `delete_doc`) through a
    /// real `Location`, end to end. `CreateDoc`/`DocVersion` are
    /// private to `crate::location`, so the round-trip runs and
    /// asserts entirely inside `Location::probe_conditional_docs` —
    /// this wrapper only opens the location and forwards the plain
    /// `Result`. Used by the live S3 probe in `tests/cases/test_s3.rs`
    /// (037 US2 T5) to pin the verbs against a real store, not just
    /// the raw client the sibling probe drives directly.
    pub async fn probe_conditional_docs(
        config: &super::super::Config,
        name: &str,
    ) -> Result<(), DestinationError> {
        let location = Location::for_dest(&config.path, config.location.as_ref())?;
        location.probe_conditional_docs(name).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Both registries carry their frozen spellings — the engine's
    /// sweep binds to the first list, the crate's S3 sweep to the
    /// second.
    #[test]
    fn the_registries_are_the_frozen_spellings() {
        assert_eq!(
            FAIL_POINTS,
            &[
                "pq.replace.truncate",
                "pq.manifest.write",
                "pq.staged.sync",
                "pq.part.rename",
                "pq.dir.fsync",
                "pq.state.write",
                "pq.receipt.write",
            ]
        );
        assert_eq!(
            S3_FAIL_POINTS,
            &[
                "file.stage.put",
                "file.finalize.copy",
                "file.finalize.delete"
            ]
        );
        assert_eq!(
            LEASE_FAIL_POINTS,
            &["file.lease.acquire", "file.lease.release"]
        );
    }

    /// The capability declaration is the frozen truth the host plans
    /// from.
    #[test]
    fn capabilities_declare_the_frozen_truth() {
        let file = File::assemble(Config::new("out")).expect("assembles");
        let caps = file.capabilities();
        assert!(!caps.merge);
        assert!(caps.structs);
        assert!(caps.scalar_lists);
        assert!(!caps.json_type);
        assert!(caps.decimal);
        assert!(caps.requires_durable_identity);
    }

    /// 037 US2 T7's mandatory ledger rule, pinned directly: two `File`
    /// instances built from the SAME config — the shape two concurrent
    /// `rdlt run` invocations of one pipeline take — must mint DISTINCT
    /// session-lease owners. A config-derived token would make every
    /// such pair collide on the lease's "same owner reacquires" branch,
    /// silently defeating S6's whole point.
    #[test]
    fn two_connector_instances_mint_distinct_owners() {
        let config = Config::new("out");
        let a = File::assemble(config.clone()).expect("assembles");
        let b = File::assemble(config).expect("assembles");
        assert_ne!(a.owner, b.owner);
        // Also process-unique in shape: both carry this process's pid
        // as the token's prefix.
        let pid_prefix = format!("{}-", std::process::id());
        assert!(a.owner.starts_with(&pid_prefix), "{}", a.owner);
        assert!(b.owner.starts_with(&pid_prefix), "{}", b.owner);
    }
}
