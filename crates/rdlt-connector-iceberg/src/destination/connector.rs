//! The framework face: [`Iceberg`] implements the sdk's
//! `DestinationConnector`, and `connect` does everything that must
//! precede a session — the catalog handshake, the namespace, and the
//! writer properties a load must not discover are broken partway
//! through writing.

use async_trait::async_trait;
use iceberg::NamespaceIdent;
use rdlt_connector_sdk::destination::DestinationConnector;
use rdlt_connector_sdk::spi::core::naming::IdentRules;
use rdlt_connector_sdk::spi::{DestinationCapabilities, DestinationError, OpenContext};

use super::client::{self, fatal};
use super::config::{Config, ConfigError, config_schema};
use super::load::{Load, ensure_namespace};
use super::write::writer_properties;

/// The crash points this destination arms — exported so the sweep
/// iterates exactly this list; a point in the code but not the sweep
/// is a protocol edge nobody ever crashes at.
pub const FAIL_POINTS: &[&str] = &["ice.files.write", "ice.commit", "ice.receipt.visible"];

/// The Iceberg destination.
#[derive(Debug, Clone)]
pub struct Iceberg {
    config: Config,
}

#[async_trait]
impl DestinationConnector for Iceberg {
    // Reverse-DNS, not bare `iceberg` (042, the identity rule reaching
    // this crate): NAME is what the wire handshake reports and the
    // client strictly verifies (D-039-2), and D-039-1 derives the
    // binary name `rdlt-connector-iceberg` from its last segment.
    const NAME: &'static str = "io.rapidbyte.iceberg";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");

    type Config = Config;
    type Backend = Load;

    fn assemble(config: Config) -> Result<Self, ConfigError> {
        Ok(Self { config })
    }

    fn config_schema() -> Option<serde_json::Value> {
        Some(config_schema())
    }

    fn capabilities(&self) -> DestinationCapabilities {
        // Append-only lakehouse tables: merge stays SQL-side; structs
        // and scalar lists are native; Json maps to STRING (the
        // closed-table row), so json_type is honestly false.
        DestinationCapabilities::default()
            .with_merge(false)
            .with_structs(true)
            .with_scalar_lists(true)
            .with_json_type(false)
            .with_decimal(true)
            // Non-atomic multi-table publish (029 N2): a mid-publish
            // transient restart without a workdir mints a fresh load id
            // and re-appends rows the first attempt already committed.
            .with_requires_durable_identity(true)
            .with_ident_rules(IdentRules::default())
    }

    async fn connect(&self, context: &OpenContext) -> Result<Load, DestinationError> {
        // Resolved FIRST: the translation is pure and can fail (the
        // parquet library rejects level ranges the config gate cannot
        // see), and a refusal must not leave a freshly created
        // namespace behind as its only trace.
        let properties =
            writer_properties(&self.config.parquet.clone().unwrap_or_default()).map_err(fatal)?;
        let catalog = client::connect(&self.config).await?;
        let namespace = NamespaceIdent::from_vec(self.config.namespace_levels())
            .map_err(|e| fatal(format!("namespace `{}`: {e}", self.config.namespace)))?;
        ensure_namespace(&catalog, &namespace, self.config.create_namespace).await?;
        Ok(Load::new(
            self.config.clone(),
            catalog,
            namespace,
            &context.pipeline,
            context.load_id.clone(),
            properties,
            super::load::PartsWiring {
                options: self.config.parts.unwrap_or_default(),
                events: context.part_events.clone(),
            },
        ))
    }
}

/// Seams the live cells need and nothing else may use. Not a public
/// API.
#[doc(hidden)]
pub mod testhook {
    use iceberg::NamespaceIdent;
    use rdlt_connector_sdk::spi::DestinationError;

    use super::super::config::Config;
    use super::super::{client, state};

    /// The marker table's name, for cells asserting the reserved-name
    /// refusal and the state protocol against the live catalog.
    pub const STATE_TABLE: &str = state::STATE_TABLE;

    /// Read a pipeline scope's raw state JSON off the marker table.
    pub async fn read_raw_state(
        config: &Config,
        namespace: &[String],
        scope: &str,
    ) -> Result<Option<String>, DestinationError> {
        let catalog = client::connect(config).await?;
        let namespace = NamespaceIdent::from_vec(namespace.to_vec())
            .map_err(|e| DestinationError::fatal(format!("namespace: {e}")))?;
        state::read_state_doc(&catalog, &namespace, scope).await
    }

    /// The scope hash a pipeline name resolves to — cells assert
    /// snapshot identities without re-deriving the width.
    pub fn scope_of(pipeline: &str) -> String {
        rdlt_connector_sdk::spi::core::naming::ident_hash(
            pipeline,
            super::super::load::SCOPE_HASH_LEN,
        )
    }

    /// TEST-ONLY: remove a pipeline's state property from the marker
    /// table — simulating the `ice.receipt.visible` crash residue
    /// (every table's data committed, the state write never reached
    /// the catalog), so recovery cells can prove the replay path
    /// re-persists it.
    pub async fn remove_state(
        config: &Config,
        namespace: &[String],
        pipeline: &str,
    ) -> Result<(), DestinationError> {
        let catalog = client::connect(config).await?;
        let namespace = NamespaceIdent::from_vec(namespace.to_vec())
            .map_err(|e| DestinationError::fatal(format!("namespace: {e}")))?;
        state::remove_state(&catalog, &namespace, &scope_of(pipeline)).await
    }

    /// TEST-ONLY: remove one load's receipt property from the marker
    /// table — with [`remove_state`], the exact residue of a crash
    /// BETWEEN a table's `append_commit` and the receipt stamp (data
    /// committed under the dying attempt's scope, neither receipt nor
    /// state written), so recovery cells can prove a sibling-scope
    /// re-drive converges instead of duplicating.
    pub async fn remove_receipt(
        config: &Config,
        namespace: &[String],
        load_id: &str,
    ) -> Result<(), DestinationError> {
        let catalog = client::connect(config).await?;
        let namespace = NamespaceIdent::from_vec(namespace.to_vec())
            .map_err(|e| DestinationError::fatal(format!("namespace: {e}")))?;
        state::remove_receipt(&catalog, &namespace, load_id).await
    }

    /// TEST-ONLY: relocate a pipeline's state property from the
    /// current 32-hex scope key to the pre-037 12-hex legacy key, and
    /// remove it from the current key — simulating a warehouse whose
    /// only state predates the 037 scope-hash widen, so the
    /// legacy-key refusal gate (037 D1) has something real to refuse
    /// against.
    pub async fn move_state_to_legacy_key(
        config: &Config,
        namespace: &[String],
        pipeline: &str,
    ) -> Result<(), DestinationError> {
        let catalog = client::connect(config).await?;
        let namespace = NamespaceIdent::from_vec(namespace.to_vec())
            .map_err(|e| DestinationError::fatal(format!("namespace: {e}")))?;
        let scope = scope_of(pipeline);
        let legacy_scope = rdlt_connector_sdk::spi::core::naming::ident_hash(
            pipeline,
            super::super::state::LEGACY_SCOPE_HASH_LEN,
        );
        let raw = state::read_state_doc(&catalog, &namespace, &scope)
            .await?
            .ok_or_else(|| {
                DestinationError::fatal(format!(
                    "no state at the current scope key for pipeline `{pipeline}` — nothing to \
                     relocate to the legacy key"
                ))
            })?;
        state::write_state(&catalog, &namespace, &legacy_scope, raw, None).await?;
        state::remove_state(&catalog, &namespace, &scope).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The capability sheet is the honest one: no merge, no json type,
    /// structs and lists native.
    #[test]
    fn the_capability_sheet_matches_what_the_code_delivers() {
        use rdlt_connector_sdk::config::Document;
        let config = Config::from_value(serde_json::json!({
            "catalog": {
                "uri": "http://c/api/catalog",
                "warehouse": "wh",
                "auth": {"bearer": {"token": "t"}},
            },
            "namespace": "raw",
        }))
        .expect("valid");
        let dest = Iceberg::assemble(config).expect("assembles");
        let caps = dest.capabilities();
        assert!(!caps.merge);
        assert!(caps.structs && caps.scalar_lists);
        assert!(!caps.json_type);
        assert!(caps.decimal);
        assert!(caps.requires_durable_identity);
    }

    /// The registry is the three frozen ids, macro-armed in their
    /// modules — the sweep iterates exactly this list.
    #[test]
    fn the_fail_point_registry_is_the_frozen_three() {
        assert_eq!(
            FAIL_POINTS,
            &["ice.files.write", "ice.commit", "ice.receipt.visible"]
        );
    }
}
