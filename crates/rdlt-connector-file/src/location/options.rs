//! Where files live: the Local | S3 location vocabulary, its
//! validation, and the ONE place an object-store client is built.
//!
//! `location` is a STRUCT of optional kinds, not an enum, so the YAML
//! stays the natural `location: {s3: {...}}` and future kinds (gcs:,
//! azure:) arrive as ADDITIVE fields behind exactly-one-set
//! validation. Absent entirely means the local filesystem.
//!
//! Secrets are revealed in the s3 module's `build_store` and nowhere
//! else — the one function a credential audit reads.

use rdlt_connector_sdk::spi::secret::Secret;

/// The storage-kind block. Absent = local filesystem.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct LocationOptions {
    /// S3-compatible object storage.
    #[serde(default)]
    pub s3: Option<S3Options>,
}

impl LocationOptions {
    /// The S3 form.
    pub fn s3(options: S3Options) -> Self {
        Self { s3: Some(options) }
    }

    /// Exactly one kind must be declared.
    pub fn validate(&self, context: &str) -> Result<(), String> {
        match &self.s3 {
            Some(s3) => s3.validate(context),
            None => Err(format!(
                "{context}: `location` block declares no storage kind (expected `s3`)"
            )),
        }
    }
}

/// S3-compatible object storage: `location: {s3: {...}}`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct S3Options {
    /// `http://127.0.0.1:9000`, `https://s3.amazonaws.com`, …
    pub endpoint: String,
    pub bucket: String,
    /// Defaults to `us-east-1` at build time — S3-compatible servers
    /// commonly ignore it.
    #[serde(default)]
    pub region: Option<String>,
    pub access_key: Secret,
    pub secret_key: Secret,
    /// Path-style addressing — the test-server form; AWS accepts it
    /// for most operations too.
    #[serde(default = "default_path_style")]
    pub path_style: bool,
    /// Send `UNSIGNED-PAYLOAD` instead of the body's SHA-256 in the
    /// SigV4 canonical request. Off by default and NEVER inferred —
    /// this is a security setting, not a tuning knob. Signed, the
    /// request signature covers the BODY, so nothing between client
    /// and store can alter the bytes unnoticed; unsigned, TLS is the
    /// only integrity the body has. The throughput on the table is
    /// real (hashing measured at ~6.7% of the parquet-to-S3 cell),
    /// which is exactly why the trade belongs to the person who knows
    /// the deployment: reasonable over trusted HTTPS, unreasonable
    /// over plain HTTP.
    #[serde(default)]
    pub unsigned_payload: bool,
}

fn default_path_style() -> bool {
    true
}

impl S3Options {
    /// The programmatic seam — documents are the primary path;
    /// fixtures and embedders build options directly.
    pub fn new(
        endpoint: impl Into<String>,
        bucket: impl Into<String>,
        access_key: impl Into<Secret>,
        secret_key: impl Into<Secret>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            bucket: bucket.into(),
            region: None,
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            path_style: true,
            unsigned_payload: false,
        }
    }

    /// Opt out of payload signing — read the field's doc first; this
    /// trades request-body integrity for throughput.
    #[must_use]
    pub fn with_unsigned_payload(mut self, unsigned: bool) -> Self {
        self.unsigned_payload = unsigned;
        self
    }

    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = Some(region.into());
        self
    }

    pub fn with_path_style(mut self, path_style: bool) -> Self {
        self.path_style = path_style;
        self
    }

    /// Everything a document can get wrong here, each refusal naming
    /// its field: a non-http(s) endpoint, an empty bucket, and the
    /// no-integrity unsigned-payload-over-plain-http combination.
    pub fn validate(&self, context: &str) -> Result<(), String> {
        let host = self
            .endpoint
            .strip_prefix("http://")
            .or_else(|| self.endpoint.strip_prefix("https://"));
        if host.is_none_or(|h| h.is_empty()) {
            return Err(format!(
                "{context}: location.s3.endpoint `{}` must be an http(s) URL",
                self.endpoint
            ));
        }
        if self.bucket.is_empty() {
            return Err(format!("{context}: location.s3.bucket must not be empty"));
        }
        // The one combination with NO integrity left: unsigned payload
        // removes the signature over the body, plain HTTP removes TLS —
        // together nothing checks the bytes (the field's own doc calls
        // the trade reasonable over trusted HTTPS only; 030 review made
        // the gate say so rather than trusting prose).
        if self.unsigned_payload && self.endpoint.starts_with("http://") {
            return Err(format!(
                "{context}: `unsigned_payload: true` over a plain-http endpoint leaves \
                 request bodies with no integrity check at all — use https, or keep \
                 payload signing on"
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The refusals, with their frozen spellings and the context frame
    /// the caller supplies.
    #[test]
    fn refusals_name_the_field_with_the_frozen_spelling() {
        let empty = LocationOptions { s3: None };
        assert_eq!(
            empty.validate("stream `a`").unwrap_err(),
            "stream `a`: `location` block declares no storage kind (expected `s3`)"
        );

        let bad_endpoint = S3Options::new("ftp://x", "b", "ak", "sk");
        assert_eq!(
            bad_endpoint.validate("stream `a`").unwrap_err(),
            "stream `a`: location.s3.endpoint `ftp://x` must be an http(s) URL"
        );
        let no_host = S3Options::new("http://", "b", "ak", "sk");
        assert!(no_host.validate("c").is_err(), "an empty host is not a URL");

        let empty_bucket = S3Options::new("http://s3", "", "ak", "sk");
        assert_eq!(
            empty_bucket.validate("file destination").unwrap_err(),
            "file destination: location.s3.bucket must not be empty"
        );
    }

    /// The defaults: path-style ON (the test-server form), payload
    /// signing ON (the security default), region deferred to build.
    #[test]
    fn the_defaults_are_the_safe_forms() {
        let options = S3Options::new("http://s3", "b", "ak", "sk");
        assert!(options.path_style);
        assert!(!options.unsigned_payload, "signing is the default");
        assert!(options.region.is_none());
        let parsed: S3Options = serde_yaml_ng::from_str(
            "{endpoint: 'http://s3', bucket: b, access_key: ak, secret_key: sk}",
        )
        .expect("parses");
        assert_eq!(parsed, options, "serde defaults equal the builder's");
    }

    /// Secrets never render.
    #[test]
    fn secrets_are_grep_proof() {
        let options = S3Options::new("http://s3", "b", "ak-123", "sk-456");
        let rendered = format!("{options:?}");
        assert!(!rendered.contains("ak-123") && !rendered.contains("sk-456"));
    }

    /// The no-integrity combination is refused at the gate, not left
    /// to the field's documentation (030 review, docket S8).
    #[test]
    fn unsigned_payload_over_plain_http_is_refused() {
        let options = S3Options::new("http://minio.local:9000", "raw", "ak", "sk")
            .with_unsigned_payload(true);
        let err = options.validate("stream `a`").expect_err("refused");
        assert!(
            err.contains("`unsigned_payload: true` over a plain-http endpoint"),
            "{err}"
        );
        let https =
            S3Options::new("https://s3.example", "raw", "ak", "sk").with_unsigned_payload(true);
        https
            .validate("stream `a`")
            .expect("https keeps the trade available");
    }
}
