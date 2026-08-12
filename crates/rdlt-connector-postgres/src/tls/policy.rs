//! The config-shaped TLS vocabulary: the policy type, its mode ladder, the
//! config-error enum, and the resolution rules that reconcile a connection
//! string's `sslmode` with an explicit `tls:` block. No network and no
//! rustls here — only the posture and its rules.

use serde::{Deserialize, Serialize};
use tokio_postgres::config::SslMode;

/// A PEM input — trust root, client certificate, or client key. The shared
/// SPI type: one statement of the path-or-inline rule for every connector
/// that takes PEM material.
pub use rdlt_connector_sdk::spi::PemSource;

/// How strictly a connection encrypts and verifies, on libpq's ladder.
///
/// Serde spellings (`snake_case`) are the frozen YAML vocabulary.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// Plaintext only.
    Disable,
    /// Opportunistic: encrypted when the server offers TLS, plaintext
    /// otherwise. Never validates — it exists for opportunistic encryption.
    #[default]
    Prefer,
    /// Encrypted, certificate NOT validated (libpq semantics — use
    /// `verify_full` in production).
    Require,
    /// Encrypted + certificate chain verified; hostname NOT checked.
    VerifyCa,
    /// Encrypted + chain + hostname — the production recommendation.
    VerifyFull,
}

impl Mode {
    pub(crate) fn wants_encryption(self) -> bool {
        !matches!(self, Mode::Disable)
    }

    /// Position on the ladder, for the never-weaken rule: an explicit
    /// connection-string mode may be kept or strengthened by a `tls:` block,
    /// never silently weakened.
    pub(crate) fn strength(self) -> u8 {
        match self {
            Mode::Disable => 0,
            Mode::Prefer => 1,
            Mode::Require => 2,
            Mode::VerifyCa => 3,
            Mode::VerifyFull => 4,
        }
    }
}

/// The per-connection TLS posture shared by source and destination.
///
/// Field spellings are the frozen YAML vocabulary.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    #[serde(default)]
    pub mode: Mode,
    #[serde(default)]
    pub root_cert: Option<PemSource>,
    /// Client certificate for mutual TLS. Path or inline PEM; requires
    /// `client_key`.
    #[serde(default)]
    pub client_cert: Option<PemSource>,
    /// Private key matching `client_cert` (PKCS#8/RSA/SEC1, unencrypted).
    #[serde(default)]
    pub client_key: Option<PemSource>,
}

/// Config-shaped TLS failures — everything decidable before a connection.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error(
        "tls.mode `{override_mode:?}` contradicts the conn string's sslmode \
         `{connection_sslmode}` — silently out-ranking an explicit sslmode is \
         how plaintext surprises happen; align them or drop one"
    )]
    Contradiction {
        connection_sslmode: &'static str,
        override_mode: Mode,
    },
    /// TLS setup failure naming its subject — a PEM input (`root_cert
    /// `path``), the crypto provider, or verifier construction. One variant
    /// because every arm is "the TLS stack could not be built from this
    /// input".
    #[error("tls {subject}: {detail}")]
    Setup { subject: String, detail: String },
    #[error(
        "tls.mode `{0:?}` verifies certificates but no trust root resolved \
         (no tls.root_cert and the platform trust store is empty/unavailable)"
    )]
    NoRoots(Mode),
    #[error("tls client credential `{input}`: {detail}")]
    ClientCredential { input: String, detail: String },
    #[error(
        "conn parameter `{parameter}={connection_value}` conflicts with the \
         tls block's {policy_field} `{override_value}` — align them or drop one"
    )]
    ParameterConflict {
        parameter: &'static str,
        policy_field: &'static str,
        connection_value: String,
        override_value: String,
    },
    #[error("unsupported connection parameter `{parameter}`: {hint}")]
    UnsupportedParameter { parameter: String, hint: String },
    #[error("conn string does not parse: {0}")]
    Syntax(String),
}

/// Resolve the effective policy from the parsed connection string and the
/// optional `tls:` block. The block wins ONLY when consistent: an explicit
/// connection-string `sslmode` may be refined (require → verify_*) but never
/// silently reversed.
pub(crate) fn resolve(
    driver: &tokio_postgres::Config,
    tls_override: Option<&Policy>,
) -> Result<Policy, ConfigError> {
    let connection_sslmode = driver.get_ssl_mode();
    let Some(tls_override) = tls_override else {
        return Ok(Policy {
            mode: match connection_sslmode {
                SslMode::Disable => Mode::Disable,
                SslMode::Require => Mode::Require,
                _ => Mode::Prefer,
            },
            ..Policy::default()
        });
    };
    let contradiction = match connection_sslmode {
        // Explicit plaintext vs a block DEMANDING encryption. `prefer`
        // tolerates plaintext by its own semantics: a block whose mode
        // defaulted to prefer must compose with disable.
        SslMode::Disable => matches!(
            tls_override.mode,
            Mode::Require | Mode::VerifyCa | Mode::VerifyFull
        ),
        // Explicit encryption vs a block demanding plaintext.
        SslMode::Require => tls_override.mode == Mode::Disable,
        // Prefer (the unsignaled default) composes with anything.
        _ => false,
    };
    if contradiction {
        return Err(ConfigError::Contradiction {
            connection_sslmode: match connection_sslmode {
                SslMode::Disable => "disable",
                SslMode::Require => "require",
                _ => "prefer",
            },
            override_mode: tls_override.mode,
        });
    }
    Ok(tls_override.clone())
}

/// Client-credential shape rules, enforced BEFORE any connection:
/// both-or-neither, and never with plaintext.
pub(crate) fn validate_credentials(policy: &Policy) -> Result<(), ConfigError> {
    match (&policy.client_cert, &policy.client_key) {
        (Some(_), None) => Err(ConfigError::ClientCredential {
            input: "client_cert".into(),
            detail: "client_key is missing — a certificate cannot authenticate without \
                     its private key"
                .into(),
        }),
        (None, Some(_)) => Err(ConfigError::ClientCredential {
            input: "client_key".into(),
            detail: "client_cert is missing — a private key alone is not a credential".into(),
        }),
        (Some(_), Some(_)) if policy.mode == Mode::Disable => Err(ConfigError::ClientCredential {
            input: "client_cert".into(),
            detail: "tls.mode is `disable` — a client certificate cannot be presented \
                     over plaintext; enable TLS or drop the credential"
                .into(),
        }),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn driver_config(connection_string: &str) -> tokio_postgres::Config {
        connection_string.parse().expect("conn string parses")
    }

    #[test]
    fn resolution_maps_sslmode_and_refuses_contradictions() {
        // Without a block, the driver's sslmode maps straight through.
        assert_eq!(
            resolve(&driver_config("host=h sslmode=require"), None)
                .unwrap()
                .mode,
            Mode::Require
        );
        assert_eq!(
            resolve(&driver_config("host=h"), None).unwrap().mode,
            Mode::Prefer
        );
        // A block may refine require → verify_full.
        let verify_full = Policy {
            mode: Mode::VerifyFull,
            ..Policy::default()
        };
        assert_eq!(
            resolve(&driver_config("host=h sslmode=require"), Some(&verify_full))
                .unwrap()
                .mode,
            Mode::VerifyFull
        );
        // Contradictions both ways: disable vs encryption, require vs disable.
        assert!(resolve(&driver_config("host=h sslmode=disable"), Some(&verify_full)).is_err());
        let disable = Policy {
            mode: Mode::Disable,
            ..Policy::default()
        };
        assert!(resolve(&driver_config("host=h sslmode=require"), Some(&disable)).is_err());
        // Prefer composes with anything.
        assert!(resolve(&driver_config("host=h sslmode=prefer"), Some(&verify_full)).is_ok());
        // A block whose mode is prefer (the DEFAULT — e.g. a block that only
        // sets root_cert) tolerates plaintext by its own semantics and must
        // compose with conn sslmode=disable.
        let root_only = Policy {
            root_cert: Some(PemSource("/some/ca.pem".into())),
            ..Policy::default()
        };
        let resolved = resolve(&driver_config("host=h sslmode=disable"), Some(&root_only))
            .expect("a prefer-mode block composes with disable");
        assert_eq!(resolved.mode, Mode::Prefer);
    }

    #[test]
    fn credential_shape_rules_are_typed_and_early() {
        let certificate = PemSource("cert".into());
        let key = PemSource("key".into());
        let policy = |mode, cert: Option<&PemSource>, key: Option<&PemSource>| Policy {
            mode,
            root_cert: None,
            client_cert: cert.cloned(),
            client_key: key.cloned(),
        };
        // Certificate without key, and key without certificate: each error
        // names the missing counterpart.
        let err =
            validate_credentials(&policy(Mode::Require, Some(&certificate), None)).unwrap_err();
        assert!(err.to_string().contains("client_key is missing"), "{err}");
        let err = validate_credentials(&policy(Mode::Require, None, Some(&key))).unwrap_err();
        assert!(err.to_string().contains("client_cert is missing"), "{err}");
        // A credential over plaintext is a contradiction.
        let err = validate_credentials(&policy(Mode::Disable, Some(&certificate), Some(&key)))
            .unwrap_err();
        assert!(err.to_string().contains("disable"), "{err}");
        // A complete pair with TLS active is a valid shape.
        validate_credentials(&policy(Mode::Require, Some(&certificate), Some(&key)))
            .expect("valid shape");
    }

    #[test]
    fn strength_orders_the_whole_ladder() {
        let ladder = [
            Mode::Disable,
            Mode::Prefer,
            Mode::Require,
            Mode::VerifyCa,
            Mode::VerifyFull,
        ];
        for pair in ladder.windows(2) {
            assert!(pair[0].strength() < pair[1].strength(), "{pair:?}");
        }
    }
}
