//! Turning a resolved [`Policy`] into a rustls `ClientConfig`: trust-store
//! loading, client-credential loading, and the per-mode verifier wiring.
//! Every material failure is a typed [`ConfigError`] naming the offending
//! input, and construction happens BEFORE any connection so a mismatched
//! certificate/key pair fails as a config error, not mid-handshake.

use std::sync::Arc;

use rustls::RootCertStore;

use super::policy::{ConfigError, Mode, Policy, validate_credentials};
use super::verify::{AcceptAnyCertificate, ChainOnly, provider};
use rdlt_connector_sdk::pem::Material;

/// Resolve PEM material to bytes plus a label safe to put in an error — the
/// label must never be the material itself, so an inline key cannot reach a
/// log line through a failure path.
fn pem_bytes(source: &Material, description: &str) -> Result<(String, Vec<u8>), ConfigError> {
    // The label is what an error may quote, so it is the source's own
    // describe-rule: a path names itself, inline material never does.
    // The description narrows the inline case to the credential it is.
    let label = if source.is_inline() {
        format!("<inline {description} pem>")
    } else {
        source.describe()
    };
    let bytes = source.read().map_err(|e| ConfigError::ClientCredential {
        input: label.clone(),
        detail: format!("unreadable: {e}"),
    })?;
    Ok((label, bytes))
}

/// Load the client credential when configured: certificate chain + private
/// key, with typed errors naming the offending input.
fn client_credential(
    policy: &Policy,
) -> Result<
    Option<(
        Vec<rustls::pki_types::CertificateDer<'static>>,
        rustls::pki_types::PrivateKeyDer<'static>,
    )>,
    ConfigError,
> {
    validate_credentials(policy)?;
    let (Some(certificate), Some(key)) = (&policy.client_cert, &policy.client_key) else {
        return Ok(None);
    };
    let (certificate_label, certificate_bytes) = pem_bytes(certificate, "client cert")?;
    let mut reader = std::io::Cursor::new(certificate_bytes);
    let chain: Vec<_> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<_, _>>()
        .map_err(|e| ConfigError::ClientCredential {
            input: certificate_label.clone(),
            detail: format!("PEM parse error: {e}"),
        })?;
    if chain.is_empty() {
        return Err(ConfigError::ClientCredential {
            input: certificate_label,
            detail: "no certificates found in PEM input".into(),
        });
    }
    let (key_label, key_bytes) = pem_bytes(key, "client key")?;
    let mut reader = std::io::Cursor::new(key_bytes);
    let key = rustls_pemfile::private_key(&mut reader)
        .map_err(|e| ConfigError::ClientCredential {
            input: key_label.clone(),
            detail: format!("PEM parse error: {e}"),
        })?
        .ok_or_else(|| ConfigError::ClientCredential {
            input: key_label,
            detail: "no private key found in PEM input (encrypted keys are \
                     unsupported — provide an unencrypted PKCS#8/RSA/SEC1 key)"
                .into(),
        })?;
    Ok(Some((chain, key)))
}

/// Load the trust store for a verifying mode: the configured root when given,
/// else the platform store. Typed errors name what failed.
fn root_store(policy: &Policy) -> Result<RootCertStore, ConfigError> {
    let mut store = RootCertStore::empty();
    match &policy.root_cert {
        Some(source) => {
            let label = source.describe();
            let bytes = source.read().map_err(|e| ConfigError::Setup {
                subject: format!("root_cert `{label}`"),
                detail: format!("unreadable: {e}"),
            })?;
            let mut reader = std::io::Cursor::new(bytes);
            let mut added = 0usize;
            for item in rustls_pemfile::certs(&mut reader) {
                let certificate = item.map_err(|e| ConfigError::Setup {
                    subject: format!("root_cert `{label}`"),
                    detail: format!("PEM parse error: {e}"),
                })?;
                store.add(certificate).map_err(|e| ConfigError::Setup {
                    subject: format!("root_cert `{label}`"),
                    detail: format!("not a usable CA certificate: {e}"),
                })?;
                added += 1;
            }
            if added == 0 {
                return Err(ConfigError::Setup {
                    subject: format!("root_cert `{label}`"),
                    detail: "no certificates found in PEM input".into(),
                });
            }
        }
        None => {
            let native = rustls_native_certs::load_native_certs();
            for certificate in native.certs {
                // Tolerate individual oddities in the platform store; an
                // empty result is the failure that matters.
                let _ = store.add(certificate);
            }
            if store.is_empty() {
                return Err(ConfigError::NoRoots(policy.mode));
            }
        }
    }
    Ok(store)
}

fn builder()
-> Result<rustls::ConfigBuilder<rustls::ClientConfig, rustls::WantsVerifier>, ConfigError> {
    rustls::ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|e| ConfigError::Setup {
            subject: "crypto provider".into(),
            detail: e.to_string(),
        })
}

/// Build the rustls `ClientConfig` for a policy — `None` for plaintext
/// (`disable`). A mismatched certificate/key pair fails HERE (rustls checks
/// consistency at config construction), a config error before any
/// connection.
pub(crate) fn build(policy: &Policy) -> Result<Option<rustls::ClientConfig>, ConfigError> {
    let credential = client_credential(policy)?;
    let with_credential = move |builder: rustls::ConfigBuilder<
        rustls::ClientConfig,
        rustls::client::WantsClientCert,
    >|
          -> Result<rustls::ClientConfig, ConfigError> {
        match credential {
            Some((chain, key)) => builder.with_client_auth_cert(chain, key).map_err(|e| {
                ConfigError::ClientCredential {
                    input: "client_cert/client_key".into(),
                    detail: format!("rejected by TLS stack (mismatched pair?): {e}"),
                }
            }),
            None => Ok(builder.with_no_client_auth()),
        }
    };
    let config = match policy.mode {
        Mode::Disable => return Ok(None),
        Mode::Prefer | Mode::Require => with_credential(
            builder()?
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(AcceptAnyCertificate::new())),
        )?,
        Mode::VerifyCa => {
            let store = root_store(policy)?;
            with_credential(
                builder()?
                    .dangerous()
                    .with_custom_certificate_verifier(Arc::new(ChainOnly::new(store).map_err(
                        |e| ConfigError::Setup {
                            subject: "trust store".into(),
                            detail: format!("building verifier: {e}"),
                        },
                    )?)),
            )?
        }
        Mode::VerifyFull => {
            let store = root_store(policy)?;
            with_credential(builder()?.with_root_certificates(store))?
        }
    };
    Ok(Some(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn self_signed_pair() -> (String, String) {
        let key = rcgen::KeyPair::generate().expect("key generates");
        let certificate = rcgen::CertificateParams::new(Vec::<String>::new())
            .expect("params build")
            .self_signed(&key)
            .expect("certificate signs");
        (certificate.pem(), key.serialize_pem())
    }

    fn policy(mode: Mode, certificate: Option<&str>, key: Option<&str>) -> Policy {
        Policy {
            mode,
            root_cert: None,
            client_cert: certificate.map(Material::new),
            client_key: key.map(Material::new),
        }
    }

    #[test]
    fn root_errors_are_typed_and_name_the_input() {
        let missing = Policy {
            mode: Mode::VerifyFull,
            root_cert: Some(Material::new("/nonexistent/ca.pem")),
            ..Policy::default()
        };
        let err = root_store(&missing).unwrap_err();
        assert!(err.to_string().contains("/nonexistent/ca.pem"), "{err}");

        // Inline garbage: the label says "inline", never the material.
        let garbage = Policy {
            mode: Mode::VerifyFull,
            root_cert: Some(Material::new("-----BEGIN CERTIFICATE-----\ngarbage")),
            ..Policy::default()
        };
        let message = root_store(&garbage).unwrap_err().to_string();
        assert!(message.contains("inline"), "{message}");
    }

    #[test]
    fn root_loads_inline_and_from_path() {
        let authority = rcgen::CertificateParams::new(Vec::<String>::new())
            .expect("params build")
            .self_signed(&rcgen::KeyPair::generate().expect("key generates"))
            .expect("authority signs");
        let pem = authority.pem();
        let inline = Policy {
            mode: Mode::VerifyFull,
            root_cert: Some(Material::new(pem.clone())),
            ..Policy::default()
        };
        assert_eq!(root_store(&inline).expect("inline loads").len(), 1);

        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("ca.pem");
        std::fs::write(&path, pem).expect("write pem");
        let from_path = Policy {
            mode: Mode::VerifyCa,
            root_cert: Some(Material::new(path.display().to_string())),
            ..Policy::default()
        };
        assert_eq!(root_store(&from_path).expect("path loads").len(), 1);
    }

    #[test]
    fn complete_credential_pair_builds_config() {
        let (certificate, key) = self_signed_pair();
        let complete = policy(Mode::Require, Some(&certificate), Some(&key));
        assert!(build(&complete).expect("config builds").is_some());
    }

    #[test]
    fn credential_material_errors_name_the_input() {
        let (certificate, _) = self_signed_pair();
        // Unreadable key path.
        let err = client_credential(&policy(
            Mode::Require,
            Some(&certificate),
            Some("/nonexistent/client.key"),
        ))
        .unwrap_err();
        assert!(err.to_string().contains("/nonexistent/client.key"), "{err}");
        // A PEM with no key in it (e.g. a certificate pasted as the key) —
        // the message names the encrypted-key limitation too.
        let err = client_credential(&policy(
            Mode::Require,
            Some(&certificate),
            Some(&certificate),
        ))
        .unwrap_err();
        let message = err.to_string();
        assert!(
            message.contains("no private key") && message.contains("encrypted"),
            "{message}"
        );
        // Garbage certificate PEM.
        let (_, key) = self_signed_pair();
        let err = client_credential(&policy(
            Mode::Require,
            Some("-----BEGIN CERTIFICATE-----\ngarbage"),
            Some(&key),
        ))
        .unwrap_err();
        assert!(err.to_string().contains("inline client cert"), "{err}");
    }

    #[test]
    fn credentials_compose_with_every_tls_active_mode() {
        // Adding a credential must not change what any mode means — the
        // config still BUILDS under all four TLS-active modes.
        let (certificate, key) = self_signed_pair();
        let authority = rcgen::CertificateParams::new(Vec::<String>::new())
            .expect("params build")
            .self_signed(&rcgen::KeyPair::generate().expect("key generates"))
            .expect("authority signs");
        for mode in [
            Mode::Prefer,
            Mode::Require,
            Mode::VerifyCa,
            Mode::VerifyFull,
        ] {
            let mut with_root = policy(mode, Some(&certificate), Some(&key));
            with_root.root_cert = Some(Material::new(authority.pem()));
            assert!(
                build(&with_root).expect("config builds").is_some(),
                "mode {mode:?}"
            );
        }
    }
}
