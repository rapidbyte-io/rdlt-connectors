//! Quarantined certificate verifiers for the deliberately-weaker libpq
//! levels. SAFE Rust throughout — "dangerous" is rustls's API vocabulary for
//! custom verification, not `unsafe`.
//!
//! - [`AcceptAnyCertificate`]: `require`/`prefer` — encrypt, validate
//!   NOTHING. Exactly libpq's `sslmode=require`. Never a default: the policy
//!   layer builds it only for those modes, and the crate documentation says
//!   `verify_full` is what production should use.
//! - [`ChainOnly`]: `verify_ca` — full webpki chain verification with ONLY
//!   the hostname check waived.

use std::sync::Arc;

use rustls::DigitallySignedStruct;
use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};

/// The one crypto provider this crate uses (ring), chosen EXPLICITLY at every
/// construction site — never the ambient process default, which is ambiguous
/// when multiple provider features land in the dependency tree.
pub(crate) fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// Encrypt without validating anything — libpq's `require` (and `prefer`).
#[derive(Debug)]
pub(crate) struct AcceptAnyCertificate {
    provider: Arc<CryptoProvider>,
}

impl AcceptAnyCertificate {
    pub(crate) fn new() -> Self {
        Self {
            provider: provider(),
        }
    }
}

impl ServerCertVerifier for AcceptAnyCertificate {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _certificate: &CertificateDer<'_>,
        _signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _certificate: &CertificateDer<'_>,
        _signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Full webpki verification with the hostname check waived — libpq's
/// `verify_ca`. Every other failure keeps its meaning.
#[derive(Debug)]
pub(crate) struct ChainOnly {
    webpki: Arc<WebPkiServerVerifier>,
}

impl ChainOnly {
    pub(crate) fn new(
        roots: rustls::RootCertStore,
    ) -> Result<Self, rustls::client::VerifierBuilderError> {
        Ok(Self {
            webpki: WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider())
                .build()?,
        })
    }
}

impl ServerCertVerifier for ChainOnly {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self.webpki.verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        ) {
            // ONLY the name-mismatch error is forgiven; the chain must hold.
            Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::NotValidForName
                | rustls::CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.webpki
            .verify_tls12_signature(message, certificate, signature)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &CertificateDer<'_>,
        signature: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.webpki
            .verify_tls13_signature(message, certificate, signature)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.webpki.supported_verify_schemes()
    }
}
