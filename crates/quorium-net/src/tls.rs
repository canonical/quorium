use std::io;
use std::sync::Arc;

use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair, KeyUsagePurpose, SanType};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig};

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub ca_pem: String,
    pub cert_pem: String,
    pub key_pem: String,
    pub server_name: String,
}

fn invalid(e: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, e.to_string())
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

impl TlsConfig {
    pub fn with_server_name(mut self, name: &str) -> Self {
        self.server_name = name.to_string();
        self
    }

    fn certs(&self) -> io::Result<Vec<CertificateDer<'static>>> {
        CertificateDer::pem_slice_iter(self.cert_pem.as_bytes()).collect::<Result<_, _>>().map_err(invalid)
    }

    fn key(&self) -> io::Result<PrivateKeyDer<'static>> {
        PrivateKeyDer::from_pem_slice(self.key_pem.as_bytes()).map_err(|_| invalid("no private key in pem"))
    }

    fn roots(&self) -> io::Result<RootCertStore> {
        let mut roots = RootCertStore::empty();
        for cert in CertificateDer::pem_slice_iter(self.ca_pem.as_bytes()) {
            roots.add(cert.map_err(invalid)?).map_err(invalid)?;
        }
        Ok(roots)
    }

    pub fn server_config(&self) -> io::Result<ServerConfig> {
        let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(self.roots()?), provider())
            .build()
            .map_err(invalid)?;
        ServerConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .map_err(invalid)?
            .with_client_cert_verifier(verifier)
            .with_single_cert(self.certs()?, self.key()?)
            .map_err(invalid)
    }

    pub fn client_config(&self) -> io::Result<ClientConfig> {
        ClientConfig::builder_with_provider(provider())
            .with_safe_default_protocol_versions()
            .map_err(invalid)?
            .with_root_certificates(self.roots()?)
            .with_client_auth_cert(self.certs()?, self.key()?)
            .map_err(invalid)
    }

    pub fn server_name(&self) -> io::Result<ServerName<'static>> {
        ServerName::try_from(self.server_name.clone()).map_err(invalid)
    }
}

pub struct TlsMaterial {
    pub name: String,
    pub cert_pem: String,
    pub key_pem: String,
    params: CertificateParams,
    key: KeyPair,
}

impl TlsMaterial {
    pub fn generate_ca(name: &str) -> io::Result<TlsMaterial> {
        let key = KeyPair::generate().map_err(invalid)?;
        let mut params = CertificateParams::new(Vec::<String>::new()).map_err(invalid)?;
        params.distinguished_name.push(DnType::CommonName, name);
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let cert = params.self_signed(&key).map_err(invalid)?;
        Ok(TlsMaterial { name: name.into(), cert_pem: cert.pem(), key_pem: key.serialize_pem(), params, key })
    }

    pub fn issue(&self, name: &str) -> io::Result<TlsMaterial> {
        let key = KeyPair::generate().map_err(invalid)?;
        let mut params = CertificateParams::new(vec![name.to_string(), "localhost".to_string()]).map_err(invalid)?;
        params.subject_alt_names.push(SanType::IpAddress([127, 0, 0, 1].into()));
        params.distinguished_name.push(DnType::CommonName, name);
        let issuer = Issuer::from_params(&self.params, &self.key);
        let cert = params.signed_by(&key, &issuer).map_err(invalid)?;
        Ok(TlsMaterial { name: name.into(), cert_pem: cert.pem(), key_pem: key.serialize_pem(), params, key })
    }

    pub fn config(&self, ca: &TlsMaterial) -> TlsConfig {
        TlsConfig {
            ca_pem: ca.cert_pem.clone(),
            cert_pem: self.cert_pem.clone(),
            key_pem: self.key_pem.clone(),
            server_name: self.name.clone(),
        }
    }
}
