use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use quinn::{Connection, Endpoint, VarInt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use tokio::task::AbortHandle;

use crate::tls::TlsConfig;
use crate::transport::{Handler, RpcError};

const ALPN: &[u8] = b"quorium/1";
const MAX_MESSAGE: usize = 64 << 20;
const PLAINTEXT_NAME: &str = "quorium";

fn invalid(e: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, e.to_string())
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn transport() -> Arc<quinn::TransportConfig> {
    let mut t = quinn::TransportConfig::default();
    t.max_concurrent_bidi_streams(VarInt::from_u32(4096));
    t.keep_alive_interval(Some(Duration::from_secs(1)));
    t.max_idle_timeout(Some(VarInt::from_u32(10_000).into()));
    Arc::new(t)
}

fn server_config(tls: Option<&TlsConfig>) -> io::Result<quinn::ServerConfig> {
    let mut rustls_config = match tls {
        Some(t) => t.server_config()?,
        None => {
            let generated = rcgen::generate_simple_self_signed(vec![PLAINTEXT_NAME.to_string()]).map_err(invalid)?;
            let cert = CertificateDer::from(generated.cert.der().to_vec());
            let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(generated.signing_key.serialize_der()));
            rustls::ServerConfig::builder_with_provider(provider())
                .with_safe_default_protocol_versions()
                .map_err(invalid)?
                .with_no_client_auth()
                .with_single_cert(vec![cert], key)
                .map_err(invalid)?
        }
    };
    rustls_config.alpn_protocols = vec![ALPN.to_vec()];
    let crypto = QuicServerConfig::try_from(rustls_config).map_err(invalid)?;
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    config.transport_config(transport());
    Ok(config)
}

#[derive(Debug)]
struct AnyServer(Arc<rustls::crypto::CryptoProvider>);

impl ServerCertVerifier for AnyServer {
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
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0.signature_verification_algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

fn client_config(tls: Option<&TlsConfig>) -> io::Result<(quinn::ClientConfig, String)> {
    let (mut rustls_config, name) = match tls {
        Some(t) => (t.client_config()?, t.server_name.clone()),
        None => (
            rustls::ClientConfig::builder_with_provider(provider())
                .with_safe_default_protocol_versions()
                .map_err(invalid)?
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(AnyServer(provider())))
                .with_no_client_auth(),
            PLAINTEXT_NAME.to_string(),
        ),
    };
    rustls_config.alpn_protocols = vec![ALPN.to_vec()];
    let crypto = QuicClientConfig::try_from(rustls_config).map_err(invalid)?;
    let mut config = quinn::ClientConfig::new(Arc::new(crypto));
    config.transport_config(transport());
    Ok((config, name))
}

fn encode_reply(result: Result<Vec<u8>, String>) -> Vec<u8> {
    match result {
        Ok(body) => [&[0u8][..], &body].concat(),
        Err(e) => [&[1u8][..], e.as_bytes()].concat(),
    }
}

fn decode_reply(bytes: Vec<u8>) -> Result<Vec<u8>, RpcError> {
    match bytes.split_first() {
        Some((0, body)) => Ok(body.to_vec()),
        Some((1, e)) => Err(RpcError::Remote(String::from_utf8_lossy(e).into_owned())),
        _ => Err(RpcError::Closed),
    }
}

pub struct QuicServer {
    addr: SocketAddr,
    endpoint: Endpoint,
    accept: Option<tokio::task::JoinHandle<()>>,
    tasks: Arc<Mutex<Vec<AbortHandle>>>,
}

impl Drop for QuicServer {
    fn drop(&mut self) {
        self.endpoint.close(VarInt::from_u32(0), b"shutdown");
        if let Some(a) = &self.accept {
            a.abort();
        }
        for t in self.tasks.lock().unwrap().drain(..) {
            t.abort();
        }
    }
}

impl QuicServer {
    pub async fn bind(addr: SocketAddr, tls: Option<TlsConfig>, handler: impl Handler) -> io::Result<QuicServer> {
        let endpoint = Endpoint::server(server_config(tls.as_ref())?, addr)?;
        let local = endpoint.local_addr()?;
        let handler: Arc<dyn Handler> = Arc::new(handler);
        let tasks: Arc<Mutex<Vec<AbortHandle>>> = Arc::default();
        let registry = tasks.clone();
        let listener = endpoint.clone();
        let accept = tokio::spawn(async move {
            while let Some(incoming) = listener.accept().await {
                let handler = handler.clone();
                let conn = tokio::spawn(async move {
                    let Ok(conn) = incoming.await else { return };
                    while let Ok((mut send, mut recv)) = conn.accept_bi().await {
                        let handler = handler.clone();
                        tokio::spawn(async move {
                            let Ok(body) = recv.read_to_end(MAX_MESSAGE).await else { return };
                            let reply = encode_reply(handler.handle(body).await);
                            if send.write_all(&reply).await.is_ok() {
                                let _ = send.finish();
                            }
                        });
                    }
                });
                let mut reg = registry.lock().unwrap();
                reg.retain(|t| !t.is_finished());
                reg.push(conn.abort_handle());
            }
        });
        Ok(QuicServer { addr: local, endpoint, accept: Some(accept), tasks })
    }

    pub async fn shutdown(mut self) {
        self.endpoint.close(VarInt::from_u32(0), b"shutdown");
        if let Some(accept) = self.accept.take() {
            accept.abort();
            let _ = accept.await;
        }
        for t in self.tasks.lock().unwrap().drain(..) {
            t.abort();
        }
        self.endpoint.wait_idle().await;
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }
}

pub struct QuicClient {
    addr: SocketAddr,
    config: Result<(quinn::ClientConfig, String), String>,
    endpoint: Mutex<Option<Endpoint>>,
    conn: tokio::sync::Mutex<Option<Connection>>,
    opened: AtomicU64,
}

impl Drop for QuicClient {
    fn drop(&mut self) {
        if let Some(endpoint) = self.endpoint.lock().unwrap().take() {
            endpoint.close(VarInt::from_u32(0), b"closed");
        }
    }
}

impl QuicClient {
    pub fn new(addr: SocketAddr, tls: Option<TlsConfig>) -> QuicClient {
        QuicClient {
            addr,
            config: client_config(tls.as_ref()).map_err(|e| e.to_string()),
            endpoint: Mutex::new(None),
            conn: tokio::sync::Mutex::new(None),
            opened: AtomicU64::new(0),
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn connections_opened(&self) -> u64 {
        self.opened.load(Ordering::Relaxed)
    }

    fn endpoint(&self) -> Result<Endpoint, RpcError> {
        let mut slot = self.endpoint.lock().unwrap();
        if let Some(e) = slot.as_ref() {
            return Ok(e.clone());
        }
        let local: SocketAddr = if self.addr.is_ipv4() { "0.0.0.0:0" } else { "[::]:0" }.parse().expect("valid");
        let endpoint = Endpoint::client(local).map_err(|e| RpcError::Connect(e.to_string()))?;
        *slot = Some(endpoint.clone());
        Ok(endpoint)
    }

    async fn connection(&self) -> Result<Connection, RpcError> {
        let mut slot = self.conn.lock().await;
        if let Some(c) = slot.as_ref().filter(|c| c.close_reason().is_none()) {
            return Ok(c.clone());
        }
        let (config, name) = self.config.as_ref().map_err(|e| RpcError::Connect(e.clone()))?;
        let connecting = self
            .endpoint()?
            .connect_with(config.clone(), self.addr, name)
            .map_err(|e| RpcError::Connect(e.to_string()))?;
        let conn = connecting.await.map_err(|e| RpcError::Connect(e.to_string()))?;
        self.opened.fetch_add(1, Ordering::Relaxed);
        *slot = Some(conn.clone());
        Ok(conn)
    }

    pub async fn call(&self, body: Vec<u8>, timeout: Duration) -> Result<Vec<u8>, RpcError> {
        let attempt = async {
            let conn = self.connection().await?;
            let (mut send, mut recv) = conn.open_bi().await.map_err(|_| RpcError::Closed)?;
            send.write_all(&body).await.map_err(|_| RpcError::Closed)?;
            send.finish().map_err(|_| RpcError::Closed)?;
            decode_reply(recv.read_to_end(MAX_MESSAGE).await.map_err(|_| RpcError::Closed)?)
        };
        tokio::time::timeout(timeout, attempt).await.unwrap_or(Err(RpcError::Timeout))
    }
}
