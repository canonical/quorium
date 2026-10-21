use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::AbortHandle;
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::codec::{CodecError, FrameDecoder, encode_frame};
use crate::tls::TlsConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RpcError {
    Connect(String),
    Timeout,
    Closed,
    Remote(String),
    Codec(CodecError),
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RpcError::Connect(e) => write!(f, "connect failed: {e}"),
            RpcError::Timeout => f.write_str("request timed out"),
            RpcError::Closed => f.write_str("connection closed"),
            RpcError::Remote(e) => write!(f, "remote error: {e}"),
            RpcError::Codec(e) => write!(f, "codec error: {e}"),
        }
    }
}

impl std::error::Error for RpcError {}

#[derive(Debug, Serialize, Deserialize)]
struct Bytes(#[serde(with = "serde_bytes")] Vec<u8>);

#[derive(Debug, Serialize, Deserialize)]
enum Wire {
    Request {
        id: u64,
        #[serde(with = "serde_bytes")]
        body: Vec<u8>,
    },
    Response {
        id: u64,
        result: Result<Bytes, String>,
    },
}

trait Stream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Stream for T {}

type BoxStream = Box<dyn Stream>;
type HandlerFuture = Pin<Box<dyn Future<Output = Result<Vec<u8>, String>> + Send>>;

pub trait Handler: Send + Sync + 'static {
    fn handle(&self, body: Vec<u8>) -> HandlerFuture;
}

impl<F, Fut> Handler for F
where
    F: Fn(Vec<u8>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Vec<u8>, String>> + Send + 'static,
{
    fn handle(&self, body: Vec<u8>) -> HandlerFuture {
        Box::pin(self(body))
    }
}

fn encode(wire: &Wire) -> Result<Vec<u8>, CodecError> {
    encode_frame(&postcard::to_allocvec(wire).map_err(|_| CodecError::Malformed)?)
}

const MAX_BATCH_BYTES: usize = 256 * 1024;

async fn write_loop(
    mut writer: tokio::io::WriteHalf<BoxStream>,
    mut rx: mpsc::Receiver<Vec<u8>>,
    batches: Arc<AtomicU64>,
) {
    while let Some(mut batch) = rx.recv().await {
        while batch.len() < MAX_BATCH_BYTES {
            match rx.try_recv() {
                Ok(frame) => batch.extend_from_slice(&frame),
                Err(_) => break,
            }
        }
        batches.fetch_add(1, Ordering::Relaxed);
        if writer.write_all(&batch).await.is_err() {
            return;
        }
    }
}

async fn read_frames(
    reader: &mut tokio::io::ReadHalf<BoxStream>,
    decoder: &mut FrameDecoder,
    buf: &mut [u8],
) -> Option<Vec<Wire>> {
    let n = reader.read(buf).await.ok().filter(|n| *n > 0)?;
    decoder.push(&buf[..n]);
    let mut out = Vec::new();
    loop {
        match decoder.next_frame() {
            Ok(Some(frame)) => out.push(postcard::from_bytes(&frame).ok()?),
            Ok(None) => return Some(out),
            Err(_) => return None,
        }
    }
}

pub struct Server {
    addr: SocketAddr,
    accept: Option<tokio::task::JoinHandle<()>>,
    tasks: Arc<Mutex<Vec<AbortHandle>>>,
}

impl Drop for Server {
    fn drop(&mut self) {
        if let Some(a) = &self.accept {
            a.abort();
        }
        for t in self.tasks.lock().unwrap().drain(..) {
            t.abort();
        }
    }
}

impl Server {
    pub async fn bind(addr: SocketAddr, tls: Option<TlsConfig>, handler: impl Handler) -> io::Result<Server> {
        let listener = TcpListener::bind(addr).await?;
        let local = listener.local_addr()?;
        let acceptor = match tls {
            Some(t) => Some(TlsAcceptor::from(Arc::new(t.server_config()?))),
            None => None,
        };
        let handler: Arc<dyn Handler> = Arc::new(handler);
        let tasks: Arc<Mutex<Vec<AbortHandle>>> = Arc::default();
        let registry = tasks.clone();
        let accept = tokio::spawn(async move {
            loop {
                let Ok((sock, _)) = listener.accept().await else { continue };
                let _ = sock.set_nodelay(true);
                let acceptor = acceptor.clone();
                let handler = handler.clone();
                let conn = tokio::spawn(async move {
                    let stream: BoxStream = match acceptor {
                        Some(a) => match a.accept(sock).await {
                            Ok(s) => Box::new(s),
                            Err(_) => return,
                        },
                        None => Box::new(sock),
                    };
                    serve(stream, handler).await;
                });
                let mut reg = registry.lock().unwrap();
                reg.retain(|t| !t.is_finished());
                reg.push(conn.abort_handle());
            }
        });
        Ok(Server { addr: local, accept: Some(accept), tasks })
    }

    pub async fn shutdown(mut self) {
        if let Some(accept) = self.accept.take() {
            accept.abort();
            let _ = accept.await;
        }
        for t in self.tasks.lock().unwrap().drain(..) {
            t.abort();
        }
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }
}

async fn serve(stream: BoxStream, handler: Arc<dyn Handler>) {
    let (mut reader, writer) = tokio::io::split(stream);
    let (tx, rx) = mpsc::channel::<Vec<u8>>(1024);
    let writer_task = tokio::spawn(write_loop(writer, rx, Arc::default()));
    let mut decoder = FrameDecoder::default();
    let mut buf = vec![0u8; 64 * 1024];
    while let Some(wires) = read_frames(&mut reader, &mut decoder, &mut buf).await {
        for wire in wires {
            let Wire::Request { id, body } = wire else { continue };
            let handler = handler.clone();
            let tx = tx.clone();
            tokio::spawn(async move {
                let result = handler.handle(body).await;
                if let Ok(frame) = encode(&Wire::Response { id, result: result.map(Bytes) }) {
                    let _ = tx.send(frame).await;
                }
            });
        }
    }
    writer_task.abort();
}

type Pending = Arc<Mutex<HashMap<u64, oneshot::Sender<Result<Vec<u8>, RpcError>>>>>;

#[derive(Clone)]
struct Conn {
    tx: mpsc::Sender<Vec<u8>>,
    pending: Pending,
    alive: Arc<AtomicBool>,
}

struct PendingGuard {
    pending: Pending,
    id: u64,
}

impl Drop for PendingGuard {
    fn drop(&mut self) {
        self.pending.lock().unwrap().remove(&self.id);
    }
}

pub struct Client {
    addr: SocketAddr,
    tls: Option<(TlsConnector, TlsConfig)>,
    tls_error: Option<String>,
    conn: tokio::sync::Mutex<Option<Conn>>,
    next_id: AtomicU64,
    opened: AtomicU64,
    batches: Arc<AtomicU64>,
}

impl Client {
    pub fn new(addr: SocketAddr, tls: Option<TlsConfig>) -> Client {
        let (tls, tls_error) = match tls {
            Some(t) => match t.client_config() {
                Ok(c) => (Some((TlsConnector::from(Arc::new(c)), t)), None),
                Err(e) => (None, Some(e.to_string())),
            },
            None => (None, None),
        };
        Client {
            addr,
            tls,
            tls_error,
            conn: tokio::sync::Mutex::new(None),
            next_id: AtomicU64::new(1),
            opened: AtomicU64::new(0),
            batches: Arc::default(),
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn write_batches(&self) -> u64 {
        self.batches.load(Ordering::Relaxed)
    }

    pub fn connections_opened(&self) -> u64 {
        self.opened.load(Ordering::Relaxed)
    }

    async fn connect(&self) -> Result<Conn, RpcError> {
        if let Some(e) = &self.tls_error {
            return Err(RpcError::Connect(e.clone()));
        }
        let sock = TcpStream::connect(self.addr).await.map_err(|e| RpcError::Connect(e.to_string()))?;
        let _ = sock.set_nodelay(true);
        let stream: BoxStream = match &self.tls {
            Some((connector, cfg)) => {
                let name = cfg.server_name().map_err(|e| RpcError::Connect(e.to_string()))?;
                Box::new(connector.connect(name, sock).await.map_err(|e| RpcError::Connect(e.to_string()))?)
            }
            None => Box::new(sock),
        };
        self.opened.fetch_add(1, Ordering::Relaxed);
        let (mut reader, writer) = tokio::io::split(stream);
        let (tx, rx) = mpsc::channel::<Vec<u8>>(1024);
        tokio::spawn(write_loop(writer, rx, self.batches.clone()));
        let pending: Pending = Arc::default();
        let alive = Arc::new(AtomicBool::new(true));
        let conn = Conn { tx, pending: pending.clone(), alive: alive.clone() };
        tokio::spawn(async move {
            let mut decoder = FrameDecoder::default();
            let mut buf = vec![0u8; 64 * 1024];
            while let Some(wires) = read_frames(&mut reader, &mut decoder, &mut buf).await {
                for wire in wires {
                    if let Wire::Response { id, result } = wire
                        && let Some(waiter) = pending.lock().unwrap().remove(&id)
                    {
                        let _ = waiter.send(result.map(|b| b.0).map_err(RpcError::Remote));
                    }
                }
            }
            alive.store(false, Ordering::SeqCst);
            for (_, waiter) in pending.lock().unwrap().drain() {
                let _ = waiter.send(Err(RpcError::Closed));
            }
        });
        Ok(conn)
    }

    async fn connection(&self) -> Result<Conn, RpcError> {
        let mut slot = self.conn.lock().await;
        if let Some(c) = slot.as_ref().filter(|c| c.alive.load(Ordering::SeqCst)) {
            return Ok(c.clone());
        }
        let conn = self.connect().await?;
        *slot = Some(conn.clone());
        Ok(conn)
    }

    pub async fn pending_requests(&self) -> usize {
        self.conn.lock().await.as_ref().map_or(0, |c| c.pending.lock().unwrap().len())
    }

    pub async fn call(&self, body: Vec<u8>, timeout: Duration) -> Result<Vec<u8>, RpcError> {
        let attempt = async {
            let conn = self.connection().await?;
            let id = self.next_id.fetch_add(1, Ordering::Relaxed);
            let (tx, rx) = oneshot::channel();
            conn.pending.lock().unwrap().insert(id, tx);
            let _guard = PendingGuard { pending: conn.pending.clone(), id };
            let frame = encode(&Wire::Request { id, body }).map_err(RpcError::Codec)?;
            if conn.tx.send(frame).await.is_err() {
                conn.alive.store(false, Ordering::SeqCst);
                return Err(RpcError::Closed);
            }
            rx.await.unwrap_or(Err(RpcError::Closed))
        };
        tokio::time::timeout(timeout, attempt).await.unwrap_or(Err(RpcError::Timeout))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum Protocol {
    #[cfg_attr(not(feature = "quic"), default)]
    Tcp,
    #[cfg(feature = "quic")]
    #[default]
    Quic,
}

pub enum Peer {
    Tcp(Client),
    #[cfg(feature = "quic")]
    Quic(crate::quic::QuicClient),
}

impl Peer {
    pub fn new(protocol: Protocol, addr: SocketAddr, tls: Option<TlsConfig>) -> Peer {
        match protocol {
            Protocol::Tcp => Peer::Tcp(Client::new(addr, tls)),
            #[cfg(feature = "quic")]
            Protocol::Quic => Peer::Quic(crate::quic::QuicClient::new(addr, tls)),
        }
    }

    pub fn addr(&self) -> SocketAddr {
        match self {
            Peer::Tcp(c) => c.addr(),
            #[cfg(feature = "quic")]
            Peer::Quic(c) => c.addr(),
        }
    }

    pub fn connections_opened(&self) -> u64 {
        match self {
            Peer::Tcp(c) => c.connections_opened(),
            #[cfg(feature = "quic")]
            Peer::Quic(c) => c.connections_opened(),
        }
    }

    pub async fn call(&self, body: Vec<u8>, timeout: Duration) -> Result<Vec<u8>, RpcError> {
        match self {
            Peer::Tcp(c) => c.call(body, timeout).await,
            #[cfg(feature = "quic")]
            Peer::Quic(c) => c.call(body, timeout).await,
        }
    }
}

pub enum Listener {
    Tcp(Server),
    #[cfg(feature = "quic")]
    Quic(crate::quic::QuicServer),
}

impl Listener {
    pub async fn bind(
        protocol: Protocol,
        addr: SocketAddr,
        tls: Option<TlsConfig>,
        handler: impl Handler,
    ) -> io::Result<Listener> {
        Ok(match protocol {
            Protocol::Tcp => Listener::Tcp(Server::bind(addr, tls, handler).await?),
            #[cfg(feature = "quic")]
            Protocol::Quic => Listener::Quic(crate::quic::QuicServer::bind(addr, tls, handler).await?),
        })
    }

    pub fn local_addr(&self) -> SocketAddr {
        match self {
            Listener::Tcp(s) => s.local_addr(),
            #[cfg(feature = "quic")]
            Listener::Quic(s) => s.local_addr(),
        }
    }

    pub async fn shutdown(self) {
        match self {
            Listener::Tcp(s) => s.shutdown().await,
            #[cfg(feature = "quic")]
            Listener::Quic(s) => s.shutdown().await,
        }
    }
}
