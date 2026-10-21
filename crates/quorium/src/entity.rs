use std::convert::Infallible;
use std::fmt;
use std::marker::PhantomData;
use std::time::{SystemTime, UNIX_EPOCH};

use quorium_core::entity::{EntityCall, lookup};
use quorium_core::kv::{KvOp, Reply};
use quorium_net::raft::{Request, Response};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::node::{ClientError, Quorium};

#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error<E> {
    Rejected(E),
    SoldOut,
    Unavailable,
    Timeout,
    Shutdown,
    Internal(String),
}

impl<E> Error<E> {
    pub fn is_retryable(&self) -> bool {
        matches!(self, Error::Unavailable | Error::Timeout)
    }
}

fn widen<F>(e: Error<Infallible>) -> Error<F> {
    match e {
        Error::Rejected(never) => match never {},
        Error::SoldOut => Error::SoldOut,
        Error::Unavailable => Error::Unavailable,
        Error::Timeout => Error::Timeout,
        Error::Shutdown => Error::Shutdown,
        Error::Internal(s) => Error::Internal(s),
    }
}

impl<E: fmt::Debug> fmt::Display for Error<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Rejected(e) => write!(f, "rejected: {e:?}"),
            Error::SoldOut => f.write_str("sold out"),
            Error::Unavailable => f.write_str("cluster unavailable"),
            Error::Timeout => f.write_str("timed out"),
            Error::Shutdown => f.write_str("node is shutting down"),
            Error::Internal(e) => write!(f, "internal error: {e}"),
        }
    }
}

impl<E: fmt::Debug> std::error::Error for Error<E> {}

impl<E> From<ClientError> for Error<E> {
    fn from(e: ClientError) -> Self {
        match e {
            ClientError::Timeout => Error::Timeout,
            ClientError::Unavailable(_) => Error::Unavailable,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationError(String);

impl MigrationError {
    pub fn new(reason: impl Into<String>) -> Self {
        MigrationError(reason.into())
    }
}

impl fmt::Display for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for MigrationError {}

pub trait Key: Serialize + Send + Sync + 'static {
    fn to_key_bytes(&self) -> Vec<u8> {
        postcard::to_allocvec(self).expect("keys always serialize")
    }
}

macro_rules! impl_key {
    ($($t:ty),*) => { $(impl Key for $t {})* };
}

impl_key!(String, u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, (), bool, char);

#[cfg(feature = "uuid")]
impl Key for uuid::Uuid {}

impl<A: Key, B: Key> Key for (A, B) {}
impl<A: Key, B: Key, C: Key> Key for (A, B, C) {}

pub trait State: Serialize + DeserializeOwned + Default + Send + Sync + 'static {
    type Key: Key;
    type Handle: Handle<State = Self>;
    const NAME: &'static str;
    const VERSION: u32;

    fn locality(key: &Self::Key) -> Option<String> {
        let _ = key;
        None
    }

    fn migrate(from: u32, bytes: &[u8]) -> Result<Self, MigrationError> {
        let _ = bytes;
        Err(MigrationError::new(format!("{} has no migration from version {from}", Self::NAME)))
    }
}

pub struct Watch<T: State> {
    rx: tokio::sync::watch::Receiver<Option<(u32, Vec<u8>)>>,
    _state: PhantomData<fn() -> T>,
}

impl<T: State> Watch<T> {
    pub async fn changed(&mut self) -> Result<T, Error<Infallible>> {
        self.rx.changed().await.map_err(|_| Error::Shutdown)?;
        let latest = self.rx.borrow_and_update().clone();
        decode_state(latest)
    }
}

pub trait Handle: Sized {
    type State: State;
    fn from_entity(entity: Entity<Self::State>) -> Self;
}

#[cfg(feature = "testing")]
pub(crate) type SimHandle = crate::testing::SimCluster;

#[cfg(not(feature = "testing"))]
#[derive(Clone)]
pub(crate) enum SimHandle {}

#[cfg(not(feature = "testing"))]
impl SimHandle {
    pub(crate) fn submit(&self, _: KvOp) -> Result<Reply, Error<Infallible>> {
        match *self {}
    }

    pub(crate) fn entity_state(&self, _: &str, _: &[u8]) -> Result<Option<(u32, Vec<u8>)>, Error<Infallible>> {
        match *self {}
    }

    pub(crate) fn escrow_view(&self, _: &str) -> Result<quorium_core::kv::EscrowView, Error<Infallible>> {
        match *self {}
    }

    pub(crate) fn now(&self) -> std::time::Duration {
        match *self {}
    }
}

#[derive(Clone)]
pub(crate) enum Backend {
    Node(Quorium),
    #[cfg_attr(not(feature = "testing"), allow(dead_code))]
    Sim(SimHandle),
}

pub struct Entity<T: State> {
    backend: Backend,
    key: Vec<u8>,
    locality: Option<String>,
    stale: bool,
    _state: PhantomData<fn() -> T>,
}

impl<T: State> Clone for Entity<T> {
    fn clone(&self) -> Self {
        Entity {
            backend: self.backend.clone(),
            key: self.key.clone(),
            locality: self.locality.clone(),
            stale: self.stale,
            _state: PhantomData,
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

fn decode<R: DeserializeOwned>(bytes: &[u8]) -> Result<R, Error<Infallible>> {
    postcard::from_bytes(bytes).map_err(|e| Error::Internal(format!("undecodable reply: {e}")))
}

pub(crate) fn decode_state<T: State>(stored: Option<(u32, Vec<u8>)>) -> Result<T, Error<Infallible>> {
    match stored {
        None => Ok(T::default()),
        Some((v, bytes)) if v == T::VERSION => decode(&bytes),
        Some((v, bytes)) if v < T::VERSION => T::migrate(v, &bytes).map_err(|e| Error::Internal(e.to_string())),
        Some((v, _)) => Err(Error::Internal(format!("{} stored at newer version {v}", T::NAME))),
    }
}

impl<T: State> Entity<T> {
    pub(crate) fn parts(&self) -> (Backend, Vec<u8>, Option<String>) {
        (self.backend.clone(), self.key.clone(), self.locality.clone())
    }

    pub(crate) fn new(backend: Backend, key: &T::Key) -> Self {
        Entity { backend, key: key.to_key_bytes(), locality: T::locality(key), stale: false, _state: PhantomData }
    }

    pub fn stale(&self) -> Self {
        Entity { stale: true, ..self.clone() }
    }

    async fn stored(&self) -> Result<Option<(u32, Vec<u8>)>, Error<Infallible>> {
        let node = match &self.backend {
            Backend::Node(node) => node,
            Backend::Sim(sim) => return sim.entity_state(T::NAME, &self.key),
        };
        if self.stale
            && let Some(local) = node.local_entity(T::NAME, &self.key, self.locality.as_deref()).await
        {
            return Ok(local);
        }
        let request = || Request::ReadEntity {
            type_name: T::NAME.to_string(),
            key: self.key.clone(),
            locality: self.locality.clone(),
        };
        match node.route(request).await? {
            Response::Entity(v) => Ok(v),
            other => Err(Error::Internal(format!("unexpected response {other:?}"))),
        }
    }

    async fn apply(&self, op: KvOp) -> Result<Reply, Error<Infallible>> {
        let node = match &self.backend {
            Backend::Node(node) => node,
            Backend::Sim(sim) => return sim.submit(op),
        };
        let (cmd, _in_flight) = node.next_command(op);
        match node.route(|| Request::Write(cmd.clone())).await? {
            Response::Written(reply) => Ok(reply),
            other => Err(Error::Internal(format!("unexpected response {other:?}"))),
        }
    }

    fn now_ms(&self) -> u64 {
        match &self.backend {
            Backend::Node(_) => now_ms(),
            Backend::Sim(sim) => sim.now().as_millis() as u64,
        }
    }

    pub fn watch(&self) -> Watch<T> {
        let rx = match &self.backend {
            Backend::Node(node) => node.subscribe(T::NAME, &self.key),
            Backend::Sim(_) => tokio::sync::watch::channel(None).1,
        };
        Watch { rx, _state: PhantomData }
    }

    pub async fn get(&self) -> Result<T, Error<Infallible>> {
        decode_state(self.stored().await?)
    }

    pub async fn exists(&self) -> Result<bool, Error<Infallible>> {
        Ok(self.stored().await?.is_some())
    }

    pub async fn read<R>(&self, f: impl FnOnce(&T) -> R) -> Result<R, Error<Infallible>> {
        Ok(f(&self.get().await?))
    }

    pub async fn read_result<R, E>(&self, f: impl FnOnce(&T) -> Result<R, E>) -> Result<R, Error<E>> {
        let state = self.get().await.map_err(widen)?;
        f(&state).map_err(Error::Rejected)
    }

    pub async fn delete(&self) -> Result<(), Error<Infallible>> {
        self.apply(KvOp::DeleteEntity {
            type_name: T::NAME.to_string(),
            key: self.key.clone(),
            locality: self.locality.clone(),
        })
        .await
        .map(|_| ())
    }

    async fn submit(&self, method: u32, args: Vec<u8>) -> Result<Vec<u8>, Error<Infallible>> {
        if !lookup(T::NAME).is_some_and(|r| r.accepts(method)) {
            return Err(Error::Internal(format!("{} has no write method {method:#x}", T::NAME)));
        }
        let node = match &self.backend {
            Backend::Node(node) => Some(node),
            Backend::Sim(_) => None,
        };
        if let Some(node) = node {
            let shard = node.group_of_position(quorium_core::shard::entity_position(
                T::NAME,
                &self.key,
                self.locality.as_deref(),
            ));
            if !node.method_enabled(shard, T::NAME, method).await {
                return Err(Error::Unavailable);
            }
        }
        let call = EntityCall {
            type_name: T::NAME.to_string(),
            version: T::VERSION,
            key: self.key.clone(),
            method,
            args,
            now_ms: self.now_ms(),
            locality: self.locality.clone(),
        };
        match self.apply(KvOp::Apply(call)).await? {
            Reply::Applied(bytes) => Ok(bytes),
            Reply::ApplyFailed(e) => Err(Error::Internal(e)),
            Reply::Stale => Err(Error::Internal("write refused as stale".into())),
            other => Err(Error::Internal(format!("unexpected reply {other:?}"))),
        }
    }

    pub async fn write<R: DeserializeOwned>(&self, method: u32, args: &impl Serialize) -> Result<R, Error<Infallible>> {
        let args = postcard::to_allocvec(args).map_err(|e| Error::Internal(e.to_string()))?;
        decode(&self.submit(method, args).await?)
    }

    pub async fn write_result<R: DeserializeOwned, E: DeserializeOwned>(
        &self,
        method: u32,
        args: &impl Serialize,
    ) -> Result<R, Error<E>> {
        let args = postcard::to_allocvec(args).map_err(|e| Error::Internal(e.to_string()))?;
        let bytes = self.submit(method, args).await.map_err(widen)?;
        let result: Result<R, E> = decode(&bytes).map_err(widen)?;
        result.map_err(Error::Rejected)
    }
}
