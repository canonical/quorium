pub use quorium_core::entity::{ApplyError, Ctx, REGISTRY, Registration, linkme, lookup};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::entity::State;

pub const fn method_id(name: &str) -> u32 {
    let bytes = name.as_bytes();
    let mut hash: u32 = 0x811c_9dc5;
    let mut i = 0;
    while i < bytes.len() {
        hash ^= bytes[i] as u32;
        hash = hash.wrapping_mul(0x0100_0193);
        i += 1;
    }
    hash
}

pub fn decode_state<T: State>(state: Option<&[u8]>) -> Result<T, ApplyError> {
    match state {
        None => Ok(T::default()),
        Some(bytes) => postcard::from_bytes(bytes).map_err(|_| ApplyError::Decode),
    }
}

pub fn decode_args<A: DeserializeOwned>(args: &[u8]) -> Result<A, ApplyError> {
    postcard::from_bytes(args).map_err(|_| ApplyError::Decode)
}

pub fn encode<V: Serialize>(value: &V) -> Result<Vec<u8>, ApplyError> {
    postcard::to_allocvec(value).map_err(|_| ApplyError::Decode)
}

pub fn migrate<T: State>(from: u32, bytes: &[u8]) -> Result<Vec<u8>, ApplyError> {
    let migrated = T::migrate(from, bytes).map_err(|e| ApplyError::Migration(e.to_string()))?;
    encode(&migrated)
}

pub use crate::escrow::ESCROW_METHOD;
pub use quorium_core::kv::EscrowView;

pub trait EscrowFields {
    fn escrow_view(&self, field: &str) -> Option<&EscrowView>;
    fn escrow_view_mut(&mut self, field: &str) -> Option<&mut EscrowView>;
}

pub fn escrow_field_apply<T: EscrowFields>(state: &mut T, args: &[u8]) -> Result<Vec<u8>, ApplyError> {
    let (field, node, action): (String, u64, quorium_core::kv::EscrowAction) = decode_args(args)?;
    let outcome: Result<(u64, u64), String> = match state.escrow_view_mut(&field) {
        Some(view) => view.apply(node, action).map(|granted| (granted, view.pool)),
        None => Err(format!("no escrow field named {field}")),
    };
    encode(&outcome)
}

pub fn escrow_counter<T: State + EscrowFields>(entity: &crate::entity::Entity<T>, field: &str) -> crate::EscrowCounter {
    crate::escrow::EscrowCounter::for_field(entity, field)
}

pub fn read_escrow_field<T: State + EscrowFields>(stored: Option<(u32, Vec<u8>)>, field: &str) -> Option<EscrowView> {
    let state: T = crate::entity::decode_state(stored).ok()?;
    state.escrow_view(field).cloned()
}

pub fn assert_argument<A: Serialize + DeserializeOwned + Send>() {}

pub fn assert_return<R: Serialize + DeserializeOwned>() {}

pub trait Registered {
    fn registration() -> &'static Registration;
}
