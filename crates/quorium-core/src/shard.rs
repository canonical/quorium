use crate::hash::fnv1a;

pub type ShardId = u64;

pub fn key_hash(key: &[u8]) -> u64 {
    let z = fnv1a(key).wrapping_add(0x9E37_79B9_7F4A_7C15);
    let z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    let z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

pub fn position_shard(position: u64, shards: u64) -> ShardId {
    ((u128::from(position) * u128::from(shards.max(1))) >> 64) as u64
}

pub fn shard_of(key: &[u8], shards: u64) -> ShardId {
    position_shard(key_hash(key), shards)
}

pub fn entity_position(type_name: &str, key: &[u8], locality: Option<&str>) -> u64 {
    let mut bytes = Vec::with_capacity(type_name.len() + 1 + key.len());
    bytes.extend_from_slice(type_name.as_bytes());
    bytes.push(0);
    bytes.extend_from_slice(key);
    match locality {
        None => key_hash(&bytes),
        Some(l) => (key_hash(l.as_bytes()) & 0xFFFF_0000_0000_0000) | (key_hash(&bytes) >> 16),
    }
}

pub fn shard_range(shard: ShardId, shards: u64) -> (u64, u64) {
    let shards = u128::from(shards.max(1));
    let start = |i: u128| ((i << 64).div_ceil(shards)) as u64;
    let lo = start(u128::from(shard));
    let hi = if u128::from(shard) + 1 >= shards { u64::MAX } else { start(u128::from(shard) + 1) - 1 };
    (lo, hi)
}

pub fn entity_shard(type_name: &str, key: &[u8], shards: u64) -> ShardId {
    position_shard(entity_position(type_name, key, None), shards)
}

pub fn entity_shard_with_locality(type_name: &str, key: &[u8], locality: Option<&str>, shards: u64) -> ShardId {
    position_shard(entity_position(type_name, key, locality), shards)
}
