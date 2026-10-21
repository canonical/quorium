#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = quorium_core::kv::KvState::from_bytes(data);
});
