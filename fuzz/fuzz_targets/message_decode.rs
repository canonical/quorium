#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = quorium_net::raft::Message::decode(data);
    let mut decoder = quorium_net::codec::FrameDecoder::default();
    decoder.push(data);
    while let Ok(Some(_)) = decoder.next_frame() {}
});
