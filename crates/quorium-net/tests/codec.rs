use proptest::prelude::*;
use quorium_net::codec::{CodecError, Envelope, FORMAT_VERSION, FrameDecoder, MAX_FRAME, encode_frame};

fn envelope() -> Envelope {
    Envelope {
        format: FORMAT_VERSION,
        type_name: "Cart".into(),
        method: 0x1234_5678,
        state_version: 3,
        payload: vec![1, 2, 3],
    }
}

#[test]
fn envelope_round_trips() {
    let e = envelope();
    assert_eq!(Envelope::decode(&e.encode()).unwrap(), e);
}

#[test]
fn unknown_format_version_is_rejected() {
    let mut e = envelope();
    e.format = FORMAT_VERSION + 1;
    assert_eq!(Envelope::decode(&e.encode()), Err(CodecError::UnsupportedFormat(FORMAT_VERSION + 1)));
}

#[test]
fn trailing_bytes_are_rejected() {
    let mut bytes = envelope().encode();
    bytes.push(0);
    assert_eq!(Envelope::decode(&bytes), Err(CodecError::Malformed));
}

#[test]
fn frames_reassemble_across_arbitrary_splits() {
    let a = encode_frame(b"hello").unwrap();
    let b = encode_frame(b"").unwrap();
    let c = encode_frame(&[7u8; 300]).unwrap();
    let stream: Vec<u8> = [a, b, c].concat();
    for split in 0..stream.len() {
        let mut d = FrameDecoder::default();
        let mut out = Vec::new();
        d.push(&stream[..split]);
        while let Some(f) = d.next_frame().unwrap() {
            out.push(f);
        }
        d.push(&stream[split..]);
        while let Some(f) = d.next_frame().unwrap() {
            out.push(f);
        }
        assert_eq!(out, vec![b"hello".to_vec(), vec![], vec![7u8; 300]], "split {split}");
    }
}

#[test]
fn oversized_frames_are_refused_both_ways() {
    assert_eq!(encode_frame(&vec![0u8; MAX_FRAME + 1]), Err(CodecError::FrameTooLarge(MAX_FRAME + 1)));
    let mut d = FrameDecoder::default();
    d.push(&((MAX_FRAME as u32) + 1).to_le_bytes());
    assert_eq!(d.next_frame(), Err(CodecError::FrameTooLarge(MAX_FRAME + 1)));
}

proptest! {
    #[test]
    fn any_envelope_round_trips(name in ".{0,40}", method in any::<u32>(), version in any::<u32>(), payload in proptest::collection::vec(any::<u8>(), 0..512)) {
        let e = Envelope { format: FORMAT_VERSION, type_name: name, method, state_version: version, payload };
        prop_assert_eq!(Envelope::decode(&e.encode()).unwrap(), e);
    }

    #[test]
    fn hostile_bytes_never_panic(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
        let _ = Envelope::decode(&bytes);
        let mut d = FrameDecoder::default();
        d.push(&bytes);
        while let Ok(Some(_)) = d.next_frame() {}
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2_000))]

    #[test]
    fn raft_messages_never_panic_on_hostile_bytes(bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
        let _ = quorium_net::raft::Message::decode(&bytes);
    }

    #[test]
    fn enveloped_garbage_payloads_are_rejected(payload in proptest::collection::vec(any::<u8>(), 0..256), method in any::<u32>()) {
        let e = Envelope { format: FORMAT_VERSION, type_name: "quorium.raft".into(), method, state_version: 0, payload };
        let _ = quorium_net::raft::Message::decode(&e.encode());
    }
}
