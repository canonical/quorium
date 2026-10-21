use std::path::PathBuf;

use quorium_core::kv::{Command, KvOp, Reply};
use quorium_core::ladder::LogPosition;
use quorium_net::raft::{Message, Request, Response};

fn fixture(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures").join(name)
}

fn messages() -> Vec<Message> {
    vec![
        Message {
            from: 7,
            shard: 1,
            request: Request::Ping {
                logs: vec![(1, LogPosition { term: 3, index: 9 })],
                quiesced: vec![1],
                datacenter: "eu".into(),
                addr: "10.0.0.7:7000".into(),
                initialized: true,
                methods: vec![("Cart".into(), 42)],
            },
        },
        Message {
            from: 7,
            shard: 0,
            request: Request::Write(Command::new(9, 4, KvOp::Put { key: "k".into(), value: vec![1, 2, 3] })),
        },
        Message {
            from: 7,
            shard: 0,
            request: Request::ReadEntity { type_name: "Cart".into(), key: vec![5], locality: Some("eu".into()) },
        },
        Message { from: 7, shard: 0, request: Request::ReadRouting },
        Message { from: 7, shard: 1, request: Request::ReadLoad },
    ]
}

fn responses() -> Vec<Response> {
    vec![
        Response::Written(Reply::Applied(vec![9])),
        Response::Value(Some(vec![1])),
        Response::Entity(Some((2, vec![3]))),
        Response::NotLeader { leader: Some(3), addr: Some("10.0.0.3:7000".into()) },
        Response::Load { per_second: 12, samples: vec![1, 2] },
    ]
}

#[test]
fn wire_format_is_stable() {
    let path = fixture("wire-v1.bin");
    let encoded: Vec<Vec<u8>> = messages().iter().map(Message::encode).collect();
    let replies: Vec<Vec<u8>> = responses().iter().map(|r| postcard::to_allocvec(r).unwrap()).collect();
    if std::env::var("QUORIUM_WRITE_FIXTURES").is_ok() {
        std::fs::write(&path, postcard::to_allocvec(&(&encoded, &replies)).unwrap()).unwrap();
    }
    let bytes = std::fs::read(&path).expect("fixture exists; regenerate with QUORIUM_WRITE_FIXTURES=1");
    let (stored, stored_replies): (Vec<Vec<u8>>, Vec<Vec<u8>>) = postcard::from_bytes(&bytes).unwrap();
    assert_eq!(stored, encoded, "today's encoder changed the wire format");
    for (bytes, original) in stored.iter().zip(messages()) {
        let decoded = Message::decode(bytes).expect("today's decoder reads stored messages");
        assert_eq!(format!("{decoded:?}"), format!("{original:?}"));
    }
    for (bytes, original) in stored_replies.iter().zip(responses()) {
        let decoded: Response = postcard::from_bytes(bytes).expect("today's decoder reads stored replies");
        assert_eq!(format!("{decoded:?}"), format!("{original:?}"));
    }
}
