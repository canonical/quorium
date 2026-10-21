use std::convert::Infallible;
use std::io::Write;
use std::path::Path;

use quorium_core::kv::{KvOp, Reply};
use quorium_net::raft::{Request, Response};
use serde::{Deserialize, Serialize};

use crate::entity::Error;
use crate::node::Quorium;

const MAGIC: &[u8; 4] = b"QBAK";
const FORMAT: u8 = 1;

#[derive(Serialize, Deserialize)]
struct Archive {
    shards: Vec<Vec<u8>>,
}

fn internal(e: impl std::fmt::Display) -> Error<Infallible> {
    Error::Internal(e.to_string())
}

impl Quorium {
    pub async fn export(&self, path: &Path) -> Result<(), Error<Infallible>> {
        let mut shards = Vec::new();
        for shard in 0..self.shards() {
            match self.route(|| Request::ExportShard { shard }).await? {
                Response::ShardState(bytes) => shards.push(bytes),
                other => return Err(internal(format!("unexpected response {other:?}"))),
            }
        }
        let body = postcard::to_allocvec(&Archive { shards }).map_err(internal)?;
        let mut bytes = MAGIC.to_vec();
        bytes.push(FORMAT);
        bytes.extend_from_slice(&crc32fast::hash(&body).to_le_bytes());
        bytes.extend_from_slice(&body);
        let tmp = path.with_extension("partial");
        let mut file = std::fs::File::create(&tmp).map_err(internal)?;
        file.write_all(&bytes).map_err(internal)?;
        file.sync_all().map_err(internal)?;
        std::fs::rename(&tmp, path).map_err(internal)?;
        Ok(())
    }

    pub async fn import(&self, path: &Path) -> Result<(), Error<Infallible>> {
        let bytes = std::fs::read(path).map_err(internal)?;
        if bytes.len() < 9 || &bytes[..4] != MAGIC || bytes[4] != FORMAT {
            return Err(internal("not a quorium backup archive"));
        }
        let crc = u32::from_le_bytes(bytes[5..9].try_into().expect("four bytes"));
        if crc32fast::hash(&bytes[9..]) != crc {
            return Err(internal("backup archive checksum mismatch"));
        }
        let archive: Archive = postcard::from_bytes(&bytes[9..]).map_err(internal)?;
        if archive.shards.len() as u64 != self.shards() {
            return Err(internal(format!(
                "archive has {} shards but this cluster has {}",
                archive.shards.len(),
                self.shards()
            )));
        }
        for (shard, state) in archive.shards.into_iter().enumerate() {
            let (cmd, _in_flight) = self.next_command(KvOp::Restore { state, shard: shard as u64 });
            match self.route(|| Request::Write(cmd.clone())).await? {
                Response::Written(Reply::Done) => {}
                Response::Written(Reply::ApplyFailed(e)) => return Err(internal(e)),
                other => return Err(internal(format!("unexpected response {other:?}"))),
            }
        }
        Ok(())
    }
}
