use std::fmt::Write;
use std::net::SocketAddr;
use std::sync::Weak;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::node::Quorium;

fn family(out: &mut String, name: &str, kind: &str, help: &str) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
}

impl Quorium {
    pub fn metrics_text(&self) -> String {
        let mut out = String::new();
        let status = self.status();
        family(&mut out, "quorium_node_info", "gauge", "Static facts about this node.");
        let _ = writeln!(out, "quorium_node_info{{datacenter=\"{}\",id=\"{}\"}} 1", self.datacenter(), status.id);
        let rows: Vec<(u64, openraft::RaftMetrics<quorium_net::raft::ProdTypes>, bool)> =
            (0..self.shards()).map(|s| (s, self.shard_metrics(s), self.shard_quiesced(s))).collect();
        family(&mut out, "quorium_shard_leader", "gauge", "1 when this node leads the shard.");
        for (s, m, _) in &rows {
            let _ = writeln!(
                out,
                "quorium_shard_leader{{shard=\"{s}\"}} {}",
                u8::from(m.current_leader == Some(status.id))
            );
        }
        family(&mut out, "quorium_shard_term", "gauge", "Current Raft term of the shard.");
        for (s, m, _) in &rows {
            let _ = writeln!(out, "quorium_shard_term{{shard=\"{s}\"}} {}", m.current_term);
        }
        family(&mut out, "quorium_shard_last_applied", "gauge", "Last log index applied to the shard state.");
        for (s, m, _) in &rows {
            let _ =
                writeln!(out, "quorium_shard_last_applied{{shard=\"{s}\"}} {}", m.last_applied.map_or(0, |l| l.index));
        }
        family(
            &mut out,
            "quorium_shard_writes_per_second",
            "gauge",
            "Writes per second led by this node, last full second.",
        );
        for (s, _, _) in &rows {
            let _ = writeln!(out, "quorium_shard_writes_per_second{{shard=\"{s}\"}} {}", self.shard_write_rate(*s));
        }
        family(&mut out, "quorium_shard_checkpoint_index", "gauge", "Log index of the last durable state checkpoint.");
        for (s, m, _) in &rows {
            let _ =
                writeln!(out, "quorium_shard_checkpoint_index{{shard=\"{s}\"}} {}", m.snapshot.map_or(0, |l| l.index));
        }
        family(&mut out, "quorium_shard_quiesced", "gauge", "1 when the shard is quiesced on this leader.");
        for (s, _, q) in &rows {
            let _ = writeln!(out, "quorium_shard_quiesced{{shard=\"{s}\"}} {}", u8::from(*q));
        }
        let stats = self.stats();
        family(&mut out, "quorium_appends_sent_total", "counter", "Append requests sent to peers.");
        let _ = writeln!(out, "quorium_appends_sent_total {}", stats.appends_sent);
        family(&mut out, "quorium_pings_received_total", "counter", "Host liveness pings received.");
        let _ = writeln!(out, "quorium_pings_received_total {}", stats.pings_received);
        let (fsyncs, bytes) = self.wal_stats();
        family(
            &mut out,
            "quorium_large_entity_writes_total",
            "counter",
            "Applied writes leaving an entity above 1 MiB.",
        );
        let _ = writeln!(out, "quorium_large_entity_writes_total {}", stats.large_entity_writes);
        family(&mut out, "quorium_wal_fsyncs_total", "counter", "Write-ahead log fsyncs.");
        let _ = writeln!(out, "quorium_wal_fsyncs_total {fsyncs}");
        family(&mut out, "quorium_wal_sync_latency_seconds", "gauge", "Smoothed write-ahead log fsync latency.");
        let _ = writeln!(out, "quorium_wal_sync_latency_seconds {}", self.inner_wal_sync_latency().as_secs_f64());
        family(&mut out, "quorium_wal_bytes", "gauge", "Size of the write-ahead log on disk.");
        let _ = writeln!(out, "quorium_wal_bytes {bytes}");
        out
    }
}

pub(crate) async fn serve_metrics(addr: SocketAddr, node: Weak<crate::node::Inner>) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else { continue };
            let Some(inner) = node.upgrade() else { return };
            let body = Quorium::from_inner(inner).metrics_text();
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let _ = sock.read(&mut buf).await;
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let _ = sock.write_all(head.as_bytes()).await;
                let _ = sock.write_all(body.as_bytes()).await;
                let _ = sock.shutdown().await;
            });
        }
    });
    Ok(())
}
