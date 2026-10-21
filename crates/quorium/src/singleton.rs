use std::future::Future;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use quorium_core::kv::{KvOp, LeaseAction, LeaseOp, Reply};
use quorium_net::raft::{Request, Response};
use tokio::task::JoinHandle;

use crate::node::Quorium;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lease {
    pub token: u64,
}

pub struct Singleton {
    task: JoinHandle<()>,
}

impl Drop for Singleton {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

impl Quorium {
    async fn lease_op(&self, name: &str, ttl: Duration, action: LeaseAction) -> Option<(bool, u64)> {
        let op = LeaseOp {
            name: name.to_string(),
            holder: self.status().id,
            ttl_ms: ttl.as_millis() as u64,
            now_ms: now_ms(),
            action,
        };
        let (cmd, _in_flight) = self.next_command(KvOp::Lease(op));
        let call = self.route(|| Request::Write(cmd.clone()));
        match tokio::time::timeout(ttl / 3, call).await {
            Ok(Ok(Response::Written(Reply::Lease { held, token, .. }))) => Some((held, token)),
            _ => None,
        }
    }

    pub fn singleton<F, Fut>(&self, name: &str, job: F) -> Singleton
    where
        F: Fn(Lease) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        self.singleton_with(name, Duration::from_secs(5), job)
    }

    pub fn singleton_with<F, Fut>(&self, name: &str, ttl: Duration, job: F) -> Singleton
    where
        F: Fn(Lease) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let node = self.clone();
        let name = name.to_string();
        let task = tokio::spawn(async move {
            loop {
                let Some((true, token)) = node.lease_op(&name, ttl, LeaseAction::Acquire).await else {
                    tokio::time::sleep(ttl / 3).await;
                    continue;
                };
                let running = AbortOnDrop(tokio::spawn(job(Lease { token })));
                let mut confirmed = Instant::now();
                loop {
                    tokio::time::sleep(ttl / 3).await;
                    if running.0.is_finished() {
                        let _ = node.lease_op(&name, ttl, LeaseAction::Release { token }).await;
                        return;
                    }
                    let sent = Instant::now();
                    match node.lease_op(&name, ttl, LeaseAction::Renew { token }).await {
                        Some((true, _)) => confirmed = sent,
                        Some((false, _)) => break,
                        None => {}
                    }
                    if confirmed.elapsed() >= ttl * 2 / 3 {
                        break;
                    }
                }
                drop(running);
            }
        });
        Singleton { task }
    }
}
