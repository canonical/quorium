use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use quorium::Client;
use quorium_sim::checkers::linearizability::{History, Register, RegisterOp, RegisterRet, check_key};
use quorium_sim::rng::SimRng;

fn free_addrs(n: usize) -> Vec<SocketAddr> {
    quorium::testing::free_addrs(n)
}

struct NodeProcess(Child);

impl std::ops::Deref for NodeProcess {
    type Target = Child;

    fn deref(&self) -> &Child {
        &self.0
    }
}

impl std::ops::DerefMut for NodeProcess {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.0
    }
}

impl Drop for NodeProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn spawn_node(dir: &Path, addr: SocketAddr, seeds: &[SocketAddr], dc: &str) -> NodeProcess {
    let seeds: Vec<String> = seeds.iter().map(SocketAddr::to_string).collect();
    let mut child = Command::new(env!("CARGO_BIN_EXE_quorium-node"))
        .args(["--data-dir", dir.to_str().unwrap(), "--bind", &addr.to_string(), "--seeds", &seeds.join(",")])
        .args(["--dc", dc, "--insecure-plaintext", "--checkpoint-every", "50"])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut line = String::new();
    BufReader::new(child.stdout.take().unwrap()).read_line(&mut line).unwrap();
    let node = NodeProcess(child);
    assert!(line.starts_with("ready"), "node failed to start: {line}");
    node
}

fn encode(v: u64) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

fn decode(v: &[u8]) -> Option<u64> {
    v.try_into().ok().map(u64::from_be_bytes)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn five_processes_stay_linearizable_under_kill_9() {
    let dcs = ["a", "b", "c", "a", "b"];
    let dirs: Vec<_> = (0..5).map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs = free_addrs(5);
    let mut children: Vec<Option<NodeProcess>> =
        (0..5).map(|i| Some(spawn_node(dirs[i].path(), addrs[i], &addrs, dcs[i]))).collect();
    let probe = Client::new(addrs.clone(), None).with_timeouts(Duration::from_secs(20), Duration::from_secs(1));
    probe.write("warmup", encode(0)).await.unwrap();

    let epoch = Instant::now();
    let micros = move || epoch.elapsed().as_micros() as u64;
    let histories: Arc<Mutex<Vec<History<Register>>>> = Arc::new(Mutex::new((0..3).map(|_| History::new()).collect()));
    let acked = Arc::new(Mutex::new(0u64));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut workers = Vec::new();
    for w in 0..6u64 {
        let histories = histories.clone();
        let acked = acked.clone();
        let addrs = addrs.clone();
        let stop = stop.clone();
        workers.push(tokio::spawn(async move {
            let client = Client::new(addrs, None).with_timeouts(Duration::from_secs(3), Duration::from_millis(500));
            let mut rng = SimRng::new(w);
            let mut seq = 0u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let key = rng.range(0, 3) as usize;
                let name = format!("k{key}");
                let call = micros();
                if rng.chance(0.5) {
                    seq += 1;
                    let value = (w << 32) | seq;
                    let result = client.write(&name, encode(value)).await;
                    let ret = micros();
                    let completed = result.is_ok().then_some((ret, RegisterRet::Ok));
                    if result.is_ok() {
                        *acked.lock().unwrap() += 1;
                    }
                    histories.lock().unwrap()[key].record(w, RegisterOp::Write(value), call, completed);
                } else if let Ok(v) = client.read(&name).await {
                    let ret = micros();
                    let value = v.as_deref().and_then(decode);
                    histories.lock().unwrap()[key].record(
                        w,
                        RegisterOp::Read,
                        call,
                        Some((ret, RegisterRet::Value(value))),
                    );
                }
                tokio::time::sleep(Duration::from_millis(rng.range(5, 30))).await;
            }
        }));
    }

    let mut chaos = SimRng::new(42);
    let mut kills = 0;
    let mut pauses = 0;
    let started = Instant::now();
    while (kills < 3 || pauses < 2 || started.elapsed() < Duration::from_secs(13))
        && started.elapsed() < Duration::from_secs(60)
    {
        tokio::time::sleep(Duration::from_millis(chaos.range(800, 1600))).await;
        let victim = chaos.range(0, 5) as usize;
        if chaos.chance(0.4) {
            if let Some(child) = children[victim].as_ref() {
                let pid = child.id().to_string();
                Command::new("kill").args(["-STOP", &pid]).status().unwrap();
                tokio::time::sleep(Duration::from_millis(chaos.range(300, 2000))).await;
                Command::new("kill").args(["-CONT", &pid]).status().unwrap();
                pauses += 1;
            }
            continue;
        }
        if let Some(mut child) = children[victim].take() {
            child.kill().unwrap();
            child.wait().unwrap();
            kills += 1;
            tokio::time::sleep(Duration::from_millis(chaos.range(200, 800))).await;
            let (dir, addr) = (dirs[victim].path().to_path_buf(), addrs[victim]);
            let all = addrs.clone();
            let dc = dcs[victim];
            children[victim] =
                Some(tokio::task::spawn_blocking(move || spawn_node(&dir, addr, &all, dc)).await.unwrap());
        }
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    for w in workers {
        w.await.unwrap();
    }
    probe.write("final", encode(1)).await.unwrap();

    let acked = *acked.lock().unwrap();
    let ops: usize = histories.lock().unwrap().iter().map(|h| h.len()).sum();
    eprintln!("kills {kills}, pauses {pauses}, acknowledged writes {acked}, recorded operations {ops}");
    assert!(kills >= 3, "only {kills} kills");
    assert!(pauses >= 2, "only {pauses} pauses");
    assert!(acked > 50, "only {acked} acknowledged writes");
    for (i, h) in histories.lock().unwrap().iter().enumerate() {
        check_key(&format!("k{i}"), h).unwrap();
    }
    for c in children.iter_mut().flatten() {
        let _ = c.kill();
        let _ = c.wait();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn whole_cluster_power_loss_keeps_acknowledged_writes() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let addrs = free_addrs(3);
    let dcs = ["a", "b", "c"];
    let mut children: Vec<NodeProcess> = (0..3).map(|i| spawn_node(dirs[i].path(), addrs[i], &addrs, dcs[i])).collect();
    let client = Client::new(addrs.clone(), None).with_timeouts(Duration::from_secs(20), Duration::from_secs(1));
    for i in 0..200u64 {
        client.write(&format!("k{}", i % 7), encode(i)).await.unwrap();
    }
    for c in children.iter_mut() {
        c.kill().unwrap();
    }
    for c in children.iter_mut() {
        c.wait().unwrap();
    }
    let mut children: Vec<NodeProcess> =
        [2usize, 0, 1].iter().map(|&i| spawn_node(dirs[i].path(), addrs[i], &addrs, dcs[i])).collect();
    let client = Client::new(addrs.clone(), None).with_timeouts(Duration::from_secs(20), Duration::from_secs(1));
    for k in 0..7u64 {
        let last = (0..200u64).filter(|i| i % 7 == k).max().unwrap();
        assert_eq!(client.read(&format!("k{k}")).await.unwrap(), Some(encode(last)));
    }
    for c in children.iter_mut() {
        let _ = c.kill();
        let _ = c.wait();
    }
}
