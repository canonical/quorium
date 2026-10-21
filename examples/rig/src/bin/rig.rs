use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use quorium::{Client, TlsConfig, TlsMaterial};
use quorium_sim::checkers::linearizability::{History, Register, RegisterOp, RegisterRet, check_key};
use quorium_sim::rng::SimRng;
use rig::{Event, Region, boot_cleanup_commands, check_bank, check_sales, parse_events, kill_time_ms, log_limit_commands, netem_commands, partition_commands};

const PORT: u16 = 7000;
const IMAGE: &str = "ubuntu:24.04";
const STOCK: u64 = 2_000;
const BANK_TOTAL: i64 = 1_000;
const KEYS: usize = 3;
const LOG_MB: u32 = 64;

fn arg(args: &[String], name: &str) -> Option<String> {
    args.iter().position(|a| a == name).and_then(|i| args.get(i + 1)).cloned()
}

fn region(i: usize) -> Region {
    [Region::A, Region::B, Region::C][i % 3]
}

static PREFIX: std::sync::OnceLock<String> = std::sync::OnceLock::new();

fn name(i: usize) -> String {
    format!("{}{}", PREFIX.get().map_or("qr", String::as_str), i + 1)
}

fn lxc(args: &[&str]) -> Result<String, String> {
    let mut attempt = 0;
    loop {
        let out = Command::new("sudo").arg("lxc").args(args).output().map_err(|e| e.to_string())?;
        if out.status.success() {
            return Ok(String::from_utf8_lossy(&out.stdout).into_owned());
        }
        let err = String::from_utf8_lossy(&out.stderr).trim().to_string();
        let transient = err.contains("context deadline exceeded") || err.contains("Failed beginning transaction");
        attempt += 1;
        if !transient || attempt >= 6 {
            return Err(format!("lxc {}: {err}", args.join(" ")));
        }
        std::thread::sleep(Duration::from_millis(500 * attempt));
    }
}

fn exec(node: &str, script: &str) -> Result<String, String> {
    lxc(&["exec", node, "--", "sh", "-c", script])
}

fn pull(node: &str, path: &str) -> Result<String, String> {
    let local = std::env::temp_dir().join(format!("rig-{node}-{}", path.replace('/', "_")));
    let _ = std::fs::remove_file(&local);
    lxc(&["file", "pull", &format!("{node}{path}"), &local.display().to_string()])?;
    let text = std::fs::read_to_string(&local).map_err(|e| e.to_string())?;
    let _ = std::fs::remove_file(&local);
    Ok(text)
}

fn capture(node: &str, script: &str) -> Result<String, String> {
    exec(node, &format!("({script}) > /tmp/rig-capture 2>&1; true"))?;
    pull(node, "/tmp/rig-capture")
}

struct Rig {
    dir: PathBuf,
    nodes: usize,
    ips: Vec<String>,
    transport: quorium::Transport,
    binary: PathBuf,
    heartbeat_ms: u64,
}

impl Rig {
    fn new(dir: &Path, nodes: usize) -> Rig {
        Rig {
            dir: dir.to_path_buf(),
            nodes,
            ips: Vec::new(),
            transport: quorium::Transport::default(),
            binary: PathBuf::from("target/release/rig-node"),
            heartbeat_ms: 150,
        }
    }

    fn ip_of(&self, i: usize) -> Result<String, String> {
        for _ in 0..120 {
            let out = lxc(&["list", &name(i), "-c", "4", "--format", "csv"])?;
            if let Some(ip) = out.split_whitespace().next().filter(|s| s.contains('.')) {
                return Ok(ip.to_string());
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        Err(format!("{} never got an address", name(i)))
    }

    fn addrs(&self) -> Vec<SocketAddr> {
        self.ips.iter().map(|ip| format!("{ip}:{PORT}").parse().expect("valid address")).collect()
    }

    fn tls_material(&self) -> Result<(), String> {
        if self.dir.join("ca.pem").exists() {
            return Ok(());
        }
        std::fs::create_dir_all(&self.dir).map_err(|e| e.to_string())?;
        let ca = TlsMaterial::generate_ca("quorium-rig").map_err(|e| e.to_string())?;
        let node = ca.issue("rig").map_err(|e| e.to_string())?;
        let write = |f: &str, text: &str| std::fs::write(self.dir.join(f), text).map_err(|e| e.to_string());
        write("ca.pem", &ca.cert_pem)?;
        write("node.pem", &node.cert_pem)?;
        write("node.key", &node.key_pem)
    }

    fn client_tls(&self) -> TlsConfig {
        let read = |f: &str| std::fs::read_to_string(self.dir.join(f)).expect("rig TLS material");
        TlsConfig {
            ca_pem: read("ca.pem"),
            cert_pem: read("node.pem"),
            key_pem: read("node.key"),
            server_name: "rig".into(),
        }
    }

    fn up(&mut self) -> Result<(), String> {
        self.tls_material()?;
        for i in 0..self.nodes {
            let n = name(i);
            if lxc(&["info", &n]).is_err() {
                lxc(&["launch", IMAGE, &n, "-c", "limits.memory=1GiB", "-c", "limits.cpu=2"])?;
            }
            let _ = lxc(&["start", &n]);
        }
        self.ips = (0..self.nodes).map(|i| self.ip_of(i)).collect::<Result<_, _>>()?;
        for i in 0..self.nodes {
            let n = name(i);
            exec(&n, "mkdir -p /etc/rig /var/lib/rig /var/log/rig")?;
            exec(&n, &log_limit_commands(LOG_MB).join(" && "))?;
            exec(&n, &boot_cleanup_commands().join(" && "))?;
            for f in ["ca.pem", "node.pem", "node.key"] {
                lxc(&["file", "push", &self.dir.join(f).display().to_string(), &format!("{n}/etc/rig/{f}")])?;
            }
            self.shape(i, 0)?;
        }
        std::fs::write(self.dir.join("ips"), self.ips.join("\n")).map_err(|e| e.to_string())
    }

    fn load(&mut self) -> Result<(), String> {
        let text = std::fs::read_to_string(self.dir.join("ips")).map_err(|_| "run `rig up` first".to_string())?;
        self.ips = text.lines().map(str::to_string).collect();
        self.nodes = self.ips.len();
        Ok(())
    }

    fn shape(&self, i: usize, loss: u32) -> Result<(), String> {
        let peers: Vec<(String, Region)> =
            self.ips.iter().enumerate().filter(|(j, _)| *j != i).map(|(j, ip)| (ip.clone(), region(j))).collect();
        let script =
            format!("tc qdisc del dev eth0 root 2>/dev/null; {}", netem_commands(region(i), &peers, loss).join(" && "));
        exec(&name(i), &script).map(|_| ())
    }

    fn start(&self, i: usize, seed: u64) -> Result<(), String> {
        let seeds: Vec<String> = self.ips.iter().map(|ip| format!("{ip}:{PORT}")).collect();
        let mut cmd = format!(
            "systemctl reset-failed rig-node 2>/dev/null; systemd-run --unit=rig-node --collect --setenv=RUST_LOG={log} /usr/local/bin/rig-node \
             --data-dir /var/lib/rig --bind {}:{PORT} --seeds {} --dc {} --tls-dir /etc/rig --log-dir /var/log/rig \
             --shards 2 --checkpoint-every 200 --heartbeat-ms {heartbeat} --seed {seed} --bank --sales --ledger --transport {}",
            self.ips[i],
            seeds.join(","),
            region(i).name(),
            if self.transport == quorium::Transport::Tcp { "tcp" } else { "quic" },
            heartbeat = self.heartbeat_ms,
            log = std::env::var("RIG_NODE_LOG").unwrap_or_else(|_| "quorium=info,quorium_core=info".into()),
        );
        if i == 0 {
            cmd.push_str(&format!(" --open-bank --refill {STOCK}"));
        }
        exec(&name(i), &cmd).map(|_| ())
    }

    fn kill(&self, i: usize) -> Result<(), String> {
        exec(&name(i), "systemctl kill -s KILL rig-node; sleep 0.2; systemctl reset-failed rig-node 2>/dev/null; true")
            .map(|_| ())
    }

    fn kill_timed(&self, i: usize) -> Result<u64, String> {
        let out = capture(
            &name(i),
            "date +%s%3N; systemctl kill -s KILL rig-node",
        )?;
        kill_time_ms(&out).ok_or_else(|| format!("no kill time in {out:?}"))
    }

    fn signal(&self, i: usize, sig: &str) -> Result<(), String> {
        exec(&name(i), &format!("systemctl kill -s {sig} rig-node")).map(|_| ())
    }

    fn install(&self, i: usize) -> Result<(), String> {
        self.install_from(i, &self.binary)
    }

    fn install_from(&self, i: usize, binary: &Path) -> Result<(), String> {
        let local = Command::new("sha256sum").arg(binary).output().map_err(|e| e.to_string())?;
        let want = String::from_utf8_lossy(&local.stdout).split_whitespace().next().unwrap_or_default().to_string();
        exec(&name(i), "rm -f /usr/local/bin/rig-node")?;
        lxc(&["file", "push", &binary.display().to_string(), &format!("{}/usr/local/bin/rig-node", name(i))])?;
        let got = capture(&name(i), "sha256sum /usr/local/bin/rig-node")?;
        if got.split_whitespace().next() != Some(want.as_str()) || want.is_empty() {
            return Err(format!("{} runs a different rig-node ({got:?}, want {want})", name(i)));
        }
        Ok(())
    }

    fn wipe(&self) -> Result<(), String> {
        for i in 0..self.nodes {
            let _ = self.signal(i, "CONT");
            self.kill(i)?;
            self.install(i)?;
            exec(&name(i), "iptables -F; rm -rf /var/lib/rig/* /var/log/rig/*")?;
            self.shape(i, 0)?;
        }
        Ok(())
    }

    fn partition(&self, side: &[usize]) -> Result<(), String> {
        for i in 0..self.nodes {
            let others: Vec<String> = (0..self.nodes)
                .filter(|j| side.contains(&i) != side.contains(j))
                .map(|j| self.ips[j].clone())
                .collect();
            if !others.is_empty() {
                exec(&name(i), &partition_commands(&others).join(" && "))?;
            }
        }
        Ok(())
    }

    fn heal(&self) -> Result<(), String> {
        for i in 0..self.nodes {
            exec(&name(i), "iptables -F")?;
        }
        Ok(())
    }

    fn power_cycle(&self, i: usize, seed: u64) -> Result<(), String> {
        lxc(&["stop", "--force", &name(i)])?;
        std::thread::sleep(Duration::from_millis(500));
        lxc(&["start", &name(i)])?;
        let ip = self.ip_of(i)?;
        if ip != self.ips[i] {
            return Err(format!("{} came back with a new address {ip}", name(i)));
        }
        self.shape(i, 0)?;
        self.start(i, seed)
    }

    fn running(&self, i: usize) -> bool {
        capture(&name(i), "systemctl is-active rig-node").is_ok_and(|s| s.trim() == "active")
    }

    fn resources(&self) -> Vec<String> {
        (0..self.nodes)
            .map(|i| {
                capture(
                    &name(i),
                    "echo rss_kb=$(ps -o rss= -C rig-node | head -1) disk_bytes=$(du -sb /var/lib/rig | cut -f1)",
                )
                .unwrap_or_default()
                .trim()
                .to_string()
            })
            .collect()
    }

    fn terms(&self) -> Vec<String> {
        (0..self.nodes)
            .map(|i| {
                capture(
                    &name(i),
                    "curl -s localhost:9100/metrics | grep -E '^quorium_shard_(term|leader)' | tr '\\n' ' '",
                )
                .unwrap_or_default()
            })
            .collect()
    }

    fn collect(&self, file: &str) -> Result<Vec<Event>, String> {
        let mut events = Vec::new();
        for i in 0..self.nodes {
            exec(&name(i), &format!("touch /var/log/rig/{file}"))?;
            let text = pull(&name(i), &format!("/var/log/rig/{file}"))?;
            events.extend(parse_events(&text).map_err(|e| format!("{} {file}: {e}", name(i)))?);
        }
        Ok(events)
    }
}

#[derive(Default)]
struct Faults {
    kills: AtomicU64,
    pauses: AtomicU64,
    partitions: AtomicU64,
    power: AtomicU64,
    loss: AtomicU64,
}

fn nemesis(rig: &Rig, kinds: &[String], seed: u64, until: Instant, faults: &Faults) -> Result<(), String> {
    let mut rng = SimRng::new(seed ^ 0x6e6d);
    let n = rig.nodes;
    while Instant::now() + Duration::from_secs(5) < until {
        std::thread::sleep(Duration::from_millis(rng.range(1_000, 4_000)));
        let Some(kind) = rng.pick(kinds).cloned() else { continue };
        let victim = rng.range(0, n as u64) as usize;
        let hold = Duration::from_millis(rng.range(500, 4_000));
        match kind.as_str() {
            "kill" => {
                rig.kill(victim)?;
                std::thread::sleep(hold);
                rig.start(victim, seed + victim as u64)?;
                faults.kills.fetch_add(1, Ordering::Relaxed);
            }
            "pause" => {
                rig.signal(victim, "STOP")?;
                std::thread::sleep(hold);
                rig.signal(victim, "CONT")?;
                faults.pauses.fetch_add(1, Ordering::Relaxed);
            }
            "partition" => {
                let mut order: Vec<usize> = (0..n).collect();
                rng.shuffle(&mut order);
                let minority = rng.range(1, (n as u64).div_ceil(2)) as usize;
                rig.partition(&order[..minority])?;
                std::thread::sleep(hold * 2);
                rig.heal()?;
                faults.partitions.fetch_add(1, Ordering::Relaxed);
            }
            "power" => {
                rig.power_cycle(victim, seed + victim as u64)?;
                faults.power.fetch_add(1, Ordering::Relaxed);
            }
            "loss" => {
                let percent = rng.range(5, 25) as u32;
                for i in 0..n {
                    rig.shape(i, percent)?;
                }
                std::thread::sleep(hold * 2);
                for i in 0..n {
                    rig.shape(i, 0)?;
                }
                faults.loss.fetch_add(1, Ordering::Relaxed);
            }
            other => return Err(format!("unknown fault {other}")),
        }
    }
    Ok(())
}

fn encode(v: u64) -> Vec<u8> {
    v.to_be_bytes().to_vec()
}

fn decode(v: &[u8]) -> Option<u64> {
    v.try_into().ok().map(u64::from_be_bytes)
}

fn leader_of(rig: &Rig, shard: u64) -> Option<usize> {
    let needle = format!("quorium_shard_leader{{shard=\"{shard}\"}} 1");
    (0..rig.nodes).find(|i| capture(&name(*i), "curl -s localhost:9100/metrics").is_ok_and(|m| m.contains(&needle)))
}

async fn failover(rig: Arc<Rig>, samples: usize, seed: u64) -> Result<serde_json::Value, String> {
    rig.wipe()?;
    for i in 0..rig.nodes {
        rig.start(i, seed + i as u64)?;
    }
    let key = (0..).map(|i| format!("fo{i}")).find(|k| quorium_core::shard::shard_of(k.as_bytes(), 2) == 0).unwrap();
    let client = Arc::new(
        Client::new(rig.addrs(), Some(rig.client_tls()))
            .with_transport(rig.transport)
            .with_timeouts(Duration::from_secs(30), Duration::from_millis(300)),
    );
    client.write(&key, encode(0)).await.map_err(|e| format!("cluster never formed: {e}"))?;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let mut gaps = Vec::new();
    let mut events = Vec::new();
    for n in 0..samples {
        let r = rig.clone();
        let Some(victim) = tokio::task::spawn_blocking(move || leader_of(&r, 0)).await.map_err(|e| e.to_string())?
        else {
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        };
        let r = rig.clone();
        let killed_wall = tokio::task::spawn_blocking(move || r.kill_timed(victim)).await.map_err(|e| e.to_string())??;
        let mut v = n as u64;
        loop {
            v += 1_000_000;
            if client.write(&key, encode(v)).await.is_ok() {
                break;
            }
        }
        let gap = wall_ms().saturating_sub(killed_wall);
        gaps.push(gap);
        events.push(serde_json::json!({"victim": name(victim), "killed_at_ms": killed_wall, "gap_ms": gap}));
        let r = rig.clone();
        tokio::task::spawn_blocking(move || r.start(victim, seed + victim as u64))
            .await
            .map_err(|e| e.to_string())??;
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
    let mut sorted = gaps.clone();
    sorted.sort_unstable();
    let pct = |p: f64| sorted.get(((sorted.len() as f64 * p) as usize).min(sorted.len().saturating_sub(1))).copied();
    Ok(serde_json::json!({
        "samples": gaps.len(),
        "failover_ms": gaps,
        "p50_ms": pct(0.5),
        "p99_ms": pct(0.99),
        "max_ms": sorted.last(),
        "events": events,
    }))
}

async fn load(rig: Arc<Rig>, levels: Vec<usize>, seconds: u64, seed: u64) -> Result<serde_json::Value, String> {
    rig.wipe()?;
    for i in 0..rig.nodes {
        rig.start(i, seed + i as u64)?;
    }
    let probe = Client::new(rig.addrs(), Some(rig.client_tls()))
        .with_transport(rig.transport)
        .with_timeouts(Duration::from_secs(30), Duration::from_secs(1));
    probe.write("load-ready", encode(0)).await.map_err(|e| format!("cluster never formed: {e}"))?;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let mut results = Vec::new();
    for clients in levels {
        let stop = Instant::now() + Duration::from_secs(seconds);
        let mut tasks = Vec::new();
        for c in 0..clients {
            let client = Client::new(rig.addrs(), Some(rig.client_tls()))
                .with_transport(rig.transport)
                .with_timeouts(Duration::from_secs(10), Duration::from_secs(1));
            tasks.push(tokio::spawn(async move {
                let mut latencies = Vec::new();
                let mut errors = 0u64;
                let mut n = 0u64;
                while Instant::now() < stop {
                    let started = Instant::now();
                    match client.write(&format!("load-{c}-{}", n % 8), encode(n)).await {
                        Ok(_) => latencies.push(started.elapsed().as_micros() as u64),
                        Err(_) => errors += 1,
                    }
                    n += 1;
                }
                (latencies, errors)
            }));
        }
        let mut latencies = Vec::new();
        let mut errors = 0;
        for t in tasks {
            let (l, e) = t.await.map_err(|e| e.to_string())?;
            latencies.extend(l);
            errors += e;
        }
        latencies.sort_unstable();
        let pct = |p: f64| latencies.get(((latencies.len() as f64 * p) as usize).min(latencies.len().saturating_sub(1))).map_or(0, |v| v / 1000);
        results.push(serde_json::json!({
            "clients": clients,
            "writes_per_second": latencies.len() as u64 / seconds,
            "p50_ms": pct(0.5),
            "p99_ms": pct(0.99),
            "errors": errors,
        }));
        eprintln!("{}", results.last().unwrap());
    }
    Ok(serde_json::json!({ "seconds_per_level": seconds, "levels": results }))
}

async fn run(rig: Arc<Rig>, seconds: u64, kinds: Vec<String>, seed: u64) -> Result<serde_json::Value, String> {
    run_window(rig, seconds, kinds, seed, true, String::new(), None).await
}

fn wall_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

fn roll(rig: &Rig, next: &Path, wait: Duration, seed: u64) -> Result<u64, String> {
    std::thread::sleep(wait);
    let mut last_started = wall_ms();
    for i in 0..rig.nodes {
        rig.kill(i)?;
        rig.install_from(i, next)?;
        last_started = wall_ms();
        rig.start(i, seed + i as u64)?;
        std::thread::sleep(Duration::from_secs(15));
    }
    Ok(last_started)
}

async fn run_window(
    rig: Arc<Rig>,
    seconds: u64,
    kinds: Vec<String>,
    seed: u64,
    fresh: bool,
    prefix: String,
    upgrade: Option<PathBuf>,
) -> Result<serde_json::Value, String> {
    if fresh {
        rig.wipe()?;
        for i in 0..rig.nodes {
            rig.start(i, seed + i as u64)?;
        }
    }
    let addrs = rig.addrs();
    let tls = rig.client_tls();
    let probe =
        Client::new(addrs.clone(), Some(tls.clone())).with_timeouts(Duration::from_secs(60), Duration::from_secs(2));
    let formed = Instant::now();
    probe.write("warmup", encode(0)).await.map_err(|e| format!("cluster never formed: {e}"))?;
    let formed = formed.elapsed();
    let epoch = Instant::now();
    let until = epoch + Duration::from_secs(seconds);
    let micros = move || epoch.elapsed().as_micros() as u64;
    let histories: Arc<Mutex<Vec<History<Register>>>> =
        Arc::new(Mutex::new((0..KEYS).map(|_| History::new()).collect()));
    let acked = Arc::new(AtomicU64::new(0));
    let latencies: Arc<Mutex<Vec<u64>>> = Arc::default();
    let failed = Arc::new(AtomicBool::new(false));
    let mut workers = Vec::new();
    for w in 0..8u64 {
        let (histories, acked, addrs, tls) = (histories.clone(), acked.clone(), addrs.clone(), tls.clone());
        let prefix = prefix.clone();
        let latencies = latencies.clone();
        workers.push(tokio::spawn(async move {
            let client =
                Client::new(addrs, Some(tls)).with_timeouts(Duration::from_secs(4), Duration::from_millis(800));
            let mut rng = SimRng::new(seed ^ (w << 8));
            let mut seq = 0u64;
            while Instant::now() < until {
                let key = rng.range(0, KEYS as u64) as usize;
                let name = format!("{prefix}k{key}");
                let call = micros();
                if rng.chance(0.5) {
                    seq += 1;
                    let value = (w << 32) | seq;
                    let result = client.write(&name, encode(value)).await;
                    let completed = result.is_ok().then_some((micros(), RegisterRet::Ok));
                    if result.is_ok() {
                        acked.fetch_add(1, Ordering::Relaxed);
                        latencies.lock().unwrap().push(micros() - call);
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
                tokio::time::sleep(Duration::from_millis(rng.range(5, 40))).await;
            }
        }));
    }
    let rolling = upgrade.map(|next| {
        let rig = rig.clone();
        let wait = Duration::from_secs(seconds / 4);
        tokio::task::spawn_blocking(move || roll(&rig, &next, wait, seed))
    });
    let faults = Arc::new(Faults::default());
    let chaos = {
        let (rig, faults, failed) = (rig.clone(), faults.clone(), failed.clone());
        tokio::task::spawn_blocking(move || {
            let r = nemesis(&rig, &kinds, seed, until, &faults);
            if r.is_err() {
                failed.store(true, Ordering::Relaxed);
            }
            r
        })
    };
    for w in workers {
        w.await.map_err(|e| e.to_string())?;
    }
    chaos.await.map_err(|e| e.to_string())??;
    let upgraded_at = match rolling {
        Some(handle) => Some(handle.await.map_err(|e| e.to_string())??),
        None => None,
    };
    rig.heal()?;
    for i in 0..rig.nodes {
        let _ = rig.signal(i, "CONT");
        rig.shape(i, 0)?;
        if !rig.running(i) {
            rig.start(i, seed + i as u64)?;
        }
    }
    let recovered = Instant::now();
    probe.write("final", encode(1)).await.map_err(|e| format!("no progress after healing: {e}"))?;
    let recovery = recovered.elapsed();
    tokio::time::sleep(Duration::from_secs(5)).await;
    let mut verdicts = Vec::new();
    let mut ops = 0;
    for (i, h) in histories.lock().unwrap().iter().enumerate() {
        ops += h.len();
        verdicts.push(check_key(&format!("k{i}"), h).map_err(|v| format!("{v:?}")));
    }
    let bank = check_bank(&rig.collect("bank.jsonl")?, BANK_TOTAL);
    let sales = check_sales(&rig.collect("sales.jsonl")?);
    let register_ok = verdicts.iter().all(Result::is_ok);
    let upgrade = match upgraded_at {
        Some(at) => Some(rig::check_upgrade(&rig.collect("ledger.jsonl")?, at)),
        None => None,
    };
    let mut lat = latencies.lock().unwrap().clone();
    lat.sort_unstable();
    let pct = |p: f64| {
        lat.get(((lat.len() as f64 * p) as usize).min(lat.len().saturating_sub(1))).copied().unwrap_or(0) / 1000
    };
    let report = serde_json::json!({
        "seed": seed,
        "seconds": seconds,
        "nodes": rig.nodes,
        "formed_ms": formed.as_millis() as u64,
        "recovery_after_heal_ms": recovery.as_millis() as u64,
        "faults": {
            "kill": faults.kills.load(Ordering::Relaxed),
            "pause": faults.pauses.load(Ordering::Relaxed),
            "partition": faults.partitions.load(Ordering::Relaxed),
            "power": faults.power.load(Ordering::Relaxed),
            "loss": faults.loss.load(Ordering::Relaxed),
        },
        "register": {
            "operations": ops,
            "acknowledged_writes": acked.load(Ordering::Relaxed),
            "write_ms_p50": pct(0.5),
            "write_ms_p99": pct(0.99),
            "linearizable": register_ok,
            "errors": verdicts.iter().filter_map(|v| v.clone().err()).collect::<Vec<_>>(),
        },
        "bank": match &bank { Ok(reads) => serde_json::json!({"ok": true, "reads": reads}), Err(e) => serde_json::json!({"ok": false, "error": e}) },
        "counter": match &sales { Ok((sold, stock)) => serde_json::json!({"ok": true, "sold": sold, "stock": stock}), Err(e) => serde_json::json!({"ok": false, "error": e}) },
        "metrics": rig.terms(),
        "resources": rig.resources(),
        "upgrade": match &upgrade {
            None => serde_json::Value::Null,
            Some(Ok((early, late))) => serde_json::json!({"ok": true, "early": early, "after": late}),
            Some(Err(e)) => serde_json::json!({"ok": false, "error": e}),
        },
        "passed": upgrade.as_ref().is_none_or(Result::is_ok) && register_ok && bank.is_ok() && sales.is_ok() && !failed.load(Ordering::Relaxed),
    });
    Ok(report)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let dir = PathBuf::from(arg(&args, "--dir").unwrap_or_else(|| "target/rig".into()));
    let nodes: usize = arg(&args, "--nodes").and_then(|n| n.parse().ok()).unwrap_or(5);
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
    let _ = PREFIX.set(arg(&args, "--containers").unwrap_or_else(|| "qr".into()));
    let mut rig = Rig::new(&dir, nodes);
    if let Some(b) = arg(&args, "--binary") {
        rig.binary = PathBuf::from(b);
    }
    if let Some(ms) = arg(&args, "--heartbeat-ms").and_then(|v| v.parse().ok()) {
        rig.heartbeat_ms = ms;
    }
    let result = match args.get(1).map(String::as_str) {
        Some("up") => {
            rig.up().map(|_| serde_json::json!({"up": rig.ips}))
        }
        Some("probe") => match rig.load() {
            Ok(()) => {
                if arg(&args, "--transport").as_deref() == Some("tcp") {
                    rig.transport = quorium::Transport::Tcp;
                }
                let client = Client::new(rig.addrs(), Some(rig.client_tls()))
                    .with_transport(rig.transport)
                    .with_timeouts(
                        Duration::from_secs(arg(&args, "--total").and_then(|t| t.parse().ok()).unwrap_or(30)),
                        Duration::from_secs(2),
                    );
                let mut out = Vec::new();
                for i in 0..arg(&args, "--count").and_then(|t| t.parse().ok()).unwrap_or(10u64) {
                    let t = Instant::now();
                    let r = client.write(&format!("probe{i}"), encode(i)).await;
                    out.push(format!("{}ms {:?}", t.elapsed().as_millis(), r.map(|_| ())));
                }
                Ok(serde_json::json!({ "writes": out, "running": (0..rig.nodes).map(|i| rig.running(i)).collect::<Vec<_>>(), "curl": capture("qr1", "which curl; curl -s localhost:9100/metrics | head -3").unwrap_or_default() }))
            }
            Err(e) => Err(e),
        },
        Some("escrow") => match rig.load() {
            Ok(()) => {
                let client = Client::new(rig.addrs(), Some(rig.client_tls()))
                    .with_transport(rig.transport)
                    .with_timeouts(Duration::from_secs(30), Duration::from_secs(2));
                let name = arg(&args, "--name").unwrap_or_else(|| "rig-stock".into());
                match client.read_escrow(&name).await {
                    Ok(view) => Ok(serde_json::json!({ "name": name, "view": format!("{view:?}") })),
                    Err(e) => Err(e.to_string()),
                }
            }
            Err(e) => Err(e),
        },
        Some("reshape") => rig.load().and_then(|_| {
            for i in 0..rig.nodes {
                rig.shape(i, 0)?;
            }
            Ok(serde_json::json!({"reshaped": rig.nodes}))
        }),
        Some("soak") => match rig.load() {
            Ok(()) => {
                let windows: u64 = arg(&args, "--windows").and_then(|s| s.parse().ok()).unwrap_or(72);
                let seconds = arg(&args, "--seconds").and_then(|s| s.parse().ok()).unwrap_or(3600);
                let seed: u64 = arg(&args, "--seed").and_then(|s| s.parse().ok()).unwrap_or(1);
                let kinds: Vec<String> = arg(&args, "--faults")
                    .unwrap_or_else(|| "kill,pause,partition,power,loss".into())
                    .split(',')
                    .filter(|k| !k.is_empty() && *k != "none")
                    .map(str::to_string)
                    .collect();
                let rig = Arc::new(rig);
                let mut failed = 0;
                for w in 0..windows {
                    let report = run_window(
                        rig.clone(),
                        seconds,
                        kinds.clone(),
                        seed + w * 1000,
                        w == 0,
                        format!("w{w}-"),
                        None,
                    )
                    .await;
                    let line = match report {
                        Ok(r) => {
                            if !r.get("passed").and_then(serde_json::Value::as_bool).unwrap_or(false) {
                                failed += 1;
                            }
                            serde_json::json!({ "window": w, "report": r })
                        }
                        Err(e) => {
                            failed += 1;
                            serde_json::json!({ "window": w, "error": e })
                        }
                    };
                    println!("{line}");
                }
                Ok(serde_json::json!({ "windows": windows, "failed": failed, "passed": failed == 0 }))
            }
            Err(e) => Err(e),
        },
        Some("upgrade") => match rig.load() {
            Ok(()) => {
                let next = PathBuf::from(arg(&args, "--next-binary").unwrap_or_else(|| "target/release/rig-node-next".into()));
                let seconds = arg(&args, "--seconds").and_then(|s| s.parse().ok()).unwrap_or(240);
                let seed = arg(&args, "--seed").and_then(|s| s.parse().ok()).unwrap_or(1);
                let kinds: Vec<String> = arg(&args, "--faults")
                    .unwrap_or_else(|| "pause,partition,loss".into())
                    .split(',')
                    .filter(|k| !k.is_empty() && *k != "none")
                    .map(str::to_string)
                    .collect();
                run_window(Arc::new(rig), seconds, kinds, seed, true, String::new(), Some(next)).await
            }
            Err(e) => Err(e),
        },
        Some("load") => match rig.load() {
            Ok(()) => {
                let levels = arg(&args, "--clients")
                    .unwrap_or_else(|| "1,8,32,128,512".into())
                    .split(',')
                    .filter_map(|v| v.parse().ok())
                    .collect();
                let seconds = arg(&args, "--seconds").and_then(|s| s.parse().ok()).unwrap_or(30);
                let seed = arg(&args, "--seed").and_then(|s| s.parse().ok()).unwrap_or(1);
                load(Arc::new(rig), levels, seconds, seed).await
            }
            Err(e) => Err(e),
        },
        Some("failover") => match rig.load() {
            Ok(()) => {
                let samples = arg(&args, "--samples").and_then(|s| s.parse().ok()).unwrap_or(20);
                let seed = arg(&args, "--seed").and_then(|s| s.parse().ok()).unwrap_or(1);
                failover(Arc::new(rig), samples, seed).await
            }
            Err(e) => Err(e),
        },
        Some("down") => (0..nodes)
            .map(|i| lxc(&["delete", "--force", &name(i)]).map(|_| ()))
            .collect::<Result<Vec<_>, _>>()
            .map(|_| serde_json::json!({"down": nodes})),
        Some("run") => match rig.load() {
            Ok(()) => {
                if arg(&args, "--transport").as_deref() == Some("tcp") {
                    rig.transport = quorium::Transport::Tcp;
                }
                let seconds = arg(&args, "--seconds").and_then(|s| s.parse().ok()).unwrap_or(60);
                let seed = arg(&args, "--seed").and_then(|s| s.parse().ok()).unwrap_or(1);
                let kinds: Vec<String> = arg(&args, "--faults")
                    .unwrap_or_else(|| "kill,pause,partition,power,loss".into())
                    .split(',')
                    .filter(|k| !k.is_empty() && *k != "none")
                    .map(str::to_string)
                    .collect();
                run(Arc::new(rig), seconds, kinds, seed).await
            }
            Err(e) => Err(e),
        },
        _ => Err("usage: rig <up|down|run> [--dir d] [--nodes n] [--binary path] [--seconds s] [--seed n] [--faults kill,pause,partition,power,loss]".into()),
    };
    match result {
        Ok(report) => {
            println!("{}", serde_json::to_string_pretty(&report).expect("report serializes"));
            let passed = report.get("passed").and_then(serde_json::Value::as_bool).unwrap_or(true);
            std::process::exit(if passed { 0 } else { 1 });
        }
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    }
}
