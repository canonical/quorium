use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = String, name = "rig.Bank")]
pub struct Bank {
    accounts: BTreeMap<u32, i64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BankError {
    Insufficient,
    NoSuchAccount,
}

#[quorium::state]
impl Bank {
    pub fn open(&mut self, accounts: u32, each: i64) -> bool {
        if !self.accounts.is_empty() {
            return false;
        }
        self.accounts = (0..accounts).map(|a| (a, each)).collect();
        true
    }

    pub fn transfer(&mut self, from: u32, to: u32, amount: i64) -> Result<(), BankError> {
        let available = *self.accounts.get(&from).ok_or(BankError::NoSuchAccount)?;
        if !self.accounts.contains_key(&to) {
            return Err(BankError::NoSuchAccount);
        }
        if available < amount {
            return Err(BankError::Insufficient);
        }
        *self.accounts.entry(from).or_default() -= amount;
        *self.accounts.entry(to).or_default() += amount;
        Ok(())
    }

    pub fn balances(&self) -> BTreeMap<u32, i64> {
        self.accounts.clone()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Event {
    BankRead { total: i64, min: i64 },
    Refill { units: u64 },
    Sale { units: u64 },
    Ledger { ok: bool, at_ms: u64 },
}

#[cfg(feature = "next")]
#[derive(quorium::State, Serialize, Deserialize, Default)]
#[state(key = String, name = "rig.Ledger")]
pub struct Ledger {
    entries: u64,
}

#[cfg(feature = "next")]
#[quorium::state]
impl Ledger {
    pub fn record(&mut self) -> u64 {
        self.entries += 1;
        self.entries
    }
}

pub fn check_upgrade(events: &[Event], upgraded_at_ms: u64) -> Result<(u64, u64), String> {
    let mut early = 0;
    let mut late = 0;
    for e in events {
        if let Event::Ledger { ok: true, at_ms } = e {
            if *at_ms < upgraded_at_ms {
                early += 1;
            } else {
                late += 1;
            }
        }
    }
    if early > 0 {
        return Err(format!("{early} writes of the new type were accepted before every node was upgraded"));
    }
    if late == 0 {
        return Err("no write of the new type succeeded after the upgrade".into());
    }
    Ok((early, late))
}

pub fn check_bank(events: &[Event], expected_total: i64) -> Result<usize, String> {
    let mut reads = 0;
    for e in events {
        match e {
            Event::BankRead { total, min } => {
                if *total != expected_total {
                    return Err(format!("bank total {total} differs from {expected_total}"));
                }
                if *min < 0 {
                    return Err(format!("an account went negative: {min}"));
                }
                reads += 1;
            }
            other => return Err(format!("not a bank event: {other:?}")),
        }
    }
    Ok(reads)
}

pub fn check_sales(events: &[Event]) -> Result<(u64, u64), String> {
    let stock: u64 = events.iter().map(|e| if let Event::Refill { units } = e { *units } else { 0 }).sum();
    let sold: u64 = events.iter().map(|e| if let Event::Sale { units } = e { *units } else { 0 }).sum();
    if sold > stock {
        return Err(format!("acknowledged sales {sold} of {stock} units of stock"));
    }
    Ok((sold, stock))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Region {
    A,
    B,
    C,
}

impl Region {
    pub fn name(self) -> &'static str {
        match self {
            Region::A => "region-a",
            Region::B => "region-b",
            Region::C => "region-c",
        }
    }

    fn band(self) -> u32 {
        match self {
            Region::A => 4,
            Region::B => 5,
            Region::C => 6,
        }
    }
}

pub fn one_way_ms(a: Region, b: Region) -> u32 {
    use Region::*;
    match (a.min(b), a.max(b)) {
        (A, B) => 15,
        (A, C) => 30,
        (B, C) => 25,
        _ => 0,
    }
}

pub fn netem_commands(own: Region, peers: &[(String, Region)], loss_percent: u32) -> Vec<String> {
    let mut out = vec![
        "tc qdisc replace dev eth0 root handle 1: prio bands 6 priomap 1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1".to_string(),
    ];
    for region in [Region::A, Region::B, Region::C] {
        let ms = one_way_ms(own, region);
        let delay = if ms == 0 { "delay 0ms".to_string() } else { format!("delay {ms}ms 1ms") };
        let loss = if loss_percent > 0 { format!(" loss {loss_percent}%") } else { String::new() };
        out.push(format!(
            "tc qdisc replace dev eth0 parent 1:{band} handle {band}0: netem {delay}{loss}",
            band = region.band()
        ));
    }
    for (ip, region) in peers {
        out.push(format!(
            "tc filter add dev eth0 parent 1:0 protocol ip prio 1 u32 match ip dst {ip}/32 flowid 1:{}",
            region.band()
        ));
    }
    out
}

pub fn partition_commands(unreachable: &[String]) -> Vec<String> {
    unreachable
        .iter()
        .flat_map(|ip| [format!("iptables -A INPUT -s {ip} -j DROP"), format!("iptables -A OUTPUT -d {ip} -j DROP")])
        .collect()
}

pub fn log_limit_commands(megabytes: u32) -> Vec<String> {
    vec![
        "mkdir -p /etc/systemd/journald.conf.d".to_string(),
        format!(
            "printf '[Journal]\\nSystemMaxUse={megabytes}M\\nRuntimeMaxUse={megabytes}M\\nForwardToSyslog=no\\n' > /etc/systemd/journald.conf.d/rig.conf"
        ),
        "systemctl restart systemd-journald".to_string(),
        "(systemctl disable --now rsyslog 2>/dev/null || true)".to_string(),
        "rm -f /var/log/syslog*".to_string(),
        format!("journalctl --vacuum-size={megabytes}M"),
    ]
}

pub fn kill_time_ms(output: &str) -> Option<u64> {
    output.lines().next()?.trim().parse().ok()
}

pub fn boot_cleanup_commands() -> Vec<String> {
    vec![
        "systemctl mask apparmor.service".to_string(),
        "find /var/cache/apparmor -type f -name '*-??????' -mmin +5 -delete".to_string(),
    ]
}

pub fn parse_events(text: &str) -> Result<Vec<Event>, String> {
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    let mut out = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        let last = i + 1 == lines.len();
        for item in serde_json::Deserializer::from_str(line).into_iter::<Event>() {
            match item {
                Ok(event) => out.push(event),
                Err(e) if e.is_eof() && last => break,
                Err(e) => return Err(format!("line {}: {e}: {line}", i + 1)),
            }
        }
    }
    Ok(out)
}
