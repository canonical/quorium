use std::time::Duration;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct Thresholds {
    pub min_seeds: u64,
    pub max_violations: u64,
    pub failover_p99_ms: u64,
    pub planted_controls: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct Measured {
    pub seeds: u64,
    pub violations: u64,
    pub failover_p99_single: Duration,
    pub failover_p99_three: Duration,
    pub planted_caught: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Check {
    pub name: String,
    pub measured: String,
    pub threshold: String,
    pub pass: bool,
}

#[derive(Debug, Clone)]
pub struct Report {
    pub checks: Vec<Check>,
}

impl Report {
    pub fn passed(&self) -> bool {
        self.checks.iter().all(|c| c.pass)
    }

    pub fn to_json(&self) -> String {
        let checks: Vec<String> = self
            .checks
            .iter()
            .map(|c| {
                format!(
                    "{{\"name\":\"{}\",\"measured\":\"{}\",\"threshold\":\"{}\",\"pass\":{}}}",
                    escape(&c.name),
                    escape(&c.measured),
                    escape(&c.threshold),
                    c.pass
                )
            })
            .collect();
        format!("{{\"passed\":{},\"checks\":[{}]}}", self.passed(), checks.join(","))
    }
}

fn escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

fn check(name: &str, measured: impl ToString, threshold: impl ToString, pass: bool) -> Check {
    Check { name: name.into(), measured: measured.to_string(), threshold: threshold.to_string(), pass }
}

pub fn evaluate(t: &Thresholds, m: &Measured) -> Report {
    let p99 = Duration::from_millis(t.failover_p99_ms);
    let missing: Vec<&String> = t.planted_controls.iter().filter(|c| !m.planted_caught.contains(c)).collect();
    Report {
        checks: vec![
            check("seeds", m.seeds, format!(">= {}", t.min_seeds), m.seeds >= t.min_seeds),
            check("violations", m.violations, format!("<= {}", t.max_violations), m.violations <= t.max_violations),
            check(
                "failover_p99_single_region",
                format!("{}ms", m.failover_p99_single.as_millis()),
                format!("< {}ms", t.failover_p99_ms),
                m.failover_p99_single < p99,
            ),
            check(
                "failover_p99_three_regions",
                format!("{}ms", m.failover_p99_three.as_millis()),
                format!("< {}ms", t.failover_p99_ms),
                m.failover_p99_three < p99,
            ),
            check(
                "planted_controls",
                format!("caught {:?}", m.planted_caught),
                format!("all of {:?}", t.planted_controls),
                missing.is_empty(),
            ),
        ],
    }
}
