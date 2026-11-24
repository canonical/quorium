use std::fmt;
use std::time::Duration;

use openraft::Config;

use crate::ladder::LadderConfig;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimingConfig {
    pub heartbeat: Duration,
    pub leader_lease: Option<Duration>,
    pub rpc_timeout: Duration,
    pub snapshot_timeout: Duration,
    pub ladder: Option<LadderConfig>,
    pub write_wait: Duration,
}

impl Default for TimingConfig {
    fn default() -> Self {
        Self {
            heartbeat: Duration::from_millis(50),
            leader_lease: None,
            rpc_timeout: Duration::from_secs(1),
            snapshot_timeout: Duration::from_secs(60),
            ladder: None,
            write_wait: Duration::from_secs(2),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timings {
    pub heartbeat: Duration,
    pub leader_lease: Duration,
    pub rpc_timeout: Duration,
    pub snapshot_timeout: Duration,
    pub ladder: LadderConfig,
    pub write_wait: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FailoverEstimate {
    pub detection: Duration,
    pub ranking: Duration,
    pub campaign: Duration,
    pub establish: Duration,
    pub total: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimingError {
    HeartbeatTooShort { heartbeat: Duration, minimum: Duration },
    LeaseTooShort { lease: Duration, minimum: Duration },
    BaseTooShort { base: Duration, minimum: Duration },
    CampaignTimeoutAboveMax { campaign_timeout: Duration, max_campaign: Duration },
    ZeroRpcTimeout,
    ZeroSnapshotTimeout,
    ZeroWriteWait,
}

impl fmt::Display for TimingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TimingError::HeartbeatTooShort { heartbeat, minimum } => {
                write!(f, "heartbeat {heartbeat:?} is below the minimum of {minimum:?}")
            }
            TimingError::LeaseTooShort { lease, minimum } => {
                write!(f, "leader lease {lease:?} must be at least twice the heartbeat ({minimum:?})")
            }
            TimingError::BaseTooShort { base, minimum } => write!(
                f,
                "ladder base {base:?} must be at least the leader lease plus one heartbeat ({minimum:?}), \
                 or followers still honour the old leader when the first candidate stands"
            ),
            TimingError::CampaignTimeoutAboveMax { campaign_timeout, max_campaign } => {
                write!(f, "ladder campaign timeout {campaign_timeout:?} exceeds its maximum {max_campaign:?}")
            }
            TimingError::ZeroRpcTimeout => f.write_str("rpc timeout must be above zero"),
            TimingError::ZeroSnapshotTimeout => f.write_str("snapshot timeout must be above zero"),
            TimingError::ZeroWriteWait => f.write_str("write wait must be above zero"),
        }
    }
}

impl std::error::Error for TimingError {}

pub const MIN_HEARTBEAT: Duration = Duration::from_millis(2);

impl TimingConfig {
    pub fn derive(&self) -> Result<Timings, TimingError> {
        let heartbeat = self.heartbeat;
        if heartbeat < MIN_HEARTBEAT {
            return Err(TimingError::HeartbeatTooShort { heartbeat, minimum: MIN_HEARTBEAT });
        }
        let heartbeat = Duration::from_millis(heartbeat.as_millis() as u64);
        let lease_floor = heartbeat * 2;
        let leader_lease = self.leader_lease.map_or(lease_floor, |l| Duration::from_millis(l.as_millis() as u64));
        if leader_lease < lease_floor {
            return Err(TimingError::LeaseTooShort { lease: leader_lease, minimum: lease_floor });
        }
        let base_floor = leader_lease + heartbeat;
        let ladder = match &self.ladder {
            Some(ladder) if ladder.base < base_floor => {
                return Err(TimingError::BaseTooShort { base: ladder.base, minimum: base_floor });
            }
            Some(ladder) => ladder.clone(),
            None => {
                let ladder = LadderConfig::default();
                LadderConfig { base: ladder.base.max(base_floor), ..ladder }
            }
        };
        if ladder.campaign_timeout > ladder.max_campaign {
            return Err(TimingError::CampaignTimeoutAboveMax {
                campaign_timeout: ladder.campaign_timeout,
                max_campaign: ladder.max_campaign,
            });
        }
        if self.rpc_timeout.is_zero() {
            return Err(TimingError::ZeroRpcTimeout);
        }
        if self.snapshot_timeout.as_millis() == 0 {
            return Err(TimingError::ZeroSnapshotTimeout);
        }
        if self.write_wait.is_zero() {
            return Err(TimingError::ZeroWriteWait);
        }
        Ok(Timings {
            heartbeat,
            leader_lease,
            rpc_timeout: self.rpc_timeout,
            snapshot_timeout: self.snapshot_timeout,
            ladder,
            write_wait: self.write_wait,
        })
    }
}

impl Timings {
    pub fn raft_config(&self) -> Config {
        let heartbeat = self.heartbeat.as_millis() as u64;
        let lease = self.leader_lease.as_millis() as u64;
        Config {
            cluster_name: "quorium".into(),
            heartbeat_interval: heartbeat,
            election_timeout_min: lease - 1,
            election_timeout_max: lease,
            install_snapshot_timeout: self.snapshot_timeout.as_millis() as u64,
            enable_elect: false,
            enable_pre_vote: Some(true),
            enable_leader_restore: Some(false),
            ..Default::default()
        }
        .validate()
        .expect("derived timings form a valid raft config")
    }

    pub fn consensus_deadline(&self, requested: Duration) -> Duration {
        requested.max(self.rpc_timeout)
    }

    pub fn stall_tolerance(&self) -> Duration {
        self.leader_lease.saturating_sub(self.heartbeat)
    }

    pub fn failover_estimate(&self, voters: usize, round_trip: Duration) -> FailoverEstimate {
        let detection = self.ladder.base;
        let ranking = self.ladder.step_for(round_trip) * voters.saturating_sub(2) as u32;
        let campaign = self.ladder.max_campaign_for(round_trip);
        let establish = round_trip;
        FailoverEstimate { detection, ranking, campaign, establish, total: detection + ranking + campaign + establish }
    }
}
