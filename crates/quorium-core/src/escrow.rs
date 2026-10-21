pub const ESCROW_METHOD: u32 = 0xFFFF_FF00;
pub const RECLAIMED: &str = "the grant was reclaimed";

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Holding {
    pub remaining: u64,
    pub copying: u64,
    pub unsettled: u64,
    pub pending: Option<(u64, u64, u64)>,
    pub sold_total: u64,
    pub pending_through: u64,
}

impl Holding {
    pub fn grant(&mut self, units: u64) {
        self.remaining += units;
    }

    pub fn take(&mut self, units: u64) -> bool {
        if self.remaining < units {
            return false;
        }
        self.remaining -= units;
        self.copying += units;
        true
    }

    pub fn confirm(&mut self, units: u64) {
        self.copying = self.copying.saturating_sub(units);
        self.unsettled += units;
        self.sold_total += units;
    }

    pub fn taken_total(&self) -> u64 {
        self.sold_total + self.copying
    }

    pub fn unsell(&mut self, units: u64) {
        self.copying = self.copying.saturating_sub(units);
        self.remaining += units;
    }

    pub fn begin_settle(&mut self, seq: u64, release_idle: bool) -> Option<(u64, u64)> {
        if self.pending.is_some() {
            return None;
        }
        let release = if release_idle { self.remaining } else { 0 };
        if self.unsettled == 0 && release == 0 {
            return None;
        }
        let sold = self.unsettled;
        self.pending = Some((seq, sold, release));
        self.pending_through = self.sold_total;
        self.unsettled = 0;
        self.remaining -= release;
        Some((sold, release))
    }

    pub fn settled(&mut self, seq: u64) {
        if self.pending.is_some_and(|(p, _, _)| p == seq) {
            self.pending = None;
        }
    }

    pub fn replay_sold(&mut self, units: u64) {
        self.remaining = self.remaining.saturating_sub(units);
        self.copying += units;
    }

    pub fn replay_settle(&mut self, seq: u64, sold: u64, release: u64) {
        let from_unsettled = sold.min(self.unsettled);
        let from_copying = (sold - from_unsettled).min(self.copying);
        self.unsettled -= from_unsettled;
        self.copying -= from_copying;
        self.sold_total += from_copying;
        self.remaining = self.remaining.saturating_sub(release);
        self.pending = Some((seq, sold, release));
        self.pending_through = self.sold_total;
    }

    pub fn finish_replay(&mut self) {
        self.unsettled += self.copying;
        self.sold_total += self.copying;
        self.copying = 0;
    }
}
