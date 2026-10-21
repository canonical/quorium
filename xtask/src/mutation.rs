use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Score {
    pub killed: u64,
    pub missed: u64,
}

#[derive(Deserialize)]
struct Summary {
    caught: u64,
    missed: u64,
    timeout: u64,
}

impl Score {
    pub fn from_outcomes(json: &str) -> Result<Score, String> {
        let s: Summary = serde_json::from_str(json).map_err(|e| e.to_string())?;
        Ok(Score { killed: s.caught + s.timeout, missed: s.missed })
    }

    pub fn ratio(&self) -> f64 {
        let total = self.killed + self.missed;
        if total == 0 { 0.0 } else { self.killed as f64 / total as f64 }
    }
}

impl std::ops::Add for Score {
    type Output = Score;

    fn add(self, other: Score) -> Score {
        Score { killed: self.killed + other.killed, missed: self.missed + other.missed }
    }
}

impl std::iter::Sum for Score {
    fn sum<I: Iterator<Item = Score>>(iter: I) -> Score {
        iter.fold(Score { killed: 0, missed: 0 }, |a, b| a + b)
    }
}

pub fn evaluate_score(score: Score, floor: f64) -> Result<(), String> {
    if score.killed + score.missed == 0 {
        return Err("no viable mutants were tested".into());
    }
    if score.ratio() + 1e-12 < floor {
        return Err(format!("mutation score {:.3} below floor {floor}", score.ratio()));
    }
    Ok(())
}
