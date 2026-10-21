use std::path::Path;

use serde::Deserialize;

#[derive(Debug, Clone, PartialEq)]
pub struct Regression {
    pub bench: String,
    pub change: f64,
}

#[derive(Deserialize)]
struct Interval {
    lower_bound: f64,
}

#[derive(Deserialize)]
struct Estimate {
    point_estimate: f64,
    confidence_interval: Interval,
}

#[derive(Deserialize)]
struct Change {
    mean: Estimate,
}

fn collect(dir: &Path, prefix: &str, out: &mut Vec<(String, f64, f64)>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().to_string();
        let path = entry.path();
        let change = path.join("change").join("estimates.json");
        if change.exists() {
            let text = std::fs::read_to_string(&change)?;
            let parsed: Change = serde_json::from_str(&text).map_err(std::io::Error::other)?;
            out.push((
                format!("{prefix}{name}"),
                parsed.mean.point_estimate,
                parsed.mean.confidence_interval.lower_bound,
            ));
        } else if name != "report" {
            collect(&path, &format!("{prefix}{name}/"), out)?;
        }
    }
    Ok(())
}

pub fn compare(criterion_dir: &Path, threshold: f64) -> Result<Vec<Regression>, String> {
    let mut changes = Vec::new();
    collect(criterion_dir, "", &mut changes).map_err(|e| e.to_string())?;
    if changes.is_empty() {
        return Err(format!("no criterion comparisons under {}", criterion_dir.display()));
    }
    changes.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(changes
        .into_iter()
        .filter(|(_, _, lower)| *lower > threshold)
        .map(|(bench, change, _)| Regression { bench, change })
        .collect())
}
