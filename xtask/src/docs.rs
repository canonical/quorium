use std::collections::HashSet;
use std::io;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    pub line: usize,
    pub code: Vec<String>,
}

fn fence(line: &str) -> Option<(usize, &str)> {
    let trimmed = line.trim_start();
    let ticks = trimmed.chars().take_while(|c| *c == '`').count();
    (ticks >= 3).then(|| (ticks, trimmed[ticks..].trim()))
}

pub fn rust_blocks(markdown: &str) -> Vec<Block> {
    let mut blocks = Vec::new();
    let mut open: Option<(usize, Option<Block>)> = None;
    for (index, line) in markdown.lines().enumerate() {
        match (&mut open, fence(line)) {
            (None, Some((ticks, info))) => {
                let block = (info == "rust").then(|| Block { line: index + 2, code: Vec::new() });
                open = Some((ticks, block));
            }
            (Some((ticks, _)), Some((closing, ""))) if closing >= *ticks => {
                if let Some((_, Some(block))) = open.take() {
                    blocks.push(block);
                }
            }
            (Some((_, Some(block))), _) => block.code.push(line.to_string()),
            _ => {}
        }
    }
    if let Some((_, Some(block))) = open {
        blocks.push(block);
    }
    blocks
}

pub fn uncovered(block: &Block, sources: &str) -> Vec<(usize, String)> {
    let known: HashSet<&str> = sources.lines().map(str::trim).collect();
    block
        .code
        .iter()
        .enumerate()
        .map(|(offset, line)| (block.line + offset, line.trim()))
        .filter(|(_, line)| !line.is_empty() && !known.contains(line))
        .map(|(number, line)| (number, line.to_string()))
        .collect()
}

fn files(dir: &Path, extension: &str, found: &mut Vec<PathBuf>) -> io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or_default();
        if name.starts_with('.') || name.starts_with('_') || name == "target" {
            continue;
        }
        if path.is_dir() {
            files(&path, extension, found)?;
        } else if path.extension().is_some_and(|e| e == extension) {
            found.push(path);
        }
    }
    Ok(())
}

pub fn check(docs: &Path, source_dirs: &[PathBuf]) -> io::Result<Vec<String>> {
    let mut sources = Vec::new();
    for dir in source_dirs {
        files(dir, "rs", &mut sources)?;
    }
    let mut text = String::new();
    for source in sources {
        text.push_str(&std::fs::read_to_string(source)?);
        text.push('\n');
    }
    let mut pages = Vec::new();
    files(docs, "md", &mut pages)?;
    pages.sort();
    let mut problems = Vec::new();
    for page in pages {
        for block in rust_blocks(&std::fs::read_to_string(&page)?) {
            for (line, code) in uncovered(&block, &text) {
                problems.push(format!("{}:{line}: {code}", page.display()));
            }
        }
    }
    Ok(problems)
}
