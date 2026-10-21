use xtask::docs::{Block, rust_blocks, uncovered};

#[test]
fn extracts_only_plain_rust_fences_with_their_first_line() {
    let page = "# Title\n\n```rust\nlet a = 1;\n```\n\n```sh\ncargo run\n```\n\n```{code-block} rust\nsketch();\n```\n\n```rust\nlet b = 2;\nlet c = 3;\n```\n";
    let blocks = rust_blocks(page);
    assert_eq!(
        blocks,
        vec![
            Block { line: 4, code: vec!["let a = 1;".to_string()] },
            Block { line: 16, code: vec!["let b = 2;".to_string(), "let c = 3;".to_string()] },
        ]
    );
}

#[test]
fn a_fence_inside_a_longer_fence_is_not_a_block() {
    let page = "````md\n```rust\nnot_code();\n```\n````\n";
    assert!(rust_blocks(page).is_empty());
}

#[test]
fn an_unclosed_fence_still_yields_its_lines() {
    let blocks = rust_blocks("```rust\nlet a = 1;\n");
    assert_eq!(blocks, vec![Block { line: 2, code: vec!["let a = 1;".to_string()] }]);
}

#[test]
fn lines_found_in_the_sources_are_covered_whatever_their_indentation() {
    let block = Block { line: 10, code: vec!["    .shards(16)".to_string(), "".to_string(), "let x = 1;".to_string()] };
    let sources = "fn f() {\n        .shards(16)\n    let x = 1;\n}\n";
    assert!(uncovered(&block, sources).is_empty());
}

#[test]
fn lines_missing_from_the_sources_are_reported_with_their_line_number() {
    let block = Block { line: 10, code: vec!["let x = 1;".to_string(), "".to_string(), "let y = 2;".to_string()] };
    assert_eq!(uncovered(&block, "let x = 1;\n"), vec![(12, "let y = 2;".to_string())]);
}

#[test]
fn a_line_must_match_a_whole_source_line() {
    let block = Block { line: 1, code: vec!["x = 1;".to_string()] };
    assert_eq!(uncovered(&block, "let x = 1;\n"), vec![(1, "x = 1;".to_string())]);
}

#[test]
fn check_reports_uncovered_lines_of_every_page_under_the_docs_directory() {
    let root = tempfile::tempdir().unwrap();
    let docs = root.path().join("documentation");
    let sources = root.path().join("examples");
    std::fs::create_dir_all(docs.join("how-to")).unwrap();
    std::fs::create_dir_all(docs.join(".venv/lib")).unwrap();
    std::fs::create_dir_all(sources.join("src")).unwrap();
    std::fs::write(docs.join("index.md"), "```rust\nlet a = 1;\n```\n").unwrap();
    std::fs::write(docs.join("how-to/page.md"), "text\n\n```rust\nlet a = 1;\nlet b = 2;\n```\n").unwrap();
    std::fs::write(docs.join(".venv/lib/vendored.md"), "```rust\nignored();\n```\n").unwrap();
    std::fs::write(docs.join("notes.txt"), "```rust\nignored();\n```\n").unwrap();
    std::fs::write(sources.join("src/lib.rs"), "fn f() {\n    let a = 1;\n}\n").unwrap();

    let problems = xtask::docs::check(&docs, &[sources]).unwrap();

    let page = docs.join("how-to/page.md");
    assert_eq!(problems, vec![format!("{}:5: let b = 2;", page.display())]);
}
