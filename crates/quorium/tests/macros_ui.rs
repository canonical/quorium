#[test]
fn macro_errors_are_explained() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
