# Contribute to Quorium

Quorium is developed on [GitHub](https://github.com/canonical/quorium). Report bugs and ask questions in [issues](https://github.com/canonical/quorium/issues), and propose changes with pull requests.

## Documents for contributors

Documents about developing and verifying Quorium itself live in the repository's [`docs/`](https://github.com/canonical/quorium/tree/main/docs) directory:

- [Stored formats](https://github.com/canonical/quorium/blob/main/docs/FORMATS.md): every versioned format and its fixtures.
- [Spec conformance](https://github.com/canonical/quorium/blob/main/docs/CONFORMANCE.md): checking implementation traces against the TLA+ model.
- [Multi-region test rig](https://github.com/canonical/quorium/blob/main/docs/RIG.md): real processes, faults and Jepsen-style workloads.
- [Releasing](https://github.com/canonical/quorium/blob/main/docs/RELEASING.md): publishing the crates.
- [Documentation](https://github.com/canonical/quorium/blob/main/docs/DOCUMENTATION.md): building, checking and publishing this site.

## Change this documentation

Every page has an edit button that opens its source on GitHub. The pages are Markdown, built with [Sphinx](https://www.sphinx-doc.org/) and [MyST](https://myst-parser.readthedocs.io/). To preview the site locally:

```sh
cd documentation
make run
```

and open `http://127.0.0.1:8000`.

Rust code blocks are checked against compiled code. Every line of a `rust` block must also appear in `examples/guestbook` or `examples/doc-snippets`, which are built and tested with the rest of the workspace:

```sh
cargo xtask docs-check
```

A block that is deliberately incomplete, such as a signature summary or a sketch with placeholders, uses `{code-block} rust` instead and is not checked.
