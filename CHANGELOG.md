Yeehaw 0.1.0
================================

This release marks our fork from Tantivy. Previous Tantivy release notes
have been removed to keep the changelog focused on Yeehaw's history.

## Bugfixes
- fix union performance regression [#2663](https://github.com/quickwit-oss/yeehaw/pull/2663)(@PSeitz-dd)
- make zstd optional in sstable [#2633](https://github.com/quickwit-oss/yeehaw/pull/2633)(@Parth)
- replace lingering references from `tantivy` to `yeehaw`.
- rename benchmark imports to use the `yeehaw` crate.
- update examples to import the `yeehaw` crate instead of `tantivy`.

## Features/Improvements
- add docs/example and Vec<u32> values to sstable [#2660](https://github.com/quickwit-oss/yeehaw/pull/2660)(@PSeitz)
- Add string fast field support to `TopDocs`. [#2642](https://github.com/quickwit-oss/yeehaw/pull/2642)(@stuhood)
- update edition to 2024 [#2620](https://github.com/quickwit-oss/yeehaw/pull/2620)(@PSeitz)
- add AGENTS.md contributor guidelines requiring `cargo fmt`, `cargo test`, and `./scripts/preflight.sh`.
- remove legacy Makefile in favor of direct cargo commands.
- remove custom `rustfmt` configuration in favor of default formatting.
- run preflight script in CI via GitHub Action.
- document Yeehaw's fork from Tantivy, reset versioning to 0.1.0, and prune legacy Tantivy release notes.
- expand preflight script to run all workspace tests and doctests.
- simplify CI by removing the separate test workflow in favor of preflight checks, and restrict preflight to pull requests to avoid duplicate runs.
- remove long-running tests from CI.
- drop coverage workflow from CI
- remove outdated TODO for columnar `list_columns` and document error handling follow-up.
- remove unused `std::iter` imports from test modules.
