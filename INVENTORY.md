# Inventory & Roadmap

This document outlines the long term plan to rewrite this project so that it relies on [Trible Space](https://github.com/triblespace/tribles-rust) for storage and metadata management. The overall goal is to remove filesystem centric pieces of Yeehaw and treat indexes as content addressed blobs stored alongside the primary data in Trible Space.

## Big Picture

- All metadata, segments and rollups live in Trible Space.
- Indexing operations produce immutable blobs and attach them to commits.
- Search results pull the original documents from Trible Space rather than a separate docstore.
- Forgetting unreachable blobs does not invalidate earlier commits; older indexes remain valid.

## Roadmap Tasks

1. **Store fulltext metadata in commit tribles**
   - Define a `fulltext` attribute in the Trible namespace.
   - Represent segment lists, schema and other metadata as entities attached to each commit.
   - Replace the `meta.json` file with this trible based representation.

2. **Implement `TribleBlobDirectory`**
   - Replace the `Directory` abstraction with a backend that reads and writes blobs via the Trible Space `BlobStore`.
   - Index writers and readers operate on blob handles instead of filesystem paths.

3. **Drop the docstore module**
   - Primary documents are kept in Trible Space; segments no longer store their own row oriented docstore.
   - Search results fetch documents via blob handles.

4. **Remove `Opstamp` and use commit handles**
   - Commits record the segments they include.
   - Merges rely on commit ancestry instead of monotonic operation stamps.

5. **Introduce 128-bit IDs with `Universe` mapping**
   - Map external `u128` identifiers to compact `DocId` values.
   - Persist the mapping so search results can translate back.

6. **Typed DSL for fuzzy search**
   - Generate search filters from Trible namespaces.
   - Provide macros that participate in both `find!` queries and full text search.

7. **Index update merge workflow**
   - Wrap indexing operations in workspace commits.
   - Use Trible's compare-and-swap push mechanism so multiple writers merge gracefully.

8. **Add contributor guidelines** *(done)*
   - Ported the `AGENTS.md` from `tribles-rust` and adapted it for this project.
   - Contributors must run `cargo fmt`, `cargo test` and `./scripts/preflight.sh` before committing.
9. **Clean up unused imports** *(done)*
   - Removed `unused import: std::iter` warnings from tests.

This inventory captures the direction of the rewrite and the major tasks required to make Yeehaw a Trible native search engine.
10. **Rename remaining `Tantivy` prefixes**
    - Update types and crates (e.g. `TantivyDocument`, `tantivy-fst`) to use `yeehaw` naming.
11. **Rename subcrate benchmarks**
    - Update benchmark crates (e.g. bitpacker, common, sstable) to use `yeehaw` naming once subcrate packages are renamed.
12. **Improve error handling in `ColumnarReader::iter_columns`**
    - Replace `unwrap` usage with a fallible API that reports unknown column types.
