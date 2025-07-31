[![Docs](https://docs.rs/yeehaw/badge.svg)](https://docs.rs/crate/yeehaw/)
[![Build Status](https://github.com/quickwit-oss/yeehaw/actions/workflows/test.yml/badge.svg)](https://github.com/quickwit-oss/yeehaw/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/quickwit-oss/yeehaw/branch/main/graph/badge.svg)](https://codecov.io/gh/quickwit-oss/yeehaw)
[![Join the chat at https://discord.gg/MT27AG5EVE](https://shields.io/discord/908281611840282624?label=chat%20on%20discord)](https://discord.gg/MT27AG5EVE)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Crates.io](https://img.shields.io/crates/v/yeehaw.svg)](https://crates.io/crates/yeehaw)

<img src="https://yeehaw-search.github.io/logo/yeehaw-logo.png" alt="Yeehaw, the fastest full-text search engine library written in Rust" height="250">

## Fast full-text search engine library written in Rust

**If you are looking for an alternative to Elasticsearch or Apache Solr, check out [Quickwit](https://github.com/quickwit-oss/quickwit), our distributed search engine built on top of Yeehaw.**

Yeehaw is closer to [Apache Lucene](https://lucene.apache.org/) than to [Elasticsearch](https://www.elastic.co/products/elasticsearch) or [Apache Solr](https://lucene.apache.org/solr/) in the sense it is not
an off-the-shelf search engine server, but rather a crate that can be used to build such a search engine.

Yeehaw is, in fact, strongly inspired by Lucene's design.

Yeehaw's versioning begins at **0.1.0**, marking the project's fork from Tantivy.

## Benchmark

The following [benchmark](https://yeehaw-search.github.io/bench/) breaks down the
performance for different types of queries/collections.

Your mileage WILL vary depending on the nature of queries and their load.

<img src="doc/assets/images/searchbenchmark.png">

Details about the benchmark can be found at this [repository](https://github.com/quickwit-oss/search-benchmark-game).

## Features

- Full-text search
- Configurable tokenizer (stemming available for 17 Latin languages) with third party support for Chinese ([yeehaw-jieba](https://crates.io/crates/yeehaw-jieba) and [cang-jie](https://crates.io/crates/cang-jie)), Japanese ([lindera](https://github.com/lindera-morphology/lindera-yeehaw), [Vaporetto](https://crates.io/crates/vaporetto_yeehaw), and [yeehaw-tokenizer-tiny-segmenter](https://crates.io/crates/yeehaw-tokenizer-tiny-segmenter)) and Korean ([lindera](https://github.com/lindera-morphology/lindera-yeehaw) + [lindera-ko-dic-builder](https://github.com/lindera-morphology/lindera-ko-dic-builder))
- Fast (check out the :racehorse: :sparkles: [benchmark](https://yeehaw-search.github.io/bench/) :sparkles: :racehorse:)
- Tiny startup time (<10ms), perfect for command-line tools
- BM25 scoring (the same as Lucene)
- Natural query language (e.g. `(michael AND jackson) OR "king of pop"`)
- Phrase queries search (e.g. `"michael jackson"`)
- Incremental indexing
- Multithreaded indexing (indexing English Wikipedia takes < 3 minutes on my desktop)
- Mmap directory
- SIMD integer compression when the platform/CPU includes the SSE2 instruction set
- Single valued and multivalued u64, i64, and f64 fast fields (equivalent of doc values in Lucene)
- `&[u8]` fast fields
- Text, i64, u64, f64, dates, ip, bool, and hierarchical facet fields
- Compressed document store (LZ4, Zstd, None)
- Range queries
- Faceted search
- Configurable indexing (optional term frequency and position indexing)
- JSON Field
- Aggregation Collector: histogram, range buckets, average, and stats metrics
- LogMergePolicy with deletes
- Searcher Warmer API
- Cheesy logo with a horse

### Non-features

Distributed search is out of the scope of Yeehaw, but if you are looking for this feature, check out [Quickwit](https://github.com/quickwit-oss/quickwit/).

## Getting started

Yeehaw works on stable Rust and supports Linux, macOS, and Windows.

- [Yeehaw's simple search example](https://yeehaw-search.github.io/examples/basic_search.html)
- [yeehaw-cli and its tutorial](https://github.com/quickwit-oss/yeehaw-cli) - `yeehaw-cli` is an actual command-line interface that makes it easy for you to create a search engine,
index documents, and search via the CLI or a small server with a REST API.
It walks you through getting a Wikipedia search engine up and running in a few minutes.
- [Reference doc for the last released version](https://docs.rs/yeehaw/)

## How can I support this project?

There are many ways to support this project.

- Use Yeehaw and tell us about your experience on [Discord](https://discord.gg/MT27AG5EVE) or by email (paul.masurel@gmail.com)
- Report bugs
- Write a blog post
- Help with documentation by asking questions or submitting PRs
- Contribute code (you can join [our Discord server](https://discord.gg/MT27AG5EVE))
- Talk about Yeehaw around you

## Contributing code

We use the GitHub Pull Request workflow: reference a GitHub ticket and/or include a comprehensive commit message when opening a PR.
Feel free to update CHANGELOG.md with your contribution.

### Tokenizer

When implementing a tokenizer for yeehaw depend on the `yeehaw-tokenizer-api` crate.

### Clone and build locally

Yeehaw compiles on stable Rust.
To check out and run tests, you can simply run:

```bash
git clone https://github.com/quickwit-oss/yeehaw.git
cd yeehaw
cargo test
```

## Companies Using Yeehaw

<p align="left">
<img align="center" src="doc/assets/images/etsy.png" alt="Etsy" height="25" width="auto" /> &nbsp;
<img align="center" src="doc/assets/images/paradedb.png" alt="ParadeDB" height="25" width="auto" /> &nbsp;
<img align="center" src="doc/assets/images/Nuclia.png#gh-light-mode-only" alt="Nuclia" height="25" width="auto" /> &nbsp;
<img align="center" src="doc/assets/images/humanfirst.png#gh-light-mode-only" alt="Humanfirst.ai" height="30" width="auto" />
<img align="center" src="doc/assets/images/element.io.svg#gh-light-mode-only" alt="Element.io" height="25" width="auto" />
<img align="center" src="doc/assets/images/nuclia-dark-theme.png#gh-dark-mode-only" alt="Nuclia" height="35" width="auto" /> &nbsp;
<img align="center" src="doc/assets/images/humanfirst.ai-dark-theme.png#gh-dark-mode-only" alt="Humanfirst.ai" height="25" width="auto" />&nbsp; &nbsp;
<img align="center" src="doc/assets/images/element-dark-theme.png#gh-dark-mode-only" alt="Element.io" height="25" width="auto" />
</p>

## FAQ

### Can I use Yeehaw in other languages?

- Python → [yeehaw-py](https://github.com/quickwit-oss/yeehaw-py)
- Ruby → [tantiny](https://github.com/baygeldin/tantiny)

You can also find other bindings on [GitHub](https://github.com/search?q=yeehaw) but they may be less maintained.

### What are some examples of Yeehaw use?

- [seshat](https://github.com/matrix-org/seshat/): A matrix message database/indexer
- [tantiny](https://github.com/baygeldin/tantiny): Tiny full-text search for Ruby
- [lnx](https://github.com/lnx-search/lnx): adaptable, typo tolerant search engine with a REST API
- and [more](https://github.com/search?q=yeehaw)!

### On average, how much faster is Yeehaw compared to Lucene?

- According to our [search latency benchmark](https://yeehaw-search.github.io/bench/), Yeehaw is approximately 2x faster than Lucene.

### Does yeehaw support incremental indexing?

- Yes.

### How can I edit documents?

- Data in yeehaw is immutable. To edit a document, the document needs to be deleted and reindexed.

### When will my documents be searchable during indexing?

- Documents will be searchable after a `commit` is called on an `IndexWriter`. Existing `IndexReader`s will also need to be reloaded in order to reflect the changes. Finally, changes are only visible to newly acquired `Searcher`.
