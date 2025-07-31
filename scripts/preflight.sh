#!/usr/bin/env bash
set -euo pipefail

cargo fmt --all -- --check
cargo test --workspace --all-features
cargo test --doc --workspace --all-features
