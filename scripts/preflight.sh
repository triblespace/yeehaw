#!/usr/bin/env bash
set -euo pipefail

cargo fmt --all -- --check
cargo test --workspace
cargo test --doc --workspace
