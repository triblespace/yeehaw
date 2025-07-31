#!/usr/bin/env bash
set -euo pipefail

cargo fmt --all -- --check
cargo test --tests --lib
