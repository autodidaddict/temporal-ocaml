#!/usr/bin/env bash
# Build the Rust sdk-core staticlib and stage it (+ its header) into lib/ where
# dune links it. Kept separate from `dune build` on purpose — see lib/dune.
set -euo pipefail

root="$(cd "$(dirname "$0")/.." && pwd)"
manifest="$root/rust/temporal_bridge/Cargo.toml"

echo ">> cargo build --release (temporal_core_bridge)"
cargo build --release --manifest-path "$manifest"

cp "$root/rust/temporal_bridge/target/release/libtemporal_core_bridge.a" "$root/lib/"
cp "$root/rust/temporal_bridge/include/temporal_bridge.h" "$root/lib/"
echo ">> staged libtemporal_core_bridge.a + temporal_bridge.h into lib/"
