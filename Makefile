.PHONY: all build bridge run dev clean native-libs

all: build

# Build the Rust staticlib and stage it into lib/ (cargo is decoupled from dune).
bridge:
	./scripts/build-bridge.sh

# Full build: staticlib, then OCaml + link. We target the native exe (we ship a
# static .a only, not a shared dll, so we skip dune's bytecode/install path).
build: bridge
	dune build examples/ecommerce/main.exe

# Run the example worker (boots real sdk-core, connects, polls task queues).
run: build
	dune exec -- ./examples/ecommerce/main.exe

# Start a local Temporal dev server (separate terminal).
dev:
	temporal server start-dev

# Print the native libs the staticlib needs (paste into lib/dune c_library_flags).
native-libs:
	cargo rustc --release --manifest-path rust/temporal_bridge/Cargo.toml -- \
	  --print native-static-libs 2>&1 | grep native-static-libs

clean:
	dune clean
	cargo clean --manifest-path rust/temporal_bridge/Cargo.toml
	rm -f lib/libtemporal_core_bridge.a lib/temporal_bridge.h
