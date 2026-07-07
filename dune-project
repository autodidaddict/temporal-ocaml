(lang dune 3.0)
(name temporal-ocaml)
(generate_opam_files true)

(package
 (name temporal-ocaml)
 (synopsis "OCaml SDK for Temporal — sdk-core FFI scaffold")
 (description
  "Static bindings to temporalio-sdk-core via a Rust staticlib, in the style of Octez's Rust integration.")
 (license "Apache-2.0")
 (authors "Kevin Hoffman")
 (maintainers "Kevin Hoffman")
 (homepage "https://github.com/autodidaddict/temporal-ocaml")
 (source
  (github autodidaddict/temporal-ocaml))
 (depends
  (ocaml (>= 5.1))
  (eio (>= 1.0))
  eio_main
  yojson
  ; Build deps for the sdk-core staticlib, which dune builds via cargo: the Rust
  ; toolchain, and protoc (sdk-core's proto crate runs tonic-prost-build at
  ; compile time).
  (conf-rust-2021 :build)
  (conf-protoc :build)))
