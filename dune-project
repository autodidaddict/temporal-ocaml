(lang dune 3.0)
(name temporal-ocaml)
(generate_opam_files true)

(package
 (name temporal-ocaml)
 (synopsis "OCaml SDK for Temporal — sdk-core FFI scaffold")
 (description
  "Static bindings to temporalio-sdk-core via a Rust staticlib, in the style of Octez's Rust integration.")
 (depends
  (ocaml (>= 5.1))
  (eio (>= 1.0))
  eio_main
  yojson
  ; The Rust toolchain is a build dependency (cargo builds the staticlib).
  (conf-rust-2021 :build)))
