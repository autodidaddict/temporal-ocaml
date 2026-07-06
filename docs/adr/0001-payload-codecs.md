# 1. Payload codecs (serialization at the SDK boundary)

- Status: Accepted
- Date: 2026-07-06

## Context

Today `Activity.define` / `Workflow.define` pass OCaml values in-process and
`execute_activity` doesn't actually run. Once the runtime executes workflows and
activities, their inputs and outputs cross the FFI as Temporal `Payload`s
(bytes + `encoding` metadata): an activity's input arrives from the server as a
`Payload`, its result goes back as one; a workflow's argument and result do the
same. So the SDK needs a serialization layer at that boundary.

Constraints that shape the choice:

1. **No ppx dependency in the SDK.** Consumers of this library must not inherit a
   ppx toolchain (see the project's dependency stance). OCaml has no generic
   serialization without either ppx or hand-written codecs.
2. **Cross-SDK interoperability.** A workflow may be started by the Go/Python/TS
   SDK and served by ours (and vice versa). The default wire format Temporal
   SDKs agree on is JSON (`encoding: json/plain`).
3. **Keep the API typed.** Activities/workflows are phantom-typed on their
   input/output; the serialization layer must preserve that.

## Decision

Introduce an abstract, typed codec and thread it through the definition API.

```ocaml
module Codec : sig
  type 'a t
  (** Encodes/decodes an OCaml value to/from a Temporal Payload. *)

  (* primitives (encoded json/plain, except [bytes] which is binary/plain) *)
  val unit   : unit t
  val bool   : bool t
  val int    : int t
  val float  : float t
  val string : string t
  val bytes  : bytes t

  (* combinators *)
  val list   : 'a t -> 'a list t
  val option : 'a t -> 'a option t
  val pair   : 'a t -> 'b t -> ('a * 'b) t
  val map    : ('a -> 'b) -> ('b -> 'a) -> 'a t -> 'b t

  (** The escape hatch for user-defined types. Sets [encoding: json/plain]. *)
  val json :
    encode:('a -> Yojson.Safe.t) -> decode:(Yojson.Safe.t -> 'a) -> 'a t
end
```

The definition functions take codecs:

```ocaml
val Activity.define :
  name:string -> input:'i Codec.t -> output:'o Codec.t ->
  (Activity.ctx -> 'i -> 'o) -> ('i, 'o) t

val Workflow.define :
  name:string -> input:'i Codec.t -> output:'o Codec.t ->
  (Workflow.ctx -> 'i -> 'o) -> ('i, 'o) t
```

**Developers write codec values for their own domain types** — the SDK ships only
primitives, combinators, and the `json` escape hatch. Whether those codecs are
hand-written or generated is a decision made *above* the SDK, in the app's own
dependencies:

```ocaml
(* hand-written, no ppx anywhere *)
let stock_unit : stock_unit Codec.t =
  Codec.json
    ~encode:(fun u -> `Assoc [ "sku", `String u.sku; "qty", `Int u.qty ])
    ~decode:(function
      | `Assoc _ as j -> { sku = member_string "sku" j; qty = member_int "qty" j }
      | _ -> failwith "stock_unit: expected object")

(* OR: the app opts into ppx_yojson_conv in ITS OWN deps — not the SDK's *)
type order = { order_id : string; items : stock_unit list } [@@deriving yojson]
let order : order Codec.t = Codec.json ~encode:yojson_of_order ~decode:order_of_yojson
```

Both produce a plain `order Codec.t`; the SDK cannot tell (or care) which was
used. Projects will typically collect these into a `Codecs` module
(`Codecs.order`, `Codecs.stock_unit`, …).

**Dependencies:** the SDK gains a dependency on `yojson` (small, pure OCaml, no
ppx) for the JSON value type. It gains **no** ppx dependency.

### Failures

Errors are **not** a single opaque payload. Temporal models them with the
structured `temporal.api.failure.v1.Failure` proto — which already crosses our
FFI as the `failed` arm of a completion — and every SDK (Go/Java/Python/TS)
handles it the same way: a **failure converter** that is distinct from, but
composed with, the data converter.

- `Failure` carries plain strings (`message`, `source`, `stack_trace`), a `cause`
  chain, and a `oneof failure_info` selecting a typed variant
  (`ApplicationFailureInfo`, `TimeoutFailureInfo`, `CanceledFailureInfo`,
  `ActivityFailureInfo`, `ChildWorkflowExecutionFailureInfo`, …).
- The only **user data** inside a failure lives in `Payloads`/`Payload` slots —
  `ApplicationFailureInfo.details`, `CanceledFailureInfo.details`,
  `TimeoutFailureInfo.last_heartbeat_details`, and the optional
  `encoded_attributes` — and those go through the same payload path as any other
  value. The rest of the proto is plain protobuf, not payloads.
- Default mapping (as in the other SDKs): a modelled application error
  (`type`, `non_retryable`, typed `details`) ↔ `ApplicationFailureInfo`; a
  cancellation ↔ `CanceledFailureInfo`. The wrapper variants
  (`ActivityFailureInfo`, `ChildWorkflowExecutionFailureInfo`, timeouts) are
  produced by sdk-core; lang reads them and re-raises a matching OCaml exception
  with `cause` chained.
- Any *un-modelled* OCaml exception from user code becomes
  `ApplicationFailureInfo { type = <constructor name>; non_retryable = false;
  message = Printexc.to_string exn }` with empty `details` — one execution fails,
  the worker does not crash. A malformed *input* payload is the same story: a
  decode error fails that single execution.
- `encode_common_attributes` (redacting `message`/`stack_trace` into an encrypted
  `encoded_attributes` Payload) is a feature the other SDKs expose; we defer it,
  but the seam below leaves room.

### The data-converter seam

A failure converter is inherently **not** per-type — it is one object for all
errors. There is also a second cross-cutting concern already on the horizon: a
**payload codec** (a `Payload -> Payload` transform for compression /
encryption-at-rest) that must apply uniformly to every payload, including those
embedded in failures. Neither belongs in a per-type `Codec.t`.

So we introduce a thin **`Data_converter`**, set once on the `Client` / `Worker`,
that composes with — rather than replaces — per-type codecs:

```
Data_converter = {
  payload_codec     : Payload -> Payload;   (* identity by default; encrypt/compress later *)
  failure_converter : exn <-> Failure;      (* details go through payload_codec *)
}
```

`define ~input ~output` keeps the per-type `Codec.t` at the call site (typed
value ↔ Payload); the `Data_converter` sits above it, post-processing produced
payloads and owning failure conversion. This is cleaner than folding either
concern into per-type codecs: cross-cutting transforms get exactly one home
instead of leaking into every codec, failure handling has a single home, and it
matches the "Data Converter" model every Temporal user already knows. The cost is
one thin (two-field) concept while the defaults are identity/JSON — accepted,
because introducing it now avoids churning workflow/activity code later when
encryption or a custom failure mapping is needed.

This mirrors the `DataConverter` that Go, Java, Python, and TypeScript all
expose, which is itself a composition of three things:

- a **PayloadConverter** — value ↔ `Payload` (a chain that dispatches on the
  runtime type/value),
- one or more **PayloadCodec**s — `Payload` ↔ `Payload` (compression, encryption),
- a **FailureConverter** — error ↔ `Failure`.

Our adaptation differs in exactly one place: OCaml has no runtime type dispatch,
so the PayloadConverter role can't be a single dynamic object — it becomes the
explicit per-type `Codec.t` at each `define` call site (above). `Data_converter`
holds the two genuinely cross-cutting parts, `payload_codec` and
`failure_converter`, and is set once per `Client`/`Worker` just as the other
SDKs' `DataConverter` is.

## Consequences

- **+** Consumers inherit no ppx; ppx becomes an opt-in convenience at the app
  level, exactly where derivers belong.
- **+** JSON-by-default keeps us interoperable with the other Temporal SDKs.
- **+** The thin `Data_converter` seam gives cross-cutting payload transforms
  (compression, encryption) and failure mapping a single, swappable home that
  composes with per-type codecs — matching the Data Converter model every
  Temporal user already knows, and keeping alternative encodings behind the same
  `Codec.t`/`Payload` types.
- **−** For a record, a hand-written codec is more boilerplate than a deriver
  would be. Mitigated by the combinators and by the ppx escape hatch — but the
  boilerplate is a real, accepted cost of keeping ppx out of the SDK.
- **−** One new SDK dependency (`yojson`). Judged acceptable: it is ubiquitous,
  ppx-free, and only used for the JSON value type.

## Alternatives considered

- **A ppx-based data converter in the SDK** (e.g. bundling `ppx_deriving_yojson`).
  Rejected: forces the ppx toolchain onto every consumer — the exact thing the
  project's dependency stance rules out.
- **Raw bytes/strings only** (`type 'a Codec.t = 'a -> string / string -> 'a`,
  developer serializes however they like). Rejected: pushes format decisions onto
  every user, and without a JSON default we lose cross-SDK interop by accident.
- **Runtime reflection / generic serialization.** Not available in OCaml without
  ppx or a universal-type encoding; same objection as the ppx converter.

## Open questions (for the implementing ADR)

- The exact `Payload` `encoding` metadata we set/accept per codec, and the
  `bytes` (binary/plain) vs JSON mapping.
- Whether the first cut exposes `payload_codec` to users, or hardcodes identity
  until an encryption use-case appears.
- Deferred: `encode_common_attributes` (failure message/stack-trace redaction).
