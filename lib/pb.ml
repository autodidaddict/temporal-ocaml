(* Minimal protobuf wire-format primitives. Internal to the SDK — just enough to
   encode/decode the handful of messages that cross the FFI, without a codegen
   dependency. *)

module Writer = struct
  type t = Buffer.t

  let create () : t = Buffer.create 64
  let contents (b : t) = Buffer.contents b

  let varint (b : t) n =
    let n = ref n and continue = ref true in
    while !continue do
      let byte = !n land 0x7f in
      n := !n lsr 7;
      if !n = 0 then (
        Buffer.add_char b (Char.chr byte);
        continue := false)
      else Buffer.add_char b (Char.chr (byte lor 0x80))
    done

  let key b field wire = varint b ((field lsl 3) lor wire)

  (* length-delimited field (wire type 2): strings, bytes, embedded messages *)
  let bytes b field s =
    key b field 2;
    varint b (String.length s);
    Buffer.add_string b s

  (* varint field (wire type 0) *)
  let int b field n =
    key b field 0;
    varint b n
end

module Reader = struct
  type t = { s : string; mutable pos : int; stop : int }

  let create ?(pos = 0) ?len s =
    let stop = match len with Some l -> pos + l | None -> String.length s in
    { s; pos; stop }

  let at_end r = r.pos >= r.stop

  let varint r =
    let shift = ref 0 and result = ref 0 and continue = ref true in
    while !continue do
      let byte = Char.code (String.get r.s r.pos) in
      r.pos <- r.pos + 1;
      result := !result lor ((byte land 0x7f) lsl !shift);
      shift := !shift + 7;
      if byte < 0x80 then continue := false
    done;
    !result

  (* returns (field_number, wire_type) *)
  let key r =
    let k = varint r in
    (k lsr 3, k land 0x7)

  let bytes r =
    let n = varint r in
    let s = String.sub r.s r.pos n in
    r.pos <- r.pos + n;
    s

  let skip r wire =
    match wire with
    | 0 -> ignore (varint r)
    | 1 -> r.pos <- r.pos + 8
    | 5 -> r.pos <- r.pos + 4
    | 2 ->
      let n = varint r in
      r.pos <- r.pos + n
    | _ -> failwith "pb: unsupported wire type"
end
