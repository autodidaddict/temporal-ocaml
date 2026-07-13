(* Copyright 2026 Kevin Hoffman

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License. *)

(* A workflow: deterministic orchestration of activities. Re-exported as
   Temporal.Workflow. *)

type 'input ctx = {
  task_queue : string;
  continue_as_new_suggested : bool; (* this activation *)
  history_length : int; (* events in this run's history so far *)
  encode_input : 'input -> Codec.payload; (* the workflow's own input codec *)
}

type ('i, 'o) t = {
  name : string;
  input : 'i Codec.t;
  output : 'o Codec.t;
  run : 'i ctx -> 'i -> 'o;
}

let define ~name ~input ~output run = { name; input; output; run }

(* execute_activity performs this effect; the worker's handler (worker.ml) either
   resolves it (activity already completed, replayed from history) or emits a
   Schedule_activity command and suspends the workflow. It lives here, with its
   performer; Worker matches it as [Workflow.Schedule_activity_effect]. *)
type _ Effect.t +=
  | Schedule_activity_effect : {
      activity_type : string;
      arg : Codec.payload;
      start_to_close : float;
      max_attempts : int;
    }
      -> Codec.payload Effect.t

let execute_activity ?(start_to_close = 10.0) ?(max_attempts = 0) (_ : _ ctx)
    (a : ('i, 'o) Activity.t) (input : 'i) : 'o =
  let arg = Codec.to_payload a.Activity.input input in
  let result =
    Effect.perform
      (Schedule_activity_effect
         { activity_type = a.Activity.name; arg; start_to_close; max_attempts })
  in
  Codec.of_payload a.Activity.output result

(* sleep performs this effect; the worker emits a Start_timer command and suspends
   the run, resuming when the matching FireTimer job arrives. *)
type _ Effect.t += Start_timer_effect : { start_to_fire : float } -> unit Effect.t

let sleep (_ : _ ctx) (seconds : float) : unit =
  Effect.perform (Start_timer_effect { start_to_fire = seconds })

(* continue_as_new ends this run and atomically starts a fresh one (same workflow
   id, new run id, empty history) with new input; the handler emits the terminal
   command and never resumes, so this does not return. *)
type _ Effect.t += Continue_as_new_effect : Codec.payload -> unit Effect.t

let continue_as_new (ctx : 'i ctx) (new_input : 'i) : 'a =
  Effect.perform (Continue_as_new_effect (ctx.encode_input new_input));
  assert false (* the handler drops this continuation; execution never resumes *)

(* core suggests continue-as-new once history grows past the worker's threshold;
   both signals are deterministic on replay, so branching on them is safe. *)
let continue_as_new_suggested (ctx : _ ctx) : bool = ctx.continue_as_new_suggested
let history_length (ctx : _ ctx) : int = ctx.history_length

(* on_signal registers a handler (the signal's decoder composed with the user's
   callback); the worker fires it as the replay cursor passes a matching signal
   event. wait_condition blocks the body until [pred] holds, re-checked after each
   activation delivers new events. *)
type _ Effect.t +=
  | Register_signal_handler_effect :
      string * (Codec.payload -> unit)
      -> unit Effect.t
  | Wait_condition_effect : (unit -> bool) -> unit Effect.t

let on_signal (_ : _ ctx) (s : 'a Signal.t) (handler : 'a -> unit) : unit =
  Effect.perform
    (Register_signal_handler_effect
       (s.Signal.name, fun p -> handler (Codec.of_payload s.Signal.codec p)))

let wait_condition (_ : _ ctx) (pred : unit -> bool) : unit =
  Effect.perform (Wait_condition_effect pred)

(* on_query registers a read-only handler (the query's decoder and encoder wrapped
   around the user's callback). The worker collects these while replaying the body
   to its frontier, then invokes the one matching an incoming QueryWorkflow job.
   The handler is a plain [payload -> payload]; performing any workflow effect from
   inside it is unhandled (query answering runs outside the effect handler), which
   is exactly the read-only guarantee. *)
type _ Effect.t +=
  | Register_query_handler_effect :
      string * (Codec.payload -> Codec.payload)
      -> unit Effect.t

let on_query (_ : _ ctx) (q : ('a, 'b) Query.t) (handler : 'a -> 'b) : unit =
  Effect.perform
    (Register_query_handler_effect
       ( q.Query.name,
         fun p ->
           Codec.to_payload q.Query.output (handler (Codec.of_payload q.Query.input p))
       ))

(* registered form: builds the typed ctx (carrying the workflow's own input
   encoder) from the per-activation runtime info, then runs the body. *)
type reg = {
  name : string;
  body :
    task_queue:string ->
    can_suggested:bool ->
    history_length:int ->
    Codec.payload ->
    Codec.payload;
}

let reg (t : (_, _) t) =
  { name = t.name;
    body =
      (fun ~task_queue ~can_suggested ~history_length p ->
        let ctx =
          { task_queue;
            continue_as_new_suggested = can_suggested;
            history_length;
            encode_input = Codec.to_payload t.input;
          }
        in
        Codec.to_payload t.output (t.run ctx (Codec.of_payload t.input p))) }
