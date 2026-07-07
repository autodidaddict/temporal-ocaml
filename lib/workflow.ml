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

type ctx = { task_queue : string }

type ('i, 'o) t = {
  name : string;
  input : 'i Codec.t;
  output : 'o Codec.t;
  run : ctx -> 'i -> 'o;
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

let execute_activity ?(start_to_close = 10.0) ?(max_attempts = 0) (_ : ctx)
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

let sleep (_ : ctx) (seconds : float) : unit =
  Effect.perform (Start_timer_effect { start_to_fire = seconds })

(* registered form: run the body on the decoded init arg, return output payload *)
type reg = { name : string; body : ctx -> Codec.payload -> Codec.payload }

let reg (t : (_, _) t) =
  { name = t.name;
    body =
      (fun ctx p ->
        Codec.to_payload t.output (t.run ctx (Codec.of_payload t.input p))) }
