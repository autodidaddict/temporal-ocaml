/*
 * Copyright 2026 Kevin Hoffman
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* OCaml <-> C glue for the Temporal sdk-core bridge.
 *
 * Responsibilities:
 *   - box opaque Rust pointers (Runtime/Client/Worker) as OCaml custom blocks,
 *   - convert OCaml strings <-> ByteArrayRef/ByteArray,
 *   - adapt OCaml closures into the C completion callbacks, keeping the closure
 *     alive (as a GC global root) across the async call.
 *
 * The async calls in the mock/scaffold fire their callback synchronously on the
 * calling thread, which already holds the OCaml runtime lock, so we can invoke
 * the closure directly. If you switch the Rust side to spawn work on a Tokio
 * thread and call back from there, wrap the body of each *_trampoline in:
 *
 *     caml_c_thread_register();        // once per foreign thread
 *     caml_acquire_runtime_system();   // take the lock before touching values
 *     ... build values + caml_callback ...
 *     caml_release_runtime_system();
 *
 * and link the `threads.posix` library.
 */
#include <caml/alloc.h>
#include <caml/callback.h>
#include <caml/custom.h>
#include <caml/fail.h>
#include <caml/memory.h>
#include <caml/mlvalues.h>
#include <stdlib.h>
#include <string.h>

#include "temporal_bridge.h"

/* ------- opaque pointer handles as OCaml custom blocks ------------------- */

#define Ptr_val(v) (*((void **)Data_custom_val(v)))

static struct custom_operations ptr_ops = {
    "temporal.ptr",
    custom_finalize_default, /* freeing is explicit via *_free externals */
    custom_compare_default,   custom_hash_default,
    custom_serialize_default, custom_deserialize_default,
    custom_compare_ext_default, custom_fixed_length_default};

static value alloc_ptr(void *p) {
  value v = caml_alloc_custom(&ptr_ops, sizeof(void *), 0, 1);
  Ptr_val(v) = p;
  return v;
}

/* ------- value builders ------------------------------------------------- */

/* Some (custom-block-boxed pointer), or None if p is NULL. */
static value some_ptr_opt(void *p) {
  CAMLparam0();
  CAMLlocal2(box, opt);
  if (p == NULL)
    CAMLreturn(Val_int(0)); /* None */
  box = alloc_ptr(p);
  opt = caml_alloc(1, 0); /* Some _ */
  Store_field(opt, 0, box);
  CAMLreturn(opt);
}

/* Some (string copied from the byte array), or None if b is NULL. */
static value some_str_opt(const ByteArray *b) {
  CAMLparam0();
  CAMLlocal2(s, opt);
  if (b == NULL)
    CAMLreturn(Val_int(0)); /* None */
  s = caml_alloc_initialized_string(b->size, (const char *)b->data);
  opt = caml_alloc(1, 0);
  Store_field(opt, 0, s);
  CAMLreturn(opt);
}

/* ------- callback slot: keeps the OCaml closure rooted across the call --- */

typedef struct {
  value closure;   /* registered as a GC global root */
  Runtime *runtime; /* used to free returned ByteArrays */
} cb_slot;

static cb_slot *slot_new(value closure, Runtime *rt) {
  cb_slot *s = (cb_slot *)malloc(sizeof(cb_slot));
  s->closure = closure;
  s->runtime = rt;
  caml_register_global_root(&s->closure);
  return s;
}

static void slot_free(cb_slot *s) {
  caml_remove_global_root(&s->closure);
  free(s);
}

/* ------- trampolines: C callback types -> OCaml closure calls ----------- */

static void connect_trampoline(void *user_data, Client *client,
                               const ByteArray *fail) {
  CAMLparam0();
  CAMLlocal2(client_opt, fail_opt);
  cb_slot *s = (cb_slot *)user_data;
  client_opt = some_ptr_opt(client);
  fail_opt = some_str_opt(fail);
  caml_callback2(s->closure, client_opt, fail_opt);
  if (fail)
    temporal_bridge_byte_array_free(s->runtime, (ByteArray *)fail);
  slot_free(s);
  CAMLreturn0;
}

static void poll_trampoline(void *user_data, const ByteArray *activation,
                            const ByteArray *fail) {
  CAMLparam0();
  CAMLlocal2(act_opt, fail_opt);
  cb_slot *s = (cb_slot *)user_data;
  act_opt = some_str_opt(activation);
  fail_opt = some_str_opt(fail);
  caml_callback2(s->closure, act_opt, fail_opt);
  if (activation)
    temporal_bridge_byte_array_free(s->runtime, (ByteArray *)activation);
  if (fail)
    temporal_bridge_byte_array_free(s->runtime, (ByteArray *)fail);
  slot_free(s);
  CAMLreturn0;
}

static void complete_trampoline(void *user_data, const ByteArray *fail) {
  CAMLparam0();
  CAMLlocal1(fail_opt);
  cb_slot *s = (cb_slot *)user_data;
  fail_opt = some_str_opt(fail);
  caml_callback(s->closure, fail_opt);
  if (fail)
    temporal_bridge_byte_array_free(s->runtime, (ByteArray *)fail);
  slot_free(s);
  CAMLreturn0;
}

/* ------- externals: 1:1 with the raw bridge functions ------------------- */

CAMLprim value ocaml_temporal_runtime_new(value unit) {
  CAMLparam1(unit);
  CAMLreturn(alloc_ptr(temporal_bridge_runtime_new()));
}

CAMLprim value ocaml_temporal_runtime_free(value v_rt) {
  CAMLparam1(v_rt);
  temporal_bridge_runtime_free((Runtime *)Ptr_val(v_rt));
  CAMLreturn(Val_unit);
}

CAMLprim value ocaml_temporal_client_connect(value v_rt, value v_target,
                                             value v_cb) {
  CAMLparam3(v_rt, v_target, v_cb);
  Runtime *rt = (Runtime *)Ptr_val(v_rt);
  ByteArrayRef target;
  target.data = (const uint8_t *)String_val(v_target);
  target.size = caml_string_length(v_target);
  cb_slot *s = slot_new(v_cb, rt);
  temporal_bridge_client_connect(rt, target, s, connect_trampoline);
  CAMLreturn(Val_unit);
}

CAMLprim value ocaml_temporal_client_free(value v_c) {
  CAMLparam1(v_c);
  temporal_bridge_client_free((Client *)Ptr_val(v_c));
  CAMLreturn(Val_unit);
}

CAMLprim value ocaml_temporal_worker_new(value v_c, value v_tq) {
  CAMLparam2(v_c, v_tq);
  ByteArrayRef tq;
  tq.data = (const uint8_t *)String_val(v_tq);
  tq.size = caml_string_length(v_tq);
  CAMLreturn(alloc_ptr(temporal_bridge_worker_new((Client *)Ptr_val(v_c), tq)));
}

CAMLprim value ocaml_temporal_worker_free(value v_w) {
  CAMLparam1(v_w);
  temporal_bridge_worker_free((Worker *)Ptr_val(v_w));
  CAMLreturn(Val_unit);
}

CAMLprim value ocaml_temporal_worker_poll(value v_rt, value v_worker,
                                          value v_cb) {
  CAMLparam3(v_rt, v_worker, v_cb);
  Runtime *rt = (Runtime *)Ptr_val(v_rt);
  Worker *w = (Worker *)Ptr_val(v_worker);
  cb_slot *s = slot_new(v_cb, rt);
  temporal_bridge_worker_poll_workflow_activation(w, s, poll_trampoline);
  CAMLreturn(Val_unit);
}

CAMLprim value ocaml_temporal_worker_complete(value v_rt, value v_worker,
                                              value v_completion, value v_cb) {
  CAMLparam4(v_rt, v_worker, v_completion, v_cb);
  Runtime *rt = (Runtime *)Ptr_val(v_rt);
  Worker *w = (Worker *)Ptr_val(v_worker);
  ByteArrayRef completion;
  completion.data = (const uint8_t *)String_val(v_completion);
  completion.size = caml_string_length(v_completion);
  cb_slot *s = slot_new(v_cb, rt);
  temporal_bridge_worker_complete_workflow_activation(w, completion, s,
                                                      complete_trampoline);
  CAMLreturn(Val_unit);
}

CAMLprim value ocaml_temporal_activity_poll(value v_rt, value v_worker,
                                            value v_cb) {
  CAMLparam3(v_rt, v_worker, v_cb);
  Runtime *rt = (Runtime *)Ptr_val(v_rt);
  Worker *w = (Worker *)Ptr_val(v_worker);
  cb_slot *s = slot_new(v_cb, rt);
  temporal_bridge_worker_poll_activity_task(w, s, poll_trampoline);
  CAMLreturn(Val_unit);
}

CAMLprim value ocaml_temporal_activity_complete(value v_rt, value v_worker,
                                                value v_completion, value v_cb) {
  CAMLparam4(v_rt, v_worker, v_completion, v_cb);
  Runtime *rt = (Runtime *)Ptr_val(v_rt);
  Worker *w = (Worker *)Ptr_val(v_worker);
  ByteArrayRef completion;
  completion.data = (const uint8_t *)String_val(v_completion);
  completion.size = caml_string_length(v_completion);
  cb_slot *s = slot_new(v_cb, rt);
  temporal_bridge_worker_complete_activity_task(w, completion, s,
                                                complete_trampoline);
  CAMLreturn(Val_unit);
}
