//! A thin C-ABI bridge over the **real** `temporalio-sdk-core`.
//!
//! The exported C symbols and structs (`ByteArrayRef`, `ByteArray`, the opaque
//! `Runtime`/`Client`/`Worker` handles, and the callback-based async model)
//! match `include/temporal_bridge.h`, so the OCaml stubs never have to change.
//! The bodies delegate to sdk-core: a real Tokio-backed `CoreRuntime`, a real
//! gRPC `Connection`, and a real `Worker` polling/completing workflow tasks.
//!
//! Async note: for a scaffold we drive each async call to completion with
//! `Handle::block_on` on the calling thread, so the completion callback fires
//! on that same (OCaml) thread and no cross-thread runtime-lock dance is needed.
//! A production bridge would instead `Handle::spawn` and invoke the callback
//! from a Tokio worker thread (see stubs.c for the `caml_c_thread_register`
//! path that supports exactly that).

use core::ffi::c_void;
use prost::Message;
use std::ptr;
use std::sync::Arc;

use temporalio_client::{Connection, ConnectionOptions};
use temporalio_common::protos::coresdk::workflow_completion::WorkflowActivationCompletion;
use temporalio_common::protos::coresdk::ActivityTaskCompletion;
use temporalio_common::worker::WorkerTaskTypes;
use temporalio_sdk_core::{
    init_worker, CoreRuntime, RuntimeOptions, TokioRuntimeBuilder, Url, WorkerConfig,
    WorkerVersioningStrategy,
};

/// Borrowed bytes (argument direction).
#[repr(C)]
pub struct ByteArrayRef {
    pub data: *const u8,
    pub size: usize,
}

/// Owned bytes handed back to the caller; freed via `temporal_bridge_byte_array_free`.
#[repr(C)]
pub struct ByteArray {
    pub data: *mut u8,
    pub size: usize,
    pub cap: usize,
}

impl ByteArray {
    fn boxed_from(v: Vec<u8>) -> *mut ByteArray {
        let mut v = std::mem::ManuallyDrop::new(v);
        Box::into_raw(Box::new(ByteArray {
            data: v.as_mut_ptr(),
            size: v.len(),
            cap: v.capacity(),
        }))
    }
    fn boxed_err(msg: String) -> *const ByteArray {
        ByteArray::boxed_from(msg.into_bytes()).cast_const()
    }
}

// Opaque handles. `CoreRuntime` is shared (Arc) so clients/workers created from
// it keep it alive regardless of when `runtime_free` is called.
pub struct Runtime {
    core: Arc<CoreRuntime>,
}
pub struct Client {
    runtime: Arc<CoreRuntime>,
    conn: Connection,
}
pub struct Worker {
    runtime: Arc<CoreRuntime>,
    worker: Arc<temporalio_sdk_core::Worker>,
}

type ClientConnectCallback =
    extern "C" fn(user_data: *mut c_void, client: *mut Client, fail: *const ByteArray);
type WorkerPollCallback =
    extern "C" fn(user_data: *mut c_void, activation: *const ByteArray, fail: *const ByteArray);
type WorkerCallback = extern "C" fn(user_data: *mut c_void, fail: *const ByteArray);

unsafe fn ref_to_string(r: ByteArrayRef) -> String {
    if r.data.is_null() || r.size == 0 {
        return String::new();
    }
    String::from_utf8_lossy(std::slice::from_raw_parts(r.data, r.size)).into_owned()
}

unsafe fn ref_to_slice<'a>(r: ByteArrayRef) -> &'a [u8] {
    if r.data.is_null() || r.size == 0 {
        &[]
    } else {
        std::slice::from_raw_parts(r.data, r.size)
    }
}

#[no_mangle]
pub extern "C" fn temporal_bridge_runtime_new() -> *mut Runtime {
    match CoreRuntime::new(RuntimeOptions::default(), TokioRuntimeBuilder::default()) {
        Ok(core) => Box::into_raw(Box::new(Runtime {
            core: Arc::new(core),
        })),
        Err(e) => {
            eprintln!("temporal_bridge_runtime_new: {e}");
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn temporal_bridge_runtime_free(rt: *mut Runtime) {
    if !rt.is_null() {
        unsafe { drop(Box::from_raw(rt)) };
    }
}

#[no_mangle]
pub extern "C" fn temporal_bridge_client_connect(
    runtime: *mut Runtime,
    target: ByteArrayRef,
    user_data: *mut c_void,
    callback: ClientConnectCallback,
) {
    let rt = unsafe { &*runtime };
    let handle = rt.core.tokio_handle().clone();
    let target = unsafe { ref_to_string(target) };

    let url = match Url::parse(&target) {
        Ok(u) => u,
        Err(e) => {
            callback(
                user_data,
                ptr::null_mut(),
                ByteArray::boxed_err(format!("Invalid target url {target:?}: {e}")),
            );
            return;
        }
    };

    let opts = ConnectionOptions::new(url)
        .identity("temporal-ocaml".to_string())
        .build();

    // Real gRPC connect (calls GetSystemInfo). Fails here if no server is up —
    // which still proves the whole real stack is linked and executing.
    match handle.block_on(Connection::connect(opts)) {
        Ok(conn) => {
            let client = Box::into_raw(Box::new(Client {
                runtime: rt.core.clone(),
                conn,
            }));
            callback(user_data, client, ptr::null());
        }
        Err(e) => callback(
            user_data,
            ptr::null_mut(),
            ByteArray::boxed_err(format!("Connection failed: {e}")),
        ),
    }
}

#[no_mangle]
pub extern "C" fn temporal_bridge_client_free(c: *mut Client) {
    if !c.is_null() {
        unsafe { drop(Box::from_raw(c)) };
    }
}

#[no_mangle]
pub extern "C" fn temporal_bridge_worker_new(
    client: *mut Client,
    task_queue: ByteArrayRef,
) -> *mut Worker {
    let c = unsafe { &*client };
    let handle = c.runtime.tokio_handle().clone();
    let _guard = handle.enter(); // init_worker expects to run inside the tokio ctx
    let task_queue = unsafe { ref_to_string(task_queue) };

    let config = match WorkerConfig::builder()
        .namespace("default".to_string())
        .task_queue(task_queue)
        .task_types(WorkerTaskTypes {
            enable_workflows: true,
            enable_local_activities: false,
            enable_remote_activities: true,
            enable_nexus: false,
        })
        .max_outstanding_workflow_tasks(100usize)
        .max_outstanding_activities(100usize)
        .versioning_strategy(WorkerVersioningStrategy::None {
            build_id: String::new(),
        })
        .build()
    {
        Ok(cfg) => cfg,
        Err(e) => {
            eprintln!("temporal_bridge_worker_new: invalid config: {e}");
            return ptr::null_mut();
        }
    };

    match init_worker(&c.runtime, config, c.conn.clone()) {
        Ok(w) => Box::into_raw(Box::new(Worker {
            runtime: c.runtime.clone(),
            worker: Arc::new(w),
        })),
        Err(e) => {
            eprintln!("temporal_bridge_worker_new: {e}");
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn temporal_bridge_worker_free(w: *mut Worker) {
    if !w.is_null() {
        unsafe { drop(Box::from_raw(w)) };
    }
}

#[no_mangle]
pub extern "C" fn temporal_bridge_worker_poll_workflow_activation(
    worker: *mut Worker,
    user_data: *mut c_void,
    callback: WorkerPollCallback,
) {
    let w = unsafe { &*worker };
    let handle = w.runtime.tokio_handle().clone();
    match handle.block_on(w.worker.poll_workflow_activation()) {
        Ok(activation) => {
            let bytes = ByteArray::boxed_from(activation.encode_to_vec());
            callback(user_data, bytes, ptr::null());
        }
        Err(e) => callback(
            user_data,
            ptr::null(),
            ByteArray::boxed_err(format!("Workflow polling failure: {e}")),
        ),
    }
}

#[no_mangle]
pub extern "C" fn temporal_bridge_worker_complete_workflow_activation(
    worker: *mut Worker,
    completion: ByteArrayRef,
    user_data: *mut c_void,
    callback: WorkerCallback,
) {
    let w = unsafe { &*worker };
    let handle = w.runtime.tokio_handle().clone();
    let completion = match WorkflowActivationCompletion::decode(unsafe { ref_to_slice(completion) })
    {
        Ok(c) => c,
        Err(e) => {
            callback(
                user_data,
                ByteArray::boxed_err(format!("Workflow completion decode failure: {e}")),
            );
            return;
        }
    };
    match handle.block_on(w.worker.complete_workflow_activation(completion)) {
        Ok(()) => callback(user_data, ptr::null()),
        Err(e) => callback(
            user_data,
            ByteArray::boxed_err(format!("Workflow completion failure: {e}")),
        ),
    }
}

#[no_mangle]
pub extern "C" fn temporal_bridge_worker_poll_activity_task(
    worker: *mut Worker,
    user_data: *mut c_void,
    callback: WorkerPollCallback,
) {
    let w = unsafe { &*worker };
    let handle = w.runtime.tokio_handle().clone();
    match handle.block_on(w.worker.poll_activity_task()) {
        Ok(task) => {
            let bytes = ByteArray::boxed_from(task.encode_to_vec());
            callback(user_data, bytes, ptr::null());
        }
        Err(e) => callback(
            user_data,
            ptr::null(),
            ByteArray::boxed_err(format!("Activity polling failure: {e}")),
        ),
    }
}

#[no_mangle]
pub extern "C" fn temporal_bridge_worker_complete_activity_task(
    worker: *mut Worker,
    completion: ByteArrayRef,
    user_data: *mut c_void,
    callback: WorkerCallback,
) {
    let w = unsafe { &*worker };
    let handle = w.runtime.tokio_handle().clone();
    let completion = match ActivityTaskCompletion::decode(unsafe { ref_to_slice(completion) }) {
        Ok(c) => c,
        Err(e) => {
            callback(
                user_data,
                ByteArray::boxed_err(format!("Activity completion decode failure: {e}")),
            );
            return;
        }
    };
    match handle.block_on(w.worker.complete_activity_task(completion)) {
        Ok(()) => callback(user_data, ptr::null()),
        Err(e) => callback(
            user_data,
            ByteArray::boxed_err(format!("Activity completion failure: {e}")),
        ),
    }
}

#[no_mangle]
pub extern "C" fn temporal_bridge_byte_array_free(_rt: *mut Runtime, bytes: *mut ByteArray) {
    if bytes.is_null() {
        return;
    }
    unsafe {
        let ba = Box::from_raw(bytes);
        if !ba.data.is_null() {
            drop(Vec::from_raw_parts(ba.data, ba.size, ba.cap));
        }
    }
}
