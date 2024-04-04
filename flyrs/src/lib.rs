use prost::Message;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::runtime::{Builder, Runtime};
use tonic::{transport::{Channel}};

// We use env macro here, typically cannot have executable code, like `std:dev is_ok()`` in this case,  directly in the global scope outside of function bodies in Rust.
// Need better error handling if environment variable is empty
pub mod datacatalog {
  include!(concat!(env!("PB_OUT_DIR"), "datacatalog.rs"));
}
pub mod flyteidl {
  pub mod admin {
    include!(concat!(env!("PB_OUT_DIR"), "flyteidl.admin.rs"));
  }
  pub mod cache {
    include!(concat!(env!("PB_OUT_DIR"), "flyteidl.cacheservice.rs"));
  }
  pub mod core {
    include!(concat!(env!("PB_OUT_DIR"), "flyteidl.core.rs"));
  }
  pub mod event {
    include!(concat!(env!("PB_OUT_DIR"), "flyteidl.event.rs"));
  }
  pub mod plugins {
    include!(concat!(env!("PB_OUT_DIR"), "flyteidl.plugins.rs"));
    pub mod kubeflow{
      include!(concat!(env!("PB_OUT_DIR"), "flyteidl.plugins.kubeflow.rs"));
    }
  }
  pub mod service {
    include!(concat!(env!("PB_OUT_DIR"), "flyteidl.service.rs"));
  }
}


use crate::flyteidl::service::{TaskGetResponse, admin_service_client::AdminServiceClient, signal_service_client, data_proxy_service_client};
use crate::flyteidl::admin::{Task, ObjectGetRequest, ResourceListRequest, TaskExecutionGetRequest};

// Unlike the normal use case of PyO3, we don't have to add attribute macros such as #[pyclass] or #[pymethods] to all of our flyteidl structs.
// In this case, we only use PyO3 to expose the client class and its methods to Python (FlyteKit).
// It would be confusing to maintain two identical data class implementations in two languages.
// Additionally, it's too complex to get/set every member value of any nested high-level data structure.
// By design, Rust forbids implementing an external trait for an external struct. This can be tricky when using well-known types from the `prost_types` crate.
// Furthermore, there's no scalable way to add attribute macros while building protos without resorting to numerous hacky workarounds.

#[pyclass(subclass)]
pub struct FlyteClient {
  admin_service: AdminServiceClient<Channel>,
  runtime: Runtime,
}

// Using temporary value(e.g., endpoint) in async is tricky w.r.t lifetime.
// The compiler will complain that the temporary value does not live long enough.
// TODO: figure out how to pass in the required initial args into constructor in a clean and neat way.
#[pymethods]
impl FlyteClient {
  #[new] // Without this, you cannot construct the underlying class in Python.
  pub fn new() -> PyResult<FlyteClient> {
    let rt =  Builder::new_multi_thread().enable_all().build().unwrap();
    // TODO: Create a channel then bind it to every stubs/clients instead of connecting everytime.
    let stub = rt.block_on(AdminServiceClient::connect("http://localhost:30080")).unwrap();
    // TODO: Add more thoughtful error handling
    Ok(FlyteClient {
      runtime: rt, // The tokio runtime is used in a blocking manner now, left lots of investigation and TODOs behind.
      admin_service: stub,
    }
    )
  }

  // fn parse_from_bytes(pb2_type, buf: &[u8]) {
  // }
  // fn serialize_tobytes(proto) {
  // }

  pub fn get_task(&mut self, py: Python, bytes_obj: &PyBytes) -> PyObject {
    let bytes = bytes_obj.as_bytes();
    let decoded: ObjectGetRequest = Message::decode(&bytes.to_vec()[..]).unwrap();
    let req =  tonic::Request::new(decoded);

    // Interacting with the gRPC server: flyteadmin
    let res = (self.runtime.block_on(self.admin_service.get_task(req))).unwrap().into_inner();

    let mut buf = vec![];
    res.encode(&mut buf).unwrap();

    PyBytes::new(py, &buf).into()
  }

  pub fn list_tasks_paginated(&mut self, py: Python, bytes_obj: &PyBytes) -> PyObject {
    let bytes = bytes_obj.as_bytes();
    let decoded: ResourceListRequest = Message::decode(&bytes.to_vec()[..]).unwrap();
    let req =  tonic::Request::new(decoded);
    // Interacting with the gRPC server: flyteadmin
    let res = (self.runtime.block_on(self.admin_service.list_tasks(req))).unwrap().into_inner();

    let mut buf = vec![];
    res.encode(&mut buf).unwrap();

    PyBytes::new(py, &buf).into()
  }

  pub fn echo_task(&mut self, py: Python, bytes_obj: &PyBytes) -> PyObject { // PyResult<Vec<u8>>
    let bytes = bytes_obj.as_bytes();
    println!("Received bytes: {:?}", bytes);
    let decoded: Task = Message::decode(&bytes.to_vec()[..]).unwrap();
    println!("Parsed Task: {:?}", decoded);
    let mut buf = vec![];
    decoded.encode(&mut buf).unwrap();
    println!("Serialized Task: {:?}", decoded);
    // Returning bytes buffer
    PyBytes::new(py, &buf).into()
  }

}


// Some trials
// fn tokio() -> &'static tokio::runtime::Runtime {
//   use std::sync::OnceLock;
//   static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
//   RT.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
// }
// async fn sleep(seconds: u64) -> u64  {
//   let sleep = async move { tokio::time::sleep(std::time::Duration::from_secs(seconds)).await };
//   tokio().spawn(sleep).await.unwrap();
//   seconds
// }
// #[pyfunction]
// async fn async_sleep_asyncio(seconds: u64) -> PyResult<u64> {
//     let t = sleep(seconds).await;
//     Ok(t)
// }
// #[pyfunction]
// fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
//     Ok((a + b).to_string())
// }
#[pymodule]
fn flyrs(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    // m.add_function(wrap_pyfunction!(async_sleep_asyncio, m)?)?;
    m.add_class::<FlyteClient>()?;
    Ok(())
}
