use prost::{Message};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use tokio::runtime::{Builder, Runtime};
use tonic::{
  metadata::MetadataValue,
  codegen::InterceptedService,
  service::Interceptor,
  transport::{Channel, Endpoint, Error},
  Request, Status,
};

use flyteidl::flyteidl::service::admin_service_client::AdminServiceClient;
use flyteidl::flyteidl::admin;
use std::option::Option;

// Unlike the normal use case of PyO3, we don't have to add attribute macros such as #[pyclass] or #[pymethods] to all of our flyteidl structs.
// In this case, we only use PyO3 to expose the client class and its methods to Python (FlyteKit).
// It would be confusing to maintain two identical data class implementations in two languages.
// Additionally, it's too complex to get/set every member value of any nested high-level data structure.
// By design, Rust forbids implementing an external trait for an external struct. This can be tricky when using well-known types from the `prost_types` crate.
// Furthermore, there's no scalable way to add attribute macros while building protos without resorting to numerous hacky workarounds.

#[pyclass(subclass)]
pub struct FlyteClient {
  admin_service: AdminServiceClient<InterceptedService<Channel, AuthUnaryInterceptor>>,
  runtime: Runtime,
}

struct AuthUnaryInterceptor;

impl Interceptor for AuthUnaryInterceptor {
  fn call(&mut self, mut request: tonic::Request<()>) -> Result<tonic::Request<()>, Status> {
    let token: MetadataValue<_> = "Bearer some-auth-token".parse().unwrap();
      request.metadata_mut().insert("authorization", token.clone());
      Ok(request)
    }
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
    let channel = rt.block_on(Endpoint::from_static("http://localhost:30080").connect()).unwrap();
    // let stub = rt.block_on(AdminServiceClient::connect("http://localhost:30080")).unwrap();
    let mut stub = AdminServiceClient::with_interceptor(channel, AuthUnaryInterceptor);
    // TODO: Add more thoughtful error handling
    Ok(FlyteClient {
      runtime: rt, // The tokio runtime is used in a blocking manner now, leaving lots of investigation and TODOs behind.
      admin_service: stub,
    }
    )
  }

  // fn parse_from_bytes(pb2_type, buf: &[u8]) {
  // }
  // fn serialize_tobytes(proto) {
  // }

  pub fn create_task(&mut self, py: Python, bytes_obj: &PyBytes) -> PyResult<PyObject> {
    let bytes = bytes_obj.as_bytes();
    let decoded: admin::TaskCreateRequest = Message::decode(&bytes.to_vec()[..]).unwrap();
    let req =  tonic::Request::new(decoded);

    // Interacting with the gRPC server: flyteadmin
    let res = (self.runtime.block_on(self.admin_service.create_task(req))).unwrap().into_inner();

    let mut buf = vec![];
    res.encode(&mut buf).unwrap();

    Ok(PyBytes::new_bound(py, &buf).into())
  }

  pub fn get_task(&mut self, py: Python, bytes_obj: &PyBytes) -> PyResult<PyObject> {
    let bytes = bytes_obj.as_bytes();
    let decoded: admin::ObjectGetRequest = Message::decode(&bytes.to_vec()[..]).unwrap();
    let req =  tonic::Request::new(decoded);

    // Interacting with the gRPC server: flyteadmin
    let res = (self.runtime.block_on(self.admin_service.get_task(req))).unwrap().into_inner();

    let mut buf = vec![];
    res.encode(&mut buf).unwrap();

    Ok(PyBytes::new_bound(py, &buf).into())
  }

  pub fn list_task_ids_paginated(&mut self, py: Python, bytes_obj: &PyBytes) -> PyResult<PyObject> {
    let bytes = bytes_obj.as_bytes();
    let decoded: admin::NamedEntityIdentifierListRequest = Message::decode(&bytes.to_vec()[..]).unwrap();
    let req =  tonic::Request::new(decoded);

    // Interacting with the gRPC server: flyteadmin
    let res = self.runtime.block_on(self.admin_service.list_task_ids(req)).unwrap().into_inner();

    let mut buf = vec![];
    res.encode(&mut buf).unwrap();

    Ok(PyBytes::new_bound(py, &buf).into())
  }

  pub fn list_tasks_paginated(&mut self, py: Python, bytes_obj: &PyBytes) -> PyResult<PyObject>  {
    let bytes = bytes_obj.as_bytes();
    let decoded: admin::ResourceListRequest = Message::decode(&bytes.to_vec()[..]).unwrap();
    let req =  tonic::Request::new(decoded);

    // Interacting with the gRPC server: flyteadmin
    let res = self.runtime.block_on(self.admin_service.list_tasks(req)).unwrap().into_inner();

    let mut buf = vec![];
    res.encode(&mut buf).unwrap();

    Ok(PyBytes::new_bound(py, &buf).into())
  }

  pub fn echo_task(&mut self, py: Python, bytes_obj: &PyBytes) -> PyResult<PyObject> { // PyResult<Vec<u8>>
    let bytes = bytes_obj.as_bytes();
    println!("Received bytes: {:?}", bytes);
    let decoded: admin::Task = Message::decode(&bytes.to_vec()[..]).unwrap();
    println!("Parsed Task: {:?}", decoded);
    let mut buf = vec![];
    decoded.encode(&mut buf).unwrap();
    println!("Serialized Task: {:?}", decoded);
    // Returning bytes buffer
    Ok(PyBytes::new_bound(py, &buf).into())
  }

  pub fn create_workflow(&mut self, py: Python, bytes_obj: &PyBytes) -> PyResult<PyObject> {
    let bytes = bytes_obj.as_bytes();
    let decoded: admin::WorkflowCreateRequest = Message::decode(&bytes.to_vec()[..]).unwrap();
    let req =  tonic::Request::new(decoded);

    // Interacting with the gRPC server: flyteadmin
    let res = (self.runtime.block_on(self.admin_service.create_workflow(req))).unwrap().into_inner();

    let mut buf = vec![];
    res.encode(&mut buf).unwrap();

    Ok(PyBytes::new_bound(py, &buf).into())
  }

  pub fn get_workflow(&mut self, py: Python, bytes_obj: &PyBytes) -> PyResult<PyObject> {
    let bytes = bytes_obj.as_bytes();
    let decoded: admin::ObjectGetRequest = Message::decode(&bytes.to_vec()[..]).unwrap();
    let req =  tonic::Request::new(decoded);

    // Interacting with the gRPC server: flyteadmin
    let res = (self.runtime.block_on(self.admin_service.get_workflow(req))).unwrap().into_inner();

    let mut buf = vec![];
    res.encode(&mut buf).unwrap();

    Ok(PyBytes::new_bound(py, &buf).into())
  }

  pub fn list_workflow_ids_paginated(&mut self, py: Python, bytes_obj: &PyBytes) -> PyResult<PyObject> {
    let bytes = bytes_obj.as_bytes();
    let decoded: admin::NamedEntityIdentifierListRequest = Message::decode(&bytes.to_vec()[..]).unwrap();
    let req =  tonic::Request::new(decoded);

    // Interacting with the gRPC server: flyteadmin
    let res = self.runtime.block_on(self.admin_service.list_workflow_ids(req)).unwrap().into_inner();

    let mut buf = vec![];
    res.encode(&mut buf).unwrap();

    Ok(PyBytes::new_bound(py, &buf).into())
  }

  pub fn list_workflows_paginated(&mut self, py: Python, bytes_obj: &PyBytes) -> PyResult<PyObject>  {
    let bytes = bytes_obj.as_bytes();
    let decoded: admin::ResourceListRequest = Message::decode(&bytes.to_vec()[..]).unwrap();
    let req =  tonic::Request::new(decoded);

    // Interacting with the gRPC server: flyteadmin
    let res = self.runtime.block_on(self.admin_service.list_workflows(req)).unwrap().into_inner();

    let mut buf = vec![];
    res.encode(&mut buf).unwrap();

    Ok(PyBytes::new_bound(py, &buf).into())
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
