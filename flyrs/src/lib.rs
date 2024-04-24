use flyteidl::flyteidl::admin;
use flyteidl::flyteidl::service::admin_service_client::AdminServiceClient;
use prost::Message;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::sync::Arc;
use tokio::runtime::{Builder, Runtime};
use tonic::{
    transport::{Channel, Uri},
    Request,
};

/// A Python class constructs the gRPC service stubs and a Tokio asynchronous runtime in Rust.
#[pyclass(subclass)]
pub struct FlyteClient {
    admin_service: AdminServiceClient<Channel>,
    runtime: Runtime,
}

#[pymethods]
impl FlyteClient {
    #[new] // Without this, you cannot construct the underlying class in Python.
    pub fn new(endpoint: &str) -> PyResult<FlyteClient> {
        // Use Atomic Reference Counting abstractions as a cheap way to pass string reference into another thread that outlives the scope.
        let s = Arc::new(endpoint);
        // Check details for constructing Tokio asynchronous `runtime`: https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html#method.new_current_thread
        let rt = match Builder::new_current_thread().enable_all().build() {
            Ok(rt) => rt,
            Err(error) => panic!("Failed to initiate Tokio multi-thread runtime: {:?}", error),
        };
        // Check details for constructing `channel`: https://docs.rs/tonic/latest/tonic/transport/struct.Channel.html#method.builder
        // TODO: Security protocol handling, e.g., `https://`
        let endpoint_uri = match format!("http://{}", *s.clone()).parse::<Uri>() {
            Ok(uri) => uri,
            Err(error) => panic!(
                "Got invalid endpoint when parsing endpoint_uri: {:?}",
                error
            ),
        };
        // `Channel::builder(endpoint_uri)` returns type `tonic::transport::Endpoint`.
        let channel = match rt.block_on(Channel::builder(endpoint_uri).connect()) {
            Ok(ch) => ch,
            Err(error) => panic!(
                "Faild at connecting to endpoint when constructing channel: {:?}",
                error
            ),
        };
        // Binding connected channel into service client stubs.
        let stub = AdminServiceClient::new(channel);
        Ok(FlyteClient {
            runtime: rt, // The tokio runtime is used in a blocking manner for now.
            admin_service: stub,
        })
    }

    pub fn get_task(&mut self, py: Python, bytes_obj: &PyBytes) -> PyResult<PyObject> {
        // Recieve the request object in bytes from Python: flytekit remote
        let bytes = bytes_obj.as_bytes();
        // Deserialize bytes object into flyteidl type
        let decoded: admin::ObjectGetRequest = match Message::decode(&bytes.to_vec()[..]) {
            Ok(de) => de,
            Err(error) => panic!(
                "Failed at decoding requested object from bytes string: {:?}",
                error
            ),
        };
        // Prepare request object for gRPC serivces
        let req = Request::new(decoded);

        // Interacting with the gRPC serivce server: flyteadmin
        let res = (match self.runtime.block_on(self.admin_service.get_task(req)) {
            Ok(res) => res,
            Err(error) => panic!(
                "Failed at awaiting response from gRPC serivce server: {:?}",
                error
            ),
        })
        .into_inner();

        // Serialize serivce response object into flyteidl bytes buffer
        let mut buf = vec![];
        match res.encode(&mut buf) {
            Ok(en) => en,
            Err(error) => panic!(
                "Failed at encoding responsed object to bytes string: {:?}",
                error
            ),
        };

        // Returning bytes buffer back to Python: flytekit remote for further parsing.
        Ok(PyBytes::new_bound(py, &buf).into())
    }

    pub fn create_task(&mut self, py: Python, bytes_obj: &PyBytes) -> PyResult<PyObject> {
        let bytes = bytes_obj.as_bytes();
        let decoded: admin::TaskCreateRequest = match Message::decode(&bytes.to_vec()[..]) {
            Ok(de) => de,
            Err(error) => panic!(
                "Failed at decoding requested object from bytes string: {:?}",
                error
            ),
        };
        let req = tonic::Request::new(decoded);

        let res = (match self.runtime.block_on(self.admin_service.create_task(req)) {
            Ok(res) => res,
            Err(error) => panic!(
                "Failed at awaiting response from  gRPC serivce server: {:?}",
                error
            ),
        })
        .into_inner();

        let mut buf = vec![];
        match res.encode(&mut buf) {
            Ok(en) => en,
            Err(error) => panic!(
                "Failed at encoding responsed object to bytes string: {:?}",
                error
            ),
        };

        Ok(PyBytes::new_bound(py, &buf).into())
    }

    pub fn list_task_ids_paginated(
        &mut self,
        py: Python,
        bytes_obj: &PyBytes,
    ) -> PyResult<PyObject> {
        let bytes = bytes_obj.as_bytes();
        let decoded: admin::NamedEntityIdentifierListRequest =
            match Message::decode(&bytes.to_vec()[..]) {
                Ok(de) => de,
                Err(error) => panic!(
                    "Failed at decoding requested object from bytes string: {:?}",
                    error
                ),
            };
        let req = tonic::Request::new(decoded);

        let res = (match self.runtime.block_on(self.admin_service.list_task_ids(req)) {
            Ok(res) => res,
            Err(error) => panic!(
                "Failed at awaiting response from  gRPC serivce server: {:?}",
                error
            ),
        })
        .into_inner();
        let mut buf = vec![];
        match res.encode(&mut buf) {
            Ok(en) => en,
            Err(error) => panic!(
                "Failed at encoding responsed object to bytes string: {:?}",
                error
            ),
        };

        Ok(PyBytes::new_bound(py, &buf).into())
    }

    pub fn list_tasks_paginated(&mut self, py: Python, bytes_obj: &PyBytes) -> PyResult<PyObject> {
        let bytes = bytes_obj.as_bytes();
        let decoded: admin::ResourceListRequest = match Message::decode(&bytes.to_vec()[..]) {
            Ok(de) => de,
            Err(error) => panic!(
                "Failed at decoding requested object from bytes string: {:?}",
                error
            ),
        };
        let req = tonic::Request::new(decoded);

        let res = (match self.runtime.block_on(self.admin_service.list_tasks(req)) {
            Ok(res) => res,
            Err(error) => panic!(
                "Failed at awaiting response from  gRPC serivce server: {:?}",
                error
            ),
        })
        .into_inner();

        let mut buf = vec![];
        match res.encode(&mut buf) {
            Ok(en) => en,
            Err(error) => panic!(
                "Failed at encoding responsed object to bytes string: {:?}",
                error
            ),
        };

        Ok(PyBytes::new_bound(py, &buf).into())
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn flyrs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<FlyteClient>()?;
    Ok(())
}
