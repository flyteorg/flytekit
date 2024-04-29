use flyteidl::flyteidl::admin;
use flyteidl::flyteidl::service::admin_service_client::AdminServiceClient;
use prost::{Message, DecodeError, EncodeError};
use pyo3::prelude::*;
use pyo3::{PyErr};
use pyo3::exceptions::{PyOSError, PyValueError};
use pyo3::types::{PyBytes, PyDict};
use std::sync::Arc;
use std::fmt;
use tokio::runtime::{Builder, Runtime};
use tonic::{
    transport::{Channel, Uri},
    Request,
    Response
};

// Foreign Rust error types: https://pyo3.rs/main/function/error-handling#foreign-rust-error-types
// Create a newtype wrapper, e.g. MyOtherError. Then implement From<MyOtherError> for PyErr (or PyErrArguments), as well as From<OtherError> for MyOtherError.

// Failed at encoding responded object to bytes string
struct MessageEncodeError(EncodeError);
// Failed at decoding requested object from bytes string
struct MessageDecodeError(DecodeError);

impl fmt::Display for MessageEncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "")
    }
}

impl fmt::Display for MessageDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "")
    }
}

impl std::convert::From<MessageEncodeError> for PyErr {
    fn from(err: MessageEncodeError) -> PyErr {
        PyOSError::new_err(err.to_string())
    }
}

impl std::convert::From<MessageDecodeError> for PyErr {
    fn from(err: MessageDecodeError) -> PyErr {
        PyOSError::new_err(err.to_string())
    }
}

impl std::convert::From<EncodeError> for MessageEncodeError {
    fn from(other: EncodeError) -> Self {
        Self(other)
    }
}

impl std::convert::From<DecodeError> for MessageDecodeError {
    fn from(other: DecodeError) -> Self {
        Self(other)
    }
}

/// A Python class constructs the gRPC service stubs and a Tokio asynchronous runtime in Rust.
#[pyclass(subclass)]
pub struct FlyteClient {
    admin_service: AdminServiceClient<Channel>,
    runtime: Runtime,
}

pub fn decode_proto<T>(bytes_obj: &PyBytes) -> Result<T, MessageDecodeError>
    where
    T: Message + Default, {
    let bytes = bytes_obj.as_bytes();
    let de = Message::decode(&bytes.to_vec()[..]);
    Ok(de?)
}

pub fn encode_proto<T>(res:T) -> Result<Vec<u8>, MessageEncodeError>
where
T: Message + Default, {
    let mut buf = vec![];
    res.encode(&mut buf)?;
    Ok(buf)
}

#[pymethods]
impl FlyteClient {
    #[new] // Without this, you cannot construct the underlying class in Python.
    #[pyo3(signature = (endpoint, **kwargs))]
    pub fn new(endpoint: &str, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<FlyteClient> {
        // Use Atomic Reference Counting abstractions as a cheap way to pass string reference into another thread that outlives the scope.
        let s = Arc::new(endpoint);
        // Check details for constructing Tokio asynchronous `runtime`: https://docs.rs/tokio/latest/tokio/runtime/struct.Builder.html#method.new_current_thread
        let rt = match Builder::new_current_thread().enable_all().build() {
            Ok(rt) => rt,
            Err(error) => panic!("Failed to initiate Tokio multi-thread runtime: {:?}", error),
        };
        // Check details for constructing `channel`: https://docs.rs/tonic/latest/tonic/transport/struct.Channel.html#method.builder
        // TODO: generally handle more protocols, e.g., `https://`
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
                "Failed at connecting to endpoint when constructing channel: {:?}",
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
        // Receive the request object in bytes from Python: flytekit remote
        let bytes = bytes_obj.as_bytes();
        // Deserialize bytes object into flyteidl type
        let decoded: admin::ObjectGetRequest = decode_proto(bytes_obj)?;
        // Prepare request object for gRPC services
        let req = Request::new(decoded);

        // Interacting with the gRPC service server: flyteadmin
        let res = (match self.runtime.block_on(self.admin_service.get_task(req)) {
            Ok(res) => res,
            Err(error) => panic!(
                "Failed at awaiting response from gRPC service server: {:?}",
                error
            ),
        })
        .into_inner();

        // Serialize service response object into flyteidl bytes buffer
        let buf:Vec<u8> = encode_proto(res)?;

        // Returning bytes buffer back to Python: flytekit remote for further parsing.
        Ok(PyBytes::new_bound(py, &buf).into())
    }

    pub fn create_task(&mut self, py: Python, bytes_obj: &PyBytes) -> PyResult<PyObject> {
        let decoded: admin::TaskCreateRequest = decode_proto(bytes_obj)?;
        let req = tonic::Request::new(decoded);

        let res = (match self.runtime.block_on(self.admin_service.create_task(req)) {
            Ok(res) => res,
            Err(error) => panic!(
                "Failed at awaiting response from  gRPC service server: {:?}",
                error
            ),
        })
        .into_inner();

        let buf:Vec<u8> = encode_proto(res)?;

        Ok(PyBytes::new_bound(py, &buf).into())
    }

    pub fn list_task_ids_paginated(
        &mut self,
        py: Python,
        bytes_obj: &PyBytes,
    ) -> PyResult<PyObject> {
        let decoded: admin::NamedEntityIdentifierListRequest = decode_proto(bytes_obj)?;
        let req = tonic::Request::new(decoded);

        let res = (match self.runtime.block_on(self.admin_service.list_task_ids(req)) {
            Ok(res) => res,
            Err(error) => panic!(
                "Failed at awaiting response from  gRPC service server: {:?}",
                error
            ),
        })
        .into_inner();

        let buf:Vec<u8> = encode_proto(res)?;

        Ok(PyBytes::new_bound(py, &buf).into())
    }

    pub fn list_tasks_paginated(&mut self, py: Python, bytes_obj: &PyBytes) -> PyResult<PyObject> {
        let decoded: admin::ResourceListRequest = decode_proto(bytes_obj)?;
        let req = tonic::Request::new(decoded);

        let res = (match self.runtime.block_on(self.admin_service.list_tasks(req)) {
            Ok(res) => res,
            Err(error) => panic!(
                "Failed at awaiting response from  gRPC service server: {:?}",
                error
            ),
        })
        .into_inner();

        let buf:Vec<u8> = encode_proto(res)?;

        Ok(PyBytes::new_bound(py, &buf).into())
    }
}

/// A Python module implemented in Rust.
#[pymodule]
fn flyrs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<FlyteClient>()?;
    Ok(())
}
