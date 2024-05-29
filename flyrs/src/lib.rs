use pyo3::prelude::*;

pub mod auth;
pub mod raw;

use crate::raw::GrpcClient;
// A Python module implemented in Rust.
#[pymodule]
fn flyrs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<GrpcClient::FlyteClient>()?;
    Ok(())
}
