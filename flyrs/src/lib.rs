use pyo3::prelude::*;

pub mod auth;
pub mod remote;

use crate::remote::Raw;
// A Python module implemented in Rust.
#[pymodule]
fn flyrs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Raw::FlyteClient>()?;
    Ok(())
}
