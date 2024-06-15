use pyo3::prelude::*;

pub mod auth;
pub mod remote;
pub mod idl;

use crate::remote::Raw;
use crate::idl::Core;
// A Python module implemented in Rust.
#[pymodule]
fn flyrs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Raw::FlyteClient>()?;
    m.add_class::<Core::SchemaType>()?;
    m.add_class::<Core::schema_type::SchemaColumn>()?;
    Ok(())
}
