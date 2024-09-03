// #![doc = include_str!("doc.md")]

pub mod app;

// use app_wrapper::CompassAppWrapper;
use pyo3::prelude::*;

#[pymodule]
fn routee_compass_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<app::UsCensusApp>()?;

    Ok(())
}
