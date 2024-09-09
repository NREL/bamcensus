// #![doc = include_str!("doc.md")]

mod us_census_python_app;
use pyo3::prelude::*;
use us_census_python_app::UsCensusPythonApi;

#[pymodule]
fn routee_compass_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<UsCensusPythonApi>()?;

    Ok(())
}
