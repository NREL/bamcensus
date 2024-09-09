// #![doc = include_str!("doc.md")]

mod us_census_python_app;
use pyo3::prelude::*;

#[pymodule]
fn us_census_api_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(us_census_python_app::wac_tiger, m)?)?;
    Ok(())
}
