use pyo3::prelude::*;
mod us_census_python_app;

#[pymodule]
#[pyo3(name = "uscensus")]
fn us_census_api_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(us_census_python_app::wac_tiger, m)?)?;
    Ok(())
}
