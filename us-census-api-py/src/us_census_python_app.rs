use pyo3::types::IntoPyDict;
use pyo3::types::PyDict;
use pyo3::{exceptions::PyException, prelude::*};
use serde::de;
use us_census_app::lodes_tiger;
use us_census_core::model::lodes::{
    LodesDataset, LodesEdition, LodesJobType, WacSegment, WorkplaceSegment,
};
use wkt::ToWkt;

#[pyclass]
pub struct UsCensusPythonApi {}

#[pymethods]
impl UsCensusPythonApi {
    /// kwds example: https://pyo3.rs/main/function/signature#using-pyo3signature--
    #[pyo3(signature = (year, **kwds))]
    fn run_lodes_wac_tiger<'a>(
        &self,
        year: u64,
        kwds: Option<&Bound<'a, PyDict>>,
        py: Python<'a>,
    ) -> PyResult<pyo3::Bound<'a, PyDict>> {
        let dataset_result: PyResult<LodesDataset> =
            kwds.map_or(Ok(LodesDataset::default()), |m| {
                let edition: LodesEdition = get_string_deserializable(&"edition", m)?;
                let job_type: LodesJobType = get_string_deserializable(&"job_type", m)?;
                let segment: WorkplaceSegment = get_string_deserializable(&"segment", m)?;
                let dataset = LodesDataset::WAC {
                    edition,
                    job_type,
                    segment,
                    year,
                };
                Ok(dataset)
            });
        let dataset = dataset_result?;
        // .map_err(|e| PyException::new_err(format!("failure building LodesDataset: {}", e)))?;
        let geoids = kwds.map_or(Ok(vec![]), |m| get_comma_separated(&"geoids", m))?;
        let wac_segments = kwds.map_or(Ok(vec![WacSegment::C000]), |m| {
            get_comma_separated(&"wac_segments", m)
        })?;
        let wildcard = kwds.map_or(Ok(None), |m| {
            get_string_deserializable(&"geoids", m).map(|g| Some(g))
        })?;

        let future = lodes_tiger::run(year, geoids, &wildcard, &wac_segments, dataset);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| {
                PyException::new_err(format!("failure creating async rust tokio runtime: {}", e))
            })?;
        let result = runtime.block_on(future).map_err(|e| {
            PyException::new_err(format!("failure running LODES WAC + TIGER workflow: {}", e))
        })?;

        let vals = result
            .join_dataset
            .into_iter()
            .map(|row| {
                let dict = PyDict::new_bound(py);
                dict.set_item("segment", row.value.segment.to_string())?;
                dict.set_item("value", row.value.value)?;
                dict.set_item("geometry", row.geometry.to_wkt().to_string())?;
                Ok((row.geoid.to_string(), dict.to_object(py)))
            })
            .collect::<PyResult<Vec<_>>>()?;
        let out_dict = vals.into_py_dict_bound(py);
        Ok(out_dict)
    }
}

fn get_comma_separated<'a, T>(key: &str, map: &Bound<'_, PyDict>) -> PyResult<Vec<T>>
where
    T: de::DeserializeOwned,
{
    let ss: String = get_string_deserializable(key, map)?;
    let result = ss
        .split(",")
        .map(|s| {
            serde_json::from_str(s).map_err(|e| {
                format!(
                    "failure decoding comma-separated arguments in '{}': {}",
                    key, e
                )
            })
        })
        .collect::<Result<Vec<T>, String>>();
    result.map_err(|e| {
        PyException::new_err(format!(
            "failure decoding '{}' argument as comma-separated list: {}",
            key, e
        ))
    })
}

fn get_string_deserializable<'a, T>(key: &str, map: &Bound<'_, PyDict>) -> PyResult<T>
where
    T: de::DeserializeOwned,
{
    let item_opt = map
        .get_item(&key)
        .map_err(|e| PyException::new_err(format!("failure retreiving key {}: {}", key, e)))?;
    let item = match item_opt {
        None => Err(PyException::new_err(format!("key {} not present", key))),
        Some(item) => Ok(item),
    }?;
    let string: String = item.extract().map_err(|e| {
        PyException::new_err(format!("value at {} is not string. error: {}", key, e))
    })?;
    let t: T = serde_json::from_str(string.as_str()).map_err(|e| {
        PyException::new_err(format!(
            "failure decoding '{}' argument from string '{}': {}",
            key, string, e
        ))
    })?;
    Ok(t)
}
