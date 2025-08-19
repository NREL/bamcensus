use bamcensus::app::acs_tiger;
use bamcensus_acs::model::AcsApiQueryParams;
use bamcensus_acs::model::AcsGeoidQuery;
use bamcensus_acs::model::AcsType;
use bamcensus_core::model::identifier::Geoid;
use itertools::Itertools;
use pyo3::types::IntoPyDict;
use pyo3::types::PyDict;
use pyo3::types::PyNone;
use pyo3::{exceptions::PyException, prelude::*};
use serde::de;
use wkt::ToWkt;

#[pyfunction]
#[pyo3(signature = (year, **kwds))]
pub fn run_acs_tiger_python<'a>(
    year: u64,
    kwds: Option<&Bound<'a, PyDict>>,
    py: Python<'a>,
) -> PyResult<pyo3::Bound<'a, PyDict>> {
    let acs_type = kwds.map_or(Ok(AcsType::FiveYear), |m| {
        if m.contains("acs_type")? {
            get_string_deserializable("acs_type", m)
        } else {
            Ok(AcsType::FiveYear)
        }
    })?;
    // default: populations total
    let acs_get_query = kwds.map_or(Ok(vec![String::from("B01001_001E")]), |m| {
        if m.contains("wac_segments")? {
            get_comma_separated("wac_segments", m)
        } else {
            Ok(vec![String::from("B01001_001E")])
        }
    })?;

    let geoids_string: String = kwds.map_or(Ok(String::from("")), |m| get_string("geoids", m))?;
    let geoids = geoids_string
        .split(',')
        .map(Geoid::try_from)
        .collect::<Result<Vec<_>, String>>()
        .map_err(|e| PyException::new_err(format!("failure decoding geoids argument: {e}")))?;

    let wildcard = kwds.map_or(Ok(None), |m| {
        if m.contains("wildcard")? {
            get_string_deserializable("wildcard", m).map(Some)
        } else {
            Ok(None)
        }
    })?;
    let acs_api_token = kwds.map_or(Ok(None), |m| {
        if m.contains("acs_api_token")? {
            get_string_deserializable("acs_api_token", m)
        } else {
            Ok(None)
        }
    })?;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| {
            PyException::new_err(format!("failure creating async rust tokio runtime: {e}"))
        })?;

    // if no geoids are supplied we can run a query across the entire ACS dataset
    let queries = if geoids.is_empty() {
        vec![AcsGeoidQuery::new(None, wildcard).unwrap()]
    } else {
        geoids
            .into_iter()
            .map(|g| AcsGeoidQuery::new(Some(g), wildcard).unwrap())
            .collect_vec()
    };

    // run ACS queries and collect ACS/TIGER joined Rows
    let results = queries
        .into_iter()
        .map(|q| {
            let query_params = AcsApiQueryParams::new(
                None,
                year,
                acs_type,
                acs_get_query.clone(),
                q,
                acs_api_token.clone(),
            );
            let future = acs_tiger::run(&query_params);
            let result = runtime.block_on(future).map_err(|e| {
                PyException::new_err(format!("failure running LODES WAC + TIGER workflow: {e}"))
            })?;
            if !result.tiger_errors.is_empty() {
                let msg = result.tiger_errors.iter().join(",");
                return Err(PyException::new_err(format!("tiger errors: {msg}")));
            }
            if !result.join_errors.is_empty() {
                let msg = result.join_errors.iter().join(",");
                return Err(PyException::new_err(format!("join errors: {msg}")));
            }

            Ok(result.join_dataset)
        })
        .collect::<Result<Vec<_>, _>>()?;

    //
    let vals = results
        .into_iter()
        .flatten()
        .map(|row| {
            let dict: Bound<'_, PyDict> = PyDict::new_bound(py);
            let value_json = row.acs_value.value;

            dict.set_item("geoid", row.geoid.to_string())?;
            dict.set_item("name", row.acs_value.name)?;
            // dict.set_item("value", value_json.to_object(py)) <-- doesn't work, hence
            // we unpack each JSON value and serialize via ToPyObject on it's underlying
            // standard rust data structure implementation
            match value_json {
                serde_json::Value::Null => dict.set_item("value", PyNone::get_bound(py)),
                serde_json::Value::Bool(b) => dict.set_item("value", b.to_object(py)),
                serde_json::Value::Number(n) => match (n.as_f64(), n.as_i64()) {
                    (None, Some(i)) => dict.set_item("value", i.to_object(py)),
                    (Some(f), None) => dict.set_item("value", f.to_object(py)),
                    _ => Err(PyException::new_err(format!(
                        "cannot convert JSON Number to Python representation: {n}"
                    ))),
                },
                serde_json::Value::String(s) => dict.set_item("value", s.to_object(py)),
                _ => {
                    // naive implementation - string serialize the value
                    dict.set_item("value", value_json.to_string())
                }
            }?;
            dict.set_item("geometry", row.geometry.to_wkt().to_string())?;
            Ok((row.geoid.to_string(), dict.to_object(py)))
        })
        .collect::<PyResult<Vec<_>>>()?;
    let out_dict = vals.into_py_dict_bound(py);
    Ok(out_dict)
}

fn get_comma_separated<T>(key: &str, map: &Bound<'_, PyDict>) -> PyResult<Vec<T>>
where
    T: de::DeserializeOwned,
{
    let ss: String = get_string(key, map)?;
    let result = ss
        .split(',')
        .map(|s| {
            serde_json::from_str(s)
                .map_err(|e| format!("failure decoding comma-separated arguments in '{key}': {e}"))
        })
        .collect::<Result<Vec<T>, String>>();
    result.map_err(|e| {
        PyException::new_err(format!(
            "failure decoding '{key}' argument as comma-separated list: {e}"
        ))
    })
}

fn get_string(key: &str, map: &Bound<'_, PyDict>) -> PyResult<String> {
    let item_opt = map
        .get_item(key)
        .map_err(|e| PyException::new_err(format!("failure retreiving key {key}: {e}")))?;
    let item = match item_opt {
        None => Err(PyException::new_err(format!("key {key} not present"))),
        Some(item) => Ok(item),
    }?;
    let string: String = item
        .extract()
        .map_err(|e| PyException::new_err(format!("value at {key} is not string. error: {e}")))?;
    Ok(string)
}

fn get_string_deserializable<T>(key: &str, map: &Bound<'_, PyDict>) -> PyResult<T>
where
    T: de::DeserializeOwned,
{
    let item_opt = map
        .get_item(key)
        .map_err(|e| PyException::new_err(format!("failure retreiving key {key}: {e}")))?;
    let item = match item_opt {
        None => Err(PyException::new_err(format!("key {key} not present"))),
        Some(item) => Ok(item),
    }?;
    let string: String = item
        .extract()
        .map_err(|e| PyException::new_err(format!("value at {key} is not string. error: {e}")))?;
    let t: T = serde_json::from_str(string.as_str()).map_err(|e| {
        PyException::new_err(format!(
            "failure decoding '{key}' argument from string '{string}': {e}"
        ))
    })?;
    Ok(t)
}
