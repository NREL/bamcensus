use crate::model::{
    acs_api_query_params::AcsApiQueryParams, acs_geoid_query::DeserializeGeoidFn,
    acs_value::AcsValue,
};
use futures::future;
use itertools::Itertools;
use reqwest::{Client, StatusCode};
use us_census_core::model::identifier::geoid::Geoid;

/// sets up a run of ACS queries.
///
/// todo:
///   * this is faster than not parallel but we could probably do better if we
///     remove the awaits and let the coroutines do the work.
///   * oh, but, we actually want a blocking wait here, because we want to know
///     all Geoids before executing a single request to GET the TIGER/Lines data,
///     as we want first to know the set of unique files we actually need to download
///     to download them exactly once
///
pub async fn batch_run<'a>(
    client: &Client,
    queries: Vec<AcsApiQueryParams>,
) -> Vec<Result<(AcsApiQueryParams, Vec<(Geoid, Vec<AcsValue>)>), String>> {
    let response = queries
        .into_iter()
        .map(|params| async move { run(client, &params).await.map(|res| (params, res)) });
    future::join_all(response).await
}

/// sets up a run of an ACS query.
///
/// todo: this is faster than not parallel but we could probably do better if we
/// remove the awaits and let the coroutines do the work.
pub async fn run(
    client: &Client,
    query: &AcsApiQueryParams,
) -> Result<Vec<(Geoid, Vec<AcsValue>)>, String> {
    let url = query.build_url()?;

    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("failure calling {}: {}", url, e))?;
    if response.status().is_client_error() {
        // todo: handle bad request
    }
    if response.status().is_server_error() {
        // todo: handle service failure
    }
    if response.status() == StatusCode::NO_CONTENT {
        // todo: handle request for non-existant data
    }

    let json = response
        .json::<serde_json::Value>()
        .await
        .map_err(|e| format!("failure parsing JSON for response from {}: {}", url, e))?;

    // confirm the correct column names in the response arrays before deserializing
    validate_header(query, &json)?;

    let deserialize_fn = query.for_query.build_deserialize_geoid_fn();
    let n_for_cols = query.for_query.response_column_count();

    let result = json
        .as_array()
        .ok_or_else(|| String::from("JSON response root must be array"))?
        .iter()
        .skip(1) // skip the header!
        .map(move |row| deserialize(row, &query.get_query, n_for_cols, deserialize_fn.clone()))
        .collect::<Result<Vec<_>, String>>()?;

    Ok(result)
}

fn validate_header(query: &AcsApiQueryParams, response: &serde_json::Value) -> Result<(), String> {
    let expected = query.column_names();

    let header_json_opt = response
        .as_array()
        .and_then(|outer| outer.first())
        .and_then(|header| header.as_array());
    let header = match header_json_opt {
        None => Err(String::from("malformed ACS header")),
        Some(h) => h
            .iter()
            .map(|v| {
                v.as_str()
                    .ok_or(format!("contents of header not a string: {}", v))
            })
            .collect::<Result<Vec<_>, String>>(),
    }?;

    for (exp, found) in expected.iter().zip(&header) {
        if exp != found {
            let exp_str = expected.iter().join(",");
            let fnd_str = header.iter().join(",");
            return Err(format!(
                "expected headers did not match found\nexpected: {}\nfound: {}",
                exp_str, fnd_str
            ));
        }
    }

    Ok(())
}

/// deserializes a row of JSON values returned from an ACS response.
/// the structure of ACS responses is a nested array, where the first
/// row is a header list, and each subsequent row is a set of values which
/// appear in the header ordering.
///
/// ```json
/// [
///     ["get_column_name_1","get_column_name_n","for_column_name_1","for_column_name_n"],
///     ["get_value_1","get_value_n","for_value_1","for_value_n"],
/// ]
/// ```
///
/// for a given row, this function will
///   1. turn all 'for' columns into a single Geoid instance (via the deserialize_fn)
///   2. for each 'get' column, create an AcsValue which pairs the get_column_name with
///      the corresponding get_value.
///
/// # Examples
///
/// this test mocks the case where a wildcard "county subdivision" query was run for each
/// county subdivision in Texas. the test query includes only the first row of that query
/// for 2020.
///
/// ```rust
/// use us_census_acs::ops::acs_api::deserialize;
/// use us_census_acs::model::acs_geoid_query::AcsGeoidQuery;
/// use us_census_core::model::identifier::fips;
/// use us_census_core::model::identifier::geoid::Geoid;
///
/// let data = r#"
/// [
///     ["NAME","B01001_001E","state","county","county subdivision"],
///     ["Campbellton CCD, Atascosa County, Texas","438","48","013","90595"]
/// ]"#;
/// let v: serde_json::Value = serde_json::from_str(data).unwrap();
/// let row = v.as_array().unwrap().get(1).unwrap();
/// let query = AcsGeoidQuery::CountySubdivision(
///     fips::State(48),
///     None,
///     None,
/// );
/// let deserialize_fn = query.build_deserialize_geoid_fn();
/// let get_cols = vec![String::from("NAME"),String::from("B01001_001E")];
/// let n_for_cols: usize = query.response_column_count();
/// let (geoid, acs_values) = deserialize(&row, &get_cols, n_for_cols, deserialize_fn.clone()).unwrap();
/// assert_eq!(geoid, Geoid::CountySubdivision(fips::State(48), fips::County(13), fips::CountySubdivision(90595)))
///
/// ```
///
pub fn deserialize(
    row: &serde_json::Value,
    get_cols: &[String],
    n_for_cols: usize,
    deserialize_fn: DeserializeGeoidFn,
) -> Result<(Geoid, Vec<AcsValue>), String> {
    let n_get_cols = get_cols.len();
    let values = row
        .as_array()
        .ok_or_else(|| format!("row should be an array, found: {}", row))?;
    let expected_len = n_get_cols + n_for_cols;
    if values.len() < expected_len {
        return Err(format!(
            "row should have length {}, found {}",
            expected_len,
            values.len()
        ));
    }

    // grab geoid from row
    let geoid_values = values[n_get_cols..].to_vec();
    let geoid = deserialize_fn(geoid_values)?;

    // grab all values from row
    let mut acs_values: Vec<AcsValue> = vec![];
    for idx in 0..n_get_cols {
        let name = &get_cols[idx];
        let value = values[idx].clone();
        let row = AcsValue {
            name: String::from(name),
            value,
        };
        acs_values.push(row);
    }
    Ok((geoid, acs_values))
}
