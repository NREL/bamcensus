use crate::model::lodes_wac_tiger_row::LodesWacTigerRow;
use geo::Geometry;
use itertools::Itertools;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use us_census_core::model::fips::state_code::StateCode;
use us_census_core::model::identifier::geoid::Geoid;
use us_census_core::model::identifier::geoid_type::GeoidType;
use us_census_core::model::lodes::{LodesDataset, WacSegment, ALL_STATES};
use us_census_lehd::api::lodes_api;
use us_census_tiger::model::tiger_uri_builder::TigerUriBuilder;
use us_census_tiger::ops::tiger_api;

#[derive(Serialize, Deserialize)]
pub struct LodesTigerResponse {
    pub join_dataset: Vec<LodesWacTigerRow>,
    pub tiger_errors: Vec<String>,
    pub join_errors: Vec<String>,
}

/// runs a query to ACS. the result will include a list of GEOIDs alongside
/// ACS data. all GEOIDs are used to run a set of downloads from the TIGER/Lines
/// datasets. the geometries from TIGER are combined with the ACS data producing
/// AcsTigerRows.
///
/// # Example
///
/// ```ignore
/// use us_census_app::acs_tiger;
/// use us_census_acs::model::acs_type::AcsType;
/// use us_census_core::model::identifier::geoid::Geoid;
/// use us_census_core::model::identifier::geoid_type::GeoidType;
/// use us_census_core::model::identifier::fips;
///
/// let year = 2020;
/// let acs_type = AcsType::FiveYear;
/// let acs_get_query = vec![String::from("NAME"), String::from("B01001_001E")];
/// let geoid = Geoid::State(fips::State(08));
/// let wildcard = GeoidType::County;
///
/// # tokio_test::block_on(async {
///     let res = acs_tiger::run(year, acs_type, acs_get_query, Some(geoid), Some(wildcard), None).await.unwrap();
///     println!(
///         "found {} responses, {}/{}/{} errors",
///         res.join_dataset.len(),
///         res.tiger_errors.len(),
///         res.join_errors.len(),
///     );
///     for row in res.join_dataset.into_iter() {
///         println!("{}", row)
///     }
/// # })
///
/// ```
pub async fn run(
    year: u64,
    geoids: Vec<Geoid>,
    wildcard: &Option<GeoidType>,
    wac_segments: &[WacSegment],
    dataset: LodesDataset,
) -> Result<LodesTigerResponse, String> {
    let state_codes = match geoids.len() {
        0 => Ok(ALL_STATES.map(String::from).to_vec()),
        _ => {
            let states_result: Result<Vec<_>, String> = geoids
                .into_iter()
                .map(|geoid| {
                    let state_fips = match geoid.to_state() {
                        Geoid::State(s) => Ok(s),
                        _ => Err(String::from("internal error")),
                    }?;
                    let state_code = StateCode::try_from(state_fips)?;
                    let state_str = state_code.to_state_abbreviation();
                    Ok(state_str)
                })
                .collect::<Result<Vec<_>, _>>();
            states_result
        }
    }?;

    let queries = state_codes
        .into_iter()
        .map(|s| dataset.create_uri(&s))
        .collect_vec();
    let client: Client = Client::new();

    // execute LODES downloads
    let output_geoid_type = wildcard.as_ref().unwrap_or(&GeoidType::Block);
    let agg = us_census_core::ops::agg::NumericAggregation::Sum;
    let lodes_rows =
        lodes_api::run(&client, &queries, wac_segments, *output_geoid_type, agg).await?;

    // execute TIGER/Lines downloads
    let tiger_uri_builder = TigerUriBuilder::new(year)?;
    let geoids = &lodes_rows.iter().map(|(geoid, _)| geoid).collect_vec();
    let tiger_response = tiger_api::run(&client, &tiger_uri_builder, geoids).await?;

    type NestedResult = (Vec<Vec<(Geoid, Geometry<f64>)>>, Vec<String>);
    let (tiger_rows_nested, tiger_errors): NestedResult =
        tiger_response.into_iter().partition_result();
    let tiger_lookup = tiger_rows_nested
        .into_iter()
        .flatten()
        .collect::<HashMap<Geoid, Geometry>>();

    // join responses by GEOID
    let (rows_nested, join_errors): (Vec<Vec<LodesWacTigerRow>>, Vec<String>) = lodes_rows
        .into_iter()
        .map(|(geoid, lodes_values)| match tiger_lookup.get(&geoid) {
            Some(geometry) => {
                let lodes_tiger_rows = lodes_values
                    .into_iter()
                    .map(|acs_value| {
                        LodesWacTigerRow::new(geoid.clone(), acs_value, geometry.clone())
                    })
                    .collect_vec();
                Ok(lodes_tiger_rows)
            }
            None => Err(format!(
                "geometry not found for geoid {}, has {} LODES values from API response",
                geoid,
                lodes_values.len()
            )),
        })
        .partition_result();

    let join_dataset = rows_nested.into_iter().flatten().collect_vec();
    let result = LodesTigerResponse {
        join_dataset,
        tiger_errors,
        join_errors,
    };
    Ok(result)
}
