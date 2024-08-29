use std::collections::HashMap;

use crate::model::acs_tiger_row::AcsTigerRow;
use geo::Geometry;
use itertools::Itertools;
use reqwest::Client;
use us_census_acs::model::acs_api_query_params::AcsApiQueryParams;
use us_census_acs::model::acs_geoid_query::AcsGeoidQuery;
use us_census_acs::model::acs_type::AcsType;
use us_census_acs::model::acs_value::AcsValue;
use us_census_acs::ops::acs_api;
use us_census_core::model::identifier::geoid::Geoid;
use us_census_core::model::identifier::geoid_type::GeoidType;
use us_census_tiger::model::tiger_uri_builder::TigerUriBuilder;
use us_census_tiger::ops::tiger_api;

pub struct AcsTigerResponse {
    pub join_dataset: Vec<AcsTigerRow>,
    pub acs_errors: Vec<String>,
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
/// ```rust
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
///         res.acs_errors.len(),
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
    acs_type: AcsType,
    acs_get_query: Vec<String>,
    geoid: Option<Geoid>,
    wildcard: Option<GeoidType>,
    acs_api_token: Option<String>,
) -> Result<AcsTigerResponse, String> {
    let client: Client = Client::new();

    // execute ACS API queries
    let query: AcsGeoidQuery = AcsGeoidQuery::new(geoid, wildcard)?;
    // let acs_year = AcsYear::try_from(year)?;
    let query_params =
        AcsApiQueryParams::new(None, year, acs_type, acs_get_query, query, acs_api_token);
    let acs_response = acs_api::batch_run(&client, vec![query_params]).await;
    let (acs_rows, acs_errors): (
        Vec<(AcsApiQueryParams, Vec<(Geoid, Vec<AcsValue>)>)>,
        Vec<String>,
    ) = acs_response.into_iter().partition_result();

    // execute TIGER/Lines downloads
    let tiger_uri_builder = TigerUriBuilder::new(year)?;
    let geoids = &acs_rows
        .iter()
        .flat_map(|(_, rows)| rows.iter().map(|(geoid, _)| geoid))
        .collect_vec();
    let tiger_response = tiger_api::run(&client, &tiger_uri_builder, &geoids).await?;
    let (tiger_rows_nested, tiger_errors): (Vec<Vec<(Geoid, Geometry<f64>)>>, Vec<String>) =
        tiger_response.into_iter().partition_result();
    let tiger_lookup = tiger_rows_nested
        .into_iter()
        .flatten()
        .collect::<HashMap<Geoid, Geometry>>();

    // join responses by GEOID
    let (acs_tiger_rows_nested, join_errors): (Vec<Vec<AcsTigerRow>>, Vec<String>) = acs_rows
        .into_iter()
        .flat_map(|(_, rows)| {
            rows.into_iter()
                .map(|(geoid, acs_values)| match tiger_lookup.get(&geoid) {
                    Some(geometry) => {
                        let acs_tiger_rows = acs_values
                            .into_iter()
                            .map(|acs_value| {
                                AcsTigerRow::new(geoid.clone(), acs_value, geometry.clone())
                            })
                            .collect_vec();
                        Ok(acs_tiger_rows)
                    }
                    None => Err(format!(
                        "geometry not found for geoid {}, has {} ACS values from API response",
                        geoid,
                        acs_values.len()
                    )),
                })
        })
        .partition_result();

    let join_dataset = acs_tiger_rows_nested.into_iter().flatten().collect_vec();
    let result = AcsTigerResponse {
        join_dataset,
        acs_errors,
        tiger_errors,
        join_errors,
    };
    Ok(result)
}
