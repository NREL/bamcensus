use crate::model::lodes_wac_tiger_row::LodesWacTigerRow;
use bamsoda_core::model::identifier::Geoid;
use bamsoda_core::model::identifier::GeoidType;
use bamsoda_lehd::api::lodes_api;
use bamsoda_lehd::model::lodes::{LodesDataset, WacSegment};
use bamsoda_tiger::model::TigerUriBuilder;
use bamsoda_tiger::ops::tiger_api;
use geo::Geometry;
use itertools::Itertools;
use reqwest::Client;
use serde::{Deserialize, Serialize};

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
/// use bamsoda_app::acs_tiger;
/// use bamsoda_acs::model::acs_type::AcsType;
/// use bamsoda_core::model::identifier::geoid::Geoid;
/// use bamsoda_core::model::identifier::geoid_type::GeoidType;
/// use bamsoda_core::model::identifier::fips;
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
    geoids: Vec<Geoid>,
    agg_geoid_type: &Option<GeoidType>,
    wac_segments: &[WacSegment],
    dataset: LodesDataset,
) -> Result<LodesTigerResponse, String> {
    // input: i have a set of geoids that describe a region. i want to download
    // lodes data and aggregate it to some GeoidType.
    // use the LODES dataset argument to build URIs for all LODES downloads
    // if the user did not provide geoids, use all states
    let geoids = match geoids.len() {
        0 => Geoid::all_states(),
        _ => geoids
            .into_iter()
            .map(|g| g.to_state())
            .unique()
            .collect_vec(),
    };
    let lodes_queries = geoids
        .iter()
        .map(|geoid| dataset.create_uri(geoid))
        .collect::<Result<Vec<_>, _>>()?;

    let agg_fn = bamsoda_core::ops::agg::NumericAggregation::Sum;
    let agg = agg_geoid_type.map(|g| (g, agg_fn));

    // execute LODES downloads

    let client: Client = Client::new();
    let lodes_rows = lodes_api::run_wac(&client, &lodes_queries, wac_segments, agg).await?;

    // filter result. LODES collects by State. here we only accept rows where the
    // input geoids are the (FIPS hierarchical) parent.
    let lodes_filtered = lodes_rows
        .into_iter()
        .filter(|(c, _)| geoids.iter().any(|p| p.is_parent_of(c)))
        .collect_vec();

    // execute TIGER/Lines downloads selecting a data vintage based on the LODES edition chosen
    let tiger_year = dataset.tiger_year();
    let tiger_uri_builder = TigerUriBuilder::new(tiger_year)?;
    let lodes_geoids = &lodes_filtered.iter().map(|(geoid, _)| geoid).collect_vec();
    let tiger_response = tiger_api::run(&client, &tiger_uri_builder, lodes_geoids).await?;

    type NestedResult = (Vec<Vec<(Geoid, Geometry<f64>)>>, Vec<String>);
    let (tiger_rows_nested, tiger_errors): NestedResult =
        tiger_response.into_iter().partition_result();

    let (join_dataset, join_errors) =
        crate::ops::join::dataset_with_geometries(lodes_filtered, tiger_rows_nested)?;
    let output_dataset = join_dataset
        .into_iter()
        .flat_map(|(geoid, geometry, lodes_values)| {
            lodes_values.into_iter().map(move |lodes_value| {
                LodesWacTigerRow::new(geoid.clone(), lodes_value, geometry.clone())
            })
        })
        .collect_vec();

    let result = LodesTigerResponse {
        join_dataset: output_dataset,
        tiger_errors,
        join_errors,
    };
    Ok(result)
}
