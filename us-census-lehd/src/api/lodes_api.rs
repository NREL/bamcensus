use crate::{
    api::lodes_api,
    model::lodes::{
        wac_row::WacRow, wac_value::WacValue, LodesDataset, LodesEdition, LodesJobType, OdPart,
        WacSegment, WorkplaceSegment,
    },
    ops::lodes_ops,
};
use csv::ReaderBuilder;
use flate2::read::GzDecoder;
use futures::future;
use itertools::Itertools;
use reqwest::Client;
use us_census_core::{
    model::identifier::{Geoid, GeoidType},
    ops::agg::aggregation_function::NumericAggregation,
};

pub fn create_queries(
    lodes_edition: &LodesEdition,
    lodes_dataset: &LodesDataset,
    state_codes: &[String],
    segment: &WorkplaceSegment,
    job_type: &LodesJobType,
    year: u64,
) -> Vec<String> {
    let lodes_queries = state_codes
        .iter()
        .map(|sc| {
            let filename = match lodes_dataset {
                LodesDataset::OD => {
                    todo!("not yet implemented: `create filename` fn for OD dataset")
                }
                LodesDataset::RAC => {
                    todo!("not yet implemented: `create filename` fn for RAC dataset")
                }
                LodesDataset::WAC => lodes_api::create_wac_filename(sc, &segment, &job_type, year),
            };
            let url = lodes_edition.create_url(sc, &lodes_dataset, &filename);
            url
        })
        .collect_vec();
    lodes_queries
}

/// runs a set of LODES queries. each required LODES file is collected in
/// memory and deserialized into rows of Geoids with WacValues for each
/// requested WacSegment. the entire dataset is aggregated to the requested
/// output GeoidType, which should be
pub async fn run(
    client: &Client,
    queries: &[String],
    wac_segments: &[WacSegment],
    output_geoid_type: &GeoidType,
    agg: NumericAggregation,
) -> Result<Vec<(Geoid, Vec<WacValue>)>, String> {
    let responses = queries.into_iter().map(|url| {
        let client = &client;
        let wac_segments = &wac_segments;
        async move {
            let res = client
                .get(url)
                .send()
                .await
                .map_err(|e| format!("failure sending LODES HTTP request: {}", e))?;
            let gzip_bytes = res
                .bytes()
                .await
                .map_err(|e| format!("failure reading response body: {}", e))?;
            let mut reader = ReaderBuilder::new().from_reader(GzDecoder::new(&gzip_bytes[..]));
            let mut result = vec![];
            for r in reader.deserialize() {
                let row: WacRow =
                    r.map_err(|e| format!("failure reading LODES response row: {}", e))?;
                let geoid = row.geoid()?;
                let mut row_result = vec![];
                for segment in wac_segments.iter() {
                    row_result.push(WacValue::new(segment.clone(), row.get(segment)));
                }
                result.push((geoid, row_result))
            }
            Ok(result)
        }
    });
    let response_rows = future::join_all(responses)
        .await
        .into_iter()
        .collect::<Result<Vec<_>, String>>()?
        .into_iter()
        .flatten()
        .collect_vec();
    let aggregated_rows = lodes_ops::aggregate_lodes_wac(&response_rows, output_geoid_type, agg)?;
    Ok(aggregated_rows)
}

/// from https://lehd.ces.census.gov/data/lodes/LODES8/LODESTechDoc8.1.pdf:
/// [ST]_od_[PART]_[TYPE]_[YEAR].csv.gz where
///   [ST] = lowercase, 2-letter postal code for a chosen state
///   [PART] = Part of the state file, can have a value of either “main” or “aux”. Complimentary parts of
///     the state file, the main part includes jobs with both workplace and residence in the state
///     and the aux part includes jobs with the workplace in the state and the residence outside
///     of the state.
///   [TYPE] = Job Type, can have a value of “JT00” for All Jobs, “JT01” for Primary Jobs, “JT02” for All
///     Private Jobs, “JT03” for Private Primary Jobs, “JT04” for All Federal Jobs, or “JT05” for
///     Federal Primary Jobs.
///   [YEAR] = Year of job data. Can have the value of 2002-2021 for most states.
pub fn create_od_filename(
    state_code: &String,
    od_part: &OdPart,
    job_type: &LodesJobType,
    year: i64,
) -> String {
    format!("{}_od_{}_{}_{}.csv.gz", state_code, od_part, job_type, year)
}

/// from https://lehd.ces.census.gov/data/lodes/LODES8/LODESTechDoc8.1.pdf:
/// [ST]_wac_[SEG]_[TYPE]_[YEAR].csv.gz where
///   [ST] = lowercase, 2-letter postal code for a chosen state
///   [SEG] = Segment of the workforce, can have the values of “S000”, “SA01”, “SA02”, “SA03”, “SE01”,
///     “SE02”, “SE03”, “SI01”, “SI02”, or “SI03”. These correspond to the same segments of the
///     workforce as are listed in the OD file structure above.
///   [TYPE] = Job Type, can have a value of “JT00” for All Jobs, “JT01” for Primary Jobs, “JT02” for All
///     Private Jobs, “JT03” for Private Primary Jobs, “JT04” for All Federal Jobs, or “JT05” for
///     Federal Primary Jobs.
///   [YEAR] = Year of job data. Can have the value of 2002-2021 for most states.
pub fn create_wac_filename(
    state_code: &str,
    segment: &WorkplaceSegment,
    job_type: &LodesJobType,
    year: u64,
) -> String {
    format!(
        "{}_wac_{}_{}_{}.csv.gz",
        state_code, segment, job_type, year
    )
}
