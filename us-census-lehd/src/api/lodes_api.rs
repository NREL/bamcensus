use crate::{
    model::lodes::{wac_row::WacRow, wac_value::WacValue, WacSegment},
    ops::lodes_agg,
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

/// runs a set of LODES queries. each required LODES file is collected in
/// memory and deserialized into rows of Geoids with WacValues for each
/// requested WacSegment. the entire dataset is aggregated to the requested
/// output GeoidType, which should be
pub async fn run(
    client: &Client,
    queries: &[String],
    wac_segments: &[WacSegment],
    output_geoid_type: GeoidType,
    agg: NumericAggregation,
) -> Result<Vec<(Geoid, Vec<WacValue>)>, String> {
    let responses = queries.iter().map(|url| {
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
                    row_result.push(WacValue::new(*segment, row.get(segment)));
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
    let aggregated_rows = lodes_agg::aggregate_lodes_wac(&response_rows, output_geoid_type, agg)?;
    Ok(aggregated_rows)
}
