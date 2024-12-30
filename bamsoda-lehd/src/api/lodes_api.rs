use crate::ops::lodes_agg;
use bamsoda_core::model::lodes::{wac_row::WacRow, wac_value::WacValue, WacSegment};
use bamsoda_core::{
    model::identifier::{Geoid, GeoidType},
    ops::agg::aggregation_function::NumericAggregation,
};
use csv::ReaderBuilder;
use flate2::read::GzDecoder;
use futures::future;
use itertools::Itertools;
use kdam::BarExt;
use reqwest::Client;
use std::sync::{Arc, Mutex};

/// runs a set of LODES queries. each required LODES file is collected in
/// memory and deserialized into rows of Geoids with WacValues for each
/// requested WacSegment. the entire dataset is aggregated to the requested
/// output GeoidType, which should be
pub async fn run_wac(
    client: &Client,
    queries: &[String],
    wac_segments: &[WacSegment],
    agg: Option<(GeoidType, NumericAggregation)>,
) -> Result<Vec<(Geoid, Vec<WacValue>)>, String> {
    let pb_builder = kdam::BarBuilder::default()
        .total(queries.len())
        .desc("LODES downloads");
    let pb = Arc::new(Mutex::new(pb_builder.build()?));

    let responses = queries.iter().map(|url| {
        let client = &client;
        let wac_segments = &wac_segments;
        let pb = pb.clone();
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
                result.push((geoid, row_result));
            }

            // update progress bar
            let mut pb_update = pb
                .lock()
                .map_err(|e| format!("failure aquiring progress bar mutex lock: {}", e))?;
            pb_update
                .update(1)
                .map_err(|e| format!("failure on pb update: {}", e))?;
            pb_update.set_description(url.split('/').last().unwrap_or_default());

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
    eprintln!(); // progress bar terminated
    let aggregated_rows = match agg {
        Some((output_geoid_type, agg)) => {
            lodes_agg::aggregate_lodes_wac(&response_rows, output_geoid_type, agg)?
        }
        None => response_rows.to_vec(),
    };
    Ok(aggregated_rows)
}
