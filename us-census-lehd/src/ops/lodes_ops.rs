use crate::model::lodes::wac_value::WacValue;
use itertools::Itertools;
use us_census_core::{
    model::identifier::{Geoid, GeoidType},
    ops::agg::aggregation_function::NumericAggregation,
};

/// groups rows to the target Geoid hierarchy level and then
/// applies the provided aggregation function to the grouped WacValues.
///
/// # Example
///
/// ```rust
/// use us_census_lehd::model::lodes
/// ```
pub fn aggregate_lodes_wac(
    rows: &Vec<(Geoid, Vec<WacValue>)>,
    target: &GeoidType,
    agg: NumericAggregation,
) -> Result<Vec<(Geoid, Vec<WacValue>)>, String> {
    // aggregate Geoids
    let (geoid_oks, geoid_errs): (Vec<(Geoid, &Vec<WacValue>)>, Vec<String>) = rows
        .into_iter()
        .map(|(geoid, values)| {
            let trunc_geoid = geoid.truncate_geoid_to_type(&target)?;
            Ok((trunc_geoid, values))
        })
        .partition_result();

    if geoid_errs.len() > 0 {
        let msg = geoid_errs.into_iter().unique().take(5).join("\n");
        return Err(format!(
            "errors during aggregation. first 5 unique errors: \n{}",
            msg
        ));
    }

    let mut geoids_grouped = vec![];
    let grouping_iter = geoid_oks.into_iter().chunk_by(|(g, _)| g.clone());
    for (geoid, grouped) in &grouping_iter {
        let vs = grouped.into_iter().flat_map(|(_, v)| v).collect_vec();
        geoids_grouped.push((geoid, vs));
    }

    // reduce by key
    let reduced = geoids_grouped
        .into_iter()
        .map(|(geoid, values)| {
            let xs = values.into_iter().chunk_by(|v| v.segment);
            let mut agg_values = vec![];
            for (wac_segment, values) in &xs {
                let aggregated = agg.aggregate(&mut values.map(|v| v.value));
                agg_values.push(WacValue::new(wac_segment, aggregated));
            }
            (geoid, agg_values)
        })
        .collect_vec();
    Ok(reduced)
}
