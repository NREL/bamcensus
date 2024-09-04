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
/// use us_census_core::model::identifier::{Geoid, GeoidType, fips};
/// use us_census_core::ops::agg::NumericAggregation;
/// use us_census_lehd::model::lodes::{WacSegment, WacValue};
/// use us_census_lehd::ops::lodes_agg;
///
/// // 2020 populations by county (WAC Segment C000) for two counties in Colorado.
/// let rows = vec![
///   (
///     Geoid::County(fips::State(08), fips::County(213)),
///     vec![WacValue::new(WacSegment::C000, 106497.0)]
///   ),
///   (
///     Geoid::County(fips::State(08), fips::County(215)),
///     vec![WacValue::new(WacSegment::C000, 3858.0)]
///   )
/// ];
/// let target = GeoidType::State;
/// let agg = NumericAggregation::Sum;
/// let result = lodes_agg::aggregate_lodes_wac(&rows, target, agg).unwrap();
/// let expected_cnt = 106497.0 + 3858.0;
/// let expected = vec![
///   (
///     Geoid::State(fips::State(08)),
///     vec![WacValue::new(WacSegment::C000, expected_cnt)]
///   )
/// ];
/// for ((g_a, vs_a), (g_b, vs_b)) in result.into_iter().zip(expected) {
///   assert_eq!(g_a, g_b);
///   for (v_a, v_b) in vs_a.into_iter().zip(vs_b) {
///     assert_eq!(v_a.segment, v_b.segment);
///     assert_eq!(v_a.value, v_b.value);
///   }
/// }
/// ```
pub fn aggregate_lodes_wac(
    rows: &Vec<(Geoid, Vec<WacValue>)>,
    target: GeoidType,
    agg: NumericAggregation,
) -> Result<Vec<(Geoid, Vec<WacValue>)>, String> {
    if target == GeoidType::Block {
        // LODES data is stored at the block level, this is a no-op
        return Ok(rows.clone());
    }

    // aggregate Geoids
    let (geoid_oks, geoid_errs): (Vec<(Geoid, &Vec<WacValue>)>, Vec<String>) = rows
        .iter()
        .map(|(geoid, values)| {
            let trunc_geoid = geoid.truncate_geoid_to_type(&target)?;
            Ok((trunc_geoid, values))
        })
        .partition_result();

    if !geoid_errs.is_empty() {
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
