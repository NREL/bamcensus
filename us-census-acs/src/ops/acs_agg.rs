use itertools::Itertools;
use serde_json::json;
use us_census_core::model::acs::AcsValue;
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
/// use us_census_acs::model::AcsValue;
/// use us_census_acs::ops::acs_agg;
/// use serde_json::json;
///
/// // 2020 populations by county (WAC Segment C000) for two counties in Colorado.
/// let rows = vec![
///   (
///     Geoid::County(fips::State(08), fips::County(213)),
///     vec![AcsValue::new(String::from("B01001_001E"), json![100000.0])]
///   ),
///   (
///     Geoid::County(fips::State(08), fips::County(215)),
///     vec![AcsValue::new(String::from("B01001_001E"), json![50000.0])]
///   )
/// ];
/// let target = GeoidType::State;
/// let agg = NumericAggregation::Sum;
/// let result = acs_agg::aggregate_acs(&rows, target, agg).unwrap();
/// let expected_cnt = json![150000.0];
/// let expected = vec![
///   (
///     Geoid::State(fips::State(08)),
///     vec![AcsValue::new(String::from("B01001_001E"), expected_cnt)]
///   )
/// ];
/// for ((g_a, vs_a), (g_b, vs_b)) in result.into_iter().zip(expected) {
///   assert_eq!(g_a, g_b);
///   for (v_a, v_b) in vs_a.into_iter().zip(vs_b) {
///     assert_eq!(v_a.name, v_b.name);
///     assert_eq!(v_a.value, v_b.value);
///   }
/// }
/// ```
pub fn aggregate_acs(
    rows: &[(Geoid, Vec<AcsValue>)],
    target: GeoidType,
    agg: NumericAggregation,
) -> Result<Vec<(Geoid, Vec<AcsValue>)>, String> {
    // aggregate Geoids
    let (geoid_oks, geoid_errs): (Vec<(Geoid, &Vec<AcsValue>)>, Vec<String>) = rows
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
            let xs = values.into_iter().chunk_by(|v| v.name.clone());
            let mut agg_values = vec![];
            for (name, values) in &xs {
                let values = values.map(|v| {
                  v.value.as_f64().ok_or_else(|| format!("ACS value for {} is not numeric (found {}) but user requested aggregation", name, v.value))
                })
                .collect::<Result<Vec<_>, _>>()?;
                let aggregated = agg.aggregate(&mut values.into_iter());
                agg_values.push(AcsValue::new(name, json![aggregated]));
            }
            Ok((geoid, agg_values))
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(reduced)
}
