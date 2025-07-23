use super::lodes_wac_tiger_row::LodesWacTigerRow;
use bamsoda_core::model::identifier::HasGeoidString;
use serde::{Deserialize, Serialize};
use wkt::ToWkt;

#[derive(Serialize, Deserialize)]
pub struct LodesTigerOutputRow {
    geoid: String,
    lodes_field: String,
    lodes_value: serde_json::Value,
    geometry: String,
}

impl From<LodesWacTigerRow> for LodesTigerOutputRow {
    fn from(row: LodesWacTigerRow) -> Self {
        let geoid = row.geoid.geoid_string();
        let lodes_field = row.value.segment.to_string();
        let lodes_value = serde_json::json![row.value.value];
        let geometry = row.geometry.to_wkt().to_string();
        Self {
            geoid,
            lodes_field,
            lodes_value,
            geometry,
        }
    }
}
