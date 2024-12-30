use super::acs_tiger_row::AcsTigerRow;
use serde::{Deserialize, Serialize};
use bamsoda_core::model::identifier::has_geoid_string::HasGeoidString;
use wkt::ToWkt;

#[derive(Serialize, Deserialize)]
pub struct AcsTigerOutputRow {
    geoid: String,
    acs_field: String,
    acs_value: serde_json::Value,
    geometry: String,
}

impl From<AcsTigerRow> for AcsTigerOutputRow {
    fn from(row: AcsTigerRow) -> Self {
        let geoid = row.geoid.geoid_string();
        let acs_field = row.acs_value.name.clone();
        let acs_value = row.acs_value.value.clone();
        let geometry = row.geometry.to_wkt().to_string();
        Self {
            geoid,
            acs_field,
            acs_value,
            geometry,
        }
    }
}
