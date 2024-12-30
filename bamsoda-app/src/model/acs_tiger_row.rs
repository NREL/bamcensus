use std::fmt::Display;

use geo::Geometry;
use serde::{Deserialize, Serialize};
use bamsoda_core::model::acs::AcsValue;
use bamsoda_core::model::identifier::{geoid::Geoid, has_geoid_string::HasGeoidString};
use wkt::ToWkt;

#[derive(Deserialize, Serialize)]
pub struct AcsTigerRow {
    pub geoid: Geoid,
    pub acs_value: AcsValue,
    pub geometry: Geometry,
}

impl AcsTigerRow {
    pub fn new(geoid: Geoid, acs_value: AcsValue, geometry: Geometry) -> AcsTigerRow {
        AcsTigerRow {
            geoid,
            acs_value,
            geometry,
        }
    }
}

impl Display for AcsTigerRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} - {} - {}",
            self.geoid.geoid_string(),
            self.acs_value,
            self.geometry.to_wkt()
        )
    }
}
