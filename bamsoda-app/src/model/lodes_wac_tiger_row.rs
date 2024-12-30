use std::fmt::Display;

use bamsoda_core::model::identifier::{geoid::Geoid, has_geoid_string::HasGeoidString};
use bamsoda_core::model::lodes::WacValue;
use geo::Geometry;
use serde::{Deserialize, Serialize};
use wkt::ToWkt;

#[derive(Deserialize, Serialize)]
pub struct LodesWacTigerRow {
    pub geoid: Geoid,
    pub value: WacValue,
    pub geometry: Geometry,
}

impl LodesWacTigerRow {
    pub fn new(geoid: Geoid, value: WacValue, geometry: Geometry) -> LodesWacTigerRow {
        LodesWacTigerRow {
            geoid,
            value,
            geometry,
        }
    }
}

impl Display for LodesWacTigerRow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} - {} - {}",
            self.geoid.geoid_string(),
            self.value,
            self.geometry.to_wkt()
        )
    }
}
