use bamcensus_core::model::identifier::{Geoid, HasGeoidString};
use bamcensus_lehd::model::WacValue;
use geo::Geometry;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
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
