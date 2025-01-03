use bamsoda_core::model::identifier::{Geoid, GeoidType};
use serde::{Deserialize, Serialize};

use super::WacSegment;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[allow(non_snake_case)]
pub struct WacRow {
    pub w_geocode: String,
    pub C000: f64,
    pub CA01: f64,
    pub CA02: f64,
    pub CA03: f64,
    pub CE01: f64,
    pub CE02: f64,
    pub CE03: f64,
    pub CNS01: f64,
    pub CNS02: f64,
    pub CNS03: f64,
    pub CNS04: f64,
    pub CNS05: f64,
    pub CNS06: f64,
    pub CNS07: f64,
    pub CNS08: f64,
    pub CNS09: f64,
    pub CNS10: f64,
    pub CNS11: f64,
    pub CNS12: f64,
    pub CNS13: f64,
    pub CNS14: f64,
    pub CNS15: f64,
    pub CNS16: f64,
    pub CNS17: f64,
    pub CNS18: f64,
    pub CNS19: f64,
    pub CNS20: f64,
    pub CR01: f64,
    pub CR02: f64,
    pub CR03: f64,
    pub CR04: f64,
    pub CR05: f64,
    pub CR07: f64,
    pub CT01: f64,
    pub CT02: f64,
    pub CD01: f64,
    pub CD02: f64,
    pub CD03: f64,
    pub CD04: f64,
    pub CS01: f64,
    pub CS02: f64,
    pub createdate: String,
}

impl WacRow {
    pub fn get(&self, segment: &WacSegment) -> f64 {
        match segment {
            WacSegment::C000 => self.C000,
            WacSegment::CA01 => self.CA01,
            WacSegment::CA02 => self.CA02,
            WacSegment::CA03 => self.CA03,
            WacSegment::CE01 => self.CE01,
            WacSegment::CE02 => self.CE02,
            WacSegment::CE03 => self.CE03,
            WacSegment::CNS01 => self.CNS01,
            WacSegment::CNS02 => self.CNS02,
            WacSegment::CNS03 => self.CNS03,
            WacSegment::CNS04 => self.CNS04,
            WacSegment::CNS05 => self.CNS05,
            WacSegment::CNS06 => self.CNS06,
            WacSegment::CNS07 => self.CNS07,
            WacSegment::CNS08 => self.CNS08,
            WacSegment::CNS09 => self.CNS09,
            WacSegment::CNS10 => self.CNS10,
            WacSegment::CNS11 => self.CNS11,
            WacSegment::CNS12 => self.CNS12,
            WacSegment::CNS13 => self.CNS13,
            WacSegment::CNS14 => self.CNS14,
            WacSegment::CNS15 => self.CNS15,
            WacSegment::CNS16 => self.CNS16,
            WacSegment::CNS17 => self.CNS17,
            WacSegment::CNS18 => self.CNS18,
            WacSegment::CNS19 => self.CNS19,
            WacSegment::CNS20 => self.CNS20,
            WacSegment::CR01 => self.CR01,
            WacSegment::CR02 => self.CR02,
            WacSegment::CR03 => self.CR03,
            WacSegment::CR04 => self.CR04,
            WacSegment::CR05 => self.CR05,
            WacSegment::CR07 => self.CR07,
            WacSegment::CT01 => self.CT01,
            WacSegment::CT02 => self.CT02,
            WacSegment::CD01 => self.CD01,
            WacSegment::CD02 => self.CD02,
            WacSegment::CD03 => self.CD03,
            WacSegment::CD04 => self.CD04,
            WacSegment::CS01 => self.CS01,
            WacSegment::CS02 => self.CS02,
        }
    }
}

impl WacRow {
    pub fn geoid(&self) -> Result<Geoid, String> {
        GeoidType::Block.geoid_from_str(&self.w_geocode)
    }
}
