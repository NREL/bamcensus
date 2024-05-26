use super::geoid_string::GeoidString;

pub struct State(pub u64);
pub struct County(pub u64);
pub struct CountySubdivision(pub u64);
pub struct Place(pub u64);
pub struct CensusTract(pub u64);
pub struct BlockGroup(pub u64);
pub struct Block(pub u64);
pub struct CongressionalDistrict(pub u64);
pub struct StateLegislativeDistrictUpperChamber(pub u64);
pub struct StateLegislativeDistrictLowerChamber(pub u64);
pub struct ZipCodeTabulationArea(pub u64);

impl GeoidString for State {
    fn geoid_string(&self) -> String {
        format!("{:02}", self.0)
    }
}

impl GeoidString for County {
    fn geoid_string(&self) -> String {
        self.0.to_string()
    }
}
impl GeoidString for CountySubdivision {
    fn geoid_string(&self) -> String {
        self.0.to_string()
    }
}
impl GeoidString for Place {
    fn geoid_string(&self) -> String {
        self.0.to_string()
    }
}
impl GeoidString for CensusTract {
    fn geoid_string(&self) -> String {
        self.0.to_string()
    }
}
impl GeoidString for BlockGroup {
    fn geoid_string(&self) -> String {
        self.0.to_string()
    }
}
impl GeoidString for Block {
    fn geoid_string(&self) -> String {
        self.0.to_string()
    }
}
impl GeoidString for CongressionalDistrict {
    fn geoid_string(&self) -> String {
        self.0.to_string()
    }
}
impl GeoidString for StateLegislativeDistrictUpperChamber {
    fn geoid_string(&self) -> String {
        self.0.to_string()
    }
}
impl GeoidString for StateLegislativeDistrictLowerChamber {
    fn geoid_string(&self) -> String {
        self.0.to_string()
    }
}
impl GeoidString for ZipCodeTabulationArea {
    fn geoid_string(&self) -> String {
        self.0.to_string()
    }
}
