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
