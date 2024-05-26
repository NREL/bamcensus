use super::{fips, geoid_string::GeoidString};
use std::fmt::Display;

pub enum Geoid {
    State(fips::State),
    County(fips::State, fips::County),
    CountySubdivision(fips::State, fips::County, fips::CountySubdivision),
    Place(fips::State, fips::Place),
    CensusTract(fips::State, fips::County, fips::CensusTract),
    BlockGroup(
        fips::State,
        fips::County,
        fips::CensusTract,
        fips::BlockGroup,
    ),
    Block(fips::State, fips::County, fips::CensusTract, fips::Block),
}

// todo:
// - Geoid methods to unpack/pack between types (Geoid::County.to_state())

impl Geoid {
    pub fn variant_name(&self) -> String {
        match self {
            Geoid::State(_) => String::from("State"),
            Geoid::County(_, _) => String::from("County"),
            Geoid::CountySubdivision(_, _, _) => String::from("CountySubdivision"),
            Geoid::Place(_, _) => String::from("Place"),
            Geoid::CensusTract(_, _, _) => String::from("CensusTract"),
            Geoid::BlockGroup(_, _, _, _) => String::from("BlockGroup"),
            Geoid::Block(_, _, _, _) => String::from("Block"),
        }
    }
}

impl GeoidString for Geoid {
    fn geoid_string(&self) -> String {
        match self {
            Geoid::State(st) => st.geoid_string(),
            Geoid::County(st, ct) => format!("{}{}", st.geoid_string(), ct.geoid_string()),
            Geoid::CountySubdivision(st, ct, cs) => format!(
                "{}{}{}",
                st.geoid_string(),
                ct.geoid_string(),
                cs.geoid_string()
            ),
            Geoid::Place(st, pl) => format!("{}{}", st.geoid_string(), pl.geoid_string()),
            Geoid::CensusTract(st, ct, tr) => format!(
                "{}{}{}",
                st.geoid_string(),
                ct.geoid_string(),
                tr.geoid_string()
            ),
            Geoid::BlockGroup(st, ct, tr, bg) => format!(
                "{}{}{}{}",
                st.geoid_string(),
                ct.geoid_string(),
                tr.geoid_string(),
                bg.geoid_string()
            ),
            Geoid::Block(st, ct, tr, bl) => format!(
                "{}{}{}{}",
                st.geoid_string(),
                ct.geoid_string(),
                tr.geoid_string(),
                bl.geoid_string()
            ),
        }
    }
}

impl Display for Geoid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}={}", self.variant_name(), self.geoid_string())
    }
}
