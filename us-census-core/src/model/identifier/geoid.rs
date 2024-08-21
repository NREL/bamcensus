use serde::{Deserialize, Serialize};

use super::{fips, has_geoid_string::HasGeoidString};
use std::fmt::Display;

#[derive(Clone, Debug, Serialize, Deserialize)]
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

    pub fn to_state(&self) -> Geoid {
        match self {
            Geoid::State(_) => self.clone(),
            Geoid::County(st, _) => Geoid::State(st.clone()),
            Geoid::CountySubdivision(st, _, _) => Geoid::State(st.clone()),
            Geoid::Place(st, _) => Geoid::State(st.clone()),
            Geoid::CensusTract(st, _, _) => Geoid::State(st.clone()),
            Geoid::BlockGroup(st, _, _, _) => Geoid::State(st.clone()),
            Geoid::Block(st, _, _, _) => Geoid::State(st.clone()),
        }
    }

    pub fn to_county(&self) -> Result<Geoid, String> {
        match self {
            Geoid::State(_) => Err(String::from("state geoid does not contain a county geoid")),
            Geoid::County(st, ct) => Ok(Geoid::County(st.clone(), ct.clone())),
            Geoid::CountySubdivision(st, ct, _) => Ok(Geoid::County(st.clone(), ct.clone())),
            Geoid::Place(_, _) => Err(String::from("place geoid does not contain a county geoid")),
            Geoid::CensusTract(st, ct, _) => Ok(Geoid::County(st.clone(), ct.clone())),
            Geoid::BlockGroup(st, ct, _, _) => Ok(Geoid::County(st.clone(), ct.clone())),
            Geoid::Block(st, ct, _, _) => Ok(Geoid::County(st.clone(), ct.clone())),
        }
    }

    pub fn to_census_tract(&self) -> Result<Geoid, String> {
        match self {
            Geoid::State(_) => Err(String::from(
                "state geoid does not contain a census tract geoid",
            )),
            Geoid::County(_, _) => Err(String::from(
                "county geoid does not contain a census tract geoid",
            )),
            Geoid::CountySubdivision(_, _, _) => Err(String::from(
                "county subdivision geoid does not contain a census tract geoid",
            )),
            Geoid::Place(_, _) => Err(String::from(
                "place geoid does not contain a census tract geoid",
            )),
            Geoid::CensusTract(st, ct, tr) => {
                Ok(Geoid::CensusTract(st.clone(), ct.clone(), tr.clone()))
            }
            Geoid::BlockGroup(st, ct, tr, _) => {
                Ok(Geoid::CensusTract(st.clone(), ct.clone(), tr.clone()))
            }
            Geoid::Block(st, ct, tr, _) => {
                Ok(Geoid::CensusTract(st.clone(), ct.clone(), tr.clone()))
            }
        }
    }
}

impl PartialEq for Geoid {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::State(l0), Self::State(r0)) => l0 == r0,
            (Self::County(l0, l1), Self::County(r0, r1)) => l0 == r0 && l1 == r1,
            (Self::CountySubdivision(l0, l1, l2), Self::CountySubdivision(r0, r1, r2)) => {
                l0 == r0 && l1 == r1 && l2 == r2
            }
            (Self::Place(l0, l1), Self::Place(r0, r1)) => l0 == r0 && l1 == r1,
            (Self::CensusTract(l0, l1, l2), Self::CensusTract(r0, r1, r2)) => {
                l0 == r0 && l1 == r1 && l2 == r2
            }
            (Self::BlockGroup(l0, l1, l2, l3), Self::BlockGroup(r0, r1, r2, r3)) => {
                l0 == r0 && l1 == r1 && l2 == r2 && l3 == r3
            }
            (Self::Block(l0, l1, l2, l3), Self::Block(r0, r1, r2, r3)) => {
                l0 == r0 && l1 == r1 && l2 == r2 && l3 == r3
            }
            _ => false,
        }
    }
}

impl HasGeoidString for Geoid {
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
