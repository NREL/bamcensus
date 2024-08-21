use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub enum GeoidType {
    State,
    County,
    CountySubdivision,
    Place,
    CensusTract,
    BlockGroup,
    Block,
}

impl ToString for GeoidType {
    fn to_string(&self) -> String {
        match self {
            GeoidType::State => String::from("state"),
            GeoidType::County => String::from("county"),
            GeoidType::CountySubdivision => String::from("county subdivision"),
            GeoidType::Place => String::from("place"),
            GeoidType::CensusTract => String::from("census tract"),
            GeoidType::BlockGroup => String::from("block group"),
            GeoidType::Block => String::from("block"),
        }
    }
}
