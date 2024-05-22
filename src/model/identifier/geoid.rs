use super::fips;

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
