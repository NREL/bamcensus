use serde::{Deserialize, Serialize};
use us_census_core::model::identifier::geoid_type::GeoidType;

#[derive(Debug, Clone, Serialize, Deserialize, Eq, Hash)]
pub struct TigerResource {
    /// complete URI to a file location in the TIGER/LINES HTTP website
    pub uri: String,
    /// the geoid type of the file contents. each row should have a column
    /// denoting the Geoid value as a string that can be decoded by this application
    /// using the GeoidType::geoid_from_string method.
    pub geoid_type: GeoidType,
    /// the file will contain a geographical data collection. the scope of
    /// that file depends on the TIGRIS year and target geoid hierarchy.
    /// for example, in 2010, county subdivisions are stored in files organized
    /// by state/state code, so their file scope would be State and the file itself
    /// would just be tagged by the state code.
    /// if file_scope is None, then the scope is "national", as in, there is one
    /// file for all values for this year.
    pub file_scope: Option<GeoidType>,
    /// the name of GEOID columns may vary based on the TIGER year.
    pub geoid_column_name: String,
}

impl TigerResource {
    pub fn new(
        uri: String,
        geoid_type: GeoidType,
        file_scope: Option<GeoidType>,
        geoid_column_name: String,
    ) -> TigerResource {
        TigerResource {
            uri,
            geoid_type,
            file_scope,
            geoid_column_name,
        }
    }
}

impl PartialEq for TigerResource {
    fn eq(&self, other: &Self) -> bool {
        self.uri == other.uri
            && self.geoid_type == other.geoid_type
            && self.file_scope == other.file_scope
            && self.geoid_column_name == other.geoid_column_name
    }
}
