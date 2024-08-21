use super::tiger_year::TigerYear;
use us_census_core::model::identifier::{
    geoid::Geoid, geoid_hierarchy_type::GeoidHierarchyType, has_geoid_string::HasGeoidString,
};

pub enum TigerUrlBuilder {
    /// https://www2.census.gov/geo/tiger/TIGER2002/01_al/tgr01001.zip
    Tiger2002,
    /// https://www2.census.gov/geo/tiger/TIGER2003/01_AL/tgr01001.zip
    Tiger2003,
    /// https://www2.census.gov/geo/tiger/TIGER2008/01_ALABAMA/01001_Autauga/fe_2007_01001_tabblock00.zip
    Tiger2008,
    /// https://www2.census.gov/geo/tiger/TIGER2010/TABBLOCK/2010/tl_2010_01001_tabblock10.zip
    Tiger2010 { year: TigerYear },
    /// https://www2.census.gov/geo/tiger/TIGER2020/TABBLOCK20/tl_2020_01_tabblock20.zip
    Tiger2020 { year: TigerYear },
}

impl TigerUrlBuilder {
    const TIGER_BASE_URL: &'static str = "https://www2.census.gov/geo/tiger";

    pub fn new(&self, year: &TigerYear) -> TigerUrlBuilder {
        match year {
            TigerYear::Tiger2022 => TigerUrlBuilder::Tiger2020 { year: year.clone() },
        }
    }

    /// creates a URL to a TIGER file location.
    pub fn create_url(&self, file_type: &GeoidHierarchyType) -> Result<String, String> {
        match self {
            TigerUrlBuilder::Tiger2002 => todo!(),
            TigerUrlBuilder::Tiger2003 => todo!(),
            TigerUrlBuilder::Tiger2008 => todo!(),
            TigerUrlBuilder::Tiger2010 { year } => url_2010(year, file_type),
            TigerUrlBuilder::Tiger2020 { year } => url_2010(year, file_type),
        }
    }

    /// creates a filename for a TIGER file.
    pub fn create_filename(
        &self,
        geoid_hierarchy_type: &GeoidHierarchyType,
        geoid: &Geoid,
    ) -> Result<String, String> {
        match self {
            TigerUrlBuilder::Tiger2002 => todo!(),
            TigerUrlBuilder::Tiger2003 => todo!(),
            TigerUrlBuilder::Tiger2008 => todo!(),
            TigerUrlBuilder::Tiger2010 { year: _ } => todo!(),
            TigerUrlBuilder::Tiger2020 { year } => {
                validate_filename_identifiers(year, geoid_hierarchy_type, geoid)?;
                let year_int = year.to_int();
                let name = geoid_hierarchy_type.name_lower(year_int)?;
                let id = geoid.geoid_string();
                let filename = format!("tl_{}_{}_{}.zip", year_int, id, name);
                Ok(filename)
            }
        }
    }
}

fn validate_filename_identifiers(
    tiger_year: &TigerYear,
    geoid_hierarchy_type: &GeoidHierarchyType,
    geoid: &Geoid,
) -> Result<(), String> {
    // todo: validation is year-specific since sometimes files are at county, state,
    // or other levels

    match (tiger_year, geoid_hierarchy_type, geoid) {
        (TigerYear::Tiger2022, GeoidHierarchyType::BlockGroup, Geoid::State(_)) => Ok(()),
        (TigerYear::Tiger2022, GeoidHierarchyType::BlockGroup, _) => {
            Err(String::from("2022 TIGER block groups use state geoids"))
        }
        (TigerYear::Tiger2022, GeoidHierarchyType::Block, Geoid::State(_)) => Ok(()),
        (TigerYear::Tiger2022, GeoidHierarchyType::Block, _) => {
            Err(String::from("2022 TIGER blocks use state geoids"))
        }
    }

    // match (geoid_hierarchy_type, geoid) {
    //     (GeoidHierarchyType::Block, Geoid::State(_)) => Ok(()),
    //     (GeoidHierarchyType::BlockGroup, Geoid::State(_)) => Ok(()),
    //     (GeoidHierarchyType::Block, _) => Err(format!(
    //         "creating tigris block filename requires a State Geoid, found {}",
    //         geoid
    //     )),
    //     (GeoidHierarchyType::BlockGroup, _) => Err(format!(
    //         "creating tigris blockgroup filename requires a State Geoid, found {}",
    //         geoid
    //     )),
    // }
}

// /// # Examples
// ///
// /// example: tl_2023_01_tabblock20.zip, where
// /// tl: tiger lines
// /// 2023: year
// /// 01: geoid (statecode in this case)
// /// tabblock20: file type name (in this case, depends on year)

/// builds a URL for tiger files in the format standardized in 2010.
fn url_2010(year: &TigerYear, file_type: &GeoidHierarchyType) -> Result<String, String> {
    let hierarchy_dir = file_type.name_upper(year.to_int())?;
    let url = format!(
        "{}/{}/{}",
        TigerUrlBuilder::TIGER_BASE_URL,
        year.to_directory_name(),
        hierarchy_dir
    );
    Ok(url)
}
