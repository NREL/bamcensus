use super::{tiger_file_type::TigerFileType, tiger_year::TigerYear};
use crate::model::identifier::{geoid::Geoid, geoid_string::GeoidString};

pub enum TigerFilenameFormat {
    Tiger2020Format,
}

impl TigerFilenameFormat {
    pub fn create_filename(
        &self,
        year: &TigerYear,
        file_type: &TigerFileType,
        geoid: &Geoid,
    ) -> Result<String, String> {
        match self {
            TigerFilenameFormat::Tiger2020Format => {
                validate_tiger_2020(year, file_type, geoid)?;
                let year_int = year.to_year_int();
                let name = file_type.name_lower(year)?;
                let id = geoid.geoid_string();
                let filename = create_tiger_2020(&year_int, &name, &id);
                Ok(filename)
            }
        }
    }
}

fn validate_tiger_2020(
    tiger_year: &TigerYear,
    file_type: &TigerFileType,
    geoid: &Geoid,
) -> Result<(), String> {
    let year_int = tiger_year.to_year_int();
    if year_int < 2020 {
        return Err(format!(
            "tiger year {} not compatible with 2020 tiger filename format",
            year_int
        ));
    }
    match (file_type, geoid) {
        (TigerFileType::Block, Geoid::State(_)) => Ok(()),
        (TigerFileType::BlockGroup, Geoid::State(_)) => Ok(()),
        (TigerFileType::Block, _) => Err(format!(
            "creating tigris block filename requires a State Geoid, found {}",
            geoid
        )),
        (TigerFileType::BlockGroup, _) => Err(format!(
            "creating tigris blockgroup filename requires a State Geoid, found {}",
            geoid
        )),
    }
}

/// # Examples
///
/// example: tl_2023_01_tabblock20.zip, where
/// tl: tiger lines
/// 2023: year
/// 01: geoid (statecode in this case)
/// tabblock20: file type name (in this case, depends on year)
fn create_tiger_2020(year: &u64, name: &String, id: &String) -> String {
    format!("tl_{}_{}_{}.zip", year, id, name)
}
