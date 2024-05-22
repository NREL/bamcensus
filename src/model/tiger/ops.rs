use crate::model::identifier::geoid::Geoid;

use super::tiger_file_type::TigerFileType;
use super::tiger_year::TigerYear;

pub fn filename(
    year: &TigerYear,
    file_type: &TigerFileType,
    geoid: &Geoid,
) -> Result<String, String> {
    // example: tl_2023_01_tabblock20.zip
    // tl: tiger lines
    // 2023: year
    // 01: geoid (statecode in this case)
    // tabblock20: file type name (in this case, depends on year)

    let year_int = year.to_year_int();
    let file_type_name = file_type.name_lower(year)?;
    // let id = match (file_type, geoid) {
    //     (TigerFileType::BlockGroup, Geoid::State(_)) => todo!(),
    //     (TigerFileType::BlockGroup, Geoid::County(_, _)) => todo!(),
    //     (TigerFileType::BlockGroup, Geoid::CountySubdivision(_, _, _)) => todo!(),
    //     (TigerFileType::BlockGroup, Geoid::Place(_, _)) => todo!(),
    //     (TigerFileType::BlockGroup, Geoid::CensusTract(_, _, _)) => todo!(),
    //     (TigerFileType::BlockGroup, Geoid::BlockGroup(_, _, _, _)) => todo!(),
    //     (TigerFileType::BlockGroup, Geoid::Block(_, _, _, _)) => todo!(),
    //     (TigerFileType::Block, Geoid::State(_)) => todo!(),
    //     (TigerFileType::Block, Geoid::County(_, _)) => todo!(),
    //     (TigerFileType::Block, Geoid::CountySubdivision(_, _, _)) => todo!(),
    //     (TigerFileType::Block, Geoid::Place(_, _)) => todo!(),
    //     (TigerFileType::Block, Geoid::CensusTract(_, _, _)) => todo!(),
    //     (TigerFileType::Block, Geoid::BlockGroup(_, _, _, _)) => todo!(),
    //     (TigerFileType::Block, Geoid::Block(_, _, _, _)) => todo!(),
    // }?;
    todo!()
}

pub fn tiger_file_directory_url(
    base_url: &str,
    tiger_year: &TigerYear,
    tiger_file_type: &TigerFileType,
) -> Result<String, String> {
    let dir = tiger_file_type.name_upper(tiger_year)?;
    let url = format!("{}/{}/{}", base_url, tiger_year.to_year_string(), dir);
    Ok(url)
}
