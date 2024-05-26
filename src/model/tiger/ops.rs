use super::tiger_file_type::TigerFileType;
use super::tiger_year::TigerYear;

pub fn tiger_file_directory_url(
    base_url: &str,
    tiger_year: &TigerYear,
    tiger_file_type: &TigerFileType,
) -> Result<String, String> {
    let dir = tiger_file_type.name_upper(tiger_year)?;
    let url = format!("{}/{}/{}", base_url, tiger_year.to_year_string(), dir);
    Ok(url)
}
