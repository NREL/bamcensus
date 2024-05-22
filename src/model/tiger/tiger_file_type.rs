use super::tiger_year::TigerYear;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum TigerFileType {
    // #[serde(rename = "BG")]
    BlockGroup,
    Block, // #[serde(rename = "TABBLOCK10")]
           // Block2010,
           // #[serde(rename = "TABBLOCK20")]
           // Block2020,
}

impl TigerFileType {
    pub fn name_upper(&self, year: &TigerYear) -> Result<String, String> {
        match self {
            TigerFileType::BlockGroup => Ok(String::from("BG")),
            TigerFileType::Block if in_range(year, 2010, 2020) => Ok(String::from("TABBLOCK10")),
            TigerFileType::Block if in_range(year, 2020, 2030) => Ok(String::from("TABBLOCK20")),
            _ => Err(format!(
                "invalid file/year: {:?} / {}",
                self,
                year.to_year_int()
            )), // TigerFileType::Block2010 => String::from("TABBLOCK10"),
                // TigerFileType::Block2020 => String::from("TABBLOCK20"),
        }
    }
    pub fn name_lower(&self, year: &TigerYear) -> Result<String, String> {
        self.name_upper(year).map(|n| n.to_lowercase())
    }
}

fn in_range(year: &TigerYear, lb: u64, ub: u64) -> bool {
    let year_int = year.to_year_int();
    lb <= year_int && year_int < ub
}
