use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub enum GeoidHierarchyType {
    // #[serde(rename = "BG")]
    BlockGroup,
    Block, // #[serde(rename = "TABBLOCK10")]
           // Block2010,
           // #[serde(rename = "TABBLOCK20")]
           // Block2020,
}

impl GeoidHierarchyType {
    pub fn name_upper(&self, year: u64) -> Result<String, String> {
        use GeoidHierarchyType as G;
        match self {
            G::BlockGroup => Ok(String::from("BG")),
            G::Block if in_range(year, 2010, 2020) => Ok(String::from("TABBLOCK10")),
            G::Block if in_range(year, 2020, 2030) => Ok(String::from("TABBLOCK20")),
            _ => Err(format!("invalid file/year: {:?} / {}", self, year)),
            // TigerFileType::Block2010 => String::from("TABBLOCK10"),
            // TigerFileType::Block2020 => String::from("TABBLOCK20"),
        }
    }
    pub fn name_lower(&self, year: u64) -> Result<String, String> {
        self.name_upper(year).map(|n| n.to_lowercase())
    }
}

fn in_range(year: u64, lb: u64, ub: u64) -> bool {
    lb <= year && year < ub
}
