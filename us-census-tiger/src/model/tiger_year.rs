use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "UPPERCASE")]
pub enum TigerYear {
    Tiger2022,
}

impl TigerYear {
    pub fn to_directory_name(&self) -> String {
        match self {
            TigerYear::Tiger2022 => String::from("TIGER2022"),
        }
    }

    pub fn to_int(&self) -> u64 {
        match self {
            TigerYear::Tiger2022 => 2022,
        }
    }
}

impl TryFrom<u64> for TigerYear {
    type Error = String;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            2022 => Ok(TigerYear::Tiger2022),
            _ => Err(format!("unknown tiger year {}", value)),
        }
    }
}

// impl Display for TigerYear {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         let string = match self {
//             TigerYear::Tiger2022 => String::from("TIGER2022"),
//         };
//         write!(f, "{}", string)
//     }
// }
