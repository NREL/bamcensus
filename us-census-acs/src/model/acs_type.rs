use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum AcsType {
    OneYear,
    FiveYear,
}

impl AcsType {
    pub fn to_directory_name(&self) -> String {
        match self {
            AcsType::OneYear => String::from("acs1"),
            AcsType::FiveYear => String::from("acs5"),
        }
    }

    pub fn to_int(&self) -> u64 {
        match self {
            AcsType::OneYear => 1,
            AcsType::FiveYear => 5,
        }
    }
}

impl TryFrom<u64> for AcsType {
    type Error = String;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(AcsType::OneYear),
            5 => Ok(AcsType::FiveYear),
            _ => Err(format!("unknown acs type {}", value)),
        }
    }
}

// impl Display for AcsType {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         let string = match self {
//             AcsType::Acs2022 => String::from("Acs2022"),
//         };
//         write!(f, "{}", string)
//     }
// }
