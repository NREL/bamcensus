use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Copy)]
#[serde(rename_all = "UPPERCASE")]
pub enum AcsYear {
    Acs2019,
    Acs2020,
    Acs2021,
    Acs2022,
}

impl AcsYear {
    pub fn to_directory_name(&self) -> String {
        format!("{}", self.to_int())
    }

    pub fn to_int(&self) -> u64 {
        match self {
            AcsYear::Acs2019 => 2019,
            AcsYear::Acs2020 => 2020,
            AcsYear::Acs2021 => 2021,
            AcsYear::Acs2022 => 2022,
        }
    }
}

impl TryFrom<u64> for AcsYear {
    type Error = String;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        match value {
            2019 => Ok(AcsYear::Acs2022),
            2020 => Ok(AcsYear::Acs2022),
            2021 => Ok(AcsYear::Acs2022),
            2022 => Ok(AcsYear::Acs2022),
            _ => Err(format!("unsupported ACS year {}", value)),
        }
    }
}

// impl Display for AcsYear {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         let string = match self {
//             AcsYear::Acs2022 => String::from("Acs2022"),
//         };
//         write!(f, "{}", string)
//     }
// }
