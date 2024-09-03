use clap::ValueEnum;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Deserialize, Serialize, ValueEnum, Default, Clone, Copy, Debug)]
#[serde(rename_all = "snake_case")]
pub enum LodesDataset {
    OD,
    RAC,
    #[default]
    WAC,
}

impl Display for LodesDataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl LodesDataset {
    pub fn description(&self) -> String {
        match self {
            LodesDataset::OD => String::from("Origin-Destination data, jobs totals are associated with both a home Census Block and a work Census Block"),
            LodesDataset::RAC => String::from("Residence Area Characteristic data, jobs are totaled by home Census Block"),
            LodesDataset::WAC => String::from("Workplace Area Characteristic data, jobs are totaled by work Census Block"),
        }
    }
}
