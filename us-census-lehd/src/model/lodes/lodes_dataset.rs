use serde::{Deserialize, Serialize};
use std::fmt::Display;

use super::{LodesEdition, LodesJobType, WorkplaceSegment, BASE_URL};

#[derive(Deserialize, Serialize, Clone, Copy, Debug)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum LodesDataset {
    OD,
    RAC,
    WAC {
        edition: LodesEdition,
        job_type: LodesJobType,
        segment: WorkplaceSegment,
        year: u64,
    },
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
            LodesDataset::WAC { edition: _, job_type: _, segment: _, year: _ } => String::from("Workplace Area Characteristic data, jobs are totaled by work Census Block"),
        }
    }

    pub fn dataset_directory(&self) -> String {
        match self {
            LodesDataset::OD => todo!(),
            LodesDataset::RAC => todo!(),
            LodesDataset::WAC {
                edition: _,
                job_type: _,
                segment: _,
                year: _,
            } => String::from("wac"),
        }
    }

    pub fn create_uri(&self, state_code: &str) -> String {
        match self {
            LodesDataset::OD => todo!(),
            LodesDataset::RAC => todo!(),
            LodesDataset::WAC {
                edition,
                job_type,
                segment,
                year,
            } => {
                let filename = format!(
                    "{}_wac_{}_{}_{}.csv.gz",
                    state_code, segment, job_type, year
                );
                format!(
                    "{}/{}/{}/{}/{}",
                    BASE_URL,
                    edition,
                    state_code,
                    self.dataset_directory(),
                    filename
                )
            }
        }
    }
}
