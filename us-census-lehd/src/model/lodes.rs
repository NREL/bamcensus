use clap::{Parser, ValueEnum};
use env_logger;
use futures::{stream, StreamExt};
use itertools::Itertools;
use log;
use serde::{Deserialize, Serialize};
use std::fmt::Display;


// scripts for downloading LODES datasets
// see https://lehd.ces.census.gov/data/lodes/LODES8/LODESTechDoc8.1.pdf

const BASE_URL: &'static str = "https://lehd.ces.census.gov/data/lodes";


#[derive(Deserialize, ValueEnum, Default, Clone, Copy, Debug)]
#[serde(rename_all = "snake_case")]
pub enum LodesEdition {
        Lodes7,
    #[default]
    Lodes8,
}

 impl Display for LodesEdition {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                        LodesEdition::Lodes7 => write!(f, "LODES7"),
            LodesEdition::Lodes8 => write!(f, "LODES8"),
        }
    }
}

 #[derive(Deserialize, ValueEnum, Default, Clone, Copy, Debug)]
#[serde(rename_all = "snake_case")]
pub enum LodesDataset {
        #[default]
    WAC,
}

 impl Display for LodesDataset {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{:?}", self)
    }
}

 /// The WAC segment, an alphanumeric code of the following:
/// /// S000 Num Total number of jobs
/// /// SA01 Num Number of jobs of workers age 29 or younger9
/// /// SA02 Num Number of jobs for workers age 30 to 549
/// /// SA03 Num Number of jobs for workers age 55 or older 9
/// /// SE01 Num Number of jobs with earnings $1250/month or less
/// /// SE02 Num Number of jobs with earnings $1251/month to $3333/month
/// /// SE03 Num Number of jobs with earnings greater than $3333/month
/// /// SI01 Num Number of jobs in Goods Producing industry sectors
/// /// SI02 Num Number of jobs in Trade, Transportation, and Utilities industry sectors
/// /// SI03 Num Number of jobs in All Other Services industry sectors
/// #[derive(Default, ValueEnum, Serialize, Deserialize, Clone, Copy, Debug)]
pub enum WacSegment {
        #[default]
    S000,
    SA01,
    SA02,
    SA03,
    SE01,
    SE02,
    SE03,
    SI01,
    SI02,
    SI03,
}

 impl Display for WacSegment {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{:?}", self)
    }
}

 impl WacSegment {
        pub fn create_filename(&self, state_code: &String, job_type: &JobType, year: i64) -> String {
                format!("{}_wac_{}_{}_{}.csv.gz", state_code, self, job_type, year)
    }
}

 ///  Job Type, can have a value of “JT00” for All Jobs, “JT01” for Primary Jobs, “JT02” for All
/// /// Private Jobs, “JT03” for Private Primary Jobs, “JT04” for All Federal Jobs, or “JT05” for
/// /// Federal Primary Jobs.
/// #[derive(Default, ValueEnum, Serialize, Deserialize, Clone, Copy, Debug)]
pub enum JobType {
        #[default]
    JT00,
    JT01,
    JT02,
    JT03,
    JT04,
    JT05,
}

 impl Display for JobType {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{:?}", self)
    }
}

 pub const ALL_STATES: [&'static str; 52] = [
        "al", "ak", "az", "ar", "ca", "co", "ct", "de", "dc", "fl", "ga", "hi", "id", "il", "in", "ia",
    "ks", "ky", "la", "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", "nm",
    "ny", "nc", "nd", "oh", "ok", "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt", "va", "wa",
    "wv", "wi", "wy", "pr",
    ];

 #[derive(Parser)]
pub struct CliArgs {
        /// LODES year in [2002, 2016]
    ///     year: i64,
    /// states to download. omit to download all states.
    ///     state_codes: Option<String>,
    /// LODES are created in editions, see website for details. LODES8 by default if not provided.
    ///     lodes_edition: Option<LodesEdition>,
    /// WAC workforce segment defined in LODES schema documentation
    ///     wac_segment: Option<WacSegment>,
    /// WAC job type defined in LODES schema documentation
    ///     job_type: Option<JobType>,
    /// how many downloads to run in parallel, should be based on system cores, 1 if not provided.
    ///     parallelism: Option<usize>,
    // todo: use clap.Parser's subcommand structures to flip between WAC, OD, and RAC data since they
    // are structurally different
    // /// LODES has 3 different types of datasets, see website for details. WAC by default if not provided.
    // lodes_dataset: Option<LodesDataset>,
}

 impl CliArgs {
        pub fn create_url(
                &self,
        state_code: &String,
        lodes_dataset: &LodesDataset,
        filename: &String,
    ) -> String {
                format!(
                        "{}/{}/{}/{}/{}",
            BASE_URL,
            self.lodes_edition.unwrap_or_default(),
            state_code,
            lodes_dataset,
            filename
        )
    }
    
     pub fn create_filename(&self, state_code: &String, year: &i64) -> String {
                self.wac_segment.unwrap_or_default().create_filename(
                        state_code,
            &self.job_type.unwrap_or_default(),
            *year,
        )
    }
    
     pub fn get_year(&self) -> Result<i64, String> {
                if 2002 <= self.year && self.year <= 2016 {
                        Ok(self.year)
        } else {
                        Err(format!("year must be in range [2002, 2016]: {}", self.year))
        }
    }
    
     pub fn get_state_codes(&self) -> Vec<String> {
                match &self.state_codes {
                        Some(s) => s.split(",").map(|sc| sc.to_lowercase()).collect_vec(),
            None => ALL_STATES.iter().map(|s| s.to_string()).collect_vec(),
        }
    }
}

 #[tokio::main]
async fn main() {
        env_logger::init();
    let args = CliArgs::parse();
    let year = &args.get_year().unwrap();
    // let lodes_dataset = args.lodes_dataset.clone().unwrap_or_default();
    // let wac_segment = &args.wac_segment.unwrap_or_default();
    // let job_type = &args.job_type.unwrap_or_default();
    let state_codes = &args.get_state_codes();
    let parallelism = &args.parallelism.unwrap_or(1);
    
     log::info!("executing LODES download");
    let lodes_queries = state_codes
    .iter()
        .map(|sc| {
                        let filename = args.create_filename(sc, year);
            let url = args.create_url(sc, &LodesDataset::WAC, &filename);
            url
        })
        .collect_vec();
     
     let client = reqwest::Client::new();
    let responses = stream::iter(lodes_queries)
    .map(|url| {
                        let client = &client;
            async move {
                                let res = client.get(url).send().await?;
                res.bytes().await
            }
        })
        .buffer_unordered(*parallelism);
     
     responses
    .for_each(|b| async {
                        match b {
                                Ok(bytes) => todo!(),
                Err(e) => log::error!("DOWNLOAD FAILURE: {}", e.to_string()),
            }
        })
        .await;
    }


