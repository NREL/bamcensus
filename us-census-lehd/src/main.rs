use clap::Parser;
use csv::ReaderBuilder;
use env_logger;
use flate2::read::GzDecoder;
use futures::{stream, StreamExt};
use itertools::Itertools;
use us_census_core::model::identifier::geoid::Geoid;
use us_census_lehd::api::lodes_api;
use us_census_lehd::model::lodes::wac_row::WacRow;
use us_census_lehd::model::lodes::wac_value::WacValue;
use us_census_lehd::model::lodes::{self as lodes_model, WacSegment};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct CliArgs {
    /// LODES year in [2002, 2016]
    #[arg(short, long)]
    year: u64,
    /// states to download. omit to download all states.
    #[arg(short, long)]
    states: Option<String>,
    /// LODES are created in editions, see website for details. LODES8 by default if not provided.
    #[arg(long)]
    edition: Option<lodes_model::LodesEdition>,
    /// WAC workforce segment defined in LODES schema documentation
    #[arg(long)]
    segment: Option<lodes_model::WorkplaceSegment>,
    /// WAC job type defined in LODES schema documentation
    #[arg(long)]
    jobtype: Option<lodes_model::LodesJobType>,
    /// how many downloads to run in parallel, should be based on system cores, 1 if not provided.
    #[arg(long)]
    parallelism: Option<usize>,
    // todo: use clap.Parser's subcommand structures to flip between WAC, OD, and RAC data since they
    // are structurally different
    // /// LODES has 3 different types of datasets, see website for details. WAC by default if not provided.
    // lodes_dataset: Option<LodesDataset>,
}

impl CliArgs {
    pub fn create_url(
        &self,
        state_code: &String,
        lodes_dataset: &lodes_model::LodesDataset,
        filename: &String,
    ) -> String {
        format!(
            "{}/{}/{}/{}/{}",
            lodes_model::BASE_URL,
            self.edition.unwrap_or_default(),
            state_code,
            lodes_dataset.to_string().to_lowercase(),
            filename
        )
    }

    pub fn create_filename(&self, state_code: &String, year: u64) -> String {
        let segment = self.segment.unwrap_or_default();
        let job_type = self.jobtype.unwrap_or_default();
        lodes_api::create_wac_filename(state_code, &segment, &job_type, year)
    }

    pub fn get_year(&self) -> Result<u64, String> {
        if 2002 <= self.year && self.year <= 2016 {
            Ok(self.year)
        } else {
            Err(format!("year must be in range [2002, 2016]: {}", self.year))
        }
    }

    pub fn get_state_codes(&self) -> Vec<String> {
        match &self.states {
            Some(s) => s.split(",").map(|sc| sc.to_lowercase()).collect_vec(),
            None => lodes_model::ALL_STATES
                .iter()
                .map(|s| s.to_string())
                .collect_vec(),
        }
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let args = CliArgs::parse_from(["", "--year", "2016", "--states", "co"]);
    let year = &args.get_year().unwrap();
    // let lodes_dataset = args.lodes_dataset.clone().unwrap_or_default();
    let wac_segments = vec![WacSegment::C000];
    // let job_type = &args.job_type.unwrap_or_default();
    let state_codes = &args.get_state_codes();
    let parallelism = &args.parallelism.unwrap_or(1);

    println!("executing LODES download");
    let lodes_queries = state_codes
        .iter()
        .map(|sc| {
            let filename = args.create_filename(sc, *year);
            let url = args.create_url(sc, &lodes_model::LodesDataset::WAC, &filename);
            url
        })
        .collect_vec();

    let client = reqwest::Client::new();
    let responses = stream::iter(&lodes_queries)
        .map(|url| {
            let client = &client;
            let wac_segments = &wac_segments;
            async move {
                let res = client
                    .get(url)
                    .send()
                    .await
                    .map_err(|e| format!("failure sending LODES HTTP request: {}", e))?;
                let gzip_bytes = res
                    .bytes()
                    .await
                    .map_err(|e| format!("failure reading response body: {}", e))?;
                let mut reader = ReaderBuilder::new().from_reader(GzDecoder::new(&gzip_bytes[..]));
                let mut result = vec![];
                for r in reader.deserialize() {
                    let row: WacRow =
                        r.map_err(|e| format!("failure reading LODES response row: {}", e))?;
                    let geoid = row.geoid()?;
                    let mut row_result = vec![];
                    for segment in wac_segments.iter() {
                        row_result.push(WacValue::new(segment.clone(), row.get(segment)));
                    }
                    result.push((geoid, row_result))
                }
                Ok(result)
            }
        })
        .buffer_unordered(*parallelism);
    let mut n_res: usize = 0;
    responses
        .for_each(|b: Result<Vec<(Geoid, Vec<WacValue>)>, String>| {
            n_res += 1;
            async {
                match b {
                    Ok(rows) => {
                        for (geoid, values) in rows {
                            println!("GEOID {}:", geoid);
                            for value in values {
                                println!("  {}", value);
                            }
                        }
                    }
                    Err(e) => println!("DOWNLOAD FAILURE: {}", e.to_string()),
                }
            }
        })
        .await;
    println!("{} responses", n_res);
    println!("queries:");
    for url in lodes_queries.iter() {
        println!("{}", url);
    }
}
