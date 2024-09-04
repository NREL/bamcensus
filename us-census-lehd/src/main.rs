use clap::Parser;
use env_logger;
use itertools::Itertools;
use us_census_core::model::identifier::GeoidType;
use us_census_core::ops::agg::aggregation_function::NumericAggregation;
use us_census_lehd::api::lodes_api;
use us_census_lehd::model::lodes::{self as lodes_model, WacSegment};

// todo: top level here should be a LEHD command
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
pub struct LodesCliArgs {
    /// LODES year in [2002, 2016]
    #[arg(short, long)]
    year: u64,
    /// states to download. omit to download all states.
    #[arg(short, long)]
    states: Option<String>,
    /// LODES has 3 datasets: OD, RAC, and WAC
    #[arg(long)]
    dataset: Option<lodes_model::LodesDataset>,
    /// LODES are created in editions, see website for details. LODES8 by default if not provided.
    #[arg(long)]
    edition: Option<lodes_model::LodesEdition>,
    /// LODES workforce segment defined in LODES schema documentation
    #[arg(long)]
    segment: Option<lodes_model::WorkplaceSegment>,
    /// WAC job type defined in LODES schema documentation
    #[arg(long)]
    jobtype: Option<lodes_model::LodesJobType>,
    /// level to aggregate result value
    #[arg(long)]
    agg_geoid_type: Option<GeoidType>,
    /// function to aggregate result value
    #[arg(long)]
    agg_fn: Option<NumericAggregation>,
    // todo: use clap.Parser's subcommand structures to flip between WAC, OD, and RAC data since they
    // are structurally different
}

impl LodesCliArgs {
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
    let args = LodesCliArgs::parse();
    let edition = args.edition.unwrap_or_default();
    let dataset = args.dataset.unwrap_or_default();
    let segment = args.segment.unwrap_or_default();
    let job_type = args.jobtype.unwrap_or_default();
    let year = args.year;
    let wac_segments = vec![WacSegment::C000];
    let state_codes = &args.get_state_codes();
    let agg_fn = args.agg_fn.unwrap_or_default();
    let output_geoid_type = args.agg_geoid_type.unwrap_or_else(|| GeoidType::Block);

    println!("executing LODES download");

    let client = reqwest::Client::new();
    let queries =
        lodes_api::create_queries(&edition, &dataset, state_codes, segment, job_type, year);
    let agg_rows = lodes_api::run(&client, &queries, &wac_segments, output_geoid_type, agg_fn)
        .await
        .unwrap();

    let n_res = agg_rows.len();
    println!("{} agg rows", n_res);
    println!("queries:");
    for (geoid, values) in agg_rows.iter() {
        println!("{}", geoid);
        for value in values.iter() {
            println!("  {}", value);
        }
    }
}
