use clap::{Args, Parser, Subcommand};
use itertools::Itertools;
use us_census_app::lodes_tiger;
use us_census_app::model::lodes_tiger_output_row::LodesTigerOutputRow;
use us_census_core::model::identifier::geoid_type::GeoidType;
use us_census_core::model::lodes::{self as lodes_model, LodesDataset, WacSegment};
use us_census_core::model::{fips::state_code::StateCode, identifier::geoid::Geoid};

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct LodesTigerApp {
    #[command(subcommand)]
    dataset: LodesDatasetCli,
}

#[derive(Subcommand)]
pub enum LodesDatasetCli {
    Wac(LodesWacTigerAppCli),
}

// #[derive(Parser, Debug)]
// #[command(author, version, about, long_about = None)]
#[derive(Args)]
pub struct LodesWacTigerAppCli {
    #[arg(short, long)]
    pub geoids: Option<String>,
    #[arg(short, long)]
    pub wildcard: Option<GeoidType>,
    #[arg(long)]
    pub year: u64,
    #[arg(long)]
    wac_segments: String,
    #[arg(long)]
    edition: Option<lodes_model::LodesEdition>,
    /// LODES workforce segment defined in LODES schema documentation
    #[arg(long)]
    segment: Option<lodes_model::WorkplaceSegment>,
    /// WAC job type defined in LODES schema documentation
    #[arg(long)]
    jobtype: Option<lodes_model::LodesJobType>,
}

#[tokio::main]
async fn main() {
    env_logger::init();
    let cli = LodesTigerApp::parse();
    match &cli.dataset {
        LodesDatasetCli::Wac(wac) => run_wac(wac).await,
    }
}

async fn run_wac(args: &LodesWacTigerAppCli) {
    let geoids = match &args.geoids {
        Some(s) => s
            .split(',')
            .map(|g| Geoid::try_from(g).unwrap())
            .collect_vec(),
        None => StateCode::ALL
            .iter()
            .map(|sc| {
                let fips = sc.to_fips_string();
                Geoid::try_from(fips.as_str()).unwrap()
            })
            .collect_vec(),
    };
    let dataset = LodesDataset::WAC {
        edition: args.edition.unwrap_or_default(),
        job_type: args.jobtype.unwrap_or_default(),
        segment: args.segment.unwrap_or_default(),
        year: args.year,
    };
    let wildcard = args.wildcard;
    let wac_segments = args
        .wac_segments
        .split(',')
        .map(WacSegment::try_from)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    let res = lodes_tiger::run(geoids, &wildcard, &wac_segments, dataset)
        .await
        .unwrap();
    println!(
        "found {} responses, {} errors",
        res.join_dataset.len(),
        res.tiger_errors.len() + res.join_errors.len(),
    );

    if !res.tiger_errors.is_empty() {
        println!("TIGER ERRORS");
        for row in res.tiger_errors.into_iter() {
            println!("{}", row)
        }
    }
    if !res.join_errors.is_empty() {
        println!("DATASET JOIN ERRORS");
        for row in res.join_errors.into_iter() {
            println!("{}", row)
        }
    }
    let mut writer = csv::WriterBuilder::new()
        .from_path(dataset.output_filename(&wildcard))
        .unwrap();
    for row in res.join_dataset {
        let out_row = LodesTigerOutputRow::from(row);
        writer.serialize(out_row).unwrap();
    }
}
