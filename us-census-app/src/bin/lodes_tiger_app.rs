use clap::{Args, Parser, Subcommand};
use itertools::Itertools;
use us_census_app::lodes_tiger;
use us_census_app::model::lodes_tiger_output_row::LodesTigerOutputRow;
use us_census_core::model::identifier::geoid::Geoid;
use us_census_core::model::identifier::geoid_type::GeoidType;
use us_census_lehd::model::lodes::{self as lodes_model, LodesDataset, WacSegment};

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
    pub geoids: String,
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
    let cli = LodesTigerApp::parse();
    match &cli.dataset {
        LodesDatasetCli::Wac(wac) => run_wac(wac).await,
    }
}

async fn run_wac(args: &LodesWacTigerAppCli) {
    let geoids = args
        .geoids
        .split(',')
        .map(|g| Geoid::try_from(g).unwrap())
        .collect_vec();
    let dataset = LodesDataset::WAC {
        edition: args.edition.unwrap_or_default(),
        job_type: args.jobtype.unwrap_or_default(),
        segment: args.segment.unwrap_or_default(),
        year: args.year,
    };
    let wildcard = args.wildcard.clone();
    let wac_segments = args
        .wac_segments
        .split(',')
        .map(WacSegment::try_from)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    println!("Dataset: {}", &dataset);
    println!("geoids: {:?}", &geoids);
    println!("wildcard: {:?}", &wildcard);
    println!("wac_segments: {:?}", &wac_segments);

    let res = lodes_tiger::run(args.year, geoids, wildcard, &wac_segments, dataset)
        .await
        .unwrap();
    println!(
        "found {} responses, {}/{} errors",
        res.join_dataset.len(),
        res.tiger_errors.len(),
        res.join_errors.len(),
    );
    // println!("RESULTS");
    // for row in res.join_dataset.into_iter() {
    //     println!("{}", row)
    // }
    println!("TIGER ERRORS");
    for row in res.tiger_errors.into_iter() {
        println!("{}", row)
    }
    println!("JOIN ERRORS");
    for row in res.join_errors.into_iter() {
        println!("{}", row)
    }
    let mut writer = csv::WriterBuilder::new().from_path("output.csv").unwrap();
    for row in res.join_dataset {
        let out_row = LodesTigerOutputRow::from(row);
        writer.serialize(out_row).unwrap();
    }
}
