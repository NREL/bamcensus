use bamcensus::app::acs_tiger;
use bamcensus::app::lodes_tiger_args::LodesTigerArgs;
use bamcensus::model::acs_tiger_output_row::AcsTigerOutputRow;
use bamcensus_acs::model::{AcsApiQueryParams, AcsGeoidQuery, AcsType};
use bamcensus_core::model::identifier::Geoid;
use bamcensus_core::model::identifier::GeoidType;
use clap::command;
use clap::Parser;
use clap::Subcommand;
use itertools::Itertools;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct bamcensusCli {
    #[command(subcommand)]
    pub command: bamcensusApp,
}

#[derive(Subcommand)]
pub enum bamcensusApp {
    AcsApp(AcsAppCli),
    #[command(subcommand)]
    LehdApp(LehdAppCli),
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct AcsAppCli {
    #[arg(short, long)]
    pub geoid: String,
    #[arg(short, long)]
    pub wildcard: Option<GeoidType>,
    #[arg(long)]
    pub year: u64,
    #[arg(long)]
    pub acs_query: String,
    #[arg(short, long)]
    pub acs_type: AcsType,
    #[arg(short, long)]
    pub acs_token: Option<String>,
}

#[derive(Subcommand)]
pub enum LehdAppCli {
    Lodes(LodesTigerArgs),
}

#[tokio::main]
async fn main() {
    let args = bamcensusCli::parse();
    match args.command {
        bamcensusApp::AcsApp(acs_args) => acs(&acs_args).await,
        bamcensusApp::LehdApp(LehdAppCli::Lodes(lodes_args)) => lodes_args.run().await,
    }
}

async fn acs(args: &AcsAppCli) {
    let acs_get_query = args.acs_query.split(',').map(String::from).collect_vec();
    let geoid = Geoid::try_from(args.geoid.as_str()).unwrap();
    let query: AcsGeoidQuery = AcsGeoidQuery::new(Some(geoid), args.wildcard).unwrap();
    let query_params = AcsApiQueryParams::new(
        None,
        args.year,
        args.acs_type,
        acs_get_query,
        query,
        args.acs_token.clone(),
    );

    let filename = &query_params.output_filename();
    let res = acs_tiger::run(&query_params).await.unwrap();
    println!(
        "found {} responses, {}/{} errors",
        res.join_dataset.len(),
        res.tiger_errors.len(),
        res.join_errors.len(),
    );
    println!("TIGER ERRORS");
    for row in res.tiger_errors.into_iter() {
        println!("{row}")
    }
    println!("JOIN ERRORS");
    for row in res.join_errors.into_iter() {
        println!("{row}")
    }

    let mut writer = csv::WriterBuilder::new().from_path(filename).unwrap();
    for row in res.join_dataset {
        let out_row = AcsTigerOutputRow::from(row);
        writer.serialize(out_row).unwrap();
    }
}
