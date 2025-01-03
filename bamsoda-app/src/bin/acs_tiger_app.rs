use bamsoda_acs::model::{AcsApiQueryParams, AcsGeoidQuery, AcsType};
use bamsoda_app::app::acs_tiger;
use bamsoda_app::model::acs_tiger_output_row::AcsTigerOutputRow;
use bamsoda_core::model::identifier::Geoid;
use bamsoda_core::model::identifier::GeoidType;
use clap::Parser;
use itertools::Itertools;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct AcsTigerAppCli {
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

#[tokio::main]
async fn main() {
    let args = AcsTigerAppCli::parse();
    let acs_get_query = args.acs_query.split(',').map(String::from).collect_vec();
    let geoid = Geoid::try_from(args.geoid.as_str()).unwrap();
    let query: AcsGeoidQuery = AcsGeoidQuery::new(Some(geoid), args.wildcard).unwrap();
    let query_params = AcsApiQueryParams::new(
        None,
        args.year,
        args.acs_type,
        acs_get_query,
        query,
        args.acs_token,
    );

    let filename = &query_params.output_filename();
    let res = acs_tiger::run(query_params).await.unwrap();
    println!(
        "found {} responses, {}/{} errors",
        res.join_dataset.len(),
        res.tiger_errors.len(),
        res.join_errors.len(),
    );
    println!("TIGER ERRORS");
    for row in res.tiger_errors.into_iter() {
        println!("{}", row)
    }
    println!("JOIN ERRORS");
    for row in res.join_errors.into_iter() {
        println!("{}", row)
    }

    let mut writer = csv::WriterBuilder::new().from_path(filename).unwrap();
    for row in res.join_dataset {
        let out_row = AcsTigerOutputRow::from(row);
        writer.serialize(out_row).unwrap();
    }
}
