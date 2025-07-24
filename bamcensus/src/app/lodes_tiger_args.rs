use super::lodes_tiger;
use crate::model::lodes_tiger_output_row::LodesTigerOutputRow;
use bamcensus_core::model::identifier::GeoidType;
use bamcensus_core::model::identifier::{Geoid, StateCode};
use bamcensus_lehd::model::{
    LodesDataset, LodesEdition, LodesJobType, WacSegment, WorkplaceSegment,
};
use clap::{Args, Parser, Subcommand};
use itertools::Itertools;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
pub struct LodesTigerArgs {
    #[command(subcommand)]
    dataset: LodesDatasetCli,
}

#[derive(Subcommand)]
pub enum LodesDatasetCli {
    Wac(LodesWacTigerAppCli),
    Od,
    Rac,
}

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
    edition: Option<LodesEdition>,
    /// LODES workforce segment defined in LODES schema documentation
    #[arg(long)]
    segment: Option<WorkplaceSegment>,
    /// WAC job type defined in LODES schema documentation
    #[arg(long)]
    jobtype: Option<LodesJobType>,
}

impl LodesTigerArgs {
    pub async fn run(&self) {
        match &self.dataset {
            LodesDatasetCli::Wac(wac) => run_wac(wac).await,
            LodesDatasetCli::Od => todo!(),
            LodesDatasetCli::Rac => todo!(),
        }
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

    let res = lodes_tiger::run(&geoids, &wildcard, &wac_segments, &dataset)
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
            println!("{row}")
        }
    }
    if !res.join_errors.is_empty() {
        println!("DATASET JOIN ERRORS");
        for row in res.join_errors.into_iter() {
            println!("{row}")
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
