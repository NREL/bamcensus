use us_census_acs::model::acs_type::AcsType;
use us_census_app::acs_tiger;
use us_census_app::model::acs_tiger_output_row::AcsTigerOutputRow;
use us_census_core::model::identifier::fips;
use us_census_core::model::identifier::geoid::Geoid;
use us_census_core::model::identifier::geoid_type::GeoidType;

#[tokio::main]
async fn main() {
    let year = 2010;
    let acs_type = AcsType::FiveYear;
    let acs_get_query = vec![String::from("B01001_001E")];
    let geoid = Geoid::State(fips::State(08));
    let wildcard = GeoidType::CensusTract;

    let res = acs_tiger::run(
        year,
        acs_type,
        acs_get_query,
        Some(geoid),
        Some(wildcard),
        None,
    )
    .await
    .unwrap();
    println!(
        "found {} responses, {}/{}/{} errors",
        res.join_dataset.len(),
        res.acs_errors.len(),
        res.tiger_errors.len(),
        res.join_errors.len(),
    );
    // println!("RESULTS");
    // for row in res.join_dataset.into_iter() {
    //     println!("{}", row)
    // }
    println!("ACS ERRORS");
    for row in res.acs_errors.into_iter() {
        println!("{}", row)
    }
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
        let out_row = AcsTigerOutputRow::from(row);
        writer.serialize(out_row).unwrap();
    }
    ()
}
