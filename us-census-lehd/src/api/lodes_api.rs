use crate::{
    api::lodes_api,
    model::lodes::{LodesDataset, LodesEdition, LodesJobType, OdPart, WorkplaceSegment},
};
use futures::{stream, StreamExt};
use itertools::Itertools;

pub async fn run(
    lodes_edition: &LodesEdition,
    lodes_dataset: &LodesDataset,
    state_codes: &[&str],
    segment: &WorkplaceSegment,
    job_type: &LodesJobType,
    year: u64,
    parallelism: usize,
) {
    let lodes_queries = state_codes
        .iter()
        .map(|sc| {
            let filename = lodes_api::create_wac_filename(sc, &segment, &job_type, year);
            let url = lodes_edition.create_url(sc, &LodesDataset::WAC, &filename);
            url
        })
        .collect_vec();

    let client = reqwest::Client::new();
    let responses = stream::iter(&lodes_queries)
        .map(|url| {
            let client = &client;
            async move {
                let res = client.get(url).send().await?;
                res.text().await
            }
        })
        .buffer_unordered(parallelism);

    let mut n_res: usize = 0;
    responses
        .for_each(|b| {
            n_res += 1;
            async {
                match b {
                    Ok(string) => println!("{}", string),
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

    todo!("unzip the GZIP, parse into records");
}

// pub fn get_year() -> Result<i64, String> {
//     if 2002 <= self.year && self.year <= 2016 {
//         Ok(self.year)
//     } else {
//         Err(format!("year must be in range [2002, 2016]: {}", self.year))
//     }
// }

// pub fn get_state_codes() -> Vec<String> {
//     match &self.states {
//         Some(s) => s.split(",").map(|sc| sc.to_lowercase()).collect_vec(),
//         None => lodes_model::ALL_STATES
//             .iter()
//             .map(|s| s.to_string())
//             .collect_vec(),
//     }
// }

/// from https://lehd.ces.census.gov/data/lodes/LODES8/LODESTechDoc8.1.pdf:
/// [ST]_od_[PART]_[TYPE]_[YEAR].csv.gz where
///   [ST] = lowercase, 2-letter postal code for a chosen state
///   [PART] = Part of the state file, can have a value of either “main” or “aux”. Complimentary parts of
///     the state file, the main part includes jobs with both workplace and residence in the state
///     and the aux part includes jobs with the workplace in the state and the residence outside
///     of the state.
///   [TYPE] = Job Type, can have a value of “JT00” for All Jobs, “JT01” for Primary Jobs, “JT02” for All
///     Private Jobs, “JT03” for Private Primary Jobs, “JT04” for All Federal Jobs, or “JT05” for
///     Federal Primary Jobs.
///   [YEAR] = Year of job data. Can have the value of 2002-2021 for most states.
pub fn create_od_filename(
    state_code: &String,
    od_part: &OdPart,
    job_type: &LodesJobType,
    year: i64,
) -> String {
    format!("{}_od_{}_{}_{}.csv.gz", state_code, od_part, job_type, year)
}

/// from https://lehd.ces.census.gov/data/lodes/LODES8/LODESTechDoc8.1.pdf:
/// [ST]_wac_[SEG]_[TYPE]_[YEAR].csv.gz where
///   [ST] = lowercase, 2-letter postal code for a chosen state
///   [SEG] = Segment of the workforce, can have the values of “S000”, “SA01”, “SA02”, “SA03”, “SE01”,
///     “SE02”, “SE03”, “SI01”, “SI02”, or “SI03”. These correspond to the same segments of the
///     workforce as are listed in the OD file structure above.
///   [TYPE] = Job Type, can have a value of “JT00” for All Jobs, “JT01” for Primary Jobs, “JT02” for All
///     Private Jobs, “JT03” for Private Primary Jobs, “JT04” for All Federal Jobs, or “JT05” for
///     Federal Primary Jobs.
///   [YEAR] = Year of job data. Can have the value of 2002-2021 for most states.
pub fn create_wac_filename(
    state_code: &str,
    segment: &WorkplaceSegment,
    job_type: &LodesJobType,
    year: u64,
) -> String {
    format!(
        "{}_wac_{}_{}_{}.csv.gz",
        state_code, segment, job_type, year
    )
}
