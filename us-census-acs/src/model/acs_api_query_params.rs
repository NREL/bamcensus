use crate::model::{acs_geoid_query::AcsGeoidQuery, acs_type::AcsType, acs_year::AcsYear};
use itertools::Itertools;

pub struct AcsApiQueryParams<'a> {
    pub url: String,
    pub get_query: &'a [String],
    pub for_query: &'a AcsGeoidQuery,
    pub api_token: Option<&'a String>,
}

impl<'a> AcsApiQueryParams<'a> {
    pub fn new(
        base_url: &str,
        acs_year: &AcsYear,
        acs_type: &AcsType,
        get_query: &'a [String],
        for_query: &'a AcsGeoidQuery,
        api_token: Option<&'a String>,
    ) -> AcsApiQueryParams<'a> {
        let year_s = acs_year.to_directory_name();
        let type_s = acs_type.to_directory_name();
        let url = format!("{}/{}/acs/{}", base_url, year_s, type_s);
        AcsApiQueryParams {
            url,
            get_query,
            for_query,
            api_token,
        }
    }

    /// builds an ACS REST query URL from application parameters.
    ///
    /// # Examples
    ///
    /// Example 1. Get 2022 Five-Year ACS state-level population estimates for all states, returning
    /// their state name, population value, and state FIPS code.
    ///
    /// ```rust
    /// use us_census_core::model::identifier::{fips, geoid_type::GeoidType};
    /// use us_census_acs::model::{
    ///     acs_geoid_query::AcsGeoidQuery, acs_type::AcsType, acs_year::AcsYear,
    /// };
    /// use us_census_acs::ops::api;
    ///
    /// let base_url: &str = "https://api.census.gov/data";
    /// let acs_year: AcsYear = AcsYear::Acs2022;
    /// let acs_type: AcsType = AcsType::FiveYear;
    /// let queries: [&str; 2] = ["NAME", "B01001_001E"];
    /// let acs_geoid_query: AcsGeoidQuery = AcsGeoidQuery::new(None, Some(&GeoidType::State)).unwrap();
    /// let api_query_params = api::AcsApiQueryParams::new(base_url, &acs_year, &acs_type, &queries, &acs_geoid_query, None);
    /// let api_url = api_query_params.build_url().unwrap();
    /// assert_eq!(api_url, String::from("https://api.census.gov/data/2022/acs/acs5?get=NAME,B01001_001E&for=state:*"))
    /// ```
    pub fn build_url(&self) -> Result<String, String> {
        let get_query = self.get_query.iter().join(",");
        let for_query = self.for_query.to_query_key();
        let token_query = match self.api_token {
            Some(k) => format!("&key={}", k),
            None => String::from(""),
        };
        let query = format!(
            "{}?get={}&for={}{}",
            self.url, get_query, for_query, token_query,
        );
        Ok(query)
    }

    /// in order to deconstruct an API response, we need the list of
    /// column names in the order that they would appear in the array-
    /// shaped ACS response object.
    pub fn column_names(&self) -> Vec<String> {
        let mut cols = self
            .get_query
            .iter()
            // .map(|s| String::from(s))
            .cloned()
            .collect_vec();
        cols.extend(self.for_query.response_column_names());
        cols
    }
}
