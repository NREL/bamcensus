use std::rc::Rc;

use itertools::Itertools;
use us_census_core::model::identifier::{
    fips, geoid::Geoid, geoid_type::GeoidType, has_geoid_string::HasGeoidString,
    has_geoid_type::HasGeoidType,
};

pub type DeserializeGeoidFn = Rc<dyn Fn(Vec<serde_json::Value>) -> Result<Geoid, String>>;

#[derive(Clone)]
pub enum AcsGeoidQuery {
    State(Option<fips::State>),
    County(Option<fips::State>, Option<fips::County>),
    CountySubdivision(
        Option<fips::State>,
        Option<fips::County>,
        Option<fips::CountySubdivision>,
    ),
    Place(Option<fips::State>, Option<fips::Place>),
    CensusTract(
        Option<fips::State>,
        Option<fips::County>,
        Option<fips::CensusTract>,
    ),
    BlockGroup(
        Option<fips::State>,
        Option<fips::County>,
        Option<fips::CensusTract>,
        Option<fips::BlockGroup>,
    ),
}

impl AcsGeoidQuery {
    /// # Examples
    ///
    /// when no wildcard is provided, the query is constructed for an exact location, which
    /// should return a single result.
    /// ```rust
    /// use us_census_core::model::identifier::{fips, geoid::Geoid, geoid_type::GeoidType};
    /// use us_census_acs::model::acs_geoid_query::AcsGeoidQuery;;
    ///
    /// let geoid = Geoid::County(fips::State(8), fips::County(1));
    /// let query = AcsGeoidQuery::new(Some(&geoid), None).unwrap();
    /// let key = query.to_query_key();
    /// assert_eq!(key, String::from("state=08&county=001"));
    /// ```
    ///
    /// some combinations simply append a wildcard one level below the geoid, for example,
    /// tacking a county=* on top of state=08.
    /// ```rust
    /// use us_census_core::model::identifier::{fips, geoid::Geoid, geoid_type::GeoidType};
    /// use us_census_acs::model::acs_geoid_query::AcsGeoidQuery;;
    ///
    /// let geoid = Geoid::State(fips::State(8));
    /// let wildcard = GeoidType::County;
    /// let query = AcsGeoidQuery::new(Some(&geoid), Some(&wildcard)).unwrap();
    /// let key = query.to_query_key();
    /// assert_eq!(key, String::from("state=08&county=*"));
    /// ```
    ///
    /// some interpolate the wildcard into a geoid, which is probably never useful, such as
    /// replacing the county in state=08&county=001&tract=000001 with county=*. this effectively
    /// means "i'm interested in any tracts numbered 000001 in CO in any county"... why?

    /// ```rust
    /// use us_census_core::model::identifier::{fips, geoid::Geoid, geoid_type::GeoidType};
    /// use us_census_acs::model::acs_geoid_query::AcsGeoidQuery;;
    ///
    /// let geoid = Geoid::CensusTract(fips::State(8), fips::County(1), fips::CensusTract(1));
    /// let wildcard = GeoidType::County;
    /// let query = AcsGeoidQuery::new(Some(&geoid), Some(&wildcard)).unwrap();
    /// let key = query.to_query_key();
    /// assert_eq!(key, String::from("state=08&county=*&tract=000001"));
    /// ```
    ///
    /// # Returns
    ///
    /// URL query string for calls to the US Census ACS API "for" section, which set the
    /// spatial scope and granularity of the query result.
    pub fn new(
        geoid: Option<&Geoid>,
        wildcard: Option<&GeoidType>,
    ) -> Result<AcsGeoidQuery, String> {
        use Geoid as G;
        use GeoidType as GT;

        match (geoid, wildcard) {
            // ~~ errors ~~
            // - invalid combinations of geoid/wildcard values
            (None, None) => Err(String::from(
                "cannot create query without at least a geoid or wildcard",
            )),
            (_, Some(GT::Block)) => Err(String::from("acs does not support block-level queries")),
            (Some(G::Block(_, _, _, _)), _) => {
                Err(String::from("acs does not support block-level queries"))
            }
            // - mismatched wildcards not present in hierarchy
            (Some(Geoid::County(_, _)), Some(GT::Place)) => Err(String::from(
                "cannot append a 'Place' wildcard to a County Geoid",
            )),
            (Some(G::CountySubdivision(_, _, _)), Some(GT::Place)) => Err(String::from(
                "cannot append a 'Place' wildcard to a CountySubdivision Geoid",
            )),
            (Some(G::CountySubdivision(_, _, _)), Some(GT::CensusTract)) => Err(String::from(
                "cannot append a 'CensusTract' wildcard to a CountySubdivision Geoid",
            )),
            (Some(G::CountySubdivision(_, _, _)), Some(GT::BlockGroup)) => Err(String::from(
                "cannot append a 'BlockGroup' wildcard to a CountySubdivision Geoid",
            )),
            (Some(Geoid::Place(_, _)), Some(GT::County)) => Err(String::from(
                "cannot append a 'County' wildcard to a Place Geoid",
            )),
            (Some(Geoid::Place(_, _)), Some(GT::CountySubdivision)) => Err(String::from(
                "cannot append a 'CountySubdivision' wildcard to a Place Geoid",
            )),
            (Some(Geoid::Place(_, _)), Some(GT::CensusTract)) => Err(String::from(
                "cannot append a 'CensusTract' wildcard to a Place Geoid",
            )),
            (Some(Geoid::Place(_, _)), Some(GT::BlockGroup)) => Err(String::from(
                "cannot append a 'BlockGroup' wildcard to a Place Geoid",
            )),
            (Some(Geoid::CensusTract(_, _, _)), Some(GT::CountySubdivision)) => Err(String::from(
                "cannot append a 'CountySubdivision' wildcard to a CensusTract Geoid",
            )),
            (Some(Geoid::CensusTract(_, _, _)), Some(GT::Place)) => Err(String::from(
                "cannot append a 'Place' wildcard to a CensusTract Geoid",
            )),
            (Some(Geoid::BlockGroup(_, _, _, _)), Some(GT::CountySubdivision)) => Err(
                String::from("cannot append a 'CountySubdivision' wildcard to a BlockGroup Geoid"),
            ),
            (Some(Geoid::BlockGroup(_, _, _, _)), Some(GT::Place)) => Err(String::from(
                "cannot append a 'Place' wildcard to a BlockGroup Geoid",
            )),

            // ~~ wildcard-only queries for different GEOID levels ~~
            (None, Some(GT::State)) => Ok(AcsGeoidQuery::State(None)),
            (None, Some(GT::County)) => Ok(AcsGeoidQuery::County(None, None)),
            (None, Some(GT::CountySubdivision)) => {
                Ok(AcsGeoidQuery::CountySubdivision(None, None, None))
            }
            (None, Some(GT::Place)) => Ok(AcsGeoidQuery::Place(None, None)),
            (None, Some(GT::CensusTract)) => Ok(AcsGeoidQuery::CensusTract(None, None, None)),
            (None, Some(GT::BlockGroup)) => Ok(AcsGeoidQuery::BlockGroup(None, None, None, None)),

            // ~~ queries for wildcards inserted into specific geoids ~~
            // - STATE -
            (Some(Geoid::State(_)), Some(GT::State)) => Ok(AcsGeoidQuery::State(None)),
            (Some(Geoid::State(s)), Some(GT::County)) => Ok(AcsGeoidQuery::County(Some(*s), None)),
            (Some(Geoid::State(s)), Some(GT::CountySubdivision)) => {
                Ok(AcsGeoidQuery::CountySubdivision(Some(*s), None, None))
            }
            (Some(Geoid::State(s)), Some(GT::Place)) => Ok(AcsGeoidQuery::Place(Some(*s), None)),
            (Some(Geoid::State(s)), Some(GT::CensusTract)) => {
                Ok(AcsGeoidQuery::CensusTract(Some(*s), None, None))
            }
            (Some(Geoid::State(s)), Some(GT::BlockGroup)) => {
                Ok(AcsGeoidQuery::BlockGroup(Some(*s), None, None, None))
            }

            // - COUNTY -
            (Some(Geoid::County(_, c)), Some(GT::State)) => {
                Ok(AcsGeoidQuery::County(None, Some(*c)))
            }
            (Some(Geoid::County(s, _)), Some(GT::County)) => {
                Ok(AcsGeoidQuery::County(Some(*s), None))
            }
            (Some(Geoid::County(s, c)), Some(GT::CountySubdivision)) => {
                Ok(AcsGeoidQuery::CountySubdivision(Some(*s), Some(*c), None))
            }
            (Some(Geoid::County(s, c)), Some(GT::CensusTract)) => {
                Ok(AcsGeoidQuery::CensusTract(Some(*s), Some(*c), None))
            }
            (Some(Geoid::County(s, c)), Some(GT::BlockGroup)) => {
                Ok(AcsGeoidQuery::BlockGroup(Some(*s), Some(*c), None, None))
            }

            // - COUNTY SUBDIVISION -
            (Some(G::CountySubdivision(_, ct, cs)), Some(GT::State)) => {
                Ok(AcsGeoidQuery::CountySubdivision(None, Some(*ct), Some(*cs)))
            }
            (Some(G::CountySubdivision(s, _, cs)), Some(GT::County)) => {
                Ok(AcsGeoidQuery::CountySubdivision(Some(*s), None, Some(*cs)))
            }
            (Some(G::CountySubdivision(s, ct, _)), Some(GT::CountySubdivision)) => {
                Ok(AcsGeoidQuery::CountySubdivision(Some(*s), Some(*ct), None))
            }

            // - PLACE -
            (Some(Geoid::Place(_, p)), Some(GT::State)) => Ok(AcsGeoidQuery::Place(None, Some(*p))),
            (Some(Geoid::Place(s, _)), Some(GT::Place)) => Ok(AcsGeoidQuery::Place(Some(*s), None)),

            // - CENSUS TRACT -
            (Some(Geoid::CensusTract(_, c, t)), Some(GT::State)) => {
                Ok(AcsGeoidQuery::CensusTract(None, Some(*c), Some(*t)))
            }
            (Some(Geoid::CensusTract(s, _, t)), Some(GT::County)) => {
                Ok(AcsGeoidQuery::CensusTract(Some(*s), None, Some(*t)))
            }
            (Some(Geoid::CensusTract(s, c, _)), Some(GT::CensusTract)) => {
                Ok(AcsGeoidQuery::CensusTract(Some(*s), Some(*c), None))
            }
            (Some(Geoid::CensusTract(s, c, t)), Some(GT::BlockGroup)) => Ok(
                AcsGeoidQuery::BlockGroup(Some(*s), Some(*c), Some(*t), None),
            ),

            // - BLOCK GROUP -
            (Some(Geoid::BlockGroup(_, c, t, b)), Some(GT::State)) => Ok(
                AcsGeoidQuery::BlockGroup(None, Some(*c), Some(*t), Some(*b)),
            ),
            (Some(Geoid::BlockGroup(s, _, t, b)), Some(GT::County)) => Ok(
                AcsGeoidQuery::BlockGroup(Some(*s), None, Some(*t), Some(*b)),
            ),
            (Some(Geoid::BlockGroup(s, c, _, b)), Some(GT::CensusTract)) => Ok(
                AcsGeoidQuery::BlockGroup(Some(*s), Some(*c), None, Some(*b)),
            ),
            (Some(Geoid::BlockGroup(s, c, t, _)), Some(GT::BlockGroup)) => Ok(
                AcsGeoidQuery::BlockGroup(Some(*s), Some(*c), Some(*t), None),
            ),

            // ~~ queries for specific geoids (no wildcards) ~~
            (Some(Geoid::State(s)), None) => Ok(AcsGeoidQuery::State(Some(*s))),
            (Some(Geoid::County(s, c)), None) => Ok(AcsGeoidQuery::County(Some(*s), Some(*c))),
            (Some(Geoid::CountySubdivision(s, ct, cs)), None) => Ok(
                AcsGeoidQuery::CountySubdivision(Some(*s), Some(*ct), Some(*cs)),
            ),
            (Some(Geoid::Place(s, p)), None) => Ok(AcsGeoidQuery::Place(Some(*s), Some(*p))),
            (Some(Geoid::CensusTract(s, c, t)), None) => {
                Ok(AcsGeoidQuery::CensusTract(Some(*s), Some(*c), Some(*t)))
            }
            (Some(Geoid::BlockGroup(s, c, t, b)), None) => Ok(AcsGeoidQuery::BlockGroup(
                Some(*s),
                Some(*c),
                Some(*t),
                Some(*b),
            )),
        }
    }

    const FOR_PREFIX: &'static str = "&for=";
    const IN_PREFIX: &'static str = "&in=";

    /// a query key for a unique data row in the census API. depending on the AcsGeoidQuery
    /// and the presence/absence of FIPS values, wildcards ("*") will be inserted at any level.
    pub fn to_query_key(&self) -> String {
        use AcsGeoidQuery as G;
        match self {
            // &for=state:*
            // &for=state:06
            G::State(state) => format!("{}state:{}", G::FOR_PREFIX, unpack_wildcard(*state)),
            // &for=county:*
            // &for=county:*&in=state:*
            // &for=county:037&in=state:06
            G::County(state, county) => format!(
                "{}county:{}{}",
                G::FOR_PREFIX,
                unpack_wildcard(*county),
                unpack_optional(*state, G::IN_PREFIX),
            ),
            // &for=county%20subdivision:*&in=state:48
            // &for=county%20subdivision:*&in=state:48&in=county:*
            // &for=county%20subdivision:91835&in=state:48%20county:201
            G::CountySubdivision(state, county, cousub) => format!(
                "{}county%20subdivision:{}{}state:{}{}",
                G::FOR_PREFIX,
                unpack_wildcard(*cousub),
                G::IN_PREFIX,
                unpack_wildcard(*state),
                unpack_optional(*county, G::IN_PREFIX),
            ),
            G::Place(state, place) => format!(
                "for=place:{}{}",
                unpack_wildcard(*place),
                unpack_optional(*state, G::IN_PREFIX),
            ),
            // &for=tract:*&in=state:06
            // &for=tract:*&in=state:06&in=county:*
            // &for=tract:018700&in=state:06%20county:073
            G::CensusTract(state, county, tract) => format!(
                "{}tract:{}{}state:{}&county:{}",
                G::FOR_PREFIX,
                unpack_wildcard(*tract),
                G::IN_PREFIX,
                unpack_wildcard(*state),
                unpack_optional(*county, G::IN_PREFIX),
            ),
            // &for=block%20group:*&in=state:06%20county:073
            // &for=block%20group:*&in=state:06&in=county:*&in=tract:*
            // &for=block%20group:*&in=state:06&in=county:073&in=tract:*
            // &for=block%20group:1&in=state:06%20county:073%20tract:018700
            G::BlockGroup(state, county, tract, block_group) => {
                let tract_safe = if county.is_none() { None } else { *tract };
                format!(
                    "{}block%20group:{}{}state:{}{}{}",
                    G::FOR_PREFIX,
                    unpack_wildcard(*block_group),
                    G::IN_PREFIX,
                    unpack_wildcard(*state),
                    unpack_optional(*county, G::IN_PREFIX),
                    unpack_optional(tract_safe, G::IN_PREFIX),
                )
            }
        }
    }

    pub fn response_geoid_type(&self) -> GeoidType {
        use AcsGeoidQuery as G;
        match self {
            G::State(_) => GeoidType::State,
            G::County(_, _) => GeoidType::County,
            G::CountySubdivision(_, _, _) => GeoidType::CountySubdivision,
            G::Place(_, _) => GeoidType::Place,
            G::CensusTract(_, _, _) => GeoidType::CensusTract,
            G::BlockGroup(_, _, _, _) => GeoidType::BlockGroup,
        }
    }

    pub fn response_column_names(&self) -> Vec<String> {
        use AcsGeoidQuery as G;
        match self {
            G::State(_) => vec![String::from("state")],
            G::County(_, _) => vec![String::from("state"), String::from("county")],
            G::CountySubdivision(_, _, _) => vec![
                String::from("state"),
                String::from("county"),
                String::from("county subdivision"),
            ],
            G::Place(_, _) => vec![String::from("state"), String::from("place")],
            G::CensusTract(_, _, _) => vec![
                String::from("state"),
                String::from("county"),
                String::from("tract"),
            ],
            G::BlockGroup(_, _, _, _) => vec![
                String::from("state"),
                String::from("county"),
                String::from("tract"),
                String::from("block group"),
            ],
        }
    }

    pub fn response_column_count(&self) -> usize {
        match self {
            AcsGeoidQuery::State(_) => 1,
            AcsGeoidQuery::County(_, _) => 2,
            AcsGeoidQuery::CountySubdivision(_, _, _) => 3,
            AcsGeoidQuery::Place(_, _) => 2,
            AcsGeoidQuery::CensusTract(_, _, _) => 3,
            AcsGeoidQuery::BlockGroup(_, _, _, _) => 4,
        }
    }

    /// builds a function that unpacks the ACS query values representing a geoid.
    /// these return as values in an array of different lengths, depending on the scope of
    /// the original query.
    pub fn build_deserialize_geoid_fn(&self) -> DeserializeGeoidFn {
        match self {
            AcsGeoidQuery::State(_) => {
                let f = |vals| {
                    let arr = as_usizes(vals)?;
                    if arr.len() != 1 {
                        Err(format!(
                            "for state-level query, expected 1 geoid column, found: {}",
                            arr.into_iter().join(",")
                        ))
                    } else {
                        Ok(Geoid::State(fips::State(arr[0])))
                    }
                };
                Rc::new(f)
            }
            AcsGeoidQuery::County(_, _) => {
                let f = |vals| {
                    let arr = as_usizes(vals)?;
                    if arr.len() != 2 {
                        Err(format!(
                            "for county-level query, expected 2 geoid columns, found: {}",
                            arr.into_iter().join(",")
                        ))
                    } else {
                        Ok(Geoid::County(fips::State(arr[0]), fips::County(arr[1])))
                    }
                };
                Rc::new(f)
            }
            AcsGeoidQuery::CountySubdivision(_, _, _) => {
                let f = |vals| {
                    let arr = as_usizes(vals)?;
                    if arr.len() != 3 {
                        Err(format!(
                            "for county subdivision-level query, expected 3 geoid columns, found: {}",
                            arr.into_iter().join(",")
                        ))
                    } else {
                        Ok(Geoid::CountySubdivision(
                            fips::State(arr[0]),
                            fips::County(arr[1]),
                            fips::CountySubdivision(arr[2]),
                        ))
                    }
                };
                Rc::new(f)
            }
            AcsGeoidQuery::Place(_, _) => {
                let f = |vals| {
                    let arr = as_usizes(vals)?;
                    if arr.len() != 2 {
                        Err(format!(
                            "for place-level query, expected 2 geoid columns, found: {}",
                            arr.into_iter().join(",")
                        ))
                    } else {
                        Ok(Geoid::Place(fips::State(arr[0]), fips::Place(arr[1])))
                    }
                };
                Rc::new(f)
            }
            AcsGeoidQuery::CensusTract(_, _, _) => {
                let f = |vals| {
                    let arr = as_usizes(vals)?;
                    if arr.len() != 3 {
                        Err(format!(
                            "for census tract-level query, expected 3 geoid column, found: {}",
                            arr.into_iter().join(",")
                        ))
                    } else {
                        Ok(Geoid::CensusTract(
                            fips::State(arr[0]),
                            fips::County(arr[1]),
                            fips::CensusTract(arr[2]),
                        ))
                    }
                };
                Rc::new(f)
            }
            AcsGeoidQuery::BlockGroup(_, _, _, _) => {
                let f = |vals| {
                    let arr = as_usizes(vals)?;
                    if arr.len() != 4 {
                        Err(format!(
                            "for block group-level query, expected 4 geoid columns, found: {}",
                            arr.into_iter().join(",")
                        ))
                    } else {
                        Ok(Geoid::BlockGroup(
                            fips::State(arr[0]),
                            fips::County(arr[1]),
                            fips::CensusTract(arr[2]),
                            fips::BlockGroup(arr[3]),
                        ))
                    }
                };
                Rc::new(f)
            }
        }
    }
}

fn as_usizes(arr: Vec<serde_json::Value>) -> Result<Vec<u64>, String> {
    arr.iter()
        .map(|v| {
            let v_str = v
                .as_str()
                .ok_or_else(|| format!("raw geoid value should be string, found {}", v))?;
            let v_u64 = v_str.parse::<u64>().map_err(|e| {
                format!(
                    "raw geoid value should be a string wrapping an integer, found {}. error: {}",
                    v_str, e
                )
            })?;
            Ok(v_u64)
        })
        .collect::<Result<Vec<u64>, String>>()
}

fn unpack_optional<T: HasGeoidString + HasGeoidType>(value: Option<T>, prefix: &str) -> String {
    match value {
        Some(v) => {
            format!(
                "{}{}:{}",
                prefix,
                v.geoid_type().to_string(),
                v.geoid_string()
            )
        }
        None => String::from(""),
    }
}

/// helper to transform a "None" GeoidString'd FIPS identifier into a wildcard ("*")
/// see [`fips`] for possible (expected) values of type GeoidString
fn unpack_wildcard<T: HasGeoidString>(value: Option<T>) -> String {
    value.map_or_else(|| String::from("*"), |v| v.geoid_string())
}
