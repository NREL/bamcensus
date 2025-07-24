pub mod constants;

mod acs_api_query_params;
mod acs_geoid_query;
mod acs_type;
mod acs_value;

pub use acs_api_query_params::AcsApiQueryParams;
pub use acs_geoid_query::AcsGeoidQuery;
pub use acs_type::AcsType;
pub use acs_value::AcsValue;

use bamcensus_core::model::identifier::Geoid;
use std::rc::Rc;
pub type DeserializeGeoidFn = Rc<dyn Fn(Vec<serde_json::Value>) -> Result<Geoid, String>>;
