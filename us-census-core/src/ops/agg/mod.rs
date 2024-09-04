pub mod aggregation_function;

// pub mod can_aggregate;
// use crate::model::identifier::{Geoid, GeoidType};
// pub use can_aggregate::CanAggregateAsNumber;

// type DataCollection = Vec<(Geoid, Vec<dyn CanAggregateAsNumber>)>;

// // todo! nope, if we do this, we lose information about what kind of value
// // was upstream. it's time to think more generally about the value type.
// pub type NumericAggregationFunction =
//     Box<dyn Fn(GeoidType, DataCollection) -> Result<DataCollection, String>>;
