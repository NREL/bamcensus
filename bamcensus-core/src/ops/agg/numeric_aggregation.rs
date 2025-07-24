use std::fmt::Display;

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, ValueEnum, Default)]
#[serde(rename_all = "snake_case")]
/// operations for aggregating a collection of numeric values
pub enum NumericAggregation {
    #[default]
    Sum,
    Mean,
}

impl NumericAggregation {
    pub fn aggregate(&self, values: &mut dyn Iterator<Item = f64>) -> f64 {
        use NumericAggregation as Fn;
        match self {
            Fn::Sum => values.fold(0.0, |acc, v| acc + v),
            Fn::Mean => {
                let (acc, n) = values.fold((0.0, 0.0), |(acc, n), v| (acc + v, n + 1.0));
                if n == 0.0 {
                    0.0
                } else {
                    acc / n
                }
            }
        }
    }
}

impl Display for NumericAggregation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NumericAggregation::Sum => write!(f, "sum"),
            NumericAggregation::Mean => write!(f, "mean"),
        }
    }
}
