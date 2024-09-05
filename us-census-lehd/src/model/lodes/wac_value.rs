use super::WacSegment;
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct WacValue {
    pub segment: WacSegment,
    pub value: f64,
}

impl WacValue {
    pub fn new(segment: WacSegment, value: f64) -> WacValue {
        WacValue { segment, value }
    }
}

impl Display for WacValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} ({}) = {}",
            self.segment,
            self.segment.description(),
            self.value,
        )
    }
}
