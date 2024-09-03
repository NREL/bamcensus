use std::fmt::Display;

use super::WacSegment;

#[derive(Debug)]
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
