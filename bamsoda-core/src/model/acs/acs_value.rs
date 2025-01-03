use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcsValue {
    pub name: String,
    pub value: serde_json::Value,
}

impl AcsValue {
    pub fn new(name: String, value: serde_json::Value) -> AcsValue {
        AcsValue { name, value }
    }

    /// to numeric operation.
    ///
    /// # Background
    ///
    /// `serde_json::Value.as_f64()` is for values that are actual JSON number types, but when
    /// dealing with ACS values, they are always string types, and so the built-in `as_f64()`
    /// method gives the following error, for example:
    ///
    ///   > cannot parse acs value for B01001_001E as number, found \"3889\""
    ///
    /// this method also ensures we are dealing with a trimmed string before attempting to parse it as a number.
    pub fn as_f64_safe(&self) -> Result<f64, String> {
        let value_str = self
            .value
            .as_str()
            .ok_or_else(|| format!("failed to decode value as string: {}", self.value))?;
        value_str
            .trim()
            .parse::<f64>()
            .map_err(|e| format!("failed to decode value as f64: {}", e))
    }
}

impl Display for AcsValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.name, self.value)
    }
}
