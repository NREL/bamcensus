use std::fmt::Display;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcsValue {
    pub name: String,
    pub value: serde_json::Value,
}

impl AcsValue {
    pub fn new(name: String, value: serde_json::Value) -> AcsValue {
        AcsValue { name, value }
    }
}

impl Display for AcsValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.name, self.value)
    }
}
