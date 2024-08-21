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
