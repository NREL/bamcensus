use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(Default, ValueEnum, Serialize, Deserialize, Clone, Copy, Debug)]
pub enum OdJobSegment {
    #[default]
    S000,
    SA01,
    SA02,
    SA03,
    SE01,
    SE02,
    SE03,
    SI01,
    SI02,
    SI03,
}
