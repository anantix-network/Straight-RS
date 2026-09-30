use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Common,
    Suspicious,
    Fault,
    #[serde(other)]
    Unknown,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Exception {
    #[serde(default)]
    pub message: Option<String>,
    pub severity: Severity,
    #[serde(default)]
    pub cause: String,
    #[serde(default)]
    pub cause_stack_trace: Option<String>,
}
