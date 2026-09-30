use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RoutePlannerStatus {
    pub class: Option<String>,
    pub details: Option<RoutePlannerDetails>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RoutePlannerDetails {
    pub ip_block: Option<IpBlock>,
    #[serde(default)]
    pub failing_addresses: Vec<FailingAddress>,
    pub rotate_index: Option<String>,
    pub ip_index: Option<String>,
    pub current_address: Option<String>,
    pub current_address_index: Option<String>,
    pub block_index: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IpBlock {
    #[serde(rename = "type")]
    pub kind: String,
    pub size: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FailingAddress {
    pub failing_address: String,
    pub failing_timestamp: u64,
    pub failing_time: String,
}
