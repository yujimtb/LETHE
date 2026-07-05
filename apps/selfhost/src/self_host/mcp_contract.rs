use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct SearchLakeArguments {
    pub query: String,
    #[serde(default)]
    pub source_types: Vec<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetRecordArguments {
    pub record_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GetThreadArguments {
    pub record_id: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ClaimQueueArguments {
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub verification_mode: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SearchDecisionsArguments {
    pub query: String,
    #[serde(default)]
    pub limit: Option<usize>,
}
