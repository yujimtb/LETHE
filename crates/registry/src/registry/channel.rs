use serde::{Deserialize, Serialize};

use lethe_core::domain::SourceSystemRef;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChannelKind {
    Slack,
    Gmail,
    Discord,
}

impl ChannelKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Slack => "slack",
            Self::Gmail => "gmail",
            Self::Discord => "discord",
        }
    }

    pub fn from_source_system(source: &SourceSystemRef) -> Option<Self> {
        match source.as_str() {
            "sys:slack" => Some(Self::Slack),
            "sys:gmail" => Some(Self::Gmail),
            "sys:discord" => Some(Self::Discord),
            _ => None,
        }
    }
}

impl std::fmt::Display for ChannelKind {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChannelRecord {
    pub id: String,
    pub kind: ChannelKind,
    pub source_instance_id: String,
    pub external_id: String,
    pub connection_ref: String,
    pub default_consent_scope: String,
    pub reply_slo_seconds: u64,
    pub freshness_threshold_seconds: u64,
    pub break_glass_channel: bool,
    pub break_glass_senders: Vec<String>,
    pub enabled: bool,
}

impl ChannelRecord {
    pub fn lookup_key(kind: ChannelKind, source_instance_id: &str, external_id: &str) -> String {
        format!("{}:{source_instance_id}:{external_id}", kind.as_str())
    }

    pub fn key(&self) -> String {
        Self::lookup_key(self.kind, &self.source_instance_id, &self.external_id)
    }
}
