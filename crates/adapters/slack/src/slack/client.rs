//! M10 — Slack API client abstraction
//!
//! The trait is implemented by fixture stubs in tests and by a real
//! HTTP client in production.  The adapter never talks to the network
//! directly.

use serde::{Deserialize, Serialize};

use lethe_adapter_api::error::AdapterError;

/// A single Slack message (or event) as returned by the API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlackMessage {
    pub channel_id: String,
    pub channel_name: String,
    pub ts: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_ts: Option<String>,
    pub user_id: String,
    pub user_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    pub text: String,
    pub message_type: SlackMessageType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub edited: Option<SlackEdited>,
    #[serde(default)]
    pub reactions: Vec<SlackReaction>,
    #[serde(default)]
    pub files: Vec<SlackFile>,
    #[serde(default)]
    pub reply_count: u32,
    #[serde(default)]
    pub reply_users_count: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlackMessageType {
    Message,
    Edit,
    Delete,
    ReactionAdd,
    ReactionRemove,
    FileShare,
    ChannelJoin,
    ChannelLeave,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlackEdited {
    pub user: String,
    pub ts: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlackReaction {
    pub name: String,
    pub count: u32,
    pub users: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlackFile {
    pub id: String,
    pub name: String,
    pub mimetype: String,
    pub size: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub download_url: Option<String>,
    /// Set after blob upload.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob_ref: Option<String>,
}

/// Channel snapshot metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlackChannelSnapshot {
    pub channel_id: String,
    pub channel_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub purpose: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    pub member_count: u32,
    #[serde(default)]
    pub members: Vec<String>,
    pub is_archived: bool,
    pub snapshot_at: chrono::DateTime<chrono::Utc>,
}

/// Paginated response from Slack API.
#[derive(Debug, Clone)]
pub struct SlackHistoryPage {
    pub messages: Vec<SlackMessage>,
    pub has_more: bool,
    pub next_cursor: Option<String>,
}

/// Abstraction over Slack Web API.
///
/// Implementations:
/// - `FixtureSlackClient` for tests
/// - (future) `HttpSlackClient` for production
pub trait SlackClient {
    /// Fetch channel history (paginated).
    fn conversations_history(
        &self,
        channel_id: &str,
        oldest: Option<&str>,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<SlackHistoryPage, AdapterError>;

    /// Fetch thread replies.
    fn conversations_replies(
        &self,
        channel_id: &str,
        thread_ts: &str,
        oldest: Option<&str>,
    ) -> Result<Vec<SlackMessage>, AdapterError>;

    /// Fetch channel info (for snapshots).
    fn conversations_info(&self, channel_id: &str) -> Result<SlackChannelSnapshot, AdapterError>;

    /// Download file content.
    fn file_download(&self, file: &SlackFile) -> Result<Vec<u8>, AdapterError>;
}

// ---------------------------------------------------------------------------
// Fixture client for testing
// ---------------------------------------------------------------------------

/// A test double that returns pre-loaded data.
#[derive(Debug, Default)]
pub struct FixtureSlackClient {
    pub history_pages: Vec<SlackHistoryPage>,
    pub replies: std::collections::HashMap<String, Vec<SlackMessage>>,
    pub channels: std::collections::HashMap<String, SlackChannelSnapshot>,
    pub files: std::collections::HashMap<String, Vec<u8>>,
    page_index: std::cell::Cell<usize>,
}

impl FixtureSlackClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_history(mut self, pages: Vec<SlackHistoryPage>) -> Self {
        self.history_pages = pages;
        self
    }

    pub fn with_channel(mut self, id: &str, snapshot: SlackChannelSnapshot) -> Self {
        self.channels.insert(id.to_string(), snapshot);
        self
    }

    pub fn with_file(mut self, file_id: &str, data: Vec<u8>) -> Self {
        self.files.insert(file_id.to_string(), data);
        self
    }
}

impl SlackClient for FixtureSlackClient {
    fn conversations_history(
        &self,
        _channel_id: &str,
        _oldest: Option<&str>,
        _cursor: Option<&str>,
        _limit: u32,
    ) -> Result<SlackHistoryPage, AdapterError> {
        let idx = self.page_index.get();
        if idx < self.history_pages.len() {
            self.page_index.set(idx + 1);
            Ok(self.history_pages[idx].clone())
        } else {
            Ok(SlackHistoryPage {
                messages: vec![],
                has_more: false,
                next_cursor: None,
            })
        }
    }

    fn conversations_replies(
        &self,
        _channel_id: &str,
        thread_ts: &str,
        oldest: Option<&str>,
    ) -> Result<Vec<SlackMessage>, AdapterError> {
        let mut replies = self.replies.get(thread_ts).cloned().unwrap_or_default();
        if let Some(oldest) = oldest {
            let oldest = slack_ts_value(oldest)?;
            let mut filtered = Vec::with_capacity(replies.len());
            for reply in replies {
                if slack_ts_value(&reply.ts)? > oldest {
                    filtered.push(reply);
                }
            }
            replies = filtered;
        }
        Ok(replies)
    }

    fn conversations_info(&self, channel_id: &str) -> Result<SlackChannelSnapshot, AdapterError> {
        self.channels
            .get(channel_id)
            .cloned()
            .ok_or_else(|| AdapterError::Other(format!("channel {channel_id} not found")))
    }

    fn file_download(&self, file: &SlackFile) -> Result<Vec<u8>, AdapterError> {
        self.files
            .get(&file.id)
            .cloned()
            .ok_or_else(|| AdapterError::Other(format!("file {} not found", file.id)))
    }
}

fn slack_ts_value(value: &str) -> Result<(i64, u32), AdapterError> {
    let (seconds, fractional) =
        value
            .split_once('.')
            .ok_or_else(|| AdapterError::MalformedResponse {
                message: format!("invalid Slack timestamp: {value}"),
            })?;
    if fractional.len() != 6 || !fractional.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(AdapterError::MalformedResponse {
            message: format!("invalid Slack timestamp: {value}"),
        });
    }
    let seconds = seconds
        .parse::<i64>()
        .map_err(|_| AdapterError::MalformedResponse {
            message: format!("invalid Slack timestamp: {value}"),
        })?;
    let micros = fractional
        .parse::<u32>()
        .map_err(|_| AdapterError::MalformedResponse {
            message: format!("invalid Slack timestamp: {value}"),
        })?;
    Ok((seconds, micros))
}
