use reqwest::blocking::Client;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use serde::Deserialize;

use crate::adapter::error::AdapterError;
use crate::adapter::slack::client::{
    SlackChannelSnapshot, SlackClient, SlackEdited, SlackFile, SlackHistoryPage, SlackMessage,
    SlackMessageType, SlackReaction,
};

#[derive(Clone)]
pub struct HttpSlackClient {
    http: Client,
    token: String,
}

impl HttpSlackClient {
    pub fn new(token: impl Into<String>) -> Result<Self, AdapterError> {
        let token = token.into();
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).map_err(|err| {
                AdapterError::AuthFailure {
                    message: err.to_string(),
                }
            })?,
        );

        let http = Client::builder()
            .default_headers(headers)
            .build()
            .map_err(|err| AdapterError::Network {
                message: err.to_string(),
            })?;

        Ok(Self { http, token })
    }

    fn get_json<T: for<'de> Deserialize<'de>>(
        &self,
        endpoint: &str,
        query: &[(&str, &str)],
    ) -> Result<T, AdapterError> {
        let response = self
            .http
            .get(format!("https://slack.com/api/{endpoint}"))
            .query(query)
            .send()
            .map_err(|err| AdapterError::Network {
                message: err.to_string(),
            })?;

        let status = response.status();

        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after_secs = response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse().ok())
                .unwrap_or(30);
            return Err(AdapterError::RateLimited { retry_after_secs });
        }

        let body = response.text().map_err(|err| AdapterError::Network {
            message: err.to_string(),
        })?;

        if !status.is_success() {
            return Err(AdapterError::MalformedResponse {
                message: format!("slack api {endpoint} returned {status}: {body}"),
            });
        }

        serde_json::from_str::<T>(&body).map_err(|err| AdapterError::MalformedResponse {
            message: format!("slack api {endpoint} decode error: {err}; body: {body}"),
        })
    }

    fn conversation_name(&self, channel_id: &str) -> Result<String, AdapterError> {
        Ok(self.conversations_info(channel_id)?.channel_name)
    }

    fn user_profiles(
        &self,
        user_ids: &[String],
    ) -> Result<std::collections::HashMap<String, SlackUser>, AdapterError> {
        let mut users = std::collections::HashMap::new();
        for user_id in user_ids {
            if users.contains_key(user_id) {
                continue;
            }
            let response: UsersInfoResponse = self.get_json("users.info", &[("user", user_id)])?;
            if !response.ok {
                return Err(map_slack_error(response.error));
            }
            if let Some(user) = response.user {
                users.insert(user.id.clone(), user);
            }
        }
        Ok(users)
    }
}

impl SlackClient for HttpSlackClient {
    fn conversations_history(
        &self,
        channel_id: &str,
        oldest: Option<&str>,
        cursor: Option<&str>,
        limit: u32,
    ) -> Result<SlackHistoryPage, AdapterError> {
        let mut query = vec![
            ("channel", channel_id),
            ("limit", Box::leak(limit.to_string().into_boxed_str())),
        ];
        if let Some(oldest) = oldest {
            query.push(("oldest", oldest));
        }
        if let Some(cursor) = cursor {
            query.push(("cursor", cursor));
        }

        let response: ConversationsHistoryResponse =
            self.get_json("conversations.history", &query)?;
        if !response.ok {
            return Err(map_slack_error(response.error));
        }

        let channel_name = self.conversation_name(channel_id)?;
        let user_ids: Vec<String> = response
            .messages
            .iter()
            .filter_map(|message| message.user.clone())
            .collect();
        let users = self.user_profiles(&user_ids)?;

        Ok(SlackHistoryPage {
            messages: response
                .messages
                .into_iter()
                .map(|message| to_slack_message(message, &channel_name, &users))
                .collect(),
            has_more: response.has_more,
            next_cursor: response.response_metadata.and_then(|meta| meta.next_cursor),
        })
    }

    fn conversations_replies(
        &self,
        channel_id: &str,
        thread_ts: &str,
        oldest: Option<&str>,
    ) -> Result<Vec<SlackMessage>, AdapterError> {
        let channel_name = self.conversation_name(channel_id)?;
        let mut messages = Vec::new();
        let mut cursor: Option<String> = None;

        loop {
            let mut query = vec![("channel", channel_id), ("ts", thread_ts)];
            if let Some(oldest) = oldest {
                query.push(("oldest", oldest));
            }
            if let Some(cursor_value) = cursor.as_deref() {
                query.push(("cursor", cursor_value));
            }
            let response: ConversationsHistoryResponse =
                self.get_json("conversations.replies", &query)?;
            if !response.ok {
                return Err(map_slack_error(response.error));
            }

            let user_ids: Vec<String> = response
                .messages
                .iter()
                .filter_map(|message| message.user.clone())
                .collect();
            let users = self.user_profiles(&user_ids)?;
            messages.extend(
                response
                    .messages
                    .into_iter()
                    .map(|message| to_slack_message(message, &channel_name, &users)),
            );

            cursor = response.response_metadata.and_then(|meta| meta.next_cursor);
            if cursor.as_deref().is_none_or(str::is_empty) {
                break;
            }
        }

        Ok(messages)
    }

    fn conversations_info(&self, channel_id: &str) -> Result<SlackChannelSnapshot, AdapterError> {
        let response: ConversationsInfoResponse =
            self.get_json("conversations.info", &[("channel", channel_id)])?;
        if !response.ok {
            return Err(map_slack_error(response.error));
        }

        let channel = response
            .channel
            .ok_or_else(|| AdapterError::MalformedResponse {
                message: "missing channel payload".to_string(),
            })?;

        Ok(SlackChannelSnapshot {
            channel_id: channel.id,
            channel_name: channel.name,
            purpose: channel.purpose.and_then(|value| value.value),
            topic: channel.topic.and_then(|value| value.value),
            member_count: channel.num_members.unwrap_or(0),
            members: channel.members.unwrap_or_default(),
            is_archived: channel.is_archived.unwrap_or(false),
            snapshot_at: chrono::Utc::now(),
        })
    }

    fn file_download(&self, file: &SlackFile) -> Result<Vec<u8>, AdapterError> {
        let download_url =
            file.download_url
                .as_deref()
                .ok_or_else(|| AdapterError::MalformedResponse {
                    message: format!("slack file {} missing download url", file.id),
                })?;
        let response = self
            .http
            .get(download_url)
            .header(AUTHORIZATION, format!("Bearer {}", self.token))
            .send()
            .map_err(|err| AdapterError::Network {
                message: err.to_string(),
            })?;

        let status = response.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let retry_after_secs = response
                .headers()
                .get("retry-after")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse().ok())
                .unwrap_or(30);
            return Err(AdapterError::RateLimited { retry_after_secs });
        }
        if !status.is_success() {
            let body = response.text().unwrap_or_default();
            return Err(AdapterError::MalformedResponse {
                message: format!("slack file download returned {status}: {body}"),
            });
        }

        response
            .bytes()
            .map(|body| body.to_vec())
            .map_err(|err| AdapterError::Network {
                message: err.to_string(),
            })
    }
}

#[derive(Debug, Deserialize)]
struct ConversationsHistoryResponse {
    ok: bool,
    error: Option<String>,
    #[serde(default)]
    messages: Vec<RawSlackMessage>,
    #[serde(default)]
    has_more: bool,
    response_metadata: Option<ResponseMetadata>,
}

#[derive(Debug, Deserialize)]
struct ConversationsInfoResponse {
    ok: bool,
    error: Option<String>,
    channel: Option<RawSlackChannel>,
}

#[derive(Debug, Deserialize)]
struct UsersInfoResponse {
    ok: bool,
    error: Option<String>,
    user: Option<SlackUser>,
}

#[derive(Debug, Deserialize)]
struct ResponseMetadata {
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawSlackMessage {
    ts: String,
    #[serde(default)]
    thread_ts: Option<String>,
    #[serde(default)]
    user: Option<String>,
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    subtype: Option<String>,
    #[serde(default)]
    edited: Option<RawSlackEdited>,
    #[serde(default)]
    reactions: Vec<RawSlackReaction>,
    #[serde(default)]
    files: Vec<RawSlackFile>,
    #[serde(default)]
    reply_count: Option<u32>,
    #[serde(default)]
    reply_users_count: Option<u32>,
    #[serde(default)]
    user_profile: Option<RawSlackUserProfile>,
}

#[derive(Debug, Deserialize)]
struct RawSlackEdited {
    user: String,
    ts: String,
}

#[derive(Debug, Deserialize)]
struct RawSlackReaction {
    name: String,
    #[serde(default)]
    count: u32,
    #[serde(default)]
    users: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RawSlackFile {
    #[serde(default)]
    id: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    mimetype: String,
    #[serde(default)]
    size: u64,
    #[serde(default)]
    url_private: Option<String>,
    #[serde(default)]
    url_private_download: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawSlackChannel {
    id: String,
    name: String,
    purpose: Option<RawSlackTopic>,
    topic: Option<RawSlackTopic>,
    num_members: Option<u32>,
    members: Option<Vec<String>>,
    is_archived: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct RawSlackTopic {
    value: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct SlackUser {
    id: String,
    #[serde(default)]
    profile: SlackUserProfile,
    #[serde(default)]
    real_name: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
struct SlackUserProfile {
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    real_name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RawSlackUserProfile {
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    real_name: Option<String>,
}

fn map_slack_error(error: Option<String>) -> AdapterError {
    match error.as_deref() {
        Some("invalid_auth") | Some("not_authed") | Some("account_inactive") => {
            AdapterError::AuthFailure {
                message: error.unwrap_or_else(|| "slack auth failure".to_string()),
            }
        }
        Some(other) => AdapterError::Other(other.to_string()),
        None => AdapterError::Other("unknown slack error".to_string()),
    }
}

fn to_slack_message(
    raw: RawSlackMessage,
    channel_name: &str,
    users: &std::collections::HashMap<String, SlackUser>,
) -> SlackMessage {
    let user_id = raw.user.unwrap_or_else(|| "unknown".to_string());
    let user = users.get(&user_id);
    let profile = user.map(|user| &user.profile);
    let fallback_profile = raw.user_profile.as_ref();
    let user_name = profile
        .and_then(|profile| profile.display_name.clone())
        .or_else(|| profile.and_then(|profile| profile.real_name.clone()))
        .or_else(|| user.and_then(|user| user.real_name.clone()))
        .or_else(|| fallback_profile.and_then(|profile| profile.display_name.clone()))
        .or_else(|| fallback_profile.and_then(|profile| profile.real_name.clone()))
        .unwrap_or_else(|| user_id.clone());

    SlackMessage {
        channel_id: String::new(),
        channel_name: channel_name.to_string(),
        ts: raw.ts,
        thread_ts: raw.thread_ts,
        user_id,
        user_name,
        email: profile.and_then(|profile| profile.email.clone()),
        text: raw.text.unwrap_or_default(),
        message_type: map_message_type(raw.subtype.as_deref()),
        edited: raw.edited.map(|edited| SlackEdited {
            user: edited.user,
            ts: edited.ts,
        }),
        reactions: raw
            .reactions
            .into_iter()
            .map(|reaction| SlackReaction {
                name: reaction.name,
                count: reaction.count,
                users: reaction.users,
            })
            .collect(),
        files: raw
            .files
            .into_iter()
            .map(|file| SlackFile {
                id: file.id,
                name: file.name,
                mimetype: file.mimetype,
                size: file.size,
                download_url: file.url_private_download.or(file.url_private),
                blob_ref: None,
            })
            .collect(),
        reply_count: raw.reply_count.unwrap_or(0),
        reply_users_count: raw.reply_users_count.unwrap_or(0),
    }
}

fn map_message_type(subtype: Option<&str>) -> SlackMessageType {
    match subtype {
        Some("message_changed") => SlackMessageType::Edit,
        Some("message_deleted") => SlackMessageType::Delete,
        Some("file_share") => SlackMessageType::FileShare,
        Some("channel_join") => SlackMessageType::ChannelJoin,
        Some("channel_leave") => SlackMessageType::ChannelLeave,
        _ => SlackMessageType::Message,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    #[test]
    fn to_slack_message_preserves_download_url() {
        let raw = RawSlackMessage {
            ts: "1234567890.123456".into(),
            thread_ts: None,
            user: Some("U123".into()),
            text: Some("hello".into()),
            subtype: Some("file_share".into()),
            edited: None,
            reactions: vec![],
            files: vec![RawSlackFile {
                id: "F123".into(),
                name: "photo.jpg".into(),
                mimetype: "image/jpeg".into(),
                size: 42,
                url_private: None,
                url_private_download: Some("https://files.slack.test/F123".into()),
            }],
            reply_count: Some(0),
            reply_users_count: Some(0),
            user_profile: None,
        };

        let msg = to_slack_message(raw, "general", &HashMap::new());
        assert_eq!(msg.message_type, SlackMessageType::FileShare);
        assert_eq!(
            msg.files[0].download_url.as_deref(),
            Some("https://files.slack.test/F123")
        );
    }
}
