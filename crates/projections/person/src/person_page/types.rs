//! M13: Person Page types — output tables and API response models.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use lethe_core::domain::EntityRef;
use lethe_profile_model::StudentProfile;

/// Person profile (person_profiles table, M13 §3.1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersonProfile {
    pub person_id: EntityRef,
    pub display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub self_intro_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub self_intro_slide_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub self_intro_thumbnail: Option<String>,
    pub identities: Vec<IdentityInfo>,
    pub source_count: usize,
    pub last_activity: Option<DateTime<Utc>>,
    pub profile_updated_at: DateTime<Utc>,
    #[serde(skip)]
    pub frontend_profile: Option<FrontendProfile>,
    #[serde(skip)]
    pub frontend_profile_created_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrontendProfile {
    pub source_document_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_canonical_uri: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thumbnail_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thumbnail_url: Option<String>,
    pub profile: StudentProfile,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityInfo {
    pub system: String,
    pub external_id: String,
}

/// Related slide (person_slides table, M13 §3.2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersonSlide {
    pub id: String,
    #[serde(skip)]
    pub source_observation_id: String,
    pub person_id: EntityRef,
    pub document_id: String,
    pub title: String,
    pub role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_seen_revision: Option<String>,
    pub slide_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thumbnail_ref: Option<String>,
    pub last_modified: Option<DateTime<Utc>>,
}

/// Related message (person_messages table, M13 §3.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersonMessage {
    pub id: String,
    #[serde(skip)]
    pub source_observation_id: String,
    pub person_id: EntityRef,
    pub channel: String,
    pub text: String,
    pub ts: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_ts: Option<String>,
    pub has_attachments: bool,
}

/// Activity summary (person_activity table, M13 §3.4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersonActivity {
    pub person_id: EntityRef,
    pub total_slides_related: usize,
    pub total_messages: usize,
    pub first_activity: Option<DateTime<Utc>>,
    pub last_activity: Option<DateTime<Utc>>,
    pub active_channels: Vec<String>,
}

/// Timeline event (M13 §4.4).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineEvent {
    pub event_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub document_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    pub ts: DateTime<Utc>,
}

/// Person detail response (M13 §4.2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersonDetailResponse {
    pub person_id: EntityRef,
    pub display_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub self_introduction: Option<SelfIntroduction>,
    pub identities: Vec<IdentityInfo>,
    pub related_slides: Vec<PersonSlide>,
    pub recent_messages: Vec<PersonMessage>,
    pub activity_summary: PersonActivity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelfIntroduction {
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slide_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thumbnail_url: Option<String>,
}

/// Person list item (M13 §4.3).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersonListItem {
    pub person_id: EntityRef,
    pub display_name: String,
    pub source_count: usize,
    pub total_slides: usize,
    pub total_messages: usize,
    pub last_activity: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thumbnail_url: Option<String>,
}

/// Complete output of the person page projector.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PersonPageOutput {
    pub profiles: Vec<PersonProfile>,
    pub slides: Vec<PersonSlide>,
    pub messages: Vec<PersonMessage>,
    pub activities: Vec<PersonActivity>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn person_profile_serializes() {
        let profile = PersonProfile {
            person_id: EntityRef::new("person:test-1"),
            display_name: "Test".into(),
            self_intro_text: None,
            self_intro_slide_id: None,
            self_intro_thumbnail: None,
            identities: vec![IdentityInfo {
                system: "slack".into(),
                external_id: "U123".into(),
            }],
            source_count: 1,
            last_activity: None,
            profile_updated_at: Utc::now(),
            frontend_profile: None,
            frontend_profile_created_at: None,
        };
        let json = serde_json::to_string(&profile).unwrap();
        assert!(json.contains("person:test-1"));
    }
}
