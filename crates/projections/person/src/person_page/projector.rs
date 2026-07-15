//! M13: Person Page Projector — builds person page data from identity resolution + lake.
//!
//! Input: M12 IdentityResolutionOutput + Lake observations.
//! Output: PersonPageOutput (profiles, slides, messages, activities).

use std::collections::HashMap;

use chrono::{DateTime, Utc};

use lethe_core::domain::{EntityRef, Observation, ObservationId, SchemaRef, SupplementalRecord};
use lethe_engine::identity::types::{IdentifierType, IdentityResolutionOutput, ResolvedPerson};
use lethe_policy::governance::types::ConsentStatus;
use lethe_profile_model::StudentProfile;

use super::types::*;

/// Person page projector — pure functional core.
pub struct PersonPageProjector;

pub const CONSENT_DECISION_SCHEMA: &str = "schema:consent-decision";

impl PersonPageProjector {
    pub fn project_frontend_profiles(
        identity: &IdentityResolutionOutput,
        observations: &[Observation],
        supplemental_records: &[&SupplementalRecord],
    ) -> HashMap<String, (DateTime<Utc>, FrontendProfile)> {
        let person_identifiers = Self::build_person_identifier_map(identity);
        Self::build_frontend_profile_map(observations, supplemental_records, &person_identifiers)
    }

    /// Build person page output from identity resolution and observations.
    ///
    /// Only uses `resolved_persons` with confirmed status (not pending candidates).
    pub fn project(
        identity: &IdentityResolutionOutput,
        observations: &[Observation],
        supplemental_records: &[&SupplementalRecord],
    ) -> PersonPageOutput {
        let mut profiles = Vec::new();
        let mut all_slides = Vec::new();
        let mut all_messages = Vec::new();
        let mut all_activities = Vec::new();

        // Build person-id → identifiers map for matching.
        let person_identifiers = Self::build_person_identifier_map(identity);
        let frontend_profiles =
            Self::project_frontend_profiles(identity, observations, supplemental_records);

        for person in &identity.resolved_persons {
            if Self::consent_status_for_person(person, observations) == ConsentStatus::OptedOut {
                continue;
            }
            let (slides, messages) =
                Self::collect_related(person, observations, &person_identifiers);

            let activity = Self::build_activity(&person.person_id, &slides, &messages);
            let frontend_profile = frontend_profiles
                .get(person.person_id.as_str())
                .map(|(_, profile)| profile.clone());
            let frontend_profile_updated_at = frontend_profiles
                .get(person.person_id.as_str())
                .map(|(updated_at, _)| *updated_at);
            let profile_updated_at = activity
                .last_activity
                .into_iter()
                .chain(frontend_profile_updated_at)
                .max()
                .or(activity.first_activity)
                .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);

            let profile = PersonProfile {
                person_id: person.person_id.clone(),
                display_name: person.canonical_name.clone(),
                self_intro_text: frontend_profile
                    .as_ref()
                    .and_then(|profile| profile.profile.bio_text.clone()),
                self_intro_slide_id: frontend_profile
                    .as_ref()
                    .map(|profile| profile.source_document_id.clone()),
                self_intro_thumbnail: frontend_profile.as_ref().and_then(|profile| {
                    profile
                        .thumbnail_url
                        .clone()
                        .or_else(|| profile.thumbnail_ref.clone())
                }),
                identities: person
                    .identifiers
                    .iter()
                    .filter_map(|ident| match ident.identifier_type {
                        IdentifierType::Email | IdentifierType::UserId => Some(IdentityInfo {
                            system: ident.source.clone(),
                            external_id: ident.value.clone(),
                        }),
                        _ => None,
                    })
                    .collect(),
                source_count: person.sources.len(),
                last_activity: activity.last_activity,
                profile_updated_at,
                frontend_profile,
            };

            profiles.push(profile);
            all_slides.extend(slides);
            all_messages.extend(messages);
            all_activities.push(activity);
        }

        PersonPageOutput {
            profiles,
            slides: all_slides,
            messages: all_messages,
            activities: all_activities,
        }
    }

    /// Resolve the latest explicit consent decision for a person.
    ///
    /// The default is restricted capture. Decisions can target the resolved
    /// person directly via `subject`, or one of its source identifiers via the
    /// payload `identifier` field.
    pub fn consent_status_for_person(
        person: &ResolvedPerson,
        observations: &[Observation],
    ) -> ConsentStatus {
        observations
            .iter()
            .filter(|observation| observation.schema.as_str() == CONSENT_DECISION_SCHEMA)
            .filter(|observation| {
                observation.subject == person.person_id
                    || observation
                        .payload
                        .get("identifier")
                        .and_then(serde_json::Value::as_str)
                        .is_some_and(|identifier| {
                            person
                                .identifiers
                                .iter()
                                .any(|candidate| candidate.value == identifier)
                        })
            })
            .max_by(|left, right| {
                (left.published, left.recorded_at, left.id.as_str()).cmp(&(
                    right.published,
                    right.recorded_at,
                    right.id.as_str(),
                ))
            })
            .and_then(|observation| {
                observation
                    .payload
                    .get("status")
                    .and_then(serde_json::Value::as_str)
            })
            .and_then(parse_consent_status)
            .unwrap_or_default()
    }

    fn build_frontend_profile_map(
        observations: &[Observation],
        supplemental_records: &[&SupplementalRecord],
        identifier_map: &HashMap<String, EntityRef>,
    ) -> HashMap<String, (DateTime<Utc>, FrontendProfile)> {
        let observation_index: HashMap<ObservationId, &Observation> = observations
            .iter()
            .map(|observation| (observation.id.clone(), observation))
            .collect();
        let mut results: HashMap<String, (usize, DateTime<Utc>, FrontendProfile)> = HashMap::new();

        for record in supplemental_records {
            if record.kind != "slide-analysis" {
                continue;
            }
            if record.consent_metadata.as_ref().is_some_and(|metadata| {
                metadata.retracted_at.is_some() || metadata.opt_out_effective_at.is_some()
            }) {
                continue;
            }

            let mut profile = match serde_json::from_value::<StudentProfile>(record.payload.clone())
            {
                Ok(profile) => profile,
                Err(_) => continue,
            };
            profile.normalize_in_place();
            if profile.companion_to_slide_object_id.is_some() {
                continue;
            }
            let source_observation = record
                .derived_from
                .observations
                .first()
                .and_then(|id| observation_index.get(id))
                .copied();
            let Some(source_observation) = source_observation else {
                continue;
            };

            let person_id = profile
                .email
                .as_deref()
                .or(profile.generated_email.as_deref())
                .and_then(|value| identifier_map.get(value))
                .cloned()
                .or_else(|| {
                    Self::person_id_from_slide_observation(source_observation, identifier_map)
                });
            let Some(person_id) = person_id else {
                continue;
            };

            let frontend_profile = FrontendProfile {
                source_document_id: profile
                    .source_document_id
                    .clone()
                    .unwrap_or_else(|| source_observation.subject.as_str().to_string()),
                source_canonical_uri: profile.source_canonical_uri.clone().or_else(|| {
                    source_observation
                        .payload
                        .pointer("/artifact/canonicalUri")
                        .and_then(|value| value.as_str())
                        .map(ToOwned::to_owned)
                }),
                thumbnail_ref: profile.thumbnail_blob_ref.clone().or_else(|| {
                    record
                        .derived_from
                        .blobs
                        .first()
                        .map(|blob| blob.as_str().to_string())
                }),
                thumbnail_url: profile.thumbnail_url.clone(),
                profile,
            };

            let created_at = record.created_at;
            let richness_score = frontend_profile.profile.richness_score();
            match results.get(person_id.as_str()) {
                Some((current_score, current_created_at, _))
                    if *current_score > richness_score
                        || (*current_score == richness_score
                            && *current_created_at >= created_at) => {}
                _ => {
                    results.insert(
                        person_id.as_str().to_string(),
                        (richness_score, created_at, frontend_profile),
                    );
                }
            }
        }

        results
            .into_iter()
            .map(|(person_id, (_, created_at, frontend_profile))| {
                (person_id, (created_at, frontend_profile))
            })
            .collect()
    }

    fn person_id_from_slide_observation(
        observation: &Observation,
        identifier_map: &HashMap<String, EntityRef>,
    ) -> Option<EntityRef> {
        observation
            .payload
            .pointer("/relations/owner")
            .and_then(|value| value.as_str())
            .and_then(|owner| identifier_map.get(owner))
            .cloned()
            .or_else(|| {
                observation
                    .payload
                    .pointer("/relations/editors")
                    .and_then(|value| value.as_array())
                    .and_then(|editors| editors.iter().find_map(|editor| editor.as_str()))
                    .and_then(|email| identifier_map.get(email))
                    .cloned()
            })
    }

    /// Build a map from source identifier values to person_id.
    fn build_person_identifier_map(
        identity: &IdentityResolutionOutput,
    ) -> HashMap<String, EntityRef> {
        let mut map = HashMap::new();
        for person in &identity.resolved_persons {
            for ident in &person.identifiers {
                map.insert(ident.value.clone(), person.person_id.clone());
            }
        }
        map
    }

    /// Collect slides and messages related to a person.
    fn collect_related(
        person: &ResolvedPerson,
        observations: &[Observation],
        identifier_map: &HashMap<String, EntityRef>,
    ) -> (Vec<PersonSlide>, Vec<PersonMessage>) {
        let mut slides = Vec::new();
        let mut messages = Vec::new();

        for obs in observations {
            let belongs = Self::observation_belongs_to(obs, person, identifier_map);
            if !belongs {
                continue;
            }

            if obs.schema == SchemaRef::new("schema:workspace-object-snapshot") {
                let Some(claim) = Self::slide_claim(obs, person, identifier_map) else {
                    continue;
                };
                let title = obs
                    .payload
                    .get("title")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Untitled")
                    .to_string();
                let revision = obs
                    .payload
                    .pointer("/revision/sourceRevisionId")
                    .and_then(|v| v.as_str())
                    .map(String::from);

                slides.push(PersonSlide {
                    id: format!("ps:{}:{claim}", obs.id),
                    source_observation_id: obs.id.as_str().to_owned(),
                    person_id: person.person_id.clone(),
                    document_id: obs.subject.as_str().to_string(),
                    title,
                    role: "editor".into(),
                    last_seen_revision: revision,
                    slide_count: None,
                    thumbnail_ref: None,
                    last_modified: Some(obs.published),
                });
            } else if obs.schema == SchemaRef::new("schema:slide-analysis-result") {
                slides.push(PersonSlide {
                    id: format!("ps:{}:analysis", obs.id),
                    source_observation_id: obs.id.as_str().to_owned(),
                    person_id: person.person_id.clone(),
                    document_id: obs
                        .target
                        .as_ref()
                        .map(|target| target.as_str().to_string())
                        .unwrap_or_else(|| obs.subject.as_str().to_string()),
                    title: obs
                        .payload
                        .get("person_name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("Analyzed Slide")
                        .to_string(),
                    role: "self-intro".into(),
                    last_seen_revision: None,
                    slide_count: Some(1),
                    thumbnail_ref: obs
                        .payload
                        .get("thumbnail_url")
                        .and_then(|v| v.as_str())
                        .map(ToOwned::to_owned)
                        .or_else(|| {
                            obs.payload
                                .get("thumbnail_blob_ref")
                                .and_then(|v| v.as_str())
                                .map(ToOwned::to_owned)
                        }),
                    last_modified: Some(obs.published),
                });
            } else if obs.schema == SchemaRef::new("schema:slack-message") {
                let text = obs
                    .payload
                    .get("text")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();
                let channel = obs
                    .payload
                    .get("channel_name")
                    .or_else(|| obs.payload.get("channel"))
                    .or_else(|| obs.payload.get("channel_id"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let thread_ts = obs
                    .payload
                    .get("thread_ts")
                    .and_then(|v| v.as_str())
                    .map(String::from);

                messages.push(PersonMessage {
                    id: format!("pm:{}", obs.id),
                    source_observation_id: obs.id.as_str().to_owned(),
                    person_id: person.person_id.clone(),
                    channel,
                    text,
                    ts: obs.published,
                    thread_ts,
                    has_attachments: !obs.attachments.is_empty(),
                });
            }
        }

        (slides, messages)
    }

    fn slide_claim(
        observation: &Observation,
        person: &ResolvedPerson,
        identifier_map: &HashMap<String, EntityRef>,
    ) -> Option<String> {
        let mut claims = Vec::new();
        if observation
            .payload
            .pointer("/relations/owner")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|owner| identifier_map.get(owner) == Some(&person.person_id))
        {
            claims.push("owner".to_owned());
        }
        if let Some(editors) = observation
            .payload
            .pointer("/relations/editors")
            .and_then(serde_json::Value::as_array)
        {
            for (index, editor) in editors.iter().enumerate() {
                if editor
                    .as_str()
                    .is_some_and(|email| identifier_map.get(email) == Some(&person.person_id))
                {
                    claims.push(format!("editor-{index:020}"));
                }
            }
        }
        claims.into_iter().min()
    }

    /// Check if an observation belongs to a person.
    fn observation_belongs_to(
        obs: &Observation,
        person: &ResolvedPerson,
        identifier_map: &HashMap<String, EntityRef>,
    ) -> bool {
        if obs.schema == SchemaRef::new("schema:slack-message") {
            return obs
                .payload
                .get("user_id")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|user_id| identifier_map.get(user_id) == Some(&person.person_id));
        }

        // Check via user_id in payload.
        if let Some(user_id) = obs.payload.get("user_id").and_then(|v| v.as_str())
            && identifier_map.get(user_id) == Some(&person.person_id)
        {
            return true;
        }

        // Check via email in payload.
        if let Some(email) = obs.payload.get("email").and_then(|v| v.as_str())
            && identifier_map.get(email) == Some(&person.person_id)
        {
            return true;
        }

        if let Some(email) = obs.payload.get("person_email").and_then(|v| v.as_str())
            && identifier_map.get(email) == Some(&person.person_id)
        {
            return true;
        }

        // Check via editors in GSlides.
        if let Some(editors) = obs
            .payload
            .pointer("/relations/editors")
            .and_then(|v| v.as_array())
        {
            for editor in editors {
                if let Some(email) = editor.as_str()
                    && identifier_map.get(email) == Some(&person.person_id)
                {
                    return true;
                }
            }
        }

        // Check via owner in GSlides.
        if let Some(owner) = obs
            .payload
            .pointer("/relations/owner")
            .and_then(|v| v.as_str())
            && identifier_map.get(owner) == Some(&person.person_id)
        {
            return true;
        }

        false
    }

    /// Build activity summary for a person.
    fn build_activity(
        person_id: &EntityRef,
        slides: &[PersonSlide],
        messages: &[PersonMessage],
    ) -> PersonActivity {
        let mut channels: Vec<String> = messages
            .iter()
            .map(|m| m.channel.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();
        channels.sort();

        let first_slide = slides.iter().filter_map(|s| s.last_modified).min();
        let first_msg = messages.iter().map(|m| m.ts).min();
        let first_activity = [first_slide, first_msg].into_iter().flatten().min();

        let last_slide = slides.iter().filter_map(|s| s.last_modified).max();
        let last_msg = messages.iter().map(|m| m.ts).max();
        let last_activity = [last_slide, last_msg].into_iter().flatten().max();

        PersonActivity {
            person_id: person_id.clone(),
            total_slides_related: slides.len(),
            total_messages: messages.len(),
            first_activity,
            last_activity,
            active_channels: channels,
        }
    }

    /// Build a PersonListItem from a profile and activity.
    pub fn to_list_item(profile: &PersonProfile, activity: &PersonActivity) -> PersonListItem {
        PersonListItem {
            person_id: profile.person_id.clone(),
            display_name: profile.display_name.clone(),
            source_count: profile.source_count,
            total_slides: activity.total_slides_related,
            total_messages: activity.total_messages,
            last_activity: activity.last_activity,
            thumbnail_url: profile.self_intro_thumbnail.clone(),
        }
    }

    /// Build a PersonDetailResponse from projector output.
    pub fn to_detail(
        profile: &PersonProfile,
        slides: &[PersonSlide],
        messages: &[PersonMessage],
        activity: &PersonActivity,
    ) -> PersonDetailResponse {
        let self_introduction = profile
            .self_intro_text
            .as_ref()
            .map(|text| SelfIntroduction {
                text: text.clone(),
                slide_id: profile.self_intro_slide_id.clone(),
                thumbnail_url: profile.self_intro_thumbnail.clone(),
            });

        PersonDetailResponse {
            person_id: profile.person_id.clone(),
            display_name: profile.display_name.clone(),
            self_introduction,
            identities: profile.identities.clone(),
            related_slides: slides.to_vec(),
            recent_messages: messages.to_vec(),
            activity_summary: activity.clone(),
        }
    }
}

fn parse_consent_status(value: &str) -> Option<ConsentStatus> {
    match value {
        "unrestricted" => Some(ConsentStatus::Unrestricted),
        "restricted_capture" => Some(ConsentStatus::RestrictedCapture),
        "opted_out" => Some(ConsentStatus::OptedOut),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use lethe_core::domain::supplemental::InputAnchorSet;
    use lethe_core::domain::*;
    use lethe_engine::identity::types::*;
    use lethe_policy::governance::types::ConfidenceLevel;
    use lethe_profile_model::{StudentProfile, StudentProperties};

    fn slack_obs(user_id: &str, email: &str, text: &str, channel: &str, key: &str) -> Observation {
        Observation {
            id: Observation::new_id(),
            schema: SchemaRef::new("schema:slack-message"),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:slack-crawler"),
            source_system: Some(SourceSystemRef::new("sys:slack")),
            actor: None,
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new(format!("message:slack:{key}")),
            target: None,
            payload: serde_json::json!({
                "user_id": user_id,
                "email": email,
                "text": text,
                "channel": channel,
                "channel_id": format!("chan:{channel}"),
                "channel_name": channel,
            }),
            attachments: vec![],
            published: Utc::now(),
            recorded_at: Utc::now(),
            consent: None,
            idempotency_key: IdempotencyKey::new(key),
            meta: serde_json::json!({}),
        }
    }

    fn gslides_obs(editors: &[&str], owner: &str, key: &str) -> Observation {
        Observation {
            id: Observation::new_id(),
            schema: SchemaRef::new("schema:workspace-object-snapshot"),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:gslides-crawler"),
            source_system: Some(SourceSystemRef::new("sys:google-slides")),
            actor: None,
            authority_model: AuthorityModel::SourceAuthoritative,
            capture_model: CaptureModel::Snapshot,
            subject: EntityRef::new(format!("document:gslide:{key}")),
            target: None,
            payload: serde_json::json!({
                "title": "自己紹介スライド",
                "relations": {
                    "editors": editors,
                    "owner": owner,
                },
            }),
            attachments: vec![],
            published: Utc::now(),
            recorded_at: Utc::now(),
            consent: None,
            idempotency_key: IdempotencyKey::new(key),
            meta: serde_json::json!({}),
        }
    }

    fn sample_identity() -> IdentityResolutionOutput {
        IdentityResolutionOutput {
            resolved_persons: vec![ResolvedPerson {
                person_id: EntityRef::new("person:tanaka-2026"),
                canonical_name: "田中太郎".into(),
                aliases: vec!["田中太郎".into()],
                identifiers: vec![
                    SourceIdentifier {
                        source: "slack".into(),
                        identifier_type: IdentifierType::UserId,
                        value: "U123".into(),
                    },
                    SourceIdentifier {
                        source: "slack".into(),
                        identifier_type: IdentifierType::Email,
                        value: "tanaka@example.jp".into(),
                    },
                    SourceIdentifier {
                        source: "google".into(),
                        identifier_type: IdentifierType::Email,
                        value: "tanaka@example.jp".into(),
                    },
                ],
                confidence: ConfidenceLevel::High,
                sources: vec!["slack".into(), "google".into()],
                resolved_at: Utc::now(),
                resolved_by: "projector:identity-resolution:v1.0.0".into(),
            }],
            candidates: vec![],
            person_identifiers: vec![],
        }
    }

    #[test]
    fn person_page_with_slides_and_messages() {
        let identity = sample_identity();
        let observations = vec![
            slack_obs("U123", "tanaka@example.jp", "こんにちは", "general", "s1"),
            slack_obs("U123", "tanaka@example.jp", "明日の会議", "project-a", "s2"),
            gslides_obs(&["tanaka@example.jp"], "tanaka@example.jp", "g1"),
        ];

        let output = PersonPageProjector::project(&identity, &observations, &[]);
        assert_eq!(output.profiles.len(), 1);
        assert_eq!(output.profiles[0].display_name, "田中太郎");
        assert_eq!(output.messages.len(), 2);
        assert_eq!(output.slides.len(), 1);
        assert_eq!(output.messages[0].id, format!("pm:{}", observations[0].id));
        assert_eq!(output.messages[1].id, format!("pm:{}", observations[1].id));
        assert_eq!(
            output.slides[0].id,
            format!("ps:{}:editor-{:020}", observations[2].id, 0)
        );
        assert_eq!(output.activities.len(), 1);
        assert_eq!(output.activities[0].total_messages, 2);
        assert_eq!(output.activities[0].total_slides_related, 1);
        assert_eq!(output.activities[0].active_channels.len(), 2);
    }

    #[test]
    fn unrelated_observation_excluded() {
        let identity = sample_identity();
        let observations = vec![slack_obs(
            "U999",
            "other@example.com",
            "unrelated",
            "general",
            "s1",
        )];

        let output = PersonPageProjector::project(&identity, &observations, &[]);
        assert_eq!(output.messages.len(), 0);
    }

    #[test]
    fn person_detail_response_builds() {
        let identity = sample_identity();
        let observations = vec![slack_obs(
            "U123",
            "tanaka@example.jp",
            "hello",
            "general",
            "s1",
        )];

        let output = PersonPageProjector::project(&identity, &observations, &[]);
        let profile = &output.profiles[0];
        let activity = &output.activities[0];
        let msgs: Vec<_> = output
            .messages
            .iter()
            .filter(|m| m.person_id == profile.person_id)
            .collect();

        let detail = PersonPageProjector::to_detail(
            profile,
            &[],
            &msgs.into_iter().cloned().collect::<Vec<_>>(),
            activity,
        );
        assert_eq!(detail.person_id.as_str(), "person:tanaka-2026");
        assert_eq!(detail.display_name, "田中太郎");
        assert_eq!(detail.recent_messages.len(), 1);
    }

    #[test]
    fn person_list_item_builds() {
        let identity = sample_identity();
        let observations = vec![
            slack_obs("U123", "tanaka@example.jp", "msg", "general", "s1"),
            gslides_obs(&["tanaka@example.jp"], "tanaka@example.jp", "g1"),
        ];

        let output = PersonPageProjector::project(&identity, &observations, &[]);
        let item = PersonPageProjector::to_list_item(&output.profiles[0], &output.activities[0]);
        assert_eq!(item.display_name, "田中太郎");
        assert_eq!(item.total_messages, 1);
        assert_eq!(item.total_slides, 1);
        assert_eq!(item.source_count, 2);
    }

    #[test]
    fn replay_deterministic() {
        let identity = sample_identity();
        let observations = vec![
            slack_obs("U123", "tanaka@example.jp", "hello", "general", "s1"),
            gslides_obs(&["tanaka@example.jp"], "tanaka@example.jp", "g1"),
        ];

        let r1 = PersonPageProjector::project(&identity, &observations, &[]);
        let r2 = PersonPageProjector::project(&identity, &observations, &[]);
        assert_eq!(
            serde_json::to_value(&r1).unwrap(),
            serde_json::to_value(&r2).unwrap()
        );
    }

    #[test]
    fn adapter_payload_shape_uses_channel_name() {
        let identity = sample_identity();
        let observation = Observation {
            id: Observation::new_id(),
            schema: SchemaRef::new("schema:slack-message"),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:slack-crawler"),
            source_system: Some(SourceSystemRef::new("sys:slack")),
            actor: None,
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new("message:slack:s1"),
            target: None,
            payload: serde_json::json!({
                "user_id": "U123",
                "email": "tanaka@example.jp",
                "text": "hello",
                "channel_id": "C01ABC",
                "channel_name": "general",
            }),
            attachments: vec![],
            published: Utc::now(),
            recorded_at: Utc::now(),
            consent: None,
            idempotency_key: IdempotencyKey::new("s1"),
            meta: serde_json::json!({}),
        };

        let output = PersonPageProjector::project(&identity, &[observation], &[]);
        assert_eq!(output.messages.len(), 1);
        assert_eq!(output.messages[0].channel, "general");
    }

    #[test]
    fn frontend_profile_links_via_source_observation_not_document_identifier() {
        let identity = sample_identity();
        let observation = gslides_obs(&["tanaka@example.jp"], "tanaka@example.jp", "g1");
        let supplemental = SupplementalRecord {
            id: SupplementalId::new("sup:slide-analysis:g1"),
            kind: "slide-analysis".into(),
            derived_from: InputAnchorSet {
                observations: vec![observation.id.clone()],
                blobs: vec![],
                supplementals: vec![],
            },
            payload: serde_json::to_value(StudentProfile {
                email: None,
                generated_email: None,
                name: "田中太郎".into(),
                bio_text: Some("自己紹介".into()),
                profile_pic: None,
                gallery_images: vec![],
                properties: Default::default(),
                attributes: vec![],
                source_slide_object_id: Some("slide-1".into()),
                source_document_id: Some("document:gslides:g1#slide:slide-1".into()),
                source_canonical_uri: None,
                thumbnail_blob_ref: None,
                thumbnail_url: Some("https://example.com/thumb.png".into()),
                companion_to_slide_object_id: None,
            })
            .unwrap(),
            created_by: ActorRef::new("actor:test"),
            created_at: chrono::DateTime::parse_from_rfc3339("2026-03-24T12:00:00Z")
                .unwrap()
                .to_utc(),
            mutability: Mutability::ManagedCache,
            record_version: Some("1".into()),
            model_version: Some("fixture".into()),
            consent_metadata: None,
            lineage: None,
        };

        let output = PersonPageProjector::project(&identity, &[observation], &[&supplemental]);
        assert_eq!(output.profiles.len(), 1);
        assert_eq!(
            output.profiles[0]
                .frontend_profile
                .as_ref()
                .map(|profile| profile.source_document_id.as_str()),
            Some("document:gslides:g1#slide:slide-1")
        );
        assert!(
            output.profiles[0]
                .identities
                .iter()
                .all(|identity| !identity.external_id.starts_with("document:gslides:"))
        );
    }

    #[test]
    fn richer_frontend_profile_wins_over_newer_sparse_profile() {
        let identity = sample_identity();
        let observation = gslides_obs(&["tanaka@example.jp"], "tanaka@example.jp", "g1");

        let richer = SupplementalRecord {
            id: SupplementalId::new("sup:slide-analysis:rich"),
            kind: "slide-analysis".into(),
            derived_from: InputAnchorSet {
                observations: vec![observation.id.clone()],
                blobs: vec![],
                supplementals: vec![],
            },
            payload: serde_json::to_value(StudentProfile {
                email: Some("tanaka@example.jp".into()),
                generated_email: None,
                name: "田中太郎".into(),
                bio_text: Some("自己紹介です".into()),
                profile_pic: None,
                gallery_images: vec![],
                properties: StudentProperties {
                    nickname: Some("Taro".into()),
                    ..Default::default()
                },
                attributes: vec![],
                source_slide_object_id: Some("slide-1".into()),
                source_document_id: Some("document:gslides:g1#slide:slide-1".into()),
                source_canonical_uri: None,
                thumbnail_blob_ref: None,
                thumbnail_url: Some("https://example.com/rich.png".into()),
                companion_to_slide_object_id: None,
            })
            .unwrap(),
            created_by: ActorRef::new("actor:test"),
            created_at: chrono::DateTime::parse_from_rfc3339("2026-03-24T12:00:00Z")
                .unwrap()
                .to_utc(),
            mutability: Mutability::ManagedCache,
            record_version: Some("1".into()),
            model_version: Some("fixture".into()),
            consent_metadata: None,
            lineage: None,
        };

        let sparse_newer = SupplementalRecord {
            id: SupplementalId::new("sup:slide-analysis:sparse"),
            kind: "slide-analysis".into(),
            derived_from: InputAnchorSet {
                observations: vec![observation.id.clone()],
                blobs: vec![],
                supplementals: vec![],
            },
            payload: serde_json::to_value(StudentProfile {
                email: Some("tanaka@example.jp".into()),
                generated_email: None,
                name: "田中太郎".into(),
                bio_text: None,
                profile_pic: None,
                gallery_images: vec![],
                properties: Default::default(),
                attributes: vec![],
                source_slide_object_id: Some("slide-2".into()),
                source_document_id: Some("document:gslides:g1#slide:slide-2".into()),
                source_canonical_uri: None,
                thumbnail_blob_ref: None,
                thumbnail_url: Some("https://example.com/sparse.png".into()),
                companion_to_slide_object_id: None,
            })
            .unwrap(),
            created_by: ActorRef::new("actor:test"),
            created_at: chrono::DateTime::parse_from_rfc3339("2026-03-25T12:00:00Z")
                .unwrap()
                .to_utc(),
            mutability: Mutability::ManagedCache,
            record_version: Some("1".into()),
            model_version: Some("fixture".into()),
            consent_metadata: None,
            lineage: None,
        };

        let output =
            PersonPageProjector::project(&identity, &[observation], &[&sparse_newer, &richer]);

        assert_eq!(
            output.profiles[0]
                .frontend_profile
                .as_ref()
                .and_then(|profile| profile.profile.bio_text.as_deref()),
            Some("自己紹介です")
        );
    }
}
