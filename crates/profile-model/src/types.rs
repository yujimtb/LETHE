//! Slide analysis types — student profile extracted from a slide,
//! and the output records for supplemental store + SaaS write-back.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};

use lethe_core::domain::{BlobRef, EntityRef, ObservationId, SupplementalId};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributeAliasCatalog {
    pub version: u32,
    pub attributes: Vec<AttributeAliasDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributeAliasDefinition {
    pub id: String,
    pub display_name: String,
    pub aliases: Vec<String>,
    pub examples: Vec<String>,
}

/// Structured student profile extracted from a Google Slides self-introduction.
/// Schema mirrors the Gemini AI output from skcollege_dictionary/AIService.js.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StudentProfile {
    /// Email found on the slide (or AI-generated).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    /// AI-generated email (firstname.lastname@hlab.college).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generated_email: Option<String>,
    /// Display name (Kanji/Yomigana).
    #[serde(default, deserialize_with = "deserialize_string_or_default")]
    pub name: String,
    /// Bio text from the slide.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bio_text: Option<String>,
    /// Profile picture info.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub profile_pic: Option<ProfilePic>,
    /// Gallery images from the slide.
    #[serde(default, deserialize_with = "deserialize_vec_or_default")]
    pub gallery_images: Vec<GalleryImage>,
    /// Structured properties extracted by AI.
    #[serde(default)]
    pub properties: StudentProperties,
    /// Tags / attributes.
    #[serde(default, deserialize_with = "deserialize_vec_or_default")]
    pub attributes: Vec<String>,
    /// Source slide object id inside the presentation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_slide_object_id: Option<String>,
    /// Canonical LETHE document id for the analyzed slide.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_document_id: Option<String>,
    /// Canonical Google Slides URL for the source deck.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_canonical_uri: Option<String>,
    /// Blob ref for the rendered thumbnail used for analysis.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thumbnail_blob_ref: Option<String>,
    /// External slide image URL from Google thumbnail rendering.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thumbnail_url: Option<String>,
    /// If present, this slide is a continuation of another primary slide.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub companion_to_slide_object_id: Option<String>,
}

impl StudentProfile {
    pub fn normalize_in_place(&mut self) {
        self.email = normalize_email(self.email.take());
        self.generated_email = normalize_email(self.generated_email.take());
        if self.email.is_some() {
            self.generated_email = None;
        }

        self.name = normalize_required_text(std::mem::take(&mut self.name));
        self.bio_text = normalize_optional_text(self.bio_text.take());

        self.profile_pic = self.profile_pic.take().and_then(|mut pic| {
            pic.normalize_in_place();
            pic.is_meaningful().then_some(pic)
        });

        for image in &mut self.gallery_images {
            image.normalize_in_place();
        }
        self.gallery_images.retain(GalleryImage::is_meaningful);

        self.properties.normalize_in_place();
        normalize_string_list(&mut self.attributes);

        self.source_slide_object_id = normalize_optional_text(self.source_slide_object_id.take());
        self.source_document_id = normalize_optional_text(self.source_document_id.take());
        self.source_canonical_uri = normalize_url(self.source_canonical_uri.take());
        self.thumbnail_blob_ref = normalize_optional_text(self.thumbnail_blob_ref.take());
        self.thumbnail_url = normalize_url(self.thumbnail_url.take());
        self.companion_to_slide_object_id =
            normalize_optional_text(self.companion_to_slide_object_id.take());
    }

    pub fn normalized(mut self) -> Self {
        self.normalize_in_place();
        self
    }

    pub fn richness_score(&self) -> usize {
        let mut score = 0usize;

        if self.email.is_some() {
            score += 10;
        }
        if self.generated_email.is_some() {
            score += 4;
        }
        if !self.name.trim().is_empty() {
            score += 4;
        }
        if self
            .bio_text
            .as_ref()
            .is_some_and(|text| !text.trim().is_empty())
        {
            score += 12;
        }
        if self.profile_pic.is_some() {
            score += 6;
        }
        score += self.gallery_images.len() * 2;
        score += self.attributes.len();
        score += self.properties.richness_score();

        score
    }

    pub fn has_meaningful_content(&self) -> bool {
        self.richness_score() > 0
    }
}

/// Profile picture information.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProfilePic {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coordinates: Option<ImageCoordinates>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

impl ProfilePic {
    fn normalize_in_place(&mut self) {
        self.description = normalize_optional_text(self.description.take());
        self.url = normalize_url(self.url.take());
    }

    fn is_meaningful(&self) -> bool {
        self.description
            .as_ref()
            .is_some_and(|value| !value.is_empty())
            || self.url.as_ref().is_some_and(|value| !value.is_empty())
            || self.coordinates.is_some()
    }
}

/// Image coordinates as percentage (0-100) from top-left.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageCoordinates {
    pub x: f64,
    pub y: f64,
}

/// Gallery image entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GalleryImage {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coordinates: Option<ImageCoordinates>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

impl GalleryImage {
    fn normalize_in_place(&mut self) {
        self.description = normalize_optional_text(self.description.take());
        self.url = normalize_url(self.url.take());
    }

    fn is_meaningful(&self) -> bool {
        self.description
            .as_ref()
            .is_some_and(|value| !value.is_empty())
            || self.url.as_ref().is_some_and(|value| !value.is_empty())
            || self.coordinates.is_some()
    }
}

/// Structured student properties from AI extraction.
/// Fields mirror the Gemini prompt schema in AIService.js.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StudentProperties {
    #[serde(skip_serializing_if = "Option::is_none", rename = "Nickname")]
    pub nickname: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "Birthplace")]
    pub birthplace: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "DoB")]
    pub dob: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "Major")]
    pub major: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "Affiliation")]
    pub affiliation: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "MBTI")]
    pub mbti: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "SNS")]
    pub sns: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_vec_or_default",
        rename = "Hobbies"
    )]
    pub hobbies: Vec<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_vec_or_default",
        rename = "Interests"
    )]
    pub interests: Vec<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_vec_or_default",
        rename = "Likes"
    )]
    pub likes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "Dislikes")]
    pub dislikes: Option<String>,
    #[serde(
        default,
        deserialize_with = "deserialize_vec_or_default",
        rename = "Hashtags"
    )]
    pub hashtags: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "New Challenges")]
    pub new_challenges: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "Ask Me About")]
    pub ask_me_about: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "Turning Point")]
    pub turning_point: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "BTW")]
    pub btw: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "Message")]
    pub message: Option<String>,
}

impl StudentProperties {
    fn normalize_in_place(&mut self) {
        self.nickname = normalize_optional_text(self.nickname.take());
        self.birthplace = normalize_optional_text(self.birthplace.take());
        self.dob = normalize_optional_text(self.dob.take());
        self.major = normalize_optional_text(self.major.take());
        self.affiliation = normalize_optional_text(self.affiliation.take());
        self.mbti = normalize_optional_text(self.mbti.take()).map(|value| value.to_uppercase());
        let sns = self.sns.take();
        self.sns = normalize_url(sns.clone()).or_else(|| normalize_optional_text(sns));
        normalize_string_list(&mut self.hobbies);
        normalize_string_list(&mut self.interests);
        normalize_string_list(&mut self.likes);
        self.dislikes = normalize_optional_text(self.dislikes.take());
        normalize_string_list(&mut self.hashtags);
        self.new_challenges = normalize_optional_text(self.new_challenges.take());
        self.ask_me_about = normalize_optional_text(self.ask_me_about.take());
        self.turning_point = normalize_optional_text(self.turning_point.take());
        self.btw = normalize_optional_text(self.btw.take());
        self.message = normalize_optional_text(self.message.take());
    }

    fn richness_score(&self) -> usize {
        let mut score = 0usize;
        let scalar_fields = [
            self.nickname.as_ref(),
            self.birthplace.as_ref(),
            self.dob.as_ref(),
            self.major.as_ref(),
            self.affiliation.as_ref(),
            self.mbti.as_ref(),
            self.sns.as_ref(),
            self.dislikes.as_ref(),
            self.new_challenges.as_ref(),
            self.ask_me_about.as_ref(),
            self.turning_point.as_ref(),
            self.btw.as_ref(),
            self.message.as_ref(),
        ];
        score += scalar_fields
            .into_iter()
            .filter(|value| value.is_some_and(|text| !text.trim().is_empty()))
            .count()
            * 3;
        score += self.hobbies.len();
        score += self.interests.len();
        score += self.likes.len();
        score += self.hashtags.len();
        score
    }
}

/// A slide analysis output: links an observation to the extracted profile
/// and the supplemental record that stores it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlideAnalysisResult {
    /// The observation ID of the source slide snapshot.
    pub source_observation_id: ObservationId,
    /// Presentation ID.
    pub presentation_id: String,
    /// The extracted student profile.
    pub profile: StudentProfile,
    /// The person entity this profile was linked to.
    pub person_entity: EntityRef,
    /// Supplemental record ID if stored.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub supplemental_id: Option<SupplementalId>,
    /// When the analysis was performed.
    pub analyzed_at: DateTime<Utc>,
    /// Model or strategy version used to generate the profile.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model_version: Option<String>,
    /// The slide object id that produced this profile.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slide_object_id: Option<String>,
    /// Rendered slide thumbnail blob used as the AI input.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thumbnail_blob_ref: Option<BlobRef>,
}

fn deserialize_string_or_default<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?.unwrap_or_default())
}

fn deserialize_vec_or_default<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany<T> {
        One(T),
        Many(Vec<T>),
    }

    Ok(match Option::<OneOrMany<T>>::deserialize(deserializer)? {
        None => Vec::new(),
        Some(OneOrMany::One(value)) => vec![value],
        Some(OneOrMany::Many(values)) => values,
    })
}

fn normalize_required_text(value: String) -> String {
    value.trim().to_string()
}

fn normalize_optional_text(value: Option<String>) -> Option<String> {
    let value = value?.trim().to_string();
    if value.is_empty() || is_placeholder_value(&value) {
        return None;
    }
    Some(value)
}

fn normalize_email(value: Option<String>) -> Option<String> {
    let value = normalize_optional_text(value)?
        .trim_matches(|ch: char| matches!(ch, '<' | '>' | '(' | ')' | '[' | ']' | ',' | ';'))
        .to_lowercase();
    if value.contains('@') {
        Some(value)
    } else {
        None
    }
}

fn normalize_url(value: Option<String>) -> Option<String> {
    let value = normalize_optional_text(value)?;
    if value.starts_with("http://") || value.starts_with("https://") {
        Some(value)
    } else {
        None
    }
}

fn normalize_string_list(values: &mut Vec<String>) {
    let mut normalized = Vec::new();
    for value in values.drain(..) {
        for candidate in split_list_value(&value) {
            let trimmed = candidate.trim();
            if trimmed.is_empty() || is_placeholder_value(trimmed) {
                continue;
            }
            if !normalized
                .iter()
                .any(|existing: &String| existing.eq_ignore_ascii_case(trimmed))
            {
                normalized.push(trimmed.to_string());
            }
        }
    }
    *values = normalized;
}

fn split_list_value(value: &str) -> Vec<String> {
    let normalized = value.replace(['\n', '\r', '・', '•', '，', '、', '/'], ",");

    let mut results = Vec::new();
    for part in normalized.split(',') {
        let trimmed = part.trim();
        if trimmed.is_empty() {
            continue;
        }
        // Handle bare hashtag patterns like "#旅行 #音楽 #プログラミング"
        if trimmed.starts_with('#') || trimmed.contains(" #") {
            for tag in split_hashtag_tokens(trimmed) {
                results.push(tag);
            }
        } else {
            results.push(trimmed.to_string());
        }
    }
    results
}

/// Split "#tag1 #tag2 #tag3" into ["tag1", "tag2", "tag3"].
/// Strips leading '#' from each token.
fn split_hashtag_tokens(value: &str) -> Vec<String> {
    value
        .split('#')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

fn is_placeholder_value(value: &str) -> bool {
    matches!(
        value.trim().to_lowercase().as_str(),
        "null" | "none" | "n/a" | "na" | "unknown" | "不明" | "未記載" | "未入力"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn student_profile_round_trips() {
        let profile = StudentProfile {
            email: Some("test@hlab.college".into()),
            generated_email: None,
            name: "田中太郎".into(),
            bio_text: Some("Hello".into()),
            profile_pic: None,
            gallery_images: vec![],
            properties: StudentProperties {
                nickname: Some("Taro".into()),
                mbti: Some("ENFP".into()),
                ..Default::default()
            },
            attributes: vec!["CS".into()],
            source_slide_object_id: Some("slide-1".into()),
            source_document_id: Some("document:gslides:deck#slide:slide-1".into()),
            source_canonical_uri: Some("https://docs.google.com/presentation/d/deck".into()),
            thumbnail_blob_ref: Some("blob:sha256:test".into()),
            thumbnail_url: Some("https://lh3.googleusercontent.com/test".into()),
            companion_to_slide_object_id: None,
        };
        let json = serde_json::to_value(&profile).unwrap();
        let back: StudentProfile = serde_json::from_value(json).unwrap();
        assert_eq!(back.name, "田中太郎");
        assert_eq!(back.properties.nickname, Some("Taro".into()));
    }

    #[test]
    fn normalize_in_place_splits_lists_and_cleans_placeholders() {
        let mut profile = StudentProfile {
            email: Some(" TANAKA@EXAMPLE.JP ".into()),
            generated_email: Some("none".into()),
            name: " 田中太郎 ".into(),
            bio_text: Some("  Hello  ".into()),
            profile_pic: Some(ProfilePic {
                coordinates: None,
                description: Some(" ".into()),
                url: Some("https://example.com/pic.png".into()),
            }),
            gallery_images: vec![GalleryImage {
                coordinates: None,
                description: Some("Photo".into()),
                url: Some(" ".into()),
            }],
            properties: StudentProperties {
                hobbies: vec!["music, soccer".into(), "Music".into()],
                hashtags: vec!["rust / ai".into(), "unknown".into()],
                mbti: Some("enfp".into()),
                ..Default::default()
            },
            attributes: vec![" AI・ML ".into(), "ai".into()],
            source_slide_object_id: None,
            source_document_id: None,
            source_canonical_uri: None,
            thumbnail_blob_ref: None,
            thumbnail_url: None,
            companion_to_slide_object_id: None,
        };

        profile.normalize_in_place();

        assert_eq!(profile.email.as_deref(), Some("tanaka@example.jp"));
        assert_eq!(profile.generated_email, None);
        assert_eq!(profile.name, "田中太郎");
        assert_eq!(profile.properties.hobbies, vec!["music", "soccer"]);
        assert_eq!(profile.properties.hashtags, vec!["rust", "ai"]);
        assert_eq!(profile.properties.mbti.as_deref(), Some("ENFP"));
        assert_eq!(profile.attributes, vec!["AI", "ML"]);
    }

    #[test]
    fn normalize_handles_bare_hashtag_entries() {
        let mut profile = StudentProfile {
            email: None,
            generated_email: None,
            name: "Test".into(),
            bio_text: None,
            profile_pic: None,
            gallery_images: vec![],
            properties: StudentProperties {
                hashtags: vec![
                    "#旅行 #音楽 #プログラミング".into(),
                    "#読書".into(),
                    "already clean".into(),
                ],
                hobbies: vec!["#basketball #soccer".into()],
                ..Default::default()
            },
            attributes: vec!["#AI #ML".into()],
            source_slide_object_id: None,
            source_document_id: None,
            source_canonical_uri: None,
            thumbnail_blob_ref: None,
            thumbnail_url: None,
            companion_to_slide_object_id: None,
        };

        profile.normalize_in_place();

        assert_eq!(
            profile.properties.hashtags,
            vec!["旅行", "音楽", "プログラミング", "読書", "already clean"]
        );
        assert_eq!(profile.properties.hobbies, vec!["basketball", "soccer"]);
        assert_eq!(profile.attributes, vec!["AI", "ML"]);
    }

    #[test]
    fn split_list_handles_mixed_hashtag_and_comma() {
        let result = split_list_value("#tag1, #tag2 #tag3");
        assert_eq!(result, vec!["tag1", "tag2", "tag3"]);
    }
}
