use std::thread;
use std::time::Duration;

use base64::Engine;
use reqwest::blocking::Client;
use reqwest::header::CONTENT_TYPE;
use serde::Deserialize;

use lethe_adapter_api::config::{BackoffStrategy, RetryConfig};
use lethe_adapter_api::error::AdapterError;
use lethe_adapter_api::retry::{RetryDecision, should_retry};
use lethe_core::domain::Observation;
use lethe_engine::lake::BlobStore;

use super::provider::{DerivationLineage, DerivationProvider, DerivedStudentProfile};
use lethe_profile_model::StudentProfile;

#[derive(Debug, Clone)]
pub struct GeminiSlideAnalyzer {
    http: Client,
    api_key: String,
    model: String,
    retry_config: RetryConfig,
}

impl GeminiSlideAnalyzer {
    const BASE_URL: &'static str = "https://generativelanguage.googleapis.com/v1beta/models";

    pub fn new(api_key: impl Into<String>, model: impl Into<String>) -> Result<Self, AdapterError> {
        let http = Client::builder()
            .build()
            .map_err(|err| AdapterError::Network {
                message: err.to_string(),
            })?;

        Ok(Self {
            http,
            api_key: api_key.into(),
            model: model.into(),
            retry_config: RetryConfig {
                max_retries: 3,
                backoff: BackoffStrategy::Exponential,
                max_wait: Duration::from_secs(30),
            },
        })
    }

    pub fn model_name(&self) -> &str {
        &self.model
    }

    pub fn extract_profile(
        &self,
        observation: &Observation,
        blobs: &BlobStore,
    ) -> Result<Option<StudentProfile>, AdapterError> {
        let blob_ref = observation.attachments.first().ok_or_else(|| {
            AdapterError::Other(
                "slide analysis requires a rendered slide thumbnail attachment".to_string(),
            )
        })?;
        let image = blobs.get(blob_ref).ok_or_else(|| {
            AdapterError::Other(format!(
                "blob {} not available in blob store",
                blob_ref.as_str()
            ))
        })?;
        let title = observation
            .payload
            .get("title")
            .and_then(|value| value.as_str())
            .unwrap_or("Unknown");
        let canonical_uri = observation
            .payload
            .pointer("/artifact/canonicalUri")
            .and_then(|value| value.as_str())
            .unwrap_or_default();

        self.extract_profile_from_png(image, title, canonical_uri)
    }

    pub fn extract_profile_from_png(
        &self,
        image: &[u8],
        title: &str,
        canonical_uri: &str,
    ) -> Result<Option<StudentProfile>, AdapterError> {
        let mut attempt = 0u32;
        loop {
            match self.try_extract(image, title, canonical_uri) {
                Ok(profile) => return Ok(profile),
                Err(err) => match should_retry(&err, attempt, &self.retry_config) {
                    RetryDecision::RetryAfter(wait) => {
                        eprintln!(
                            "gemini attempt {} failed ({}), retrying in {}s",
                            attempt + 1,
                            err,
                            wait.as_secs()
                        );
                        thread::sleep(wait);
                        attempt += 1;
                    }
                    RetryDecision::GiveUp { reason } => {
                        return Err(AdapterError::Other(format!(
                            "gemini gave up after {} attempt(s): {reason}; last error: {err}",
                            attempt + 1
                        )));
                    }
                },
            }
        }
    }

    fn try_extract(
        &self,
        image: &[u8],
        title: &str,
        canonical_uri: &str,
    ) -> Result<Option<StudentProfile>, AdapterError> {
        let image_base64 = base64::engine::general_purpose::STANDARD.encode(image);
        let prompt = build_extraction_prompt(title, canonical_uri);

        let request = serde_json::json!({
            "contents": [{
                "role": "user",
                "parts": [
                    { "text": prompt },
                    {
                        "inlineData": {
                            "mimeType": "image/png",
                            "data": image_base64
                        }
                    }
                ]
            }],
            "generationConfig": {
                "temperature": 0.2,
                "responseMimeType": "application/json"
            }
        });

        let url = format!(
            "{}/{model}:generateContent?key={key}",
            Self::BASE_URL,
            model = self.model,
            key = self.api_key,
        );
        let response = self
            .http
            .post(&url)
            .header(CONTENT_TYPE, "application/json")
            .json(&request)
            .send()
            .map_err(|err| AdapterError::Network {
                message: err.to_string(),
            })?;

        let status = response.status();

        // Detect rate limiting (429) and extract Retry-After header
        if status.as_u16() == 429 {
            let retry_after = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
                .unwrap_or(5);
            return Err(AdapterError::RateLimited {
                retry_after_secs: retry_after,
            });
        }

        let body = response.text().map_err(|err| AdapterError::Network {
            message: err.to_string(),
        })?;
        if !status.is_success() {
            return Err(AdapterError::Network {
                message: format!("gemini api error ({status}): {body}"),
            });
        }

        let parsed: GeminiResponse =
            serde_json::from_str(&body).map_err(|err| AdapterError::MalformedResponse {
                message: format!("gemini decode error: {err}; body: {body}"),
            })?;

        // Check finishReason before extracting text
        if let Some(candidate) = parsed.candidates.first() {
            if let Some(reason) = &candidate.finish_reason {
                match reason.as_str() {
                    "STOP" | "" => {} // normal completion
                    "SAFETY" => {
                        return Err(AdapterError::Other(
                            "gemini blocked response due to safety filters".to_string(),
                        ));
                    }
                    "RECITATION" => {
                        return Err(AdapterError::Other(
                            "gemini blocked response due to recitation policy".to_string(),
                        ));
                    }
                    other => {
                        eprintln!("gemini unexpected finishReason: {other}");
                    }
                }
            }
        }

        let text = parsed
            .candidates
            .into_iter()
            .flat_map(|candidate| candidate.content.parts.into_iter())
            .find_map(|part| part.text)
            .ok_or_else(|| AdapterError::MalformedResponse {
                message: format!("gemini returned no text parts; body: {body}"),
            })?;
        let profile = serde_json::from_str::<StudentProfile>(&text).map_err(|err| {
            AdapterError::MalformedResponse {
                message: format!("gemini profile decode error: {err}; text: {text}"),
            }
        })?;
        Ok(Some(profile))
    }
}

impl DerivationProvider for GeminiSlideAnalyzer {
    fn provider_name(&self) -> &str {
        "gemini"
    }

    fn model_name(&self) -> &str {
        &self.model
    }

    fn provider_version(&self) -> &str {
        env!("CARGO_PKG_VERSION")
    }

    fn derive_student_profile(
        &self,
        observation: &Observation,
        blobs: &BlobStore,
    ) -> Result<Option<DerivedStudentProfile>, AdapterError> {
        let Some(profile) = self.extract_profile(observation, blobs)? else {
            return Ok(None);
        };
        Ok(Some(DerivedStudentProfile {
            profile,
            lineage: DerivationLineage {
                source_observation: observation.id.clone(),
                provider: self.provider_name().to_string(),
                model: self.model_name().to_string(),
                version: self.provider_version().to_string(),
                confidence: 1.0,
            },
        }))
    }
}

// ---------------------------------------------------------------------------
// Prompt construction (B + C)
// ---------------------------------------------------------------------------

/// JSON schema description for the extraction prompt.
/// Centralised here so that changes to StudentProfile fields are reflected
/// in a single place alongside the struct definition in types.rs.
fn extraction_json_schema() -> &'static str {
    r#"{
  "email": "Email address found on slide (or null)",
  "generated_email": "firstname.lastname@hlab.college (lowercase, romaji)",
  "name": "Name (Kanji/Yomigana)",
  "bio_text": "Full bio text",
  "profile_pic": {
    "coordinates": { "x": "<percentage 0-100 from left>", "y": "<percentage 0-100 from top>" },
    "description": "Visual description of the person in this photo",
    "url": null
  },
  "gallery_images": [{
    "coordinates": { "x": 80, "y": 80 },
    "description": "Specific text caption associated with this photo found on the slide. If no text is near the image, return null. Do NOT generate visual descriptions.",
    "url": null
  }],
  "properties": {
    "Nickname": "text",
    "Birthplace": "text (prefecture/country)",
    "DoB": "YYYY-MM-DD (or null)",
    "Major": "text",
    "Affiliation": "text",
    "MBTI": "text",
    "SNS": "URL or null",
    "Hobbies": ["array", "of", "strings"],
    "Interests": ["array", "of", "strings"],
    "Likes": ["array", "of", "strings"],
    "Dislikes": "text",
    "Hashtags": ["array", "of", "strings"],
    "New Challenges": "text",
    "Ask Me About": "text",
    "Turning Point": "text",
    "BTW": "text",
    "Message": "text"
  },
  "attributes": ["Array", "of", "tags", "or", "faculties"]
}"#
}

fn build_extraction_prompt(title: &str, canonical_uri: &str) -> String {
    format!(
        "\
Analyze this student self-introduction slide and return ONLY a raw JSON object.

Context: title={title}, canonical_uri={canonical_uri}

## Profile picture
Identify the PRIMARY photo that shows the student themselves (their face, \
portrait, or personal avatar). This is typically the largest person photo on \
the slide, or a photo explicitly labeled as a profile picture. Do NOT select \
group photos, landscape photos, pet photos, or hobby images.

The coordinates should point to the CENTER of that image as a percentage of \
the total slide dimensions (x: 0=left edge, 100=right edge; y: 0=top edge, \
100=bottom edge). If no clear personal photo exists, set profile_pic to null.

## Gallery images
List ALL other photos/images on the slide that are NOT the profile picture. \
These typically show hobbies, pets, scenery, food, etc.

## Hashtags
Hashtags may appear as a labeled section (e.g. \"ハッシュタグ:\") or as bare \
\"#tag\" entries scattered across the slide without any heading. Collect ALL \
hashtag-style entries (\"#旅行\", \"#音楽\", etc.) into the Hashtags array. \
Strip the leading '#' character from each value.

## Output schema
Extract this schema exactly:
{schema}",
        title = title,
        canonical_uri = canonical_uri,
        schema = extraction_json_schema(),
    )
}

// ---------------------------------------------------------------------------
// Response types
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct GeminiResponse {
    #[serde(default)]
    candidates: Vec<GeminiCandidate>,
}

#[derive(Debug, Deserialize)]
struct GeminiCandidate {
    content: GeminiContent,
    #[serde(default, rename = "finishReason")]
    finish_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GeminiContent {
    #[serde(default)]
    parts: Vec<GeminiPart>,
}

#[derive(Debug, Deserialize)]
struct GeminiPart {
    #[serde(default)]
    text: Option<String>,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompt_contains_schema_and_hashtag_instructions() {
        let prompt = build_extraction_prompt("Test Title", "https://example.com");
        assert!(prompt.contains("\"Hashtags\""));
        assert!(prompt.contains("\"Nickname\""));
        assert!(prompt.contains("#tag"));
        assert!(prompt.contains("title=Test Title"));
    }

    #[test]
    fn extraction_schema_is_valid_json() {
        let schema = extraction_json_schema();
        let parsed: serde_json::Value =
            serde_json::from_str(schema).expect("extraction_json_schema must be valid JSON");
        assert!(parsed.get("properties").is_some());
        assert!(parsed.get("email").is_some());
    }
}
