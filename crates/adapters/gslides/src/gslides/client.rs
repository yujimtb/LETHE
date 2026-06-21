//! M11 — Google Slides API client abstraction
//!
//! The trait is implemented by fixture stubs in tests and by a real
//! HTTP client in production.

use serde::{Deserialize, Serialize};

use lethe_adapter_api::error::AdapterError;

/// A Google Slides presentation's revision metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlideRevision {
    pub presentation_id: String,
    pub revision_id: String,
    pub modified_time: chrono::DateTime<chrono::Utc>,
    pub last_modifying_user: Option<String>,
}

/// Native presentation structure returned by presentations.get.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresentationNative {
    pub presentation_id: String,
    pub title: String,
    pub locale: Option<String>,
    pub slides: Vec<SlideNative>,
    pub page_size: Option<PageSize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlideNative {
    pub object_id: String,
    pub page_elements: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageSize {
    pub width_emu: i64,
    pub height_emu: i64,
}

/// Presentation metadata from Drive API.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresentationMeta {
    pub presentation_id: String,
    pub title: String,
    pub container_id: Option<String>,
    pub canonical_uri: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    pub editors: Vec<String>,
    pub viewers: Vec<String>,
}

/// A rendered thumbnail/export of a slide.
#[derive(Debug, Clone)]
pub struct RenderedSlide {
    pub slide_object_id: String,
    pub format: String,
    pub data: Vec<u8>,
    pub content_url: Option<String>,
}

/// Paginated revision list from Drive API.
#[derive(Debug, Clone)]
pub struct RevisionListPage {
    pub revisions: Vec<SlideRevision>,
    pub next_page_token: Option<String>,
}

/// Abstraction over Google Slides + Drive APIs.
pub trait GoogleSlidesClient {
    /// List revisions of a presentation (paginated).
    fn list_revisions(
        &self,
        presentation_id: &str,
        page_token: Option<&str>,
    ) -> Result<RevisionListPage, AdapterError>;

    /// Get the native JSON structure of a presentation at a revision.
    fn get_presentation(&self, presentation_id: &str) -> Result<PresentationNative, AdapterError>;

    /// Get Drive metadata for a presentation.
    fn get_presentation_meta(
        &self,
        presentation_id: &str,
    ) -> Result<PresentationMeta, AdapterError>;

    /// Export / render a slide as an image.
    fn render_slide(
        &self,
        presentation_id: &str,
        slide_object_id: &str,
        format: &str,
    ) -> Result<RenderedSlide, AdapterError>;

    /// Download arbitrary presentation-adjacent bytes (e.g. image contentUrl).
    fn download_bytes(&self, url: &str) -> Result<Vec<u8>, AdapterError>;

    /// Export an entire presentation as PPTX bytes.
    fn export_presentation_pptx(&self, presentation_id: &str) -> Result<Vec<u8>, AdapterError>;

    /// Export a single slide as a temporary one-slide PPTX.
    fn export_single_slide_pptx(
        &self,
        presentation_id: &str,
        slide_object_id: &str,
    ) -> Result<Vec<u8>, AdapterError>;
}

// ---------------------------------------------------------------------------
// Fixture client for testing
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
pub struct FixtureGoogleSlidesClient {
    pub revisions: Vec<SlideRevision>,
    pub presentations: std::collections::HashMap<String, PresentationNative>,
    pub metas: std::collections::HashMap<String, PresentationMeta>,
    pub rendered: std::collections::HashMap<(String, String), RenderedSlide>,
    pub exports: std::collections::HashMap<String, Vec<u8>>,
    pub single_slide_exports: std::collections::HashMap<(String, String), Vec<u8>>,
}

impl FixtureGoogleSlidesClient {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_revisions(mut self, revs: Vec<SlideRevision>) -> Self {
        self.revisions = revs;
        self
    }

    pub fn with_presentation(mut self, pres: PresentationNative) -> Self {
        self.presentations
            .insert(pres.presentation_id.clone(), pres);
        self
    }

    pub fn with_meta(mut self, meta: PresentationMeta) -> Self {
        self.metas.insert(meta.presentation_id.clone(), meta);
        self
    }

    pub fn with_rendered(mut self, pres_id: &str, slide_id: &str, rendered: RenderedSlide) -> Self {
        self.rendered
            .insert((pres_id.to_string(), slide_id.to_string()), rendered);
        self
    }

    pub fn with_exported_pptx(mut self, presentation_id: &str, bytes: Vec<u8>) -> Self {
        self.exports.insert(presentation_id.to_string(), bytes);
        self
    }

    pub fn with_exported_single_slide_pptx(
        mut self,
        presentation_id: &str,
        slide_object_id: &str,
        bytes: Vec<u8>,
    ) -> Self {
        self.single_slide_exports.insert(
            (presentation_id.to_string(), slide_object_id.to_string()),
            bytes,
        );
        self
    }
}

impl GoogleSlidesClient for FixtureGoogleSlidesClient {
    fn list_revisions(
        &self,
        _presentation_id: &str,
        _page_token: Option<&str>,
    ) -> Result<RevisionListPage, AdapterError> {
        Ok(RevisionListPage {
            revisions: self.revisions.clone(),
            next_page_token: None,
        })
    }

    fn get_presentation(&self, presentation_id: &str) -> Result<PresentationNative, AdapterError> {
        self.presentations
            .get(presentation_id)
            .cloned()
            .ok_or_else(|| AdapterError::Other(format!("presentation {presentation_id} not found")))
    }

    fn get_presentation_meta(
        &self,
        presentation_id: &str,
    ) -> Result<PresentationMeta, AdapterError> {
        self.metas
            .get(presentation_id)
            .cloned()
            .ok_or_else(|| AdapterError::Other(format!("meta for {presentation_id} not found")))
    }

    fn render_slide(
        &self,
        presentation_id: &str,
        slide_object_id: &str,
        _format: &str,
    ) -> Result<RenderedSlide, AdapterError> {
        let key = (presentation_id.to_string(), slide_object_id.to_string());
        self.rendered.get(&key).cloned().ok_or_else(|| {
            AdapterError::Other(format!(
                "rendered slide {presentation_id}/{slide_object_id} not found"
            ))
        })
    }

    fn download_bytes(&self, url: &str) -> Result<Vec<u8>, AdapterError> {
        self.rendered
            .values()
            .find(|rendered| rendered.content_url.as_deref() == Some(url))
            .map(|rendered| rendered.data.clone())
            .ok_or_else(|| AdapterError::Other(format!("download url {url} not found")))
    }

    fn export_presentation_pptx(&self, presentation_id: &str) -> Result<Vec<u8>, AdapterError> {
        self.exports.get(presentation_id).cloned().ok_or_else(|| {
            AdapterError::Other(format!("pptx export for {presentation_id} not found"))
        })
    }

    fn export_single_slide_pptx(
        &self,
        presentation_id: &str,
        slide_object_id: &str,
    ) -> Result<Vec<u8>, AdapterError> {
        self.single_slide_exports
            .get(&(presentation_id.to_string(), slide_object_id.to_string()))
            .cloned()
            .ok_or_else(|| {
                AdapterError::Other(format!(
                    "single-slide pptx export for {presentation_id}/{slide_object_id} not found"
                ))
            })
    }
}
