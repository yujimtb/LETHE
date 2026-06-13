use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use reqwest::blocking::Client;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE};
use serde::Deserialize;

use crate::adapter::error::AdapterError;
use crate::adapter::gslides::client::{
    GoogleSlidesClient, PageSize, PresentationMeta, PresentationNative, RenderedSlide,
    RevisionListPage, SlideNative, SlideRevision,
};
use crate::self_host::config::GoogleConfig;

#[derive(Clone)]
pub struct HttpGoogleSlidesClient {
    http: Client,
    auth: GoogleTokenSource,
}

impl HttpGoogleSlidesClient {
    pub fn new(config: &GoogleConfig) -> Result<Self, AdapterError> {
        let http = Client::builder()
            .build()
            .map_err(|err| AdapterError::Network {
                message: err.to_string(),
            })?;
        Ok(Self {
            http,
            auth: GoogleTokenSource::new(config.clone()),
        })
    }

    fn bearer_token(&self) -> Result<String, AdapterError> {
        self.auth.access_token(&self.http)
    }

    fn get_json<T: for<'de> Deserialize<'de>>(&self, url: &str) -> Result<T, AdapterError> {
        let mut token = self.bearer_token()?;

        for attempt in 0..2 {
            let response = self
                .http
                .get(url)
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .send()
                .map_err(|err| AdapterError::Network {
                    message: err.to_string(),
                })?;

            let status = response.status();

            if status == reqwest::StatusCode::UNAUTHORIZED {
                if attempt == 0 {
                    if let Some(refreshed) = self.auth.refresh_access_token(&self.http)? {
                        token = refreshed;
                        continue;
                    }
                }

                return Err(AdapterError::AuthFailure {
                    message: "google oauth token rejected".to_string(),
                });
            }

            let body = response.text().map_err(|err| AdapterError::Network {
                message: err.to_string(),
            })?;

            if !status.is_success() {
                return Err(AdapterError::MalformedResponse {
                    message: format!("google api {url} returned {status}: {body}"),
                });
            }

            return serde_json::from_str::<T>(&body).map_err(|err| AdapterError::MalformedResponse {
                message: format!("google api {url} decode error: {err}; body: {body}"),
            });
        }

        Err(AdapterError::AuthFailure {
            message: "google oauth token rejected".to_string(),
        })
    }

    fn get_bytes(&self, url: &str) -> Result<Vec<u8>, AdapterError> {
        let mut token = self.bearer_token()?;

        for attempt in 0..2 {
            let response = self
                .http
                .get(url)
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .send()
                .map_err(|err| AdapterError::Network {
                    message: err.to_string(),
                })?;

            let status = response.status();
            if status == reqwest::StatusCode::UNAUTHORIZED {
                if attempt == 0 {
                    if let Some(refreshed) = self.auth.refresh_access_token(&self.http)? {
                        token = refreshed;
                        continue;
                    }
                }

                return Err(AdapterError::AuthFailure {
                    message: "google oauth token rejected".to_string(),
                });
            }

            if !status.is_success() {
                let body = response.text().map_err(|err| AdapterError::Network {
                    message: err.to_string(),
                })?;
                return Err(AdapterError::MalformedResponse {
                    message: format!("google byte download {url} returned {status}: {body}"),
                });
            }

            return response
                .bytes()
                .map(|bytes| bytes.to_vec())
                .map_err(|err| AdapterError::Network {
                    message: err.to_string(),
                });
        }

        Err(AdapterError::AuthFailure {
            message: "google oauth token rejected".to_string(),
        })
    }

    fn post_json_value(
        &self,
        url: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value, AdapterError> {
        let mut token = self.bearer_token()?;

        for attempt in 0..2 {
            let response = self
                .http
                .post(url)
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .header(CONTENT_TYPE, "application/json")
                .json(body)
                .send()
                .map_err(|err| AdapterError::Network {
                    message: err.to_string(),
                })?;

            let status = response.status();
            if status == reqwest::StatusCode::UNAUTHORIZED {
                if attempt == 0 {
                    if let Some(refreshed) = self.auth.refresh_access_token(&self.http)? {
                        token = refreshed;
                        continue;
                    }
                }
                return Err(AdapterError::AuthFailure {
                    message: "google oauth token rejected".to_string(),
                });
            }

            let body_text = response.text().map_err(|err| AdapterError::Network {
                message: err.to_string(),
            })?;
            if !status.is_success() {
                return Err(AdapterError::MalformedResponse {
                    message: format!("google api {url} returned {status}: {body_text}"),
                });
            }
            return serde_json::from_str(&body_text).map_err(|err| AdapterError::MalformedResponse {
                message: format!("google api {url} decode error: {err}; body: {body_text}"),
            });
        }

        Err(AdapterError::AuthFailure {
            message: "google oauth token rejected".to_string(),
        })
    }

    fn delete_resource(&self, url: &str) -> Result<(), AdapterError> {
        let mut token = self.bearer_token()?;

        for attempt in 0..2 {
            let response = self
                .http
                .delete(url)
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .send()
                .map_err(|err| AdapterError::Network {
                    message: err.to_string(),
                })?;

            let status = response.status();
            if status == reqwest::StatusCode::UNAUTHORIZED {
                if attempt == 0 {
                    if let Some(refreshed) = self.auth.refresh_access_token(&self.http)? {
                        token = refreshed;
                        continue;
                    }
                }
                return Err(AdapterError::AuthFailure {
                    message: "google oauth token rejected".to_string(),
                });
            }

            if status.is_success() || status == reqwest::StatusCode::NO_CONTENT {
                return Ok(());
            }
            let body_text = response.text().map_err(|err| AdapterError::Network {
                message: err.to_string(),
            })?;
            return Err(AdapterError::MalformedResponse {
                message: format!("google api {url} returned {status}: {body_text}"),
            });
        }

        Err(AdapterError::AuthFailure {
            message: "google oauth token rejected".to_string(),
        })
    }
}

impl GoogleSlidesClient for HttpGoogleSlidesClient {
    fn list_revisions(
        &self,
        presentation_id: &str,
        page_token: Option<&str>,
    ) -> Result<RevisionListPage, AdapterError> {
        let mut url = format!(
            "https://www.googleapis.com/drive/v3/files/{presentation_id}/revisions?fields=revisions(id,modifiedTime,lastModifyingUser(displayName,emailAddress)),nextPageToken&pageSize=200"
        );
        if let Some(page_token) = page_token {
            url.push_str(&format!("&pageToken={page_token}"));
        }
        let response: RevisionsResponse = self.get_json(&url)?;
        Ok(RevisionListPage {
            revisions: response
                .revisions
                .into_iter()
                .map(|revision| SlideRevision {
                    presentation_id: presentation_id.to_string(),
                    revision_id: revision.id,
                    modified_time: revision.modified_time,
                    last_modifying_user: revision
                        .last_modifying_user
                        .and_then(|user| user.email_address.or(user.display_name)),
                })
                .collect(),
            next_page_token: response.next_page_token,
        })
    }

    fn get_presentation(
        &self,
        presentation_id: &str,
    ) -> Result<PresentationNative, AdapterError> {
        let response: PresentationResponse = self.get_json(&format!(
            "https://slides.googleapis.com/v1/presentations/{presentation_id}"
        ))?;
        Ok(PresentationNative {
            presentation_id: response.presentation_id,
            title: response.title,
            locale: response.locale,
            slides: response
                .slides
                .into_iter()
                .map(|slide| SlideNative {
                    object_id: slide.object_id,
                    page_elements: slide.page_elements,
                })
                .collect(),
            page_size: response.page_size.map(|page_size| PageSize {
                width_emu: page_size.width.magnitude.unwrap_or(0.0) as i64,
                height_emu: page_size.height.magnitude.unwrap_or(0.0) as i64,
            }),
        })
    }

    fn get_presentation_meta(
        &self,
        presentation_id: &str,
    ) -> Result<PresentationMeta, AdapterError> {
        let response: DriveFileResponse = self.get_json(&format!(
            "https://www.googleapis.com/drive/v3/files/{presentation_id}?fields=id,name,parents,webViewLink,owners(emailAddress),permissions(emailAddress,role)"
        ))?;
        let owner = response
            .owners
            .into_iter()
            .flatten()
            .find_map(|owner| owner.email_address);

        let mut editors = Vec::new();
        let mut viewers = Vec::new();
        for permission in response.permissions.into_iter().flatten() {
            match permission.role.as_deref() {
                Some("owner") | Some("writer") | Some("organizer") | Some("fileOrganizer") => {
                    if let Some(email) = permission.email_address {
                        editors.push(email);
                    }
                }
                Some("reader") | Some("commenter") => {
                    if let Some(email) = permission.email_address {
                        viewers.push(email);
                    }
                }
                _ => {}
            }
        }

        if let Some(owner_email) = owner.clone() {
            if !editors.contains(&owner_email) {
                editors.push(owner_email);
            }
        }

        Ok(PresentationMeta {
            presentation_id: response.id,
            title: response.name,
            container_id: response.parents.and_then(|mut parents| parents.drain(..).next()),
            canonical_uri: response.web_view_link.unwrap_or_else(|| {
                format!("https://docs.google.com/presentation/d/{presentation_id}")
            }),
            owner,
            editors,
            viewers,
        })
    }

    fn render_slide(
        &self,
        presentation_id: &str,
        slide_object_id: &str,
        format: &str,
    ) -> Result<RenderedSlide, AdapterError> {
        let mime_type = if format.eq_ignore_ascii_case("jpeg") || format.eq_ignore_ascii_case("jpg") {
            "JPEG"
        } else {
            "PNG"
        };
        let thumbnail: ThumbnailResponse = self.get_json(&format!(
            "https://slides.googleapis.com/v1/presentations/{presentation_id}/pages/{slide_object_id}/thumbnail?thumbnailProperties.mimeType={mime_type}&thumbnailProperties.thumbnailSize=LARGE"
        ))?;
        let content_url = thumbnail.content_url.ok_or_else(|| AdapterError::MalformedResponse {
            message: "missing thumbnail contentUrl".to_string(),
        })?;
        let data = self.get_bytes(&content_url)?;

        Ok(RenderedSlide {
            slide_object_id: slide_object_id.to_string(),
            format: format.to_string(),
            data,
            content_url: Some(content_url),
        })
    }

    fn download_bytes(&self, url: &str) -> Result<Vec<u8>, AdapterError> {
        self.get_bytes(url)
    }

    fn export_presentation_pptx(&self, presentation_id: &str) -> Result<Vec<u8>, AdapterError> {
        self.get_bytes(&format!(
            "https://www.googleapis.com/drive/v3/files/{presentation_id}/export?mimeType=application/vnd.openxmlformats-officedocument.presentationml.presentation"
        ))
    }

    fn export_single_slide_pptx(
        &self,
        presentation_id: &str,
        slide_object_id: &str,
    ) -> Result<Vec<u8>, AdapterError> {
        #[derive(Deserialize)]
        struct DriveCopyResponse {
            id: String,
        }

        let source = self.get_presentation(presentation_id)?;
        let slide_index = source
            .slides
            .iter()
            .position(|slide| slide.object_id == slide_object_id)
            .ok_or_else(|| {
                AdapterError::Other(format!(
                    "slide {slide_object_id} not found in presentation {presentation_id}"
                ))
            })?;

        let copied_value = self.post_json_value(
            &format!("https://www.googleapis.com/drive/v3/files/{presentation_id}/copy"),
            &serde_json::json!({
                "name": format!("lethe-temp-{presentation_id}-{slide_object_id}")
            }),
        )?;
        let copied: DriveCopyResponse =
            serde_json::from_value(copied_value).map_err(|err| AdapterError::MalformedResponse {
                message: format!("failed to decode drive copy response: {err}"),
            })?;

        let export_result = (|| -> Result<Vec<u8>, AdapterError> {
            let copied_presentation = self.get_presentation(&copied.id)?;
            let delete_requests = copied_presentation
                .slides
                .iter()
                .enumerate()
                .filter(|(index, _)| *index != slide_index)
                .map(|(_, slide)| {
                    serde_json::json!({
                        "deleteObject": { "objectId": slide.object_id }
                    })
                })
                .collect::<Vec<_>>();
            if !delete_requests.is_empty() {
                self.post_json_value(
                    &format!(
                        "https://slides.googleapis.com/v1/presentations/{}/:batchUpdate",
                        copied.id
                    ),
                    &serde_json::json!({ "requests": delete_requests }),
                )?;
            }
            self.export_presentation_pptx(&copied.id)
        })();

        if let Err(err) = self.delete_resource(&format!(
            "https://www.googleapis.com/drive/v3/files/{}",
            copied.id
        )) {
            eprintln!("failed to delete temporary presentation {}: {}", copied.id, err);
        }

        export_result
    }
}

#[derive(Clone)]
struct GoogleTokenSource {
    config: GoogleConfig,
    cached: Arc<Mutex<Option<CachedToken>>>,
}

#[derive(Clone)]
struct CachedToken {
    access_token: String,
    expires_at: Instant,
}

impl GoogleTokenSource {
    fn new(config: GoogleConfig) -> Self {
        Self {
            config,
            cached: Arc::new(Mutex::new(None)),
        }
    }

    fn access_token(&self, http: &Client) -> Result<String, AdapterError> {
        if let Some(cached) = self
            .cached
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
        {
            if cached.expires_at > Instant::now() {
                return Ok(cached.access_token);
            }
        }

        if let Some(token) = self.config.access_token.clone() {
            return Ok(token);
        }

        self.exchange_refresh_token(http)
    }

    fn refresh_access_token(&self, http: &Client) -> Result<Option<String>, AdapterError> {
        if !self.can_refresh() {
            return Ok(None);
        }

        self.clear_cached_token();
        self.exchange_refresh_token(http).map(Some)
    }

    fn can_refresh(&self) -> bool {
        self.config.client_id.is_some()
            && self.config.client_secret.is_some()
            && self.config.refresh_token.is_some()
    }

    fn clear_cached_token(&self) {
        *self
            .cached
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = None;
    }

    fn exchange_refresh_token(&self, http: &Client) -> Result<String, AdapterError> {

        let client_id = self.config.client_id.clone().ok_or_else(|| AdapterError::AuthFailure {
            message: "missing LETHE_GOOGLE_CLIENT_ID".to_string(),
        })?;
        let client_secret = self.config.client_secret.clone().ok_or_else(|| AdapterError::AuthFailure {
            message: "missing LETHE_GOOGLE_CLIENT_SECRET".to_string(),
        })?;
        let refresh_token = self.config.refresh_token.clone().ok_or_else(|| AdapterError::AuthFailure {
            message: "missing LETHE_GOOGLE_REFRESH_TOKEN".to_string(),
        })?;

        let token_response = http
            .post("https://oauth2.googleapis.com/token")
            .header(CONTENT_TYPE, "application/x-www-form-urlencoded")
            .form(&[
                ("client_id", client_id.as_str()),
                ("client_secret", client_secret.as_str()),
                ("refresh_token", refresh_token.as_str()),
                ("grant_type", "refresh_token"),
            ])
            .send()
            .map_err(|err| AdapterError::Network {
                message: err.to_string(),
            })?;

        if !token_response.status().is_success() {
            return Err(AdapterError::AuthFailure {
                message: format!("google oauth token exchange failed: {}", token_response.status()),
            });
        }

        let token: OAuthTokenResponse = token_response.json().map_err(|err| AdapterError::MalformedResponse {
            message: err.to_string(),
        })?;

        let access_token = token.access_token;
        let expires_in = token.expires_in.unwrap_or(3600).saturating_sub(60);
        *self
            .cached
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(CachedToken {
            access_token: access_token.clone(),
            expires_at: Instant::now() + Duration::from_secs(expires_in),
        });
        Ok(access_token)
    }
}

#[derive(Debug, Deserialize)]
struct OAuthTokenResponse {
    access_token: String,
    expires_in: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct RevisionsResponse {
    #[serde(default)]
    revisions: Vec<DriveRevision>,
    #[serde(default)]
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DriveRevision {
    id: String,
    modified_time: chrono::DateTime<chrono::Utc>,
    #[serde(default)]
    last_modifying_user: Option<DriveUser>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DriveUser {
    #[serde(default)]
    display_name: Option<String>,
    #[serde(default)]
    email_address: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PresentationResponse {
    presentation_id: String,
    title: String,
    locale: Option<String>,
    #[serde(default)]
    slides: Vec<PresentationSlide>,
    page_size: Option<PresentationPageSize>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PresentationSlide {
    object_id: String,
    #[serde(default)]
    page_elements: Vec<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PresentationPageSize {
    width: PresentationDimension,
    height: PresentationDimension,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PresentationDimension {
    magnitude: Option<f64>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DriveFileResponse {
    id: String,
    name: String,
    parents: Option<Vec<String>>,
    web_view_link: Option<String>,
    owners: Option<Vec<DriveOwner>>,
    permissions: Option<Vec<DrivePermission>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DriveOwner {
    email_address: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DrivePermission {
    email_address: Option<String>,
    role: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ThumbnailResponse {
    content_url: Option<String>,
}
