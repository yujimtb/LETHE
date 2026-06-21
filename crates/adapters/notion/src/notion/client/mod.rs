//! Notion API client and SaaSWriteAdapter implementation.
//!
//! Ported from skcollege_dictionary/NotionService.js — stacking update
//! algorithm, page property sync, and content block rendering.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;

use reqwest::blocking::multipart::{Form, Part};
use reqwest::blocking::{Client, Response};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderMap, HeaderValue};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use lethe_adapter_api::error::AdapterError;
use lethe_adapter_api::writeback::{SaaSWriteAdapter, WriteAction, WriteRecord, WriteResult};
use lethe_profile_model::{
    AttributeAliasCatalog, AttributeAliasDefinition, ImageCoordinates, StudentProfile,
    StudentProperties,
};

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Notion adapter configuration.
#[derive(Debug, Clone)]
pub struct NotionConfig {
    /// Notion integration token (Bearer).
    pub token: String,
    /// Target database ID for student directory pages.
    pub database_id: String,
    /// Local blob directory used to source Notion file uploads.
    pub blob_dir: Option<PathBuf>,
    /// Public base URL for blob-backed external images.
    pub public_base_url: Option<String>,
    /// Notion API version header.
    pub api_version: String,
}

impl NotionConfig {
    pub fn new(token: impl Into<String>, database_id: impl Into<String>) -> Self {
        Self {
            token: token.into(),
            database_id: database_id.into(),
            blob_dir: None,
            public_base_url: None,
            api_version: "2022-06-28".into(),
        }
    }

    pub fn with_blob_dir(mut self, blob_dir: impl Into<PathBuf>) -> Self {
        self.blob_dir = Some(blob_dir.into());
        self
    }

    pub fn with_public_base_url(mut self, public_base_url: Option<String>) -> Self {
        self.public_base_url = public_base_url;
        self
    }
}

// ---------------------------------------------------------------------------
// Notion Client
// ---------------------------------------------------------------------------

/// HTTP-based Notion API client implementing SaaSWriteAdapter.
#[derive(Clone)]
pub struct NotionClient {
    http: Client,
    config: NotionConfig,
    schema: DatabaseSchema,
}

#[derive(Debug, Clone)]
struct DatabaseSchema {
    title_property: String,
    email_property: Option<String>,
    properties: HashMap<String, NotionProperty>,
    actual_names_by_normalized: HashMap<String, String>,
}

impl DatabaseSchema {
    fn resolve_property(&self, candidates: &[&str]) -> Option<(&str, &NotionProperty)> {
        for candidate in candidates {
            let normalized = normalize_property_name(candidate);
            let Some(actual_name) = self.actual_names_by_normalized.get(&normalized) else {
                continue;
            };
            let Some(property) = self.properties.get(actual_name) else {
                continue;
            };
            return Some((actual_name.as_str(), property));
        }
        None
    }
}

impl NotionClient {
    const BASE_URL: &'static str = "https://api.notion.com/v1";
    const FILE_UPLOAD_API_VERSION: &'static str = "2026-03-11";

    pub fn new(config: NotionConfig) -> Result<Self, AdapterError> {
        let http = Client::builder()
            .build()
            .map_err(|err| AdapterError::Network {
                message: err.to_string(),
            })?;
        let schema = Self::load_database_schema(&http, &config)?;
        Ok(Self {
            http,
            config,
            schema,
        })
    }

    fn auth_headers_for_version(&self, api_version: &str) -> Result<HeaderMap, AdapterError> {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", self.config.token)).map_err(|err| {
                AdapterError::AuthFailure {
                    message: format!("invalid Notion bearer token header: {err}"),
                }
            })?,
        );
        headers.insert(
            "Notion-Version",
            HeaderValue::from_str(api_version).map_err(|err| {
                AdapterError::Other(format!("invalid Notion-Version header: {err}"))
            })?,
        );
        Ok(headers)
    }

    fn headers_for_version(&self, api_version: &str) -> Result<HeaderMap, AdapterError> {
        let mut headers = self.auth_headers_for_version(api_version)?;
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        Ok(headers)
    }

    /// Low-level API call.
    fn api_call<T: DeserializeOwned>(
        &self,
        method: &str,
        endpoint: &str,
        body: Option<&serde_json::Value>,
    ) -> Result<T, AdapterError> {
        self.api_call_with_version(method, endpoint, body, &self.config.api_version)
    }

    fn api_call_with_version<T: DeserializeOwned>(
        &self,
        method: &str,
        endpoint: &str,
        body: Option<&serde_json::Value>,
        api_version: &str,
    ) -> Result<T, AdapterError> {
        let url = format!("{}{}", Self::BASE_URL, endpoint);
        let request = match method {
            "GET" => self.http.get(&url),
            "POST" => self.http.post(&url),
            "PATCH" => self.http.patch(&url),
            "DELETE" => self.http.delete(&url),
            _ => return Err(AdapterError::Other(format!("unsupported method: {method}"))),
        };

        let request = request.headers(self.headers_for_version(api_version)?);
        let request = if let Some(body) = body {
            request.json(body)
        } else {
            request
        };

        let response = request.send().map_err(|err| AdapterError::Network {
            message: err.to_string(),
        })?;

        Self::decode_response(response)
    }

    fn decode_response<T: DeserializeOwned>(response: Response) -> Result<T, AdapterError> {
        let status = response.status();
        if status.as_u16() == 429 {
            return Err(AdapterError::RateLimited {
                retry_after_secs: 1,
            });
        }
        if status.as_u16() == 401 || status.as_u16() == 403 {
            return Err(AdapterError::AuthFailure {
                message: format!("Notion API {status}"),
            });
        }
        if status.is_client_error() || status.is_server_error() {
            let body_text = response.text().unwrap_or_default();
            return Err(AdapterError::Other(format!(
                "Notion API error ({status}): {body_text}"
            )));
        }

        response
            .json::<T>()
            .map_err(|err| AdapterError::MalformedResponse {
                message: err.to_string(),
            })
    }

    fn load_database_schema(
        http: &Client,
        config: &NotionConfig,
    ) -> Result<DatabaseSchema, AdapterError> {
        let client = Self {
            http: http.clone(),
            config: config.clone(),
            schema: DatabaseSchema {
                title_property: "Name".to_string(),
                email_property: Some("Email".to_string()),
                properties: HashMap::new(),
                actual_names_by_normalized: HashMap::new(),
            },
        };
        let database: NotionDatabase =
            client.api_call("GET", &format!("/databases/{}", config.database_id), None)?;

        let mut title_property = None;
        let mut email_property = None;
        let mut properties = HashMap::new();
        let mut actual_names_by_normalized = HashMap::new();

        for (name, property) in database.properties {
            actual_names_by_normalized
                .entry(normalize_property_name(&name))
                .or_insert_with(|| name.clone());
            if property.property_type == "title" && title_property.is_none() {
                title_property = Some(name.clone());
            }
            if property.property_type == "email" && email_property.is_none() {
                email_property = Some(name.clone());
            }
            properties.insert(name, property);
        }

        Ok(DatabaseSchema {
            title_property: title_property.unwrap_or_else(|| "Name".to_string()),
            email_property,
            properties,
            actual_names_by_normalized,
        })
    }

    fn find_page(
        &self,
        email: Option<&str>,
        title: &str,
    ) -> Result<Option<NotionPage>, AdapterError> {
        let filter = if let (Some(email), Some(email_property)) =
            (email, self.schema.email_property.as_ref())
        {
            serde_json::json!({
                "filter": {
                    "property": email_property,
                    "email": {
                        "equals": email,
                    }
                }
            })
        } else {
            serde_json::json!({
                "filter": {
                    "property": self.schema.title_property,
                    "title": {
                        "equals": title,
                    }
                }
            })
        };
        let result: NotionQueryResult = self.api_call(
            "POST",
            &format!("/databases/{}/query", self.config.database_id),
            Some(&filter),
        )?;
        Ok(result.results.into_iter().next())
    }

    /// Create a new page in the database.
    fn create_page(
        &self,
        properties: &serde_json::Value,
        cover: Option<&NotionMediaRef>,
        icon: Option<&NotionMediaRef>,
    ) -> Result<NotionPage, AdapterError> {
        let mut payload = serde_json::Map::new();
        payload.insert(
            "parent".to_string(),
            serde_json::json!({ "database_id": self.config.database_id }),
        );
        payload.insert("properties".to_string(), properties.clone());
        if let Some(cover) = cover {
            payload.insert("cover".to_string(), cover.to_page_value());
        }
        if let Some(icon) = icon {
            payload.insert("icon".to_string(), icon.to_page_value());
        }
        self.api_call_with_version(
            "POST",
            "/pages",
            Some(&serde_json::Value::Object(payload)),
            page_api_version_from_media_refs([cover, icon]),
        )
    }

    /// Update page properties and visuals.
    fn update_page(
        &self,
        page_id: &str,
        properties: &serde_json::Value,
        cover: Option<&NotionMediaRef>,
        icon: Option<&NotionMediaRef>,
    ) -> Result<(), AdapterError> {
        let mut payload = serde_json::Map::new();
        payload.insert("properties".to_string(), properties.clone());
        if let Some(cover) = cover {
            payload.insert("cover".to_string(), cover.to_page_value());
        }
        if let Some(icon) = icon {
            payload.insert("icon".to_string(), icon.to_page_value());
        }
        let _: serde_json::Value = self.api_call_with_version(
            "PATCH",
            &format!("/pages/{page_id}"),
            Some(&serde_json::Value::Object(payload)),
            page_api_version_from_media_refs([cover, icon]),
        )?;
        Ok(())
    }

    /// Get child blocks of a page/block.
    fn get_children(&self, block_id: &str) -> Result<Vec<NotionBlock>, AdapterError> {
        let result: NotionBlockChildren =
            self.api_call("GET", &format!("/blocks/{block_id}/children"), None)?;
        Ok(result.results)
    }

    /// Delete a block.
    fn delete_block(&self, block_id: &str) -> Result<(), AdapterError> {
        let _: serde_json::Value = self.api_call("DELETE", &format!("/blocks/{block_id}"), None)?;
        Ok(())
    }

    /// Append children blocks to a page/block.
    fn append_children(
        &self,
        block_id: &str,
        children: &[serde_json::Value],
    ) -> Result<Vec<NotionBlock>, AdapterError> {
        let payload = serde_json::json!({ "children": children });
        let api_version = if children.iter().any(value_contains_file_upload) {
            Self::FILE_UPLOAD_API_VERSION
        } else {
            &self.config.api_version
        };
        let result: NotionBlockChildren = self.api_call_with_version(
            "PATCH",
            &format!("/blocks/{block_id}/children"),
            Some(&payload),
            api_version,
        )?;
        Ok(result.results)
    }

    fn upload_file(
        &self,
        filename: &str,
        content_type: &str,
        bytes: &[u8],
    ) -> Result<String, AdapterError> {
        let create_payload = serde_json::json!({
            "filename": filename,
            "content_type": content_type,
            "content_length": bytes.len(),
        });
        let created: NotionFileUpload = self.api_call_with_version(
            "POST",
            "/file_uploads",
            Some(&create_payload),
            Self::FILE_UPLOAD_API_VERSION,
        )?;
        let upload_id = created.id;

        let file_part = Part::bytes(bytes.to_vec())
            .file_name(filename.to_string())
            .mime_str(content_type)
            .map_err(|err| AdapterError::Other(format!("invalid upload mime type: {err}")))?;
        let form = Form::new().part("file", file_part);
        let response = self
            .http
            .post(format!("{}/file_uploads/{upload_id}/send", Self::BASE_URL))
            .headers(self.auth_headers_for_version(Self::FILE_UPLOAD_API_VERSION)?)
            .multipart(form)
            .send()
            .map_err(|err| AdapterError::Network {
                message: err.to_string(),
            })?;
        let uploaded: NotionFileUpload = Self::decode_response(response)?;
        if uploaded.status != "uploaded" {
            return Err(AdapterError::Other(format!(
                "Notion file upload {upload_id} ended with unexpected status {}",
                uploaded.status
            )));
        }
        Ok(upload_id)
    }

    fn persist_blob_bytes(&self, bytes: &[u8]) -> Result<String, AdapterError> {
        let blob_dir = self.config.blob_dir.as_deref().ok_or_else(|| {
            AdapterError::Other(
                "Notion image materialization requires blob_dir in configuration".to_string(),
            )
        })?;
        fs::create_dir_all(blob_dir).map_err(|err| {
            AdapterError::Other(format!(
                "failed to create blob dir {}: {err}",
                blob_dir.display()
            ))
        })?;
        let hash = hex::encode(Sha256::digest(bytes));
        let blob_path = blob_dir.join(&hash);
        if !blob_path.exists() {
            fs::write(&blob_path, bytes).map_err(|err| {
                AdapterError::Other(format!(
                    "failed to persist cropped blob {}: {err}",
                    blob_path.display()
                ))
            })?;
        }
        Ok(hash)
    }

    fn load_blob_bytes(&self, blob_ref: &str) -> Result<Vec<u8>, AdapterError> {
        let hash = blob_ref_sha256(blob_ref).ok_or_else(|| {
            AdapterError::Other(format!("invalid thumbnail blob ref: {blob_ref}"))
        })?;
        let blob_dir = self.config.blob_dir.as_deref().ok_or_else(|| {
            AdapterError::Other("Notion file upload requires blob_dir in configuration".to_string())
        })?;
        let blob_path = blob_dir.join(hash);
        std::fs::read(&blob_path).map_err(|err| {
            AdapterError::Other(format!(
                "failed to read thumbnail blob {}: {err}",
                blob_path.display()
            ))
        })
    }

    fn materialize_image_bytes(
        &self,
        bytes: &[u8],
        filename_prefix: &str,
    ) -> Result<NotionMediaRef, AdapterError> {
        self.upload_image_bytes(bytes, filename_prefix)
    }

    fn upload_image_bytes(
        &self,
        bytes: &[u8],
        filename_prefix: &str,
    ) -> Result<NotionMediaRef, AdapterError> {
        let hash = self.persist_blob_bytes(bytes)?;
        let (extension, content_type) = image::guess_format(bytes)
            .map(|format| match format {
                image::ImageFormat::Jpeg => ("jpg", "image/jpeg"),
                image::ImageFormat::Gif => ("gif", "image/gif"),
                image::ImageFormat::WebP => ("webp", "image/webp"),
                _ => ("png", "image/png"),
            })
            .unwrap_or(("png", "image/png"));
        let upload_id = self.upload_file(
            &format!("{filename_prefix}-{}.{}", &hash[..8], extension),
            content_type,
            bytes,
        )?;
        Ok(NotionMediaRef::FileUpload(upload_id))
    }

    fn build_cover_media(
        &self,
        profile: &StudentProfile,
    ) -> Result<Option<NotionMediaRef>, AdapterError> {
        if let Some(blob_ref) = profile.thumbnail_blob_ref.as_deref() {
            let bytes = self.load_blob_bytes(blob_ref)?;
            return self
                .materialize_image_bytes(&bytes, "lethe-cover")
                .map(Some);
        }
        Ok(profile
            .thumbnail_url
            .as_deref()
            .filter(|url| url.starts_with("http"))
            .map(|url| NotionMediaRef::External(url.to_string())))
    }

    fn build_profile_icon_media(
        &self,
        profile: &StudentProfile,
        _thumbnail_bytes: Option<&[u8]>,
        source_images: &[SourceImageCandidate],
    ) -> Result<Option<NotionMediaRef>, AdapterError> {
        let Some(profile_pic) = profile.profile_pic.as_ref() else {
            return Ok(None);
        };
        if let Some(media_ref) = self.source_media_from_coordinates(
            profile_pic.coordinates.as_ref(),
            source_images,
            "lethe-profile",
        )? {
            return Ok(Some(media_ref));
        }
        if let Some(media_ref) =
            self.source_media_from_url(profile_pic.url.as_deref(), source_images, "lethe-profile")?
        {
            return Ok(Some(media_ref));
        }
        Ok(None)
    }

    fn build_gallery_media(
        &self,
        profile: &StudentProfile,
        _thumbnail_bytes: Option<&[u8]>,
        source_images: &[SourceImageCandidate],
    ) -> Result<Vec<(usize, NotionMediaRef)>, AdapterError> {
        let mut media = Vec::new();
        for (index, image) in profile.gallery_images.iter().enumerate().take(9) {
            if let Some(media_ref) = self.source_media_from_coordinates(
                image.coordinates.as_ref(),
                source_images,
                &format!("lethe-gallery-{index}"),
            )? {
                media.push((index, media_ref));
                continue;
            }
            if let Some(media_ref) = self.source_media_from_url(
                image.url.as_deref(),
                source_images,
                &format!("lethe-gallery-{index}"),
            )? {
                media.push((index, media_ref));
                continue;
            }
        }
        Ok(media)
    }

    fn source_media_from_coordinates(
        &self,
        coordinates: Option<&ImageCoordinates>,
        source_images: &[SourceImageCandidate],
        filename_prefix: &str,
    ) -> Result<Option<NotionMediaRef>, AdapterError> {
        let Some(coordinates) = coordinates else {
            return Ok(None);
        };
        let Some(candidate) = match_source_image_candidate(source_images, coordinates) else {
            return Ok(None);
        };
        let bytes = self.load_blob_bytes(&candidate.blob_ref)?;
        self.upload_image_bytes(&bytes, filename_prefix).map(Some)
    }

    fn source_media_from_url(
        &self,
        url: Option<&str>,
        source_images: &[SourceImageCandidate],
        filename_prefix: &str,
    ) -> Result<Option<NotionMediaRef>, AdapterError> {
        let Some(url) = url else {
            return Ok(None);
        };
        let Some(candidate) = source_images
            .iter()
            .find(|candidate| candidate.source_url == url)
        else {
            return Ok(None);
        };
        let bytes = self.load_blob_bytes(&candidate.blob_ref)?;
        self.upload_image_bytes(&bytes, filename_prefix).map(Some)
    }

    /// Replace the entire page body with the current derived projection blocks.
    fn stacking_update(
        &self,
        page_id: &str,
        children: &[serde_json::Value],
    ) -> Result<(), AdapterError> {
        let blocks = self.get_children(page_id)?;
        for block_id in blocks.iter().map(|block| block.id.as_str()) {
            self.delete_block(block_id)?;
        }
        if !children.is_empty() {
            self.append_children(page_id, children)?;
        }
        Ok(())
    }

    /// Convert student profile payload to Notion property updates.
    fn build_property_updates(
        &self,
        title: &str,
        payload: &serde_json::Value,
    ) -> serde_json::Value {
        let props = payload.get("properties").cloned().unwrap_or_default();
        let mut notion_props = serde_json::Map::new();

        if !title.trim().is_empty() {
            notion_props.insert(
                self.schema.title_property.clone(),
                serde_json::json!({
                    "title": [{ "text": { "content": title.trim() } }]
                }),
            );
        }

        if let Some(email_property) = &self.schema.email_property {
            if let Some(email) = payload
                .get("email")
                .and_then(|v| v.as_str())
                .or_else(|| payload.get("generated_email").and_then(|v| v.as_str()))
                .filter(|value| !value.trim().is_empty())
            {
                notion_props.insert(
                    email_property.clone(),
                    serde_json::json!({ "email": email.trim() }),
                );
            }
        }

        let add_text = |map: &mut serde_json::Map<String, serde_json::Value>,
                        key: &str,
                        value: Option<String>| {
            if let Some(value) = value.filter(|text| !text.trim().is_empty()) {
                map.insert(
                    key.to_string(),
                    serde_json::json!({ "rich_text": [{ "text": { "content": value } }] }),
                );
            }
        };

        let add_text_if_exists = |map: &mut serde_json::Map<String, serde_json::Value>,
                                  candidates: &[&str],
                                  value: Option<String>| {
            let Some(value) = value.filter(|text| !text.trim().is_empty()) else {
                return;
            };
            let Some((property_name, property)) = self.schema.resolve_property(candidates) else {
                return;
            };
            match property.property_type.as_str() {
                "url" if value.starts_with("http://") || value.starts_with("https://") => {
                    map.insert(
                        property_name.to_string(),
                        serde_json::json!({ "url": value }),
                    );
                }
                "email" if value.contains('@') => {
                    map.insert(
                        property_name.to_string(),
                        serde_json::json!({ "email": value }),
                    );
                }
                "date" => {
                    map.insert(
                        property_name.to_string(),
                        serde_json::json!({
                            "date": {
                                "start": value,
                            }
                        }),
                    );
                }
                "status" => {
                    map.insert(
                        property_name.to_string(),
                        serde_json::json!({
                            "status": {
                                "name": value,
                            }
                        }),
                    );
                }
                _ => add_text(map, property_name, Some(value)),
            }
        };

        let add_checkbox_if_exists = |map: &mut serde_json::Map<String, serde_json::Value>,
                                      candidates: &[&str],
                                      value: Option<bool>| {
            let Some(value) = value else {
                return;
            };
            let Some((property_name, property)) = self.schema.resolve_property(candidates) else {
                return;
            };
            if property.property_type == "checkbox" {
                map.insert(
                    property_name.to_string(),
                    serde_json::json!({ "checkbox": value }),
                );
            }
        };

        add_text_if_exists(
            &mut notion_props,
            &["Birthplace", "出身地"],
            json_text(&props["Birthplace"]),
        );
        add_text_if_exists(
            &mut notion_props,
            &["DoB", "生年月日"],
            json_text(&props["DoB"]),
        );

        let tag_str = json_list_text(props.get("Hashtags"));
        add_text_if_exists(
            &mut notion_props,
            &[
                "Hashtag",
                "Hashtags",
                "ハッシュタグ",
                "私を表すハッシュタグ",
            ],
            tag_str.clone(),
        );
        add_text_if_exists(
            &mut notion_props,
            &[
                "Hashtags",
                "Hashtag",
                "ハッシュタグ",
                "私を表すハッシュタグ",
            ],
            tag_str,
        );

        add_text_if_exists(
            &mut notion_props,
            &[
                "Major_Interests",
                "Major_interests",
                "専攻・興味分野",
                "専攻-興味分野",
            ],
            props
                .get("Major")
                .and_then(|v| v.as_str())
                .map(ToOwned::to_owned),
        );
        add_text_if_exists(
            &mut notion_props,
            &["Major", "専攻", "専攻分野"],
            props
                .get("Major")
                .and_then(|v| v.as_str())
                .map(ToOwned::to_owned),
        );

        add_text_if_exists(
            &mut notion_props,
            &["Nickname", "呼ばれたい名前", "あだ名", "通称"],
            json_text(&props["Nickname"]),
        );
        add_text_if_exists(
            &mut notion_props,
            &["Affiliation", "所属", "所属組織"],
            json_text(&props["Affiliation"]),
        );
        add_text_if_exists(&mut notion_props, &["MBTI"], json_text(&props["MBTI"]));
        add_text_if_exists(&mut notion_props, &["SNS"], json_text(&props["SNS"]));
        add_text_if_exists(
            &mut notion_props,
            &["Dislikes", "嫌いなもの", "嫌いなもの/こと", "苦手なもの"],
            json_text(&props["Dislikes"]),
        );
        add_text_if_exists(
            &mut notion_props,
            &["New Challenges", "カレッジで挑戦したいこと"],
            json_text(&props["New Challenges"]),
        );
        add_text_if_exists(
            &mut notion_props,
            &["Ask Me About", "カレッジ生に聞いてみたいこと"],
            json_text(&props["Ask Me About"]),
        );
        add_text_if_exists(
            &mut notion_props,
            &["Turning Point", "人生の転換期"],
            json_text(&props["Turning Point"]),
        );
        add_text_if_exists(
            &mut notion_props,
            &["BTW", "余談", "これ、余談なんですけど", "どうでもいいこと"],
            json_text(&props["BTW"]),
        );
        add_text_if_exists(
            &mut notion_props,
            &["Message", "一言", "ひとこと"],
            json_text(&props["Message"]),
        );

        add_text_if_exists(
            &mut notion_props,
            &["Hobbies", "趣味・特技", "趣味", "特技"],
            json_list_text(props.get("Hobbies")),
        );
        add_text_if_exists(
            &mut notion_props,
            &["Interests", "興味分野", "関心", "興味のあること"],
            json_list_text(props.get("Interests")),
        );
        add_text_if_exists(
            &mut notion_props,
            &["Likes", "好きなもの", "好きなこと", "好きなもの/こと"],
            json_list_text(props.get("Likes")),
        );

        if let Some(catalog) = load_attribute_alias_catalog() {
            for attribute in catalog.attributes {
                let value = catalog_attribute_value(&attribute, payload, &props);
                let candidates = catalog_property_candidates(&attribute);
                let candidate_refs = candidates.iter().map(String::as_str).collect::<Vec<_>>();
                add_text_if_exists(&mut notion_props, &candidate_refs, value);
            }
        }

        add_text_if_exists(
            &mut notion_props,
            &["LETHE Person ID"],
            metadata_value(payload, "person_id"),
        );
        add_text_if_exists(
            &mut notion_props,
            &["Source Slide URL"],
            metadata_str(payload, "source_slide_url")
                .or_else(|| {
                    payload
                        .get("source_canonical_uri")
                        .and_then(|value| value.as_str())
                })
                .map(ToOwned::to_owned),
        );
        add_text_if_exists(
            &mut notion_props,
            &["Last Synced At"],
            metadata_value(payload, "last_synced_at"),
        );
        add_text_if_exists(
            &mut notion_props,
            &["Projection Version"],
            metadata_value(payload, "projection_version"),
        );
        add_text_if_exists(
            &mut notion_props,
            &["Status"],
            metadata_value(payload, "status"),
        );
        add_checkbox_if_exists(
            &mut notion_props,
            &["Visibility"],
            metadata_bool(payload, "visibility"),
        );

        serde_json::Value::Object(notion_props)
    }
}

// ---------------------------------------------------------------------------
// SaaSWriteAdapter implementation
// ---------------------------------------------------------------------------

impl SaaSWriteAdapter for NotionClient {
    fn write_record(&self, record: &WriteRecord) -> Result<WriteResult, AdapterError> {
        let mut profile = serde_json::from_value::<StudentProfile>(record.payload.clone())
            .map_err(|err| AdapterError::MalformedResponse {
                message: format!("invalid StudentProfile payload for Notion write-back: {err}"),
            })?;
        profile.normalize_in_place();

        let email = profile
            .email
            .as_deref()
            .or(profile.generated_email.as_deref())
            .unwrap_or(&record.entity_id);

        let mut normalized_payload =
            serde_json::to_value(&profile).map_err(|err| AdapterError::MalformedResponse {
                message: format!("failed to re-serialize normalized StudentProfile: {err}"),
            })?;
        if let (Some(target), Some(metadata)) = (
            normalized_payload.as_object_mut(),
            record.payload.get("_lethe"),
        ) {
            target.insert("_lethe".to_string(), metadata.clone());
        }
        if let (Some(target), Some(source_images)) = (
            normalized_payload.as_object_mut(),
            record.payload.get("_lethe_source_images"),
        ) {
            target.insert("_lethe_source_images".to_string(), source_images.clone());
        }

        let thumbnail_bytes = profile
            .thumbnail_blob_ref
            .as_deref()
            .map(|blob_ref| self.load_blob_bytes(blob_ref))
            .transpose()?;
        let source_images = source_image_candidates(&normalized_payload);
        let cover = self.build_cover_media(&profile)?;
        let icon =
            self.build_profile_icon_media(&profile, thumbnail_bytes.as_deref(), &source_images)?;
        let gallery =
            self.build_gallery_media(&profile, thumbnail_bytes.as_deref(), &source_images)?;

        // Find or create the page
        let (page_id, action) = if let Some(ext_id) = &record.external_id {
            (ext_id.clone(), WriteAction::Updated)
        } else {
            match self.find_page(Some(email), &record.title)? {
                Some(page) => (page.id.clone(), WriteAction::Updated),
                None => (String::new(), WriteAction::Created),
            }
        };

        let property_updates = self.build_property_updates(&record.title, &normalized_payload);
        let body_blocks = build_page_blocks(
            &profile,
            icon.as_ref(),
            &gallery,
            profile
                .source_canonical_uri
                .as_deref()
                .or_else(|| metadata_str(&normalized_payload, "source_slide_url")),
        );

        let page_id = if action == WriteAction::Created {
            let page = self.create_page(&property_updates, cover.as_ref(), icon.as_ref())?;
            page.id
        } else {
            if property_updates.as_object().is_some_and(|m| !m.is_empty())
                || cover.is_some()
                || icon.is_some()
            {
                self.update_page(&page_id, &property_updates, cover.as_ref(), icon.as_ref())?;
            }
            page_id
        };

        if action == WriteAction::Created && body_blocks.is_empty() {
            // no-op; keep the created page with header properties only
        }

        self.stacking_update(&page_id, &body_blocks)?;

        let url = format!("https://www.notion.so/{}", page_id.replace('-', ""));
        Ok(WriteResult {
            external_id: page_id,
            action,
            url: Some(url),
        })
    }

    fn find_existing(&self, entity_id: &str) -> Result<Option<String>, AdapterError> {
        Ok(self.find_page(Some(entity_id), entity_id)?.map(|p| p.id))
    }

    fn delete_record(&self, external_id: &str) -> Result<(), AdapterError> {
        // Notion "deletes" by archiving a page
        let payload = serde_json::json!({ "in_trash": true });
        let _: serde_json::Value = self.api_call_with_version(
            "PATCH",
            &format!("/pages/{external_id}"),
            Some(&payload),
            Self::FILE_UPLOAD_API_VERSION,
        )?;
        Ok(())
    }

    fn adapter_name(&self) -> &str {
        "notion"
    }
}

// ---------------------------------------------------------------------------
// Notion API response types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
pub struct NotionPage {
    pub id: String,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub properties: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
struct NotionQueryResult {
    #[serde(default)]
    results: Vec<NotionPage>,
}

#[derive(Debug, Clone, Deserialize)]
struct NotionFileUpload {
    id: String,
    status: String,
}

#[derive(Debug, Clone, Deserialize)]
struct NotionDatabase {
    #[serde(default)]
    properties: HashMap<String, NotionProperty>,
}

#[derive(Debug, Clone, Deserialize)]
struct NotionProperty {
    #[serde(rename = "type")]
    property_type: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NotionBlock {
    pub id: String,
    #[serde(rename = "type", default)]
    pub block_type: String,
    #[serde(flatten)]
    pub raw: serde_json::Value,
}

#[derive(Debug, Clone, Deserialize)]
struct NotionBlockChildren {
    #[serde(default)]
    results: Vec<NotionBlock>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct SourceImageCandidate {
    object_id: String,
    source_url: String,
    blob_ref: String,
    center_x_pct: f64,
    center_y_pct: f64,
}

mod media;
mod page_blocks;

use media::*;
use page_blocks::*;

#[cfg(test)]
mod tests;
