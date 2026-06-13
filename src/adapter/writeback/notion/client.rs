//! Notion API client and SaaSWriteAdapter implementation.
//!
//! Ported from skcollege_dictionary/NotionService.js — stacking update
//! algorithm, page property sync, and content block rendering.

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;

use reqwest::blocking::multipart::{Form, Part};
use reqwest::blocking::{Client, Response};
use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::adapter::error::AdapterError;
use crate::attribute_inventory::{AttributeAliasCatalog, AttributeAliasDefinition};
use crate::adapter::writeback::traits::{
    SaaSWriteAdapter, WriteAction, WriteRecord, WriteResult,
};
use crate::slide_analysis::types::{ImageCoordinates, StudentProfile, StudentProperties};

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
        Ok(Self { http, config, schema })
    }

    fn auth_headers_for_version(&self, api_version: &str) -> Result<HeaderMap, AdapterError> {
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {}", self.config.token))
                .map_err(|err| AdapterError::AuthFailure {
                    message: format!("invalid Notion bearer token header: {err}"),
                })?,
        );
        headers.insert(
            "Notion-Version",
            HeaderValue::from_str(api_version)
                .map_err(|err| AdapterError::Other(format!(
                    "invalid Notion-Version header: {err}"
                )))?,
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

        response.json::<T>().map_err(|err| AdapterError::MalformedResponse {
            message: err.to_string(),
        })
    }

    fn load_database_schema(http: &Client, config: &NotionConfig) -> Result<DatabaseSchema, AdapterError> {
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
        let database: NotionDatabase = client.api_call("GET", &format!("/databases/{}", config.database_id), None)?;

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

    fn find_page(&self, email: Option<&str>, title: &str) -> Result<Option<NotionPage>, AdapterError> {
        let filter = if let (Some(email), Some(email_property)) = (email, self.schema.email_property.as_ref()) {
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
        let result: NotionQueryResult =
            self.api_call("POST", &format!("/databases/{}/query", self.config.database_id), Some(&filter))?;
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

    fn upload_file(&self, filename: &str, content_type: &str, bytes: &[u8]) -> Result<String, AdapterError> {
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
            AdapterError::Other("Notion image materialization requires blob_dir in configuration".to_string())
        })?;
        fs::create_dir_all(blob_dir).map_err(|err| {
            AdapterError::Other(format!("failed to create blob dir {}: {err}", blob_dir.display()))
        })?;
        let hash = hex::encode(Sha256::digest(bytes));
        let blob_path = blob_dir.join(&hash);
        if !blob_path.exists() {
            fs::write(&blob_path, bytes).map_err(|err| {
                AdapterError::Other(format!("failed to persist cropped blob {}: {err}", blob_path.display()))
            })?;
        }
        Ok(hash)
    }

    fn load_blob_bytes(&self, blob_ref: &str) -> Result<Vec<u8>, AdapterError> {
        let hash = blob_ref_sha256(blob_ref)
            .ok_or_else(|| AdapterError::Other(format!("invalid thumbnail blob ref: {blob_ref}")))?;
        let blob_dir = self.config.blob_dir.as_deref().ok_or_else(|| {
            AdapterError::Other(
                "Notion file upload requires blob_dir in configuration".to_string(),
            )
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

    fn build_cover_media(&self, profile: &StudentProfile) -> Result<Option<NotionMediaRef>, AdapterError> {
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
        let Some(candidate) = source_images.iter().find(|candidate| candidate.source_url == url) else {
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
    fn build_property_updates(&self, title: &str, payload: &serde_json::Value) -> serde_json::Value {
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

        let add_text = |map: &mut serde_json::Map<String, serde_json::Value>, key: &str, value: Option<String>| {
            if let Some(value) = value.filter(|text| !text.trim().is_empty()) {
                map.insert(
                    key.to_string(),
                    serde_json::json!({ "rich_text": [{ "text": { "content": value } }] }),
                );
            }
        };

        let add_text_if_exists = |
            map: &mut serde_json::Map<String, serde_json::Value>,
            candidates: &[&str],
            value: Option<String>,
        | {
            let Some(value) = value.filter(|text| !text.trim().is_empty()) else {
                return;
            };
            let Some((property_name, property)) = self.schema.resolve_property(candidates) else {
                return;
            };
            match property.property_type.as_str() {
                "url" if value.starts_with("http://") || value.starts_with("https://") => {
                    map.insert(property_name.to_string(), serde_json::json!({ "url": value }));
                }
                "email" if value.contains('@') => {
                    map.insert(property_name.to_string(), serde_json::json!({ "email": value }));
                }
                "date" => {
                    map.insert(property_name.to_string(), serde_json::json!({
                        "date": {
                            "start": value,
                        }
                    }));
                }
                "status" => {
                    map.insert(property_name.to_string(), serde_json::json!({
                        "status": {
                            "name": value,
                        }
                    }));
                }
                _ => add_text(map, property_name, Some(value)),
            }
        };

        let add_checkbox_if_exists = |
            map: &mut serde_json::Map<String, serde_json::Value>,
            candidates: &[&str],
            value: Option<bool>,
        | {
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

        add_text_if_exists(&mut notion_props, &["Birthplace", "出身地"], json_text(&props["Birthplace"]));
        add_text_if_exists(&mut notion_props, &["DoB", "生年月日"], json_text(&props["DoB"]));

        let tag_str = json_list_text(props.get("Hashtags"));
        add_text_if_exists(
            &mut notion_props,
            &["Hashtag", "Hashtags", "ハッシュタグ", "私を表すハッシュタグ"],
            tag_str.clone(),
        );
        add_text_if_exists(
            &mut notion_props,
            &["Hashtags", "Hashtag", "ハッシュタグ", "私を表すハッシュタグ"],
            tag_str,
        );

        add_text_if_exists(
            &mut notion_props,
            &["Major_Interests", "Major_interests", "専攻・興味分野", "専攻-興味分野"],
            props.get("Major").and_then(|v| v.as_str()).map(ToOwned::to_owned),
        );
        add_text_if_exists(
            &mut notion_props,
            &["Major", "専攻", "専攻分野"],
            props.get("Major").and_then(|v| v.as_str()).map(ToOwned::to_owned),
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
                .or_else(|| payload.get("source_canonical_uri").and_then(|value| value.as_str()))
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
        let mut profile = serde_json::from_value::<StudentProfile>(record.payload.clone()).map_err(|err| {
            AdapterError::MalformedResponse {
                message: format!("invalid StudentProfile payload for Notion write-back: {err}"),
            }
        })?;
        profile.normalize_in_place();

        let email = profile
            .email
            .as_deref()
            .or(profile.generated_email.as_deref())
            .unwrap_or(&record.entity_id);

        let mut normalized_payload = serde_json::to_value(&profile).map_err(|err| AdapterError::MalformedResponse {
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
                None => {
                    (String::new(), WriteAction::Created)
                }
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
            if property_updates.as_object().is_some_and(|m| !m.is_empty()) || cover.is_some() || icon.is_some() {
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

fn blob_ref_sha256(blob_ref: &str) -> Option<&str> {
    let hash = blob_ref.strip_prefix("blob:sha256:")?;
    if hash.len() == 64 && hash.chars().all(|ch| ch.is_ascii_hexdigit()) {
        Some(hash)
    } else {
        None
    }
}

fn source_image_candidates(payload: &serde_json::Value) -> Vec<SourceImageCandidate> {
    payload
        .get("_lethe_source_images")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or_default()
}

fn match_source_image_candidate<'a>(
    candidates: &'a [SourceImageCandidate],
    coordinates: &ImageCoordinates,
) -> Option<&'a SourceImageCandidate> {
    let target_x = normalize_image_selection_coordinate(coordinates.x)?;
    let target_y = normalize_image_selection_coordinate(coordinates.y)?;
    candidates.iter().min_by(|left, right| {
        let left_distance = squared_distance(left.center_x_pct, left.center_y_pct, target_x, target_y);
        let right_distance =
            squared_distance(right.center_x_pct, right.center_y_pct, target_x, target_y);
        left_distance.total_cmp(&right_distance)
    })
}

fn normalize_image_selection_coordinate(value: f64) -> Option<f64> {
    if value < 0.0 {
        return None;
    }
    if value <= 100.0 {
        Some(value)
    } else if value <= 1000.0 {
        Some(value / 10.0)
    } else {
        None
    }
}

fn squared_distance(left_x: f64, left_y: f64, right_x: f64, right_y: f64) -> f64 {
    let dx = left_x - right_x;
    let dy = left_y - right_y;
    (dx * dx) + (dy * dy)
}

// ---------------------------------------------------------------------------
// Helpers: content block rendering
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum NotionMediaRef {
    External(String),
    FileUpload(String),
}

impl NotionMediaRef {
    fn to_page_value(&self) -> serde_json::Value {
        match self {
            Self::External(url) => serde_json::json!({
                "type": "external",
                "external": { "url": url }
            }),
            Self::FileUpload(id) => serde_json::json!({
                "type": "file_upload",
                "file_upload": { "id": id }
            }),
        }
    }

    fn to_image_block(&self) -> serde_json::Value {
        match self {
            Self::External(url) => serde_json::json!({
                "object": "block",
                "type": "image",
                "image": {
                    "type": "external",
                    "external": { "url": url }
                }
            }),
            Self::FileUpload(id) => serde_json::json!({
                "object": "block",
                "type": "image",
                "image": {
                    "type": "file_upload",
                    "file_upload": { "id": id }
                }
            }),
        }
    }
}

fn page_api_version_from_media_refs<'a, const N: usize>(
    refs: [Option<&'a NotionMediaRef>; N],
) -> &'static str {
    if refs.into_iter().flatten().any(|value| matches!(value, NotionMediaRef::FileUpload(_))) {
        NotionClient::FILE_UPLOAD_API_VERSION
    } else {
        "2022-06-28"
    }
}

fn value_contains_file_upload(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(map) => {
            map.get("type").and_then(|ty| ty.as_str()) == Some("file_upload")
                || map.values().any(value_contains_file_upload)
        }
        serde_json::Value::Array(values) => values.iter().any(value_contains_file_upload),
        _ => false,
    }
}

fn build_page_blocks(
    profile: &StudentProfile,
    profile_pic: Option<&NotionMediaRef>,
    gallery_urls: &[(usize, NotionMediaRef)],
    source_url: Option<&str>,
) -> Vec<serde_json::Value> {
    let mut sections = Vec::new();

    if let Some(section) = build_bio_section(profile.bio_text.as_deref()) {
        sections.push(section);
    }
    if let Some(section) = build_about_section(&profile.properties, profile_pic) {
        sections.push(section);
    }
    if let Some(section) = build_highlights_section(&profile.properties) {
        sections.push(section);
    }
    if let Some(section) = build_toggle_section(&profile.properties) {
        sections.push(section);
    }
    if let Some(section) = build_gallery_section(profile, gallery_urls) {
        sections.push(section);
    }
    if let Some(section) = build_source_section(source_url.or(profile.source_canonical_uri.as_deref())) {
        sections.push(section);
    }

    interleave_dividers(sections)
}

fn build_bio_section(bio_text: Option<&str>) -> Option<Vec<serde_json::Value>> {
    let bio_text = bio_text.map(str::trim).filter(|text| !text.is_empty())?;
    Some(vec![serde_json::json!({
        "object": "block",
        "type": "callout",
        "callout": {
            "icon": { "type": "emoji", "emoji": "💬" },
            "rich_text": [plain_rich_text(bio_text)],
            "color": "gray_background"
        }
    })])
}

fn build_about_section(
    properties: &StudentProperties,
    profile_pic: Option<&NotionMediaRef>,
) -> Option<Vec<serde_json::Value>> {
    let rows = build_about_rows(properties);
    if rows.is_empty() && profile_pic.is_none() {
        return None;
    }

    let mut section = vec![heading_2_block("About")];
    if let Some(profile_pic) = profile_pic {
        section.push(serde_json::json!({
            "object": "block",
            "type": "column_list",
            "column_list": {
                "children": [
                    {
                        "object": "block",
                        "type": "column",
                        "column": { "children": [profile_pic.to_image_block()] }
                    },
                    {
                        "object": "block",
                        "type": "column",
                        "column": { "children": [about_table_block(&rows)] }
                    }
                ]
            }
        }));
    } else {
        section.push(about_table_block(&rows));
    }
    Some(section)
}

fn build_about_rows(properties: &StudentProperties) -> Vec<(String, AboutValue)> {
    let mut rows = Vec::new();
    if let Some(value) = properties.nickname.as_deref().filter(|value| !value.is_empty()) {
        rows.push(("呼び名".to_string(), AboutValue::Text(value.to_string())));
    }
    if let Some(value) = properties.birthplace.as_deref().filter(|value| !value.is_empty()) {
        rows.push(("出身".to_string(), AboutValue::Text(value.to_string())));
    }
    if let Some(value) = properties.dob.as_deref().filter(|value| !value.is_empty()) {
        rows.push(("誕生日".to_string(), AboutValue::Text(value.to_string())));
    }
    if let Some(value) = properties.major.as_deref().filter(|value| !value.is_empty()) {
        rows.push(("専攻".to_string(), AboutValue::Text(value.to_string())));
    }
    if let Some(value) = properties.affiliation.as_deref().filter(|value| !value.is_empty()) {
        rows.push(("所属".to_string(), AboutValue::Text(value.to_string())));
    }
    if let Some(value) = properties.mbti.as_deref().filter(|value| !value.is_empty()) {
        rows.push(("MBTI".to_string(), AboutValue::Text(value.to_string())));
    }
    if let Some(value) = properties.sns.as_deref().filter(|value| !value.is_empty()) {
        rows.push((
            "SNS".to_string(),
            if value.starts_with("http://") || value.starts_with("https://") {
                AboutValue::Link(value.to_string())
            } else {
                AboutValue::Text(value.to_string())
            },
        ));
    }
    rows
}

fn about_table_block(rows: &[(String, AboutValue)]) -> serde_json::Value {
    serde_json::json!({
        "object": "block",
        "type": "table",
        "table": {
            "table_width": 2,
            "has_column_header": false,
            "has_row_header": false,
            "children": rows.iter().map(|(label, value)| {
                serde_json::json!({
                    "object": "block",
                    "type": "table_row",
                    "table_row": {
                        "cells": [
                            [bold_rich_text(label)],
                            [about_value_rich_text(value)]
                        ]
                    }
                })
            }).collect::<Vec<_>>()
        }
    })
}

fn build_highlights_section(properties: &StudentProperties) -> Option<Vec<serde_json::Value>> {
    let mut section = vec![heading_2_block("Highlights")];
    let mut has_content = false;
    for (emoji, label, values) in [
        ("🎯", "Hobbies", &properties.hobbies),
        ("🔍", "Interests", &properties.interests),
        ("❤️", "Likes", &properties.likes),
    ] {
        let Some(values) = combine_list_texts([values.clone()]) else {
            continue;
        };
        has_content = true;
        section.push(serde_json::json!({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    bold_rich_text(&format!("{emoji} {label}: ")),
                    plain_rich_text(&values)
                ]
            }
        }));
    }
    has_content.then_some(section)
}

fn build_toggle_section(properties: &StudentProperties) -> Option<Vec<serde_json::Value>> {
    let mut toggles = Vec::new();
    for (emoji, title, value) in [
        ("🚀", "New Challenges", properties.new_challenges.as_deref()),
        ("💡", "Ask Me About", properties.ask_me_about.as_deref()),
        ("🔄", "Turning Point", properties.turning_point.as_deref()),
        ("💭", "BTW", properties.btw.as_deref()),
        ("✉️", "Message", properties.message.as_deref()),
        ("🙅", "Dislikes", properties.dislikes.as_deref()),
    ] {
        let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
            continue;
        };
        toggles.push(serde_json::json!({
            "object": "block",
            "type": "toggle",
            "toggle": {
                "rich_text": [bold_rich_text(&format!("{emoji} {title}"))],
                "children": [{
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [plain_rich_text(value)]
                    }
                }]
            }
        }));
    }
    (!toggles.is_empty()).then_some(toggles)
}

fn build_gallery_section(
    profile: &StudentProfile,
    gallery_urls: &[(usize, NotionMediaRef)],
) -> Option<Vec<serde_json::Value>> {
    let gallery_urls = &gallery_urls[..gallery_urls.len().min(9)];
    if gallery_urls.is_empty() {
        return None;
    }

    let mut section = vec![heading_2_block("Gallery")];
    for chunk in gallery_urls.chunks(3) {
        let mut columns = Vec::new();
        let mut single_column_children = None;
        for (index, media) in chunk {
            let Some(image) = profile.gallery_images.get(*index) else {
                continue;
            };
            let mut children = vec![media.to_image_block()];
            if let Some(description) = image.description.as_deref().filter(|value| !value.trim().is_empty()) {
                children.push(serde_json::json!({
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{
                            "type": "text",
                            "text": { "content": truncate_rich_text_content(description) },
                            "annotations": { "italic": true, "color": "gray" }
                        }]
                    }
                }));
            }
            if chunk.len() == 1 {
                single_column_children = Some(children);
                continue;
            }
            columns.push(serde_json::json!({
                "object": "block",
                "type": "column",
                "column": { "children": children }
            }));
        }
        if let Some(children) = single_column_children {
            section.extend(children);
            continue;
        }
        if !columns.is_empty() {
            section.push(serde_json::json!({
                "object": "block",
                "type": "column_list",
                "column_list": { "children": columns }
            }));
        }
    }
    Some(section)
}

fn build_source_section(source_url: Option<&str>) -> Option<Vec<serde_json::Value>> {
    let source_url = source_url.map(str::trim).filter(|value| !value.is_empty())?;
    Some(vec![
        heading_2_block("Source"),
        serde_json::json!({
            "object": "block",
            "type": "bookmark",
            "bookmark": {
                "url": source_url,
                "caption": [plain_rich_text("Google Slides — 自己紹介スライド原本")]
            }
        }),
    ])
}

fn interleave_dividers(sections: Vec<Vec<serde_json::Value>>) -> Vec<serde_json::Value> {
    let mut blocks = Vec::new();
    for (index, section) in sections.into_iter().enumerate() {
        if index > 0 {
            blocks.push(serde_json::json!({
                "object": "block",
                "type": "divider",
                "divider": {}
            }));
        }
        blocks.extend(section);
    }
    blocks
}

fn heading_2_block(title: &str) -> serde_json::Value {
    serde_json::json!({
        "object": "block",
        "type": "heading_2",
        "heading_2": {
            "rich_text": [plain_rich_text(title)]
        }
    })
}

fn plain_rich_text(content: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "text",
        "text": { "content": truncate_rich_text_content(content) }
    })
}

fn bold_rich_text(content: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "text",
        "text": { "content": truncate_rich_text_content(content) },
        "annotations": { "bold": true }
    })
}

fn about_value_rich_text(value: &AboutValue) -> serde_json::Value {
    match value {
        AboutValue::Text(text) => plain_rich_text(text),
        AboutValue::Link(url) => serde_json::json!({
            "type": "text",
            "text": {
                "content": truncate_rich_text_content(url),
                "link": { "url": url }
            }
        }),
    }
}

fn truncate_rich_text_content(content: &str) -> String {
    const MAX_CHARS: usize = 2000;
    let mut chars = content.chars();
    let truncated = chars.by_ref().take(MAX_CHARS).collect::<String>();
    if chars.next().is_some() {
        let mut shortened = truncated.chars().take(MAX_CHARS.saturating_sub(1)).collect::<String>();
        shortened.push('…');
        shortened
    } else {
        truncated
    }
}

enum AboutValue {
    Text(String),
    Link(String),
}

fn json_text(value: &serde_json::Value) -> Option<String> {
    value.as_str().map(str::trim).filter(|value| !value.is_empty()).map(str::to_string)
}

fn json_list_values(value: Option<&serde_json::Value>) -> Vec<String> {
    let Some(value) = value else {
        return Vec::new();
    };
    if let Some(array) = value.as_array() {
        array
            .iter()
            .filter_map(|item| item.as_str())
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(str::to_string)
            .collect()
    } else {
        json_text(value).into_iter().collect()
    }
}

fn json_list_text(value: Option<&serde_json::Value>) -> Option<String> {
    combine_list_texts([json_list_values(value)])
}

fn combine_list_texts<const N: usize>(groups: [Vec<String>; N]) -> Option<String> {
    let mut seen = HashSet::new();
    let mut merged = Vec::new();
    for value in groups.into_iter().flatten() {
        let key = normalize_property_name(&value);
        if key.is_empty() || !seen.insert(key) {
            continue;
        }
        merged.push(value);
    }
    if merged.is_empty() {
        None
    } else {
        Some(merged.join(", "))
    }
}

fn load_attribute_alias_catalog() -> Option<AttributeAliasCatalog> {
    let path = PathBuf::from("data").join("attribute_alias_catalog.json");
    let bytes = fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn catalog_property_candidates(attribute: &AttributeAliasDefinition) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut candidates = Vec::new();
    for candidate in std::iter::once(attribute.display_name.as_str())
        .chain(std::iter::once(attribute.id.as_str()))
        .chain(attribute.aliases.iter().map(String::as_str))
    {
        let normalized = normalize_property_name(candidate);
        if normalized.is_empty() || !seen.insert(normalized) {
            continue;
        }
        candidates.push(candidate.to_string());
    }
    candidates
}

fn catalog_attribute_value(
    attribute: &AttributeAliasDefinition,
    payload: &serde_json::Value,
    props: &serde_json::Value,
) -> Option<String> {
    match attribute.id.as_str() {
        "mbti" => json_text(&props["MBTI"]),
        "sns" => json_text(&props["SNS"]),
        "カレッジで挑戦したいこと" => json_text(&props["New Challenges"]),
        "カレッジ生に聞いてみたいこと" => json_text(&props["Ask Me About"]),
        "ハッシュタグ" => json_list_text(props.get("Hashtags")),
        "その他" => json_list_text(payload.get("attributes")),
        "一言" => json_text(&props["Message"]),
        "人生の転換期" => json_text(&props["Turning Point"]),
        "余談" => json_text(&props["BTW"]),
        "出身地" => json_text(&props["Birthplace"]),
        "呼ばれたい名前" => json_text(&props["Nickname"]),
        "好きなもの" => json_list_text(props.get("Likes")),
        "嫌いなもの" => json_text(&props["Dislikes"]),
        "専攻-興味分野" => combine_list_texts([
            json_list_values(props.get("Major")),
            json_list_values(props.get("Interests")),
        ]),
        "所属" => json_text(&props["Affiliation"]),
        "氏名" => payload.get("name").and_then(|value| value.as_str()).map(ToOwned::to_owned),
        "生年月日" => json_text(&props["DoB"]),
        "趣味-特技" => json_list_text(props.get("Hobbies")),
        _ => None,
    }
}

fn normalize_property_name(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn metadata_pointer<'a>(payload: &'a serde_json::Value, field: &str) -> Option<&'a serde_json::Value> {
    payload.pointer(&format!("/_lethe/{field}"))
}

fn metadata_str<'a>(payload: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    metadata_pointer(payload, field).and_then(|value| value.as_str())
}

fn metadata_value(payload: &serde_json::Value, field: &str) -> Option<String> {
    metadata_str(payload, field).map(ToOwned::to_owned)
}

fn metadata_bool(payload: &serde_json::Value, field: &str) -> Option<bool> {
    metadata_pointer(payload, field).and_then(|value| value.as_bool())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_client(person_id_property_name: &str) -> NotionClient {
        let properties = [
            ("Birthplace", "rich_text"),
            ("Major_interests", "rich_text"),
            ("Hashtag", "rich_text"),
            ("Source Slide URL", "url"),
            ("Last Synced At", "date"),
            ("Projection Version", "rich_text"),
            ("Status", "status"),
            ("Visibility", "checkbox"),
        ]
        .into_iter()
        .chain(std::iter::once((person_id_property_name, "rich_text")))
        .map(|(name, property_type)| {
            (
                name.to_string(),
                NotionProperty {
                    property_type: property_type.to_string(),
                },
            )
        })
        .collect::<HashMap<_, _>>();
        NotionClient {
            http: Client::builder().build().unwrap(),
            config: NotionConfig::new("test-token", "test-db"),
            schema: DatabaseSchema {
                title_property: "Name".into(),
                email_property: Some("Email".into()),
                actual_names_by_normalized: [
                    "Birthplace",
                    "Major_interests",
                    "Hashtag",
                    "Source Slide URL",
                    "Last Synced At",
                    "Projection Version",
                    "Status",
                    "Visibility",
                ]
                .into_iter()
                .chain(std::iter::once(person_id_property_name))
                .map(|name| (normalize_property_name(name), name.to_string()))
                .collect(),
                properties,
            },
        }
    }

    fn sample_profile() -> StudentProfile {
        StudentProfile {
            email: Some("sayaka@example.com".into()),
            generated_email: None,
            name: "彦野 沙彩花".into(),
            bio_text: Some("自己紹介テキスト".into()),
            profile_pic: None,
            gallery_images: vec![],
            properties: StudentProperties::default(),
            attributes: vec![],
            source_slide_object_id: Some("slide-1".into()),
            source_document_id: Some("document:gslides:test#slide:slide-1".into()),
            source_canonical_uri: Some("https://docs.google.com/presentation/d/test/edit#slide=id.slide-1".into()),
            thumbnail_blob_ref: None,
            thumbnail_url: None,
            companion_to_slide_object_id: None,
        }
    }

    fn heading_titles(blocks: &[serde_json::Value]) -> Vec<String> {
        blocks
            .iter()
            .filter(|block| block["type"] == "heading_2")
            .filter_map(|block| block["heading_2"]["rich_text"][0]["text"]["content"].as_str())
            .map(str::to_string)
            .collect()
    }

    fn count_block_type(value: &serde_json::Value, block_type: &str) -> usize {
        match value {
            serde_json::Value::Object(map) => {
                usize::from(map.get("type").and_then(|value| value.as_str()) == Some(block_type))
                    + map.values().map(|child| count_block_type(child, block_type)).sum::<usize>()
            }
            serde_json::Value::Array(values) => values.iter().map(|child| count_block_type(child, block_type)).sum(),
            _ => 0,
        }
    }

    #[test]
    fn source_image_matching_supports_legacy_thousand_scale_coordinates() {
        let candidates = vec![
            SourceImageCandidate {
                object_id: "left".into(),
                source_url: "https://example.com/left.png".into(),
                blob_ref: "blob:sha256:left".into(),
                center_x_pct: 20.0,
                center_y_pct: 30.0,
            },
            SourceImageCandidate {
                object_id: "right".into(),
                source_url: "https://example.com/right.png".into(),
                blob_ref: "blob:sha256:right".into(),
                center_x_pct: 80.0,
                center_y_pct: 75.0,
            },
        ];
        let coordinates = ImageCoordinates { x: 800.0, y: 750.0 };

        let matched = match_source_image_candidate(&candidates, &coordinates).unwrap();

        assert_eq!(matched.object_id, "right");
    }

    #[test]
    fn full_profile_produces_all_sections() {
        let mut profile = sample_profile();
        profile.profile_pic = Some(crate::slide_analysis::types::ProfilePic {
            coordinates: None,
            description: Some("portrait".into()),
            url: Some("https://example.com/profile.png".into()),
        });
        profile.gallery_images = vec![crate::slide_analysis::types::GalleryImage {
            coordinates: None,
            description: Some("猫の写真".into()),
            url: Some("https://example.com/gallery.png".into()),
        }];
        profile.properties.nickname = Some("さやか".into());
        profile.properties.birthplace = Some("栃木県".into());
        profile.properties.major = Some("電気工学".into());
        profile.properties.affiliation = Some("HLAB College".into());
        profile.properties.mbti = Some("ENFP".into());
        profile.properties.sns = Some("https://example.com/sns".into());
        profile.properties.hobbies = vec!["写真".into()];
        profile.properties.interests = vec!["エネルギー".into()];
        profile.properties.likes = vec!["コーンスープ".into()];
        profile.properties.new_challenges = Some("海外で学ぶ".into());

        let gallery = vec![(0usize, NotionMediaRef::External("https://example.com/gallery.png".into()))];
        let blocks = build_page_blocks(
            &profile,
            Some(&NotionMediaRef::External("https://example.com/profile.png".into())),
            &gallery,
            profile.source_canonical_uri.as_deref(),
        );

        assert_eq!(blocks.first().unwrap()["type"], "callout");
        assert_eq!(heading_titles(&blocks), vec!["About", "Highlights", "Gallery", "Source"]);
        assert!(blocks.iter().any(|block| block["type"] == "toggle"));
        assert!(blocks.iter().any(|block| block["type"] == "bookmark"));
    }

    #[test]
    fn missing_hobbies_interests_likes_skips_highlights() {
        let profile = sample_profile();
        let blocks = build_page_blocks(&profile, None, &[], profile.source_canonical_uri.as_deref());
        assert!(!heading_titles(&blocks).iter().any(|title| title == "Highlights"));
    }

    #[test]
    fn partial_toggles_only_present_fields() {
        let mut profile = sample_profile();
        profile.properties.new_challenges = Some("Rust を学ぶ".into());
        let blocks = build_page_blocks(&profile, None, &[], None);
        let toggles = blocks.iter().filter(|block| block["type"] == "toggle").count();
        assert_eq!(toggles, 1);
        assert!(blocks.iter().any(|block| block.to_string().contains("New Challenges")));
    }

    #[test]
    fn gallery_respects_max_9() {
        let mut profile = sample_profile();
        profile.gallery_images = (0..12)
            .map(|index| crate::slide_analysis::types::GalleryImage {
                coordinates: None,
                description: Some(format!("image-{index}")),
                url: Some(format!("https://example.com/{index}.png")),
            })
            .collect();
        let gallery = (0..12)
            .map(|index| (index, NotionMediaRef::External(format!("https://example.com/{index}.png"))))
            .collect::<Vec<_>>();
        let blocks = build_page_blocks(&profile, None, &gallery, None);
        let image_count = blocks.iter().map(|block| count_block_type(block, "image")).sum::<usize>();
        assert_eq!(image_count, 9);
    }

    #[test]
    fn single_gallery_image_does_not_emit_column_list() {
        let mut profile = sample_profile();
        profile.gallery_images = vec![crate::slide_analysis::types::GalleryImage {
            coordinates: None,
            description: Some("single".into()),
            url: Some("https://example.com/one.png".into()),
        }];
        let gallery = vec![(0usize, NotionMediaRef::FileUpload("upload-1".into()))];
        let blocks = build_page_blocks(&profile, None, &gallery, None);
        let gallery_section = build_gallery_section(&profile, &gallery).unwrap();
        assert!(blocks.iter().any(|block| block["type"] == "image"));
        assert!(!gallery_section.iter().any(|block| block["type"] == "column_list"));
    }

    #[test]
    fn dividers_not_orphaned() {
        let mut profile = sample_profile();
        profile.bio_text = None;
        profile.properties.hobbies = vec!["写真".into()];
        let blocks = build_page_blocks(&profile, None, &[], profile.source_canonical_uri.as_deref());
        assert_ne!(blocks.first().unwrap()["type"], "divider");
        assert_ne!(blocks.last().unwrap()["type"], "divider");
        for pair in blocks.windows(2) {
            assert!(!(pair[0]["type"] == "divider" && pair[1]["type"] == "divider"));
        }
    }

    #[test]
    fn sns_url_becomes_link() {
        let mut profile = sample_profile();
        profile.properties.sns = Some("https://example.com/sns".into());
        let blocks = build_page_blocks(&profile, None, &[], None);
        let about = blocks.iter().find(|block| block["type"] == "table").unwrap();
        assert_eq!(
            about["table"]["children"][0]["table_row"]["cells"][1][0]["text"]["link"]["url"].as_str(),
            Some("https://example.com/sns")
        );
    }

    #[test]
    fn about_table_skips_none_rows() {
        let mut profile = sample_profile();
        profile.properties.nickname = Some("さやか".into());
        let blocks = build_page_blocks(&profile, None, &[], None);
        let about = blocks.iter().find(|block| block["type"] == "table").unwrap();
        assert_eq!(about["table"]["children"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn build_property_updates_keeps_major_as_major_interests() {
        let payload = serde_json::json!({
            "properties": {
                "Major": "CS",
                "Interests": ["AI", "Robotics"],
                "Birthplace": "Tokyo",
            }
        });
        let props = fixture_client("LETHE Person ID").build_property_updates("田中太郎", &payload);
        assert_eq!(props["Major_interests"]["rich_text"][0]["text"]["content"].as_str(), Some("CS"));
        assert!(props.get("Birthplace").is_some());
        assert!(props.get("Name").is_some());
    }

    #[test]
    fn build_property_updates_populates_metadata_without_attribute_fallbacks() {
        let payload = serde_json::json!({
            "attributes": ["AI", "ML"],
            "_lethe": {
                "person_id": "person:alice",
                "projection_version": "proj:person-page@0.1.0",
                "last_synced_at": "2026-03-28T11:00:00Z",
                "source_slide_url": "https://example.com/slide",
                "status": "Done",
                "visibility": true
            },
            "properties": {
                "Hashtags": ["#rust"],
                "Major": "CS"
            }
        });

        let props = fixture_client("LETHE Person ID").build_property_updates("田中太郎", &payload);

        assert_eq!(
            props["Hashtag"]["rich_text"][0]["text"]["content"].as_str(),
            Some("#rust")
        );
        assert_eq!(
            props["LETHE Person ID"]["rich_text"][0]["text"]["content"].as_str(),
            Some("person:alice")
        );
        assert_eq!(props["Status"]["status"]["name"].as_str(), Some("Done"));
        assert_eq!(props["Visibility"]["checkbox"].as_bool(), Some(true));
    }

    #[test]
    fn build_property_updates_supports_japanese_alias_named_properties() {
        let mut client = fixture_client("LETHE Person ID");
        client.schema.actual_names_by_normalized.extend([
            ("呼ばれたい名前", "呼ばれたい名前"),
            ("趣味特技", "趣味・特技"),
            ("好きなもの", "好きなもの"),
            ("カレッジで挑戦したいこと", "カレッジで挑戦したいこと"),
        ]
        .into_iter()
        .map(|(normalized, actual)| (normalized.to_string(), actual.to_string())));
        client.schema.properties.extend([
            ("呼ばれたい名前", "rich_text"),
            ("趣味・特技", "rich_text"),
            ("好きなもの", "rich_text"),
            ("カレッジで挑戦したいこと", "rich_text"),
        ]
        .into_iter()
        .map(|(name, property_type)| {
            (
                name.to_string(),
                NotionProperty {
                    property_type: property_type.to_string(),
                },
            )
        }));

        let payload = serde_json::json!({
            "properties": {
                "Nickname": "さやか",
                "Hobbies": ["写真", "散歩"],
                "Likes": ["コーヒー"],
                "New Challenges": "もっと話す"
            }
        });

        let props = client.build_property_updates("田中太郎", &payload);

        assert_eq!(
            props["呼ばれたい名前"]["rich_text"][0]["text"]["content"].as_str(),
            Some("さやか")
        );
        assert_eq!(
            props["趣味・特技"]["rich_text"][0]["text"]["content"].as_str(),
            Some("写真, 散歩")
        );
        assert_eq!(
            props["好きなもの"]["rich_text"][0]["text"]["content"].as_str(),
            Some("コーヒー")
        );
        assert_eq!(
            props["カレッジで挑戦したいこと"]["rich_text"][0]["text"]["content"].as_str(),
            Some("もっと話す")
        );
    }

    #[test]
    fn headers_reject_invalid_bearer_token() {
        let mut client = fixture_client("LETHE Person ID");
        client.config.token = "bad\r\ntoken".into();
        assert!(matches!(
            client.headers_for_version(&client.config.api_version),
            Err(AdapterError::AuthFailure { .. })
        ));
    }

    #[test]
    fn headers_reject_invalid_api_version() {
        let mut client = fixture_client("LETHE Person ID");
        client.config.api_version = "bad\r\nversion".into();
        assert!(matches!(
            client.headers_for_version(&client.config.api_version),
            Err(AdapterError::Other(_))
        ));
    }

    #[test]
    fn media_ref_file_upload_image_block_uses_file_upload_type() {
        let block = NotionMediaRef::FileUpload("upload-123".into()).to_image_block();
        assert_eq!(block["type"], "image");
        assert_eq!(block["image"]["type"], "file_upload");
        assert_eq!(block["image"]["file_upload"]["id"], "upload-123");
    }

    #[test]
    fn media_ref_external_image_block_uses_external_type() {
        let block = NotionMediaRef::External("https://example.com/thumb.png".into()).to_image_block();
        assert_eq!(block["type"], "image");
        assert_eq!(block["image"]["type"], "external");
        assert_eq!(block["image"]["external"]["url"], "https://example.com/thumb.png");
    }
}
