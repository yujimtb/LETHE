use super::*;

pub(super) fn blob_ref_sha256(blob_ref: &str) -> Option<&str> {
    let hash = blob_ref.strip_prefix("blob:sha256:")?;
    if hash.len() == 64 && hash.chars().all(|ch| ch.is_ascii_hexdigit()) {
        Some(hash)
    } else {
        None
    }
}

pub(super) fn source_image_candidates(payload: &serde_json::Value) -> Vec<SourceImageCandidate> {
    payload
        .get("_lethe_source_images")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or_default()
}

pub(super) fn match_source_image_candidate<'a>(
    candidates: &'a [SourceImageCandidate],
    coordinates: &ImageCoordinates,
) -> Option<&'a SourceImageCandidate> {
    let target_x = normalize_image_selection_coordinate(coordinates.x)?;
    let target_y = normalize_image_selection_coordinate(coordinates.y)?;
    candidates.iter().min_by(|left, right| {
        let left_distance =
            squared_distance(left.center_x_pct, left.center_y_pct, target_x, target_y);
        let right_distance =
            squared_distance(right.center_x_pct, right.center_y_pct, target_x, target_y);
        left_distance.total_cmp(&right_distance)
    })
}

pub(super) fn normalize_image_selection_coordinate(value: f64) -> Option<f64> {
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

pub(super) fn squared_distance(left_x: f64, left_y: f64, right_x: f64, right_y: f64) -> f64 {
    let dx = left_x - right_x;
    let dy = left_y - right_y;
    (dx * dx) + (dy * dy)
}

// ---------------------------------------------------------------------------
// Helpers: content block rendering
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum NotionMediaRef {
    External(String),
    FileUpload(String),
}

impl NotionMediaRef {
    pub(super) fn to_page_value(&self) -> serde_json::Value {
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

    pub(super) fn to_image_block(&self) -> serde_json::Value {
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

pub(super) fn page_api_version_from_media_refs<'a, const N: usize>(
    refs: [Option<&'a NotionMediaRef>; N],
) -> &'static str {
    if refs
        .into_iter()
        .flatten()
        .any(|value| matches!(value, NotionMediaRef::FileUpload(_)))
    {
        NotionClient::FILE_UPLOAD_API_VERSION
    } else {
        "2022-06-28"
    }
}

pub(super) fn value_contains_file_upload(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Object(map) => {
            map.get("type").and_then(|ty| ty.as_str()) == Some("file_upload")
                || map.values().any(value_contains_file_upload)
        }
        serde_json::Value::Array(values) => values.iter().any(value_contains_file_upload),
        _ => false,
    }
}
