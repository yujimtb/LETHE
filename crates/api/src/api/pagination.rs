//! M14: Pagination — shared pagination utilities.

use serde::{Deserialize, Serialize};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

/// Versioned, API-independent cursor envelope.  The payload is opaque on the
/// wire; the scope prevents accidentally reusing a cursor from another read.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct KeysetCursorPayload {
    scope: String,
    sort_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeysetCursor {
    pub scope: String,
    pub sort_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KeysetPage<T> {
    pub data: Vec<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeysetCursorError {
    Invalid,
    WrongScope,
}

pub fn encode_keyset_cursor(scope: &str, sort_key: &str) -> Result<String, KeysetCursorError> {
    if scope.trim().is_empty() || sort_key.is_empty() {
        return Err(KeysetCursorError::Invalid);
    }
    let payload = KeysetCursorPayload {
        scope: scope.to_owned(),
        sort_key: sort_key.to_owned(),
    };
    let bytes = serde_json::to_vec(&payload).map_err(|_| KeysetCursorError::Invalid)?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

pub fn decode_keyset_cursor(
    cursor: &str,
    expected_scope: &str,
) -> Result<KeysetCursor, KeysetCursorError> {
    let bytes = URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| KeysetCursorError::Invalid)?;
    let payload: KeysetCursorPayload =
        serde_json::from_slice(&bytes).map_err(|_| KeysetCursorError::Invalid)?;
    if payload.scope != expected_scope {
        return Err(KeysetCursorError::WrongScope);
    }
    if payload.sort_key.is_empty() {
        return Err(KeysetCursorError::Invalid);
    }
    Ok(KeysetCursor {
        scope: payload.scope,
        sort_key: payload.sort_key,
    })
}

/// Pagination parameters from query string (M14 §9).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginationParams {
    #[serde(default)]
    pub offset: usize,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub sort: Option<String>,
    #[serde(default = "default_order")]
    pub order: String,
}

fn default_limit() -> usize {
    20
}

fn default_order() -> String {
    "desc".into()
}

impl Default for PaginationParams {
    fn default() -> Self {
        Self {
            offset: 0,
            limit: default_limit(),
            sort: None,
            order: default_order(),
        }
    }
}

impl PaginationParams {
    /// Validate and clamp the limit to the maximum (100).
    pub fn validated(&self) -> Self {
        Self {
            offset: self.offset,
            limit: self.limit.min(100),
            sort: self.sort.clone(),
            order: self.order.clone(),
        }
    }
}

/// A paginated response wrapper (M14 §9).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaginatedResponse<T: Serialize> {
    pub data: Vec<T>,
    pub total: usize,
    pub offset: usize,
    pub limit: usize,
}

impl<T: Serialize> PaginatedResponse<T> {
    /// Create a paginated response from a full collection.
    pub fn from_slice(items: Vec<T>, total: usize, params: &PaginationParams) -> Self {
        Self {
            data: items,
            total,
            offset: params.offset,
            limit: params.limit,
        }
    }
}

/// Apply pagination to a slice.
pub fn paginate<T: Clone>(items: &[T], params: &PaginationParams) -> (Vec<T>, usize) {
    let total = items.len();
    let params = params.validated();
    let start = params.offset.min(total);
    let end = (start + params.limit).min(total);
    (items[start..end].to_vec(), total)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_pagination() {
        let params = PaginationParams::default();
        assert_eq!(params.offset, 0);
        assert_eq!(params.limit, 20);
        assert_eq!(params.order, "desc");
    }

    #[test]
    fn limit_clamped_to_100() {
        let params = PaginationParams {
            limit: 200,
            ..Default::default()
        };
        let validated = params.validated();
        assert_eq!(validated.limit, 100);
    }

    #[test]
    fn paginate_basic() {
        let items: Vec<i32> = (0..50).collect();
        let params = PaginationParams {
            offset: 10,
            limit: 5,
            ..Default::default()
        };
        let (page, total) = paginate(&items, &params);
        assert_eq!(total, 50);
        assert_eq!(page, vec![10, 11, 12, 13, 14]);
    }

    #[test]
    fn paginate_beyond_end() {
        let items: Vec<i32> = (0..5).collect();
        let params = PaginationParams {
            offset: 10,
            limit: 5,
            ..Default::default()
        };
        let (page, total) = paginate(&items, &params);
        assert_eq!(total, 5);
        assert!(page.is_empty());
    }

    #[test]
    fn paginate_partial_page() {
        let items: Vec<i32> = (0..8).collect();
        let params = PaginationParams {
            offset: 5,
            limit: 10,
            ..Default::default()
        };
        let (page, _) = paginate(&items, &params);
        assert_eq!(page, vec![5, 6, 7]);
    }
}
