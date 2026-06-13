use axum::extract::{Path, Query, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::body::Body;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;

use crate::api::envelope::ErrorResponse;
use crate::api::pagination::PaginationParams;
use crate::self_host::app::{AppService, SelfHostError};

pub fn build_router(service: AppService) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/health/deep", get(health))
        .route("/admin/sync", post(sync_now))
        .route("/public/blobs/{blob_hash}", get(public_blob))
        .route("/api/projections/{projection_id}/records", get(projection_records))
        .route(
            "/api/projections/{projection_id}/records/{record_id}",
            get(projection_record_detail),
        )
        .route(
            "/api/projections/{projection_id}/records/{record_id}/slides",
            get(projection_record_slides),
        )
        .route(
            "/api/projections/{projection_id}/records/{record_id}/messages",
            get(projection_record_messages),
        )
        .route(
            "/api/projections/{projection_id}/records/{record_id}/timeline",
            get(projection_record_timeline),
        )
        .route("/api/persons", get(list_persons))
        .route("/api/persons/{person_id}", get(person_detail))
        .route("/api/persons/{person_id}/slides", get(person_slides))
        .route("/api/persons/{person_id}/messages", get(person_messages))
        .route("/api/persons/{person_id}/timeline", get(person_timeline))
        .with_state(service)
}

#[derive(Debug, Deserialize, Default)]
struct ReadQuery {
    mode: Option<String>,
    pin: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct PersonsQuery {
    mode: Option<String>,
    pin: Option<String>,
    #[serde(flatten)]
    pagination: PaginationParams,
}

async fn health(State(service): State<AppService>) -> Result<Json<crate::api::health::HealthResponse>, ApiError> {
    Ok(Json(service.health()?))
}

async fn sync_now(
    State(service): State<AppService>,
    headers: HeaderMap,
) -> Result<Json<crate::self_host::app::SyncReport>, ApiError> {
    service.authorize_headers(&headers, "admin:sync")?;
    let report = tokio::task::spawn_blocking(move || service.sync_all())
        .await
        .map_err(|err| ApiError::internal(err.to_string()))??;
    Ok(Json(report))
}

async fn public_blob(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(blob_hash): Path<String>,
) -> Result<Response, ApiError> {
    service.authorize_headers(&headers, "blob:read")?;
    let Some(blob_ref) = blob_ref_from_hash(&blob_hash) else {
        return Err(ApiError::not_found());
    };
    let Some(bytes) = service.blob_bytes(&blob_ref)? else {
        return Err(ApiError::not_found());
    };

    let mut response = Response::new(Body::from(bytes));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("image/png"));
    response.headers_mut().insert(
        CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=31536000, immutable"),
    );
    Ok(response)
}

async fn list_persons(
    State(service): State<AppService>,
    headers: HeaderMap,
    Query(query): Query<PersonsQuery>,
) -> Result<Response, ApiError> {
    service.authorize_headers(&headers, "projection:read")?;
    Ok(deprecated_alias_response(service.persons_response(
        query.mode.as_deref(),
        query.pin.as_deref(),
        &query.pagination,
    )?))
}

async fn person_detail(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(person_id): Path<String>,
    Query(query): Query<ReadQuery>,
) -> Result<Response, ApiError> {
    service.authorize_headers(&headers, "projection:read")?;
    Ok(deprecated_alias_response(service.person_detail_response(
        &person_id,
        query.mode.as_deref(),
        query.pin.as_deref(),
    )?))
}

async fn person_slides(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(person_id): Path<String>,
    Query(query): Query<ReadQuery>,
) -> Result<Response, ApiError> {
    service.authorize_headers(&headers, "projection:read")?;
    Ok(deprecated_alias_response(service.person_slides_response(
        &person_id,
        query.mode.as_deref(),
        query.pin.as_deref(),
    )?))
}

async fn person_messages(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(person_id): Path<String>,
    Query(query): Query<ReadQuery>,
) -> Result<Response, ApiError> {
    service.authorize_headers(&headers, "projection:read")?;
    Ok(deprecated_alias_response(service.person_messages_response(
        &person_id,
        query.mode.as_deref(),
        query.pin.as_deref(),
    )?))
}

async fn person_timeline(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(person_id): Path<String>,
    Query(query): Query<ReadQuery>,
) -> Result<Response, ApiError> {
    service.authorize_headers(&headers, "projection:read")?;
    Ok(deprecated_alias_response(service.person_timeline_response(
        &person_id,
        query.mode.as_deref(),
        query.pin.as_deref(),
    )?))
}

async fn projection_records(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(projection_id): Path<String>,
    Query(query): Query<PersonsQuery>,
) -> Result<Json<crate::api::envelope::ResponseEnvelope<serde_json::Value>>, ApiError> {
    service.authorize_headers(&headers, "projection:read")?;
    ensure_projection_person_page(&projection_id)?;
    Ok(Json(service.persons_response(
        query.mode.as_deref(),
        query.pin.as_deref(),
        &query.pagination,
    )?))
}

async fn projection_record_detail(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path((projection_id, record_id)): Path<(String, String)>,
    Query(query): Query<ReadQuery>,
) -> Result<Json<crate::api::envelope::ResponseEnvelope<serde_json::Value>>, ApiError> {
    service.authorize_headers(&headers, "projection:read")?;
    ensure_projection_person_page(&projection_id)?;
    Ok(Json(service.person_detail_response(
        &record_id,
        query.mode.as_deref(),
        query.pin.as_deref(),
    )?))
}

async fn projection_record_slides(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path((projection_id, record_id)): Path<(String, String)>,
    Query(query): Query<ReadQuery>,
) -> Result<Json<crate::api::envelope::ResponseEnvelope<serde_json::Value>>, ApiError> {
    service.authorize_headers(&headers, "projection:read")?;
    ensure_projection_person_page(&projection_id)?;
    Ok(Json(service.person_slides_response(
        &record_id,
        query.mode.as_deref(),
        query.pin.as_deref(),
    )?))
}

async fn projection_record_messages(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path((projection_id, record_id)): Path<(String, String)>,
    Query(query): Query<ReadQuery>,
) -> Result<Json<crate::api::envelope::ResponseEnvelope<serde_json::Value>>, ApiError> {
    service.authorize_headers(&headers, "projection:read")?;
    ensure_projection_person_page(&projection_id)?;
    Ok(Json(service.person_messages_response(
        &record_id,
        query.mode.as_deref(),
        query.pin.as_deref(),
    )?))
}

async fn projection_record_timeline(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path((projection_id, record_id)): Path<(String, String)>,
    Query(query): Query<ReadQuery>,
) -> Result<Json<crate::api::envelope::ResponseEnvelope<serde_json::Value>>, ApiError> {
    service.authorize_headers(&headers, "projection:read")?;
    ensure_projection_person_page(&projection_id)?;
    Ok(Json(service.person_timeline_response(
        &record_id,
        query.mode.as_deref(),
        query.pin.as_deref(),
    )?))
}

struct ApiError {
    status: StatusCode,
    body: ErrorResponse,
}

impl ApiError {
    fn not_found() -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            body: ErrorResponse::not_found(),
        }
    }

    fn internal(detail: String) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            body: ErrorResponse::internal_server_error(&detail),
        }
    }
}

impl From<SelfHostError> for ApiError {
    fn from(value: SelfHostError) -> Self {
        match value {
            SelfHostError::NotFound(_) => Self {
                status: StatusCode::NOT_FOUND,
                body: ErrorResponse::not_found(),
            },
            SelfHostError::ReadMode(detail) => Self {
                status: StatusCode::BAD_REQUEST,
                body: ErrorResponse::bad_request(&detail),
            },
            SelfHostError::Policy(detail) => Self {
                status: StatusCode::FORBIDDEN,
                body: ErrorResponse::forbidden(&detail),
            },
            SelfHostError::Auth(detail) => Self {
                status: StatusCode::UNAUTHORIZED,
                body: {
                    let mut body = ErrorResponse::unauthorized();
                    body.detail = Some(detail);
                    body
                },
            },
            other => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                body: ErrorResponse::internal_server_error(&other.to_string()),
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body)).into_response()
    }
}

fn blob_ref_from_hash(hash: &str) -> Option<crate::domain::BlobRef> {
    if hash.len() == 64 && hash.chars().all(|ch| ch.is_ascii_hexdigit()) {
        Some(crate::domain::BlobRef::new(format!("blob:sha256:{hash}")))
    } else {
        None
    }
}

fn ensure_projection_person_page(projection_id: &str) -> Result<(), ApiError> {
    if projection_id == "proj:person-page" || projection_id == "person-page" {
        Ok(())
    } else {
        Err(ApiError::not_found())
    }
}

fn deprecated_alias_response(
    body: crate::api::envelope::ResponseEnvelope<serde_json::Value>,
) -> Response {
    let mut response = Json(body).into_response();
    response
        .headers_mut()
        .insert("deprecation", HeaderValue::from_static("true"));
    response.headers_mut().insert(
        "link",
        HeaderValue::from_static("</api/projections/proj:person-page/records>; rel=\"successor-version\""),
    );
    response
}
