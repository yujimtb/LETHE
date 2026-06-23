use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;

use crate::self_host::app::{AppService, SelfHostError};
use lethe_api::api::envelope::{ErrorResponse, ResponseEnvelope};
use lethe_api::api::health::HealthResponse;
use lethe_api::api::pagination::PaginationParams;
use lethe_core::domain::BlobRef;

pub fn build_router(service: AppService) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/health/deep", get(deep_health))
        .route("/admin/sync", post(sync_now))
        .route(
            "/api/projections/{projection_id}/blobs/{blob_hash}",
            get(projection_blob),
        )
        .route(
            "/api/projections/{projection_id}/lineage",
            get(projection_lineage),
        )
        .route(
            "/api/projections/{projection_id}/records",
            get(projection_records),
        )
        .route(
            "/api/projections/{projection_id}/grep",
            post(projection_grep),
        )
        .route(
            "/api/projections/{projection_id}/records/{record_id}",
            get(projection_record_detail),
        )
        .route(
            "/api/projections/{projection_id}/threads/{thread_ts}",
            get(projection_thread),
        )
        .route(
            "/api/projections/{projection_id}/resolve-link",
            post(projection_resolve_link),
        )
        .route(
            "/api/projections/{projection_id}/prior-qa-search",
            post(projection_prior_qa_search),
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

async fn health(State(service): State<AppService>) -> Result<Json<HealthResponse>, ApiError> {
    Ok(Json(service.health()?))
}

async fn deep_health(
    State(service): State<AppService>,
    headers: HeaderMap,
) -> Result<Json<HealthResponse>, ApiError> {
    service.authorize_headers(&headers, "admin:health")?;
    Ok(Json(service.deep_health()?))
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

async fn projection_blob(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path((projection_id, blob_hash)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    service.authorize_headers(&headers, "read:persons")?;
    ensure_projection_person_page(&projection_id)?;
    let Some(blob_ref) = blob_ref_from_hash(&blob_hash) else {
        return Err(ApiError::not_found());
    };
    let Some(bytes) = service.projection_blob_bytes(&blob_ref)? else {
        return Err(ApiError::not_found());
    };

    let mut response = Response::new(Body::from(bytes));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("image/png"));
    response.headers_mut().insert(
        CACHE_CONTROL,
        HeaderValue::from_static("private, max-age=31536000, immutable"),
    );
    Ok(response)
}

async fn projection_lineage(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(projection_id): Path<String>,
) -> Result<Json<lethe_engine::projection::lineage::LineageManifest>, ApiError> {
    service.authorize_headers(&headers, "read:persons")?;
    ensure_known_projection(&projection_id)?;
    Ok(Json(service.lineage_manifest(&projection_id)?))
}
async fn projection_records(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(projection_id): Path<String>,
    Query(query): Query<PersonsQuery>,
) -> Result<Json<ResponseEnvelope<serde_json::Value>>, ApiError> {
    match projection_id.as_str() {
        "proj:person-page" => {
            service.authorize_headers(&headers, "read:persons")?;
            Ok(Json(service.persons_response(
                query.mode.as_deref(),
                query.pin.as_deref(),
                &query.pagination,
            )?))
        }
        "proj:corpus" => {
            service.authorize_headers(&headers, "read:corpus")?;
            Ok(Json(service.corpus_records_response(
                query.mode.as_deref(),
                query.pin.as_deref(),
                &query.pagination,
            )?))
        }
        _ => Err(ApiError::not_found()),
    }
}

async fn projection_grep(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(projection_id): Path<String>,
    Json(request): Json<lethe_api::api::grep::GrepRequest>,
) -> Result<Json<ResponseEnvelope<lethe_api::api::grep::GrepResponse>>, ApiError> {
    service.authorize_headers(&headers, "read:corpus")?;
    ensure_projection_corpus(&projection_id)?;
    Ok(Json(service.corpus_grep_response(&request)?))
}

async fn projection_record_detail(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path((projection_id, record_id)): Path<(String, String)>,
    Query(query): Query<ReadQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    match projection_id.as_str() {
        "proj:person-page" => {
            service.authorize_headers_all(&headers, &["read:persons", "read:timeline"])?;
            Ok(Json(serde_json::to_value(
                service.person_detail_response(
                    &record_id,
                    query.mode.as_deref(),
                    query.pin.as_deref(),
                )?,
            )?))
        }
        "proj:corpus" => {
            service.authorize_headers(&headers, "read:corpus")?;
            Ok(Json(serde_json::to_value(
                service.corpus_record_response(&record_id)?,
            )?))
        }
        _ => Err(ApiError::not_found()),
    }
}

async fn projection_thread(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path((projection_id, thread_ts)): Path<(String, String)>,
) -> Result<Json<ResponseEnvelope<lethe_api::api::grep::ThreadResponse>>, ApiError> {
    service.authorize_headers(&headers, "read:corpus")?;
    ensure_projection_corpus(&projection_id)?;
    Ok(Json(service.corpus_thread_response(&thread_ts)?))
}

async fn projection_resolve_link(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(projection_id): Path<String>,
    Json(request): Json<lethe_api::api::grep::ResolveLinkRequest>,
) -> Result<Json<ResponseEnvelope<lethe_api::api::grep::ResolveLinkResponse>>, ApiError> {
    service.authorize_headers(&headers, "read:corpus")?;
    ensure_projection_corpus(&projection_id)?;
    Ok(Json(service.resolve_link_response(&request)?))
}

async fn projection_prior_qa_search(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(projection_id): Path<String>,
    Json(request): Json<lethe_api::api::grep::PriorQaSearchRequest>,
) -> Result<
    Json<
        ResponseEnvelope<
            lethe_api::api::grep::PriorQaSearchResponse<lethe_projection_answer_log::PriorQaResult>,
        >,
    >,
    ApiError,
> {
    service.authorize_headers(&headers, "read:answer-log")?;
    ensure_projection_answer_log(&projection_id)?;
    Ok(Json(service.prior_qa_search_response(&request)?))
}

async fn projection_record_slides(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path((projection_id, record_id)): Path<(String, String)>,
    Query(query): Query<ReadQuery>,
) -> Result<Json<ResponseEnvelope<serde_json::Value>>, ApiError> {
    service.authorize_headers(&headers, "read:timeline")?;
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
) -> Result<Json<ResponseEnvelope<serde_json::Value>>, ApiError> {
    service.authorize_headers(&headers, "read:timeline")?;
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
) -> Result<Json<ResponseEnvelope<serde_json::Value>>, ApiError> {
    service.authorize_headers(&headers, "read:timeline")?;
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

impl From<serde_json::Error> for ApiError {
    fn from(value: serde_json::Error) -> Self {
        Self::internal(value.to_string())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body)).into_response()
    }
}

fn blob_ref_from_hash(hash: &str) -> Option<BlobRef> {
    if hash.len() == 64 && hash.chars().all(|ch| ch.is_ascii_hexdigit()) {
        Some(BlobRef::new(format!("blob:sha256:{hash}")))
    } else {
        None
    }
}

fn ensure_projection_person_page(projection_id: &str) -> Result<(), ApiError> {
    if projection_id == "proj:person-page" {
        Ok(())
    } else {
        Err(ApiError::not_found())
    }
}

fn ensure_projection_corpus(projection_id: &str) -> Result<(), ApiError> {
    if projection_id == "proj:corpus" {
        Ok(())
    } else {
        Err(ApiError::not_found())
    }
}

fn ensure_projection_answer_log(projection_id: &str) -> Result<(), ApiError> {
    if projection_id == "proj:answer-log" {
        Ok(())
    } else {
        Err(ApiError::not_found())
    }
}

fn ensure_known_projection(projection_id: &str) -> Result<(), ApiError> {
    match projection_id {
        "proj:person-page" | "proj:corpus" | "proj:answer-log" => Ok(()),
        _ => Err(ApiError::not_found()),
    }
}
