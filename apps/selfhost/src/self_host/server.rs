use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;

use crate::self_host::app::{
    AppService, BulkImportSessionReport, ImportReport, SelfHostError, SupplementalWriteRequest,
    WriteEnvelope,
};
use lethe_adapter_api::traits::ObservationDraft;
use lethe_api::api::envelope::{ErrorResponse, ResponseEnvelope};
use lethe_api::api::health::HealthResponse;
use lethe_api::api::pagination::PaginationParams;
use lethe_core::domain::BlobRef;
use lethe_projection_claim_queue::ClaimState;
use lethe_projection_cognition::CardState;

pub fn build_router(service: AppService) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/health/deep", get(deep_health))
        .route("/admin/sync", post(sync_now))
        .route(
            "/api/import/observation-drafts",
            post(import_observation_drafts).layer(DefaultBodyLimit::max(128 * 1024 * 1024)),
        )
        .route(
            "/api/import/bulk-sessions/begin",
            post(begin_bulk_import_session),
        )
        .route(
            "/api/import/bulk-sessions/{session_id}/end",
            post(end_bulk_import_session),
        )
        .route("/projections/claim-queue", get(claim_queue))
        .route("/projections/decisions", get(decisions))
        .route("/projections/freshness", get(freshness))
        .route("/projections/reply-slo", get(reply_slo))
        .route("/projections/break-glass", get(break_glass))
        .route("/projections/resume-snapshot", get(resume_snapshot))
        .route("/projections/plan-state", get(plan_state))
        .route("/projections/card-queue", get(card_queue))
        .route("/supplementals", post(create_supplemental))
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

#[derive(Debug, Deserialize, Default)]
struct ClaimQueueQuery {
    state: Option<ClaimState>,
    backfill: Option<bool>,
    limit: Option<usize>,
    cursor: Option<String>,
}

#[derive(Debug, Deserialize, Default)]
struct DecisionsQuery {
    q: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, Default)]
struct CardQueueQuery {
    state: Option<CardState>,
    channel: Option<String>,
    automatic: Option<bool>,
    limit: Option<usize>,
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ImportObservationDraftsRequest {
    source_instance_id: String,
    #[serde(default)]
    bulk_session_id: Option<String>,
    drafts: Vec<ObservationDraft>,
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

async fn import_observation_drafts(
    State(service): State<AppService>,
    headers: HeaderMap,
    Json(request): Json<ImportObservationDraftsRequest>,
) -> Result<Json<ImportReport>, ApiError> {
    service.authorize_headers(&headers, "write:observations")?;
    let report = tokio::task::spawn_blocking(move || {
        service.ingest_observation_drafts_with_session(
            request.drafts,
            &request.source_instance_id,
            request.bulk_session_id.as_deref(),
        )
    })
    .await
    .map_err(|err| ApiError::internal(err.to_string()))??;
    Ok(Json(report))
}

async fn begin_bulk_import_session(
    State(service): State<AppService>,
    headers: HeaderMap,
) -> Result<Json<BulkImportSessionReport>, ApiError> {
    service.authorize_headers(&headers, "write:observations")?;
    let report = tokio::task::spawn_blocking(move || service.begin_bulk_import_session())
        .await
        .map_err(|err| ApiError::internal(err.to_string()))??;
    Ok(Json(report))
}

async fn end_bulk_import_session(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(session_id): Path<String>,
) -> Result<Json<BulkImportSessionReport>, ApiError> {
    service.authorize_headers(&headers, "write:observations")?;
    let report = tokio::task::spawn_blocking(move || service.end_bulk_import_session(&session_id))
        .await
        .map_err(|err| ApiError::internal(err.to_string()))??;
    Ok(Json(report))
}

async fn claim_queue(
    State(service): State<AppService>,
    headers: HeaderMap,
    Query(query): Query<ClaimQueueQuery>,
) -> Result<Json<ResponseEnvelope<crate::self_host::app::projection_api::ClaimQueuePage>>, ApiError>
{
    service.authorize_headers(&headers, "read:corpus")?;
    Ok(Json(service.claim_queue_response_filtered(
        query.state,
        None,
        query.backfill,
        query.limit.unwrap_or(20),
        query.cursor.as_deref(),
    )?))
}

async fn decisions(
    State(service): State<AppService>,
    headers: HeaderMap,
    Query(query): Query<DecisionsQuery>,
) -> Result<
    Json<ResponseEnvelope<crate::self_host::app::projection_api::DecisionSearchPage>>,
    ApiError,
> {
    service.authorize_headers(&headers, "read:corpus")?;
    Ok(Json(service.decision_search_response(
        query.q.as_deref(),
        query.limit.unwrap_or(20),
    )?))
}

async fn freshness(
    State(service): State<AppService>,
    headers: HeaderMap,
) -> Result<Json<ResponseEnvelope<lethe_projection_cognition::FreshnessProjection>>, ApiError> {
    service.authorize_headers(&headers, "read:corpus")?;
    Ok(Json(service.freshness_response()?))
}

async fn reply_slo(
    State(service): State<AppService>,
    headers: HeaderMap,
) -> Result<Json<ResponseEnvelope<lethe_projection_cognition::ReplySloProjection>>, ApiError> {
    service.authorize_headers(&headers, "read:corpus")?;
    Ok(Json(service.reply_slo_response()?))
}

async fn break_glass(
    State(service): State<AppService>,
    headers: HeaderMap,
) -> Result<Json<ResponseEnvelope<crate::self_host::app::BreakGlassProjection>>, ApiError> {
    service.authorize_headers(&headers, "read:corpus")?;
    Ok(Json(service.break_glass_response()?))
}

async fn resume_snapshot(
    State(service): State<AppService>,
    headers: HeaderMap,
) -> Result<Json<ResponseEnvelope<lethe_projection_cognition::ResumeSnapshotProjection>>, ApiError>
{
    service.authorize_headers(&headers, "read:corpus")?;
    Ok(Json(service.resume_snapshot_response()?))
}

async fn plan_state(
    State(service): State<AppService>,
    headers: HeaderMap,
) -> Result<Json<ResponseEnvelope<lethe_projection_cognition::PlanStateProjection>>, ApiError> {
    service.authorize_headers(&headers, "read:corpus")?;
    Ok(Json(service.plan_state_response()?))
}

async fn card_queue(
    State(service): State<AppService>,
    headers: HeaderMap,
    Query(query): Query<CardQueueQuery>,
) -> Result<Json<ResponseEnvelope<crate::self_host::app::projection_api::CardQueuePage>>, ApiError>
{
    service.authorize_headers(&headers, "read:corpus")?;
    Ok(Json(service.card_queue_response(
        query.state,
        query.channel.as_deref(),
        query.automatic,
        query.limit.unwrap_or(20),
        query.cursor.as_deref(),
    )?))
}

async fn create_supplemental(
    State(service): State<AppService>,
    headers: HeaderMap,
    Json(request): Json<SupplementalWriteRequest>,
) -> Result<
    (
        StatusCode,
        Json<WriteEnvelope<lethe_core::domain::SupplementalRecord>>,
    ),
    ApiError,
> {
    service.authorize_headers(&headers, "write:supplemental")?;
    let record = service.write_supplemental(request)?;
    Ok((StatusCode::CREATED, Json(WriteEnvelope { data: record })))
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
    ensure_known_projection(&projection_id)?;
    let scope = match projection_id.as_str() {
        "proj:person-page" => "read:persons",
        "proj:answer-log" => "read:answer-log",
        _ => "read:corpus",
    };
    service.authorize_headers(&headers, scope)?;
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

    fn unprocessable_entity(code: &'static str, detail: serde_json::Value) -> Self {
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            body: ErrorResponse::unprocessable_entity(code, detail),
        }
    }

    fn conflict(code: &'static str, detail: serde_json::Value) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            body: ErrorResponse::conflict(code, detail),
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
            SelfHostError::BulkImportSessionConflict { code, detail } => Self {
                status: StatusCode::CONFLICT,
                body: ErrorResponse {
                    error: code.to_owned(),
                    detail: Some(detail),
                    details: None,
                    retry_after: None,
                },
            },
            SelfHostError::ProjectionStale(detail) => Self {
                status: StatusCode::SERVICE_UNAVAILABLE,
                body: ErrorResponse::projection_stale(&detail, 30),
            },
            SelfHostError::SearchIndexUnavailable { code, detail } => Self {
                status: StatusCode::SERVICE_UNAVAILABLE,
                body: ErrorResponse {
                    error: code.to_owned(),
                    detail: Some(detail),
                    details: None,
                    retry_after: Some(5),
                },
            },
            SelfHostError::SupplementalValidation { code, detail } => {
                Self::unprocessable_entity(code, detail)
            }
            SelfHostError::SupplementalConflict { code, detail } => Self::conflict(code, detail),
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
        "proj:person-page"
        | "proj:corpus"
        | "proj:answer-log"
        | "proj:claim-queue"
        | "proj:freshness"
        | "proj:reply-slo"
        | "proj:break-glass"
        | "proj:resume-snapshot"
        | "proj:plan-state"
        | "proj:card-queue" => Ok(()),
        _ => Err(ApiError::not_found()),
    }
}

#[cfg(test)]
mod search_index_error_tests {
    use super::*;

    #[test]
    fn search_index_unavailable_maps_to_retryable_http_503() {
        let error = ApiError::from(SelfHostError::SearchIndexUnavailable {
            code: "search_index_rebuilding",
            detail: "generation is rebuilding".to_owned(),
        });

        assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error.body.error, "search_index_rebuilding");
        assert_eq!(
            error.body.detail.as_deref(),
            Some("generation is rebuilding")
        );
        assert_eq!(error.body.retry_after, Some(5));
    }

    #[test]
    fn deferred_non_corpus_projection_maps_to_retryable_http_503() {
        let error = ApiError::from(SelfHostError::ProjectionStale(
            "proj:person-page is stale".to_owned(),
        ));

        assert_eq!(error.status, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(error.body.error, "projection_stale");
        assert_eq!(error.body.retry_after, Some(30));
    }

    #[test]
    fn bulk_import_session_conflict_preserves_machine_readable_code() {
        let error = ApiError::from(SelfHostError::BulkImportSessionConflict {
            code: "bulk_import_session_mismatch",
            detail: "wrong session".to_owned(),
        });

        assert_eq!(error.status, StatusCode::CONFLICT);
        assert_eq!(error.body.error, "bulk_import_session_mismatch");
        assert_eq!(error.body.detail.as_deref(), Some("wrong session"));
    }
}
