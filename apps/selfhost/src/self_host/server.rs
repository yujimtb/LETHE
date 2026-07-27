use axum::body::{Body, Bytes};
use axum::extract::rejection::{BytesRejection, JsonRejection, QueryRejection};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_LENGTH, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::self_host::app::{
    AppService, AtomicPageReport, BulkImportSessionReport, ImportReport, SelfHostError,
    SourceBlobReceipt, SourceObservationExportPage, SourceObservationExportQuery,
    SupplementalWriteRequest, WriteEnvelope,
};
use lethe_adapter_api::traits::ObservationDraft;
use lethe_api::api::envelope::{ErrorResponse, ResponseEnvelope};
use lethe_api::api::grep::PreparedGrepQuery;
use lethe_api::api::health::HealthResponse;
use lethe_api::api::pagination::{
    KeysetCursorError, PaginationParams, decode_keyset_cursor, encode_keyset_cursor,
};
use lethe_core::domain::OperationalEventId;
use lethe_core::domain::{BlobRef, ProjectionRef};
use lethe_history::{
    HistoryError, HistoryImportCommand, HistoryImportResult, HistoryInventoryReport,
    HistoryInventoryRequest, HistoryQueryRequest, HistoryQueryResponse,
};
use lethe_projection_claim_queue::ClaimState;
use lethe_projection_cognition::CardState;
use lethe_storage_api::{
    CutoverFixture, CutoverHealth, CutoverInventoryItem, CutoverReadinessReport, CutoverState,
    OperationalAppendOutcome, OperationalAppendRequest, OperationalEventFilter,
    OperationalEventStats, StoredOperationalEvent,
};

const IMPORT_REQUEST_BODY_LIMIT_BYTES: usize = 128 * 1024 * 1024;

pub fn build_router(service: AppService) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/health/deep", get(deep_health))
        .route("/admin/sync", post(sync_now))
        .route(
            "/api/import/observation-drafts",
            post(import_observation_drafts)
                .layer(DefaultBodyLimit::max(IMPORT_REQUEST_BODY_LIMIT_BYTES)),
        )
        .route(
            "/api/v2/import/observation-drafts",
            post(import_observation_drafts_v2)
                .layer(DefaultBodyLimit::max(IMPORT_REQUEST_BODY_LIMIT_BYTES)),
        )
        .route(
            "/api/v3/import/atomic-observation-pages",
            post(import_atomic_observation_page)
                .layer(DefaultBodyLimit::max(IMPORT_REQUEST_BODY_LIMIT_BYTES)),
        )
        .route(
            "/api/v3/import/source-blobs/{sha256}",
            put(put_source_blob).layer(DefaultBodyLimit::max(service.source_blob_body_limit())),
        )
        .route(
            "/api/v3/export/source-observations",
            get(export_source_observations),
        )
        .route(
            "/api/v3/source-units/{source_instance_id}/bootstrap",
            post(bootstrap_v3_source_unit),
        )
        .route("/api/v2/cutover/units", get(cutover_inventory))
        .route(
            "/api/v2/cutover/units/{source_instance_id}",
            get(cutover_health),
        )
        .route(
            "/api/v2/cutover/units/{source_instance_id}/register",
            post(cutover_register),
        )
        .route(
            "/api/v2/cutover/units/{source_instance_id}/drain",
            post(cutover_drain),
        )
        .route(
            "/api/v2/cutover/units/{source_instance_id}/readiness",
            post(cutover_readiness),
        )
        .route(
            "/api/v2/cutover/units/{source_instance_id}/activate",
            post(cutover_activate),
        )
        .route(
            "/api/v2/cutover/units/{source_instance_id}/rollback",
            post(cutover_rollback),
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
            "/api/operational-events",
            get(operational_event_page).post(append_operational_events),
        )
        .route(
            "/api/v2/operational-events",
            get(operational_event_filter_page),
        )
        .route(
            "/api/v2/projections/{projection_id}/records",
            get(projection_records_v2),
        )
        .route(
            "/api/v2/projections/{projection_id}/records/{record_id}",
            get(projection_record_detail_v2),
        )
        .route(
            "/api/v2/projections/{projection_id}/records/{record_id}/slides",
            get(projection_record_slides_v2),
        )
        .route(
            "/api/v2/projections/{projection_id}/records/{record_id}/messages",
            get(projection_record_messages_v2),
        )
        .route(
            "/api/v2/projections/{projection_id}/records/{record_id}/timeline",
            get(projection_record_timeline_v2),
        )
        .route(
            "/api/v2/projections/{projection_id}/exact",
            post(projection_exact_v2),
        )
        .route(
            "/api/v2/projections/{projection_id}/grep",
            post(projection_grep_v2),
        )
        .route("/api/v2/search-jobs/{job_id}", get(search_job_v2))
        .route("/api/v2/projections/claim-queue", get(claim_queue_v2))
        .route("/api/v2/projections/card-queue", get(card_queue_v2))
        .route("/api/v2/projections/reply-slo", get(reply_slo_v2))
        .route(
            "/api/operational-events/stats",
            get(operational_event_stats),
        )
        .route(
            "/api/operational-events/{event_id}",
            get(operational_event_by_id),
        )
        .route(
            "/api/operational-streams/{stream_id}",
            get(operational_stream_page),
        )
        .route(
            "/api/operational-blobs",
            post(put_operational_blob)
                .layer(DefaultBodyLimit::max(service.operational_blob_body_limit())),
        )
        .route(
            "/api/operational-blobs/{blob_hash}",
            get(get_operational_blob),
        )
        .route(
            "/api/history/imports/inventory",
            post(inventory_history)
                .layer(DefaultBodyLimit::max(service.operational_blob_body_limit())),
        )
        .route(
            "/api/history/imports",
            post(import_history)
                .layer(DefaultBodyLimit::max(service.operational_blob_body_limit())),
        )
        .route("/api/history/query", post(history_query))
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

#[derive(Debug, Deserialize)]
struct AppendOperationalEventsRequest {
    requests: Vec<OperationalAppendRequest>,
}

#[derive(Debug, Serialize)]
struct AppendOperationalEventsResponse {
    outcomes: Vec<OperationalAppendOutcome>,
}

#[derive(Debug, Deserialize)]
struct OperationalCursorQuery {
    after_cursor: u64,
    limit: usize,
}

#[derive(Debug, Deserialize)]
struct OperationalStreamQuery {
    after_stream_version: u64,
    limit: usize,
}

#[derive(Debug, Serialize)]
struct OperationalEventPageResponse {
    events: Vec<StoredOperationalEvent>,
    next_cursor: u64,
}

#[derive(Debug, Serialize)]
struct OperationalStreamPageResponse {
    events: Vec<StoredOperationalEvent>,
    next_stream_version: u64,
}

#[derive(Debug, Deserialize, Default)]
struct OperationalEventFilterQuery {
    cursor: Option<String>,
    #[serde(default = "default_keyset_limit")]
    limit: usize,
    correlation_id: Option<String>,
    causation_id: Option<String>,
    event_type: Option<String>,
    stream_id: Option<String>,
    actor_id: Option<String>,
    occurred_at_from: Option<DateTime<Utc>>,
    occurred_at_to: Option<DateTime<Utc>>,
}

fn default_keyset_limit() -> usize {
    20
}

#[derive(Debug, Deserialize, Default)]
struct KeysetReadQuery {
    mode: Option<String>,
    pin: Option<String>,
    cursor: Option<String>,
    #[serde(default = "default_keyset_limit")]
    limit: usize,
}

#[derive(Debug, Serialize)]
struct OperationalEventFilterPageResponse {
    events: Vec<StoredOperationalEvent>,
    next_cursor: Option<String>,
}

async fn operational_event_filter_page(
    State(service): State<AppService>,
    headers: HeaderMap,
    Query(query): Query<OperationalEventFilterQuery>,
) -> Result<Json<OperationalEventFilterPageResponse>, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:operational").await?;
    service.validate_operational_page_limit(query.limit)?;
    let after_cursor = query
        .cursor
        .as_deref()
        .map(|cursor| {
            decode_keyset_cursor(cursor, "v2:operational-events")
                .map_err(|error| keyset_cursor_error(error, "operational event"))?
                .sort_key
                .parse::<u64>()
                .map_err(|_| {
                    ApiError::bad_request_with_details(
                        "invalid_cursor",
                        "operational event cursor sort key is invalid".to_owned(),
                        serde_json::json!({"resource": "operational event"}),
                    )
                })
        })
        .transpose()?
        .unwrap_or(0);
    let filter = OperationalEventFilter {
        correlation_id: query.correlation_id,
        causation_id: query.causation_id.map(OperationalEventId::new),
        event_type: query.event_type,
        stream_id: query.stream_id,
        actor_id: query.actor_id,
        occurred_at_from: query.occurred_at_from,
        occurred_at_to: query.occurred_at_to,
    };
    let limit = query.limit;
    let events = tokio::task::spawn_blocking(move || {
        service.operational_events_by_filter(&filter, after_cursor, limit)
    })
    .await
    .map_err(|error| ApiError::internal(error.to_string()))??;
    let next_cursor = (events.len() == limit)
        .then(|| events.last().map(|event| event.cursor.to_string()))
        .flatten()
        .map(|sort_key| encode_keyset_cursor("v2:operational-events", &sort_key))
        .transpose()
        .map_err(|_| ApiError::internal("failed to encode operational event cursor".to_owned()))?;
    Ok(Json(OperationalEventFilterPageResponse {
        events,
        next_cursor,
    }))
}

fn keyset_cursor_error(error: KeysetCursorError, resource: &str) -> ApiError {
    let detail = match error {
        KeysetCursorError::Invalid => format!("{resource} cursor is invalid"),
        KeysetCursorError::WrongScope => format!("{resource} cursor has the wrong scope"),
    };
    ApiError::bad_request_with_details(
        "invalid_cursor",
        detail,
        serde_json::json!({"resource": resource}),
    )
}

async fn append_operational_events(
    State(service): State<AppService>,
    headers: HeaderMap,
    Json(request): Json<AppendOperationalEventsRequest>,
) -> Result<Json<AppendOperationalEventsResponse>, ApiError> {
    authorize_headers_blocking(&service, &headers, "write:operational").await?;
    if request.requests.is_empty() {
        return Err(ApiError::unprocessable_entity(
            "operational_append_empty",
            serde_json::json!({"requests": "must contain at least one event"}),
        ));
    }
    let outcomes =
        tokio::task::spawn_blocking(move || service.append_operational_events(&request.requests))
            .await
            .map_err(|error| ApiError::internal(error.to_string()))??;
    Ok(Json(AppendOperationalEventsResponse { outcomes }))
}

async fn operational_event_stats(
    State(service): State<AppService>,
    headers: HeaderMap,
) -> Result<Json<OperationalEventStats>, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:operational").await?;
    let stats = tokio::task::spawn_blocking(move || service.operational_event_stats())
        .await
        .map_err(|error| ApiError::internal(error.to_string()))??;
    Ok(Json(stats))
}

async fn operational_event_page(
    State(service): State<AppService>,
    headers: HeaderMap,
    Query(query): Query<OperationalCursorQuery>,
) -> Result<Json<OperationalEventPageResponse>, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:operational").await?;
    service.validate_operational_page_limit(query.limit)?;
    let events = tokio::task::spawn_blocking(move || {
        service.operational_event_page(query.after_cursor, query.limit)
    })
    .await
    .map_err(|error| ApiError::internal(error.to_string()))??;
    let next_cursor = events
        .last()
        .map_or(query.after_cursor, |stored| stored.cursor);
    Ok(Json(OperationalEventPageResponse {
        events,
        next_cursor,
    }))
}

async fn operational_stream_page(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(stream_id): Path<String>,
    Query(query): Query<OperationalStreamQuery>,
) -> Result<Json<OperationalStreamPageResponse>, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:operational").await?;
    service.validate_operational_page_limit(query.limit)?;
    let events = tokio::task::spawn_blocking(move || {
        service.operational_events_for_stream(&stream_id, query.after_stream_version, query.limit)
    })
    .await
    .map_err(|error| ApiError::internal(error.to_string()))??;
    let next_stream_version = events.last().map_or(query.after_stream_version, |stored| {
        stored.event.stream_version
    });
    Ok(Json(OperationalStreamPageResponse {
        events,
        next_stream_version,
    }))
}

async fn operational_event_by_id(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(event_id): Path<String>,
) -> Result<Json<StoredOperationalEvent>, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:operational").await?;
    let stored = tokio::task::spawn_blocking(move || {
        service.operational_event_by_id(&OperationalEventId::new(event_id))
    })
    .await
    .map_err(|error| ApiError::internal(error.to_string()))??
    .ok_or_else(ApiError::not_found)?;
    Ok(Json(stored))
}

async fn put_operational_blob(
    State(service): State<AppService>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<BlobRef>, ApiError> {
    authorize_headers_blocking(&service, &headers, "write:operational").await?;
    let blob_ref = tokio::task::spawn_blocking(move || service.put_operational_blob(&body))
        .await
        .map_err(|error| ApiError::internal(error.to_string()))??;
    Ok(Json(blob_ref))
}

async fn get_operational_blob(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(blob_hash): Path<String>,
) -> Result<Response, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:operational").await?;
    let blob_ref = blob_ref_from_hash(&blob_hash).ok_or_else(ApiError::not_found)?;
    let bytes = tokio::task::spawn_blocking(move || service.get_operational_blob(&blob_ref))
        .await
        .map_err(|error| ApiError::internal(error.to_string()))??
        .ok_or_else(ApiError::not_found)?;
    Ok((
        [(
            CONTENT_TYPE,
            HeaderValue::from_static("application/octet-stream"),
        )],
        bytes,
    )
        .into_response())
}

async fn inventory_history(
    State(service): State<AppService>,
    headers: HeaderMap,
    Json(request): Json<HistoryInventoryRequest>,
) -> Result<Json<HistoryInventoryReport>, ApiError> {
    authorize_headers_blocking(&service, &headers, "write:history").await?;
    let report = tokio::task::spawn_blocking(move || service.inventory_history(&request))
        .await
        .map_err(|error| ApiError::internal(error.to_string()))??;
    Ok(Json(report))
}

async fn import_history(
    State(service): State<AppService>,
    headers: HeaderMap,
    Json(command): Json<HistoryImportCommand>,
) -> Result<Json<HistoryImportResult>, ApiError> {
    authorize_headers_blocking(&service, &headers, "write:history").await?;
    let result = tokio::task::spawn_blocking(move || service.import_history(&command))
        .await
        .map_err(|error| ApiError::internal(error.to_string()))??;
    Ok(Json(result))
}

async fn history_query(
    State(service): State<AppService>,
    headers: HeaderMap,
    Json(request): Json<HistoryQueryRequest>,
) -> Result<Json<HistoryQueryResponse>, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:history").await?;
    let response = tokio::task::spawn_blocking(move || service.query_history(&request))
        .await
        .map_err(|error| ApiError::internal(error.to_string()))??;
    Ok(Json(response))
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

#[derive(Debug)]
struct AtomicObservationPageRequest {
    source_instance_id: String,
    drafts: Vec<ObservationDraft>,
}

impl<'de> Deserialize<'de> for AtomicObservationPageRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct WireRequest {
            source_instance_id: String,
            drafts: Vec<ObservationDraft>,
        }

        let request = WireRequest::deserialize(deserializer)?;
        if request.source_instance_id.trim().is_empty() {
            return Err(serde::de::Error::custom(
                "source_instance_id must not be blank",
            ));
        }
        if request.drafts.is_empty() {
            return Err(serde::de::Error::custom(
                "drafts must contain at least one item",
            ));
        }
        Ok(Self {
            source_instance_id: request.source_instance_id,
            drafts: request.drafts,
        })
    }
}

#[derive(Debug, Deserialize)]
struct CutoverTransitionRequest {
    authority: String,
    reason: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct V3SourceUnitBootstrapRequest {
    authority: String,
    reason: String,
}

#[derive(Debug, Deserialize, Default)]
struct CutoverReadinessRequest {
    fixture: Option<CutoverFixture>,
}

#[derive(Debug, Deserialize)]
struct CutoverActivateRequest {
    authority: String,
    reason: String,
    fixture: CutoverFixture,
}

async fn health(State(service): State<AppService>) -> Result<Json<HealthResponse>, ApiError> {
    let health = tokio::task::spawn_blocking(move || service.health())
        .await
        .map_err(|error| ApiError::internal(error.to_string()))??;
    Ok(Json(health))
}

async fn authorize_headers_blocking(
    service: &AppService,
    headers: &HeaderMap,
    required_scope: &str,
) -> Result<(), ApiError> {
    let service = service.clone();
    let headers = headers.clone();
    let required_scope = required_scope.to_owned();
    tokio::task::spawn_blocking(move || service.authorize_headers(&headers, &required_scope))
        .await
        .map_err(|error| ApiError::internal(error.to_string()))??;
    Ok(())
}

async fn authorize_headers_all_blocking(
    service: &AppService,
    headers: &HeaderMap,
    required_scopes: &[&str],
) -> Result<(), ApiError> {
    let service = service.clone();
    let headers = headers.clone();
    let required_scopes = required_scopes
        .iter()
        .map(|scope| (*scope).to_owned())
        .collect::<Vec<_>>();
    tokio::task::spawn_blocking(move || {
        let required_scopes = required_scopes
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        service.authorize_headers_all(&headers, &required_scopes)
    })
    .await
    .map_err(|error| ApiError::internal(error.to_string()))??;
    Ok(())
}

async fn service_blocking<T, F>(operation: F) -> Result<T, ApiError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, SelfHostError> + Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| ApiError::internal(error.to_string()))?
        .map_err(ApiError::from)
}

async fn deep_health(
    State(service): State<AppService>,
    headers: HeaderMap,
) -> Result<Json<HealthResponse>, ApiError> {
    authorize_headers_blocking(&service, &headers, "admin:health").await?;
    let health = tokio::task::spawn_blocking(move || service.deep_health())
        .await
        .map_err(|error| ApiError::internal(error.to_string()))??;
    Ok(Json(health))
}

async fn sync_now(
    State(service): State<AppService>,
    headers: HeaderMap,
) -> Result<Json<crate::self_host::app::SyncReport>, ApiError> {
    authorize_headers_blocking(&service, &headers, "admin:sync").await?;
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
    authorize_headers_blocking(&service, &headers, "write:observations").await?;
    let admission_generation = cutover_generation(&headers)?;
    let spawn_blocking_started_at = std::time::Instant::now();
    let report = tokio::task::spawn_blocking(move || {
        service.ingest_observation_drafts_with_admission_generation_and_spawn_wait(
            request.drafts,
            &request.source_instance_id,
            request.bulk_session_id.as_deref(),
            admission_generation,
            spawn_blocking_started_at.elapsed(),
        )
    })
    .await
    .map_err(|err| ApiError::internal(err.to_string()))??;
    Ok(Json(report))
}

async fn import_observation_drafts_v2(
    State(service): State<AppService>,
    headers: HeaderMap,
    request: Result<Json<ImportObservationDraftsRequest>, JsonRejection>,
) -> Result<Json<ImportReport>, ApiError> {
    authorize_headers_blocking(&service, &headers, "write:observations").await?;
    let request = match request {
        Ok(Json(request)) => request,
        Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            let actual_bytes = headers
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<usize>().ok());
            return Err(ApiError::payload_too_large(actual_bytes));
        }
        Err(rejection) => {
            return Err(ApiError::bad_request_with_details(
                "invalid_json",
                rejection.to_string(),
                serde_json::json!({"field": "request_body"}),
            ));
        }
    };
    let admission_generation = cutover_generation(&headers)?;
    let spawn_blocking_started_at = std::time::Instant::now();
    let report = tokio::task::spawn_blocking(move || {
        service.ingest_observation_drafts_v2_with_admission_generation_and_spawn_wait(
            request.drafts,
            &request.source_instance_id,
            request.bulk_session_id.as_deref(),
            admission_generation,
            spawn_blocking_started_at.elapsed(),
        )
    })
    .await
    .map_err(|err| ApiError::internal(err.to_string()))??;
    Ok(Json(report))
}

async fn import_atomic_observation_page(
    State(service): State<AppService>,
    headers: HeaderMap,
    request: Result<Json<AtomicObservationPageRequest>, JsonRejection>,
) -> Result<Json<AtomicPageReport>, ApiError> {
    authorize_headers_blocking(&service, &headers, "write:observations").await?;
    let request = match request {
        Ok(Json(request)) => request,
        Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            let actual_bytes = headers
                .get(CONTENT_LENGTH)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<usize>().ok());
            return Err(ApiError::payload_too_large(actual_bytes));
        }
        Err(rejection) => {
            return Err(ApiError::bad_request_with_details(
                "invalid_atomic_page_request",
                rejection.to_string(),
                serde_json::json!({"field": "request_body"}),
            ));
        }
    };
    let admission_generation = required_cutover_generation(&headers)?;
    let report = tokio::task::spawn_blocking(move || {
        service.ingest_atomic_observation_page(
            request.drafts,
            &request.source_instance_id,
            admission_generation,
        )
    })
    .await
    .map_err(|error| ApiError::internal(error.to_string()))??;
    Ok(Json(report))
}

async fn put_source_blob(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(expected_sha256): Path<String>,
    body: Result<Bytes, BytesRejection>,
) -> Result<Json<SourceBlobReceipt>, ApiError> {
    authorize_headers_blocking(&service, &headers, "write:observations").await?;
    let source_instance_id = required_non_blank_header(&headers, "x-lethe-source-instance")?;
    let admission_generation = required_cutover_generation(&headers)?;
    let body = match body {
        Ok(body) => body,
        Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
            return Err(ApiError::source_blob_too_large(
                service.source_blob_body_limit(),
            ));
        }
        Err(rejection) => {
            return Err(ApiError::bad_request_with_details(
                "invalid_blob_body",
                rejection.to_string(),
                serde_json::json!({"field": "request_body"}),
            ));
        }
    };
    let receipt = tokio::task::spawn_blocking(move || {
        service.put_source_blob(
            &source_instance_id,
            admission_generation,
            &expected_sha256,
            &body,
        )
    })
    .await
    .map_err(|error| ApiError::internal(error.to_string()))??;
    Ok(Json(receipt))
}

async fn export_source_observations(
    State(service): State<AppService>,
    headers: HeaderMap,
    query: Result<Query<SourceObservationExportQuery>, QueryRejection>,
) -> Result<Json<SourceObservationExportPage>, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:source-observations").await?;
    let Query(query) = query.map_err(|rejection| {
        ApiError::bad_request_with_details(
            "source_export_query_invalid",
            rejection.to_string(),
            serde_json::json!({"field": "query"}),
        )
    })?;
    let page = tokio::task::spawn_blocking(move || service.export_source_observations(&query))
        .await
        .map_err(|error| ApiError::internal(error.to_string()))??;
    Ok(Json(page))
}

async fn cutover_inventory(
    State(service): State<AppService>,
    headers: HeaderMap,
) -> Result<Json<Vec<CutoverInventoryItem>>, ApiError> {
    authorize_headers_blocking(&service, &headers, "admin:cutover").await?;
    let inventory = tokio::task::spawn_blocking(move || service.cutover_inventory())
        .await
        .map_err(|error| ApiError::internal(error.to_string()))??;
    Ok(Json(inventory))
}

async fn bootstrap_v3_source_unit(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(source_instance_id): Path<String>,
    Json(request): Json<V3SourceUnitBootstrapRequest>,
) -> Result<Json<CutoverState>, ApiError> {
    authorize_headers_blocking(&service, &headers, "admin:cutover").await?;
    let state = tokio::task::spawn_blocking(move || {
        service.bootstrap_v3_source_unit(&source_instance_id, &request.authority, &request.reason)
    })
    .await
    .map_err(|error| ApiError::internal(error.to_string()))??;
    Ok(Json(state))
}

async fn cutover_health(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(source_instance_id): Path<String>,
) -> Result<Json<CutoverHealth>, ApiError> {
    authorize_headers_blocking(&service, &headers, "admin:cutover").await?;
    let health = tokio::task::spawn_blocking(move || service.cutover_health(&source_instance_id))
        .await
        .map_err(|error| ApiError::internal(error.to_string()))??;
    Ok(Json(health))
}

async fn cutover_register(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(source_instance_id): Path<String>,
    Json(request): Json<CutoverTransitionRequest>,
) -> Result<Json<CutoverState>, ApiError> {
    authorize_headers_blocking(&service, &headers, "admin:cutover").await?;
    let state = tokio::task::spawn_blocking(move || {
        service.cutover_register_unit(&source_instance_id, &request.authority, &request.reason)
    })
    .await
    .map_err(|error| ApiError::internal(error.to_string()))??;
    Ok(Json(state))
}

async fn cutover_drain(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(source_instance_id): Path<String>,
    Json(request): Json<CutoverTransitionRequest>,
) -> Result<Json<CutoverState>, ApiError> {
    authorize_headers_blocking(&service, &headers, "admin:cutover").await?;
    let state = tokio::task::spawn_blocking(move || {
        service.cutover_begin_drain(&source_instance_id, &request.authority, &request.reason)
    })
    .await
    .map_err(|error| ApiError::internal(error.to_string()))??;
    Ok(Json(state))
}

async fn cutover_readiness(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(source_instance_id): Path<String>,
    Json(request): Json<CutoverReadinessRequest>,
) -> Result<Json<CutoverReadinessReport>, ApiError> {
    authorize_headers_blocking(&service, &headers, "admin:cutover").await?;
    let report = tokio::task::spawn_blocking(move || {
        service.cutover_readiness(&source_instance_id, request.fixture.as_ref())
    })
    .await
    .map_err(|error| ApiError::internal(error.to_string()))??;
    Ok(Json(report))
}

async fn cutover_activate(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(source_instance_id): Path<String>,
    Json(request): Json<CutoverActivateRequest>,
) -> Result<Json<CutoverState>, ApiError> {
    authorize_headers_blocking(&service, &headers, "admin:cutover").await?;
    let state = tokio::task::spawn_blocking(move || {
        service.cutover_activate(
            &source_instance_id,
            &request.authority,
            &request.reason,
            &request.fixture,
        )
    })
    .await
    .map_err(|error| ApiError::internal(error.to_string()))??;
    Ok(Json(state))
}

async fn cutover_rollback(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(source_instance_id): Path<String>,
    Json(request): Json<CutoverTransitionRequest>,
) -> Result<Json<CutoverState>, ApiError> {
    authorize_headers_blocking(&service, &headers, "admin:cutover").await?;
    let state = tokio::task::spawn_blocking(move || {
        service.cutover_rollback(&source_instance_id, &request.authority, &request.reason)
    })
    .await
    .map_err(|error| ApiError::internal(error.to_string()))??;
    Ok(Json(state))
}

fn cutover_generation(headers: &HeaderMap) -> Result<Option<u64>, ApiError> {
    let Some(value) = headers.get("x-lethe-admission-generation") else {
        return Ok(None);
    };
    let raw = value
        .to_str()
        .map_err(|_| ApiError::bad_request("invalid x-lethe-admission-generation header"))?;
    let generation = raw.parse::<u64>().map_err(|_| {
        ApiError::bad_request("x-lethe-admission-generation must be a positive integer")
    })?;
    if generation == 0 {
        return Err(ApiError::bad_request(
            "x-lethe-admission-generation must be a positive integer",
        ));
    }
    Ok(Some(generation))
}

fn required_cutover_generation(headers: &HeaderMap) -> Result<u64, ApiError> {
    cutover_generation(headers)?.ok_or_else(|| {
        ApiError::bad_request_with_details(
            "admission_generation_required",
            "x-lethe-admission-generation header is required".to_owned(),
            serde_json::json!({"header": "X-LETHE-Admission-Generation"}),
        )
    })
}

fn required_non_blank_header(
    headers: &HeaderMap,
    header_name: &'static str,
) -> Result<String, ApiError> {
    let raw = headers
        .get(header_name)
        .ok_or_else(|| {
            ApiError::bad_request_with_details(
                "source_instance_required",
                format!("{header_name} header is required"),
                serde_json::json!({"header": "X-LETHE-Source-Instance"}),
            )
        })?
        .to_str()
        .map_err(|_| {
            ApiError::bad_request_with_details(
                "source_instance_invalid",
                format!("{header_name} header must be valid UTF-8"),
                serde_json::json!({"header": "X-LETHE-Source-Instance"}),
            )
        })?;
    if raw.trim().is_empty() {
        return Err(ApiError::bad_request_with_details(
            "source_instance_required",
            format!("{header_name} header must not be blank"),
            serde_json::json!({"header": "X-LETHE-Source-Instance"}),
        ));
    }
    Ok(raw.to_owned())
}

async fn begin_bulk_import_session(
    State(service): State<AppService>,
    headers: HeaderMap,
) -> Result<Json<BulkImportSessionReport>, ApiError> {
    authorize_headers_blocking(&service, &headers, "write:observations").await?;
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
    authorize_headers_blocking(&service, &headers, "write:observations").await?;
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
    authorize_headers_blocking(&service, &headers, "read:corpus").await?;
    let response = service_blocking(move || {
        service.claim_queue_response_filtered(
            query.state,
            None,
            query.backfill,
            query.limit.unwrap_or(20),
            query.cursor.as_deref(),
        )
    })
    .await?;
    Ok(Json(response))
}

async fn decisions(
    State(service): State<AppService>,
    headers: HeaderMap,
    Query(query): Query<DecisionsQuery>,
) -> Result<
    Json<ResponseEnvelope<crate::self_host::app::projection_api::DecisionSearchPage>>,
    ApiError,
> {
    authorize_headers_blocking(&service, &headers, "read:corpus").await?;
    let response = service_blocking(move || {
        service.decision_search_response(query.q.as_deref(), query.limit.unwrap_or(20))
    })
    .await?;
    Ok(Json(response))
}

async fn freshness(
    State(service): State<AppService>,
    headers: HeaderMap,
) -> Result<Json<ResponseEnvelope<lethe_projection_cognition::FreshnessProjection>>, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:corpus").await?;
    Ok(Json(
        service_blocking(move || service.freshness_response()).await?,
    ))
}

async fn reply_slo(
    State(service): State<AppService>,
    headers: HeaderMap,
) -> Result<Json<ResponseEnvelope<lethe_projection_cognition::ReplySloProjection>>, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:corpus").await?;
    Ok(Json(
        service_blocking(move || service.reply_slo_response()).await?,
    ))
}

async fn break_glass(
    State(service): State<AppService>,
    headers: HeaderMap,
) -> Result<Json<ResponseEnvelope<crate::self_host::app::BreakGlassProjection>>, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:corpus").await?;
    Ok(Json(
        service_blocking(move || service.break_glass_response()).await?,
    ))
}

async fn resume_snapshot(
    State(service): State<AppService>,
    headers: HeaderMap,
) -> Result<Json<ResponseEnvelope<lethe_projection_cognition::ResumeSnapshotProjection>>, ApiError>
{
    authorize_headers_blocking(&service, &headers, "read:corpus").await?;
    Ok(Json(
        service_blocking(move || service.resume_snapshot_response()).await?,
    ))
}

async fn plan_state(
    State(service): State<AppService>,
    headers: HeaderMap,
) -> Result<Json<ResponseEnvelope<lethe_projection_cognition::PlanStateProjection>>, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:corpus").await?;
    Ok(Json(
        service_blocking(move || service.plan_state_response()).await?,
    ))
}

async fn card_queue(
    State(service): State<AppService>,
    headers: HeaderMap,
    Query(query): Query<CardQueueQuery>,
) -> Result<Json<ResponseEnvelope<crate::self_host::app::projection_api::CardQueuePage>>, ApiError>
{
    authorize_headers_blocking(&service, &headers, "read:corpus").await?;
    let response = service_blocking(move || {
        service.card_queue_response(
            query.state,
            query.channel.as_deref(),
            query.automatic,
            query.limit.unwrap_or(20),
            query.cursor.as_deref(),
        )
    })
    .await?;
    Ok(Json(response))
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
    authorize_headers_blocking(&service, &headers, "write:supplemental").await?;
    let record = service_blocking(move || service.write_supplemental(request)).await?;
    Ok((StatusCode::CREATED, Json(WriteEnvelope { data: record })))
}

async fn projection_blob(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path((projection_id, blob_hash)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:persons").await?;
    ensure_projection_person_page(&projection_id)?;
    let Some(blob_ref) = blob_ref_from_hash(&blob_hash) else {
        return Err(ApiError::not_found());
    };
    let bytes = service_blocking(move || {
        service.projection_blob_bytes(&ProjectionRef::new(&projection_id), &blob_ref)
    })
    .await?;
    let Some(bytes) = bytes else {
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
    authorize_headers_blocking(&service, &headers, scope).await?;
    Ok(Json(
        service_blocking(move || service.lineage_manifest(&projection_id)).await?,
    ))
}
async fn projection_records(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(projection_id): Path<String>,
    Query(query): Query<PersonsQuery>,
) -> Result<Json<ResponseEnvelope<serde_json::Value>>, ApiError> {
    match projection_id.as_str() {
        "proj:person-page" => {
            authorize_headers_blocking(&service, &headers, "read:persons").await?;
            Ok(Json(
                service_blocking(move || {
                    service.persons_response(
                        query.mode.as_deref(),
                        query.pin.as_deref(),
                        &query.pagination,
                    )
                })
                .await?,
            ))
        }
        "proj:corpus" => {
            authorize_headers_blocking(&service, &headers, "read:corpus").await?;
            Ok(Json(
                service_blocking(move || {
                    service.corpus_records_response(
                        query.mode.as_deref(),
                        query.pin.as_deref(),
                        &query.pagination,
                    )
                })
                .await?,
            ))
        }
        _ => Err(ApiError::not_found()),
    }
}

async fn claim_queue_v2(
    State(service): State<AppService>,
    headers: HeaderMap,
    Query(query): Query<ClaimQueueQuery>,
) -> Result<
    Json<ResponseEnvelope<crate::self_host::app::projection_api::ClaimQueueKeysetPage>>,
    ApiError,
> {
    authorize_headers_blocking(&service, &headers, "read:corpus").await?;
    let response = service_blocking(move || {
        service.claim_queue_keyset_response(
            query.state,
            query.backfill,
            query.limit.unwrap_or_else(default_keyset_limit),
            query.cursor.as_deref(),
        )
    })
    .await?;
    Ok(Json(response))
}

async fn card_queue_v2(
    State(service): State<AppService>,
    headers: HeaderMap,
    Query(query): Query<CardQueueQuery>,
) -> Result<
    Json<ResponseEnvelope<crate::self_host::app::projection_api::CardQueueKeysetPage>>,
    ApiError,
> {
    authorize_headers_blocking(&service, &headers, "read:corpus").await?;
    let response = service_blocking(move || {
        service.card_queue_keyset_response(
            query.state,
            query.channel.as_deref(),
            query.automatic,
            query.limit.unwrap_or_else(default_keyset_limit),
            query.cursor.as_deref(),
        )
    })
    .await?;
    Ok(Json(response))
}

async fn reply_slo_v2(
    State(service): State<AppService>,
    headers: HeaderMap,
    Query(query): Query<KeysetReadQuery>,
) -> Result<Json<ResponseEnvelope<serde_json::Value>>, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:corpus").await?;
    Ok(Json(
        service_blocking(move || {
            service.reply_slo_keyset_response(query.limit, query.cursor.as_deref())
        })
        .await?,
    ))
}

async fn projection_records_v2(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(projection_id): Path<String>,
    Query(query): Query<KeysetReadQuery>,
) -> Result<Json<ResponseEnvelope<serde_json::Value>>, ApiError> {
    match projection_id.as_str() {
        "proj:person-page" => {
            authorize_headers_blocking(&service, &headers, "read:persons").await?;
            Ok(Json(
                service_blocking(move || {
                    service.persons_keyset_response(
                        query.mode.as_deref(),
                        query.pin.as_deref(),
                        query.limit,
                        query.cursor.as_deref(),
                    )
                })
                .await?,
            ))
        }
        "proj:corpus" => {
            authorize_headers_blocking(&service, &headers, "read:corpus").await?;
            Ok(Json(
                service_blocking(move || {
                    service.corpus_records_keyset_response(
                        query.mode.as_deref(),
                        query.pin.as_deref(),
                        query.limit,
                        query.cursor.as_deref(),
                    )
                })
                .await?,
            ))
        }
        _ => Err(ApiError::not_found()),
    }
}

async fn projection_record_detail_v2(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path((projection_id, record_id)): Path<(String, String)>,
    Query(query): Query<KeysetReadQuery>,
) -> Result<Json<ResponseEnvelope<serde_json::Value>>, ApiError> {
    match projection_id.as_str() {
        "proj:person-page" => {
            authorize_headers_all_blocking(&service, &headers, &["read:persons", "read:timeline"])
                .await?;
            Ok(Json(
                service_blocking(move || {
                    service.person_detail_keyset_response(
                        &record_id,
                        query.mode.as_deref(),
                        query.pin.as_deref(),
                        query.limit,
                        query.cursor.as_deref(),
                    )
                })
                .await?,
            ))
        }
        _ => Err(ApiError::not_found()),
    }
}

async fn projection_record_slides_v2(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path((projection_id, record_id)): Path<(String, String)>,
    Query(query): Query<KeysetReadQuery>,
) -> Result<Json<ResponseEnvelope<serde_json::Value>>, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:timeline").await?;
    ensure_projection_person_page(&projection_id)?;
    Ok(Json(
        service_blocking(move || {
            service.person_slides_keyset_response(
                &record_id,
                query.mode.as_deref(),
                query.pin.as_deref(),
                query.limit,
                query.cursor.as_deref(),
            )
        })
        .await?,
    ))
}

async fn projection_record_messages_v2(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path((projection_id, record_id)): Path<(String, String)>,
    Query(query): Query<KeysetReadQuery>,
) -> Result<Json<ResponseEnvelope<serde_json::Value>>, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:timeline").await?;
    ensure_projection_person_page(&projection_id)?;
    Ok(Json(
        service_blocking(move || {
            service.person_messages_keyset_response(
                &record_id,
                query.mode.as_deref(),
                query.pin.as_deref(),
                query.limit,
                query.cursor.as_deref(),
            )
        })
        .await?,
    ))
}

async fn projection_record_timeline_v2(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path((projection_id, record_id)): Path<(String, String)>,
    Query(query): Query<KeysetReadQuery>,
) -> Result<Json<ResponseEnvelope<serde_json::Value>>, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:timeline").await?;
    ensure_projection_person_page(&projection_id)?;
    Ok(Json(
        service_blocking(move || {
            service.person_timeline_keyset_response(
                &record_id,
                query.mode.as_deref(),
                query.pin.as_deref(),
                query.limit,
                query.cursor.as_deref(),
            )
        })
        .await?,
    ))
}

async fn projection_exact_v2(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(projection_id): Path<String>,
    Json(request): Json<lethe_api::api::grep::ExactSearchRequest>,
) -> Result<Json<ResponseEnvelope<lethe_api::api::grep::ExactSearchResponse>>, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:corpus").await?;
    ensure_projection_corpus(&projection_id)?;
    Ok(Json(
        service_blocking(move || service.corpus_exact_response(&request)).await?,
    ))
}

async fn projection_grep_v2(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(projection_id): Path<String>,
    Json(request): Json<lethe_api::api::grep::GrepRequest>,
) -> Result<Response, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:corpus").await?;
    ensure_projection_corpus(&projection_id)?;
    let prepared =
        PreparedGrepQuery::compile(&request, service.max_page_size()).map_err(|error| {
            ApiError::bad_request_with_details(
                "invalid_search_request",
                error.to_string(),
                serde_json::json!({}),
            )
        })?;
    if prepared.requires_async_search_job() {
        let status = service_blocking(move || service.submit_corpus_search_job(request)).await?;
        return Ok((StatusCode::ACCEPTED, Json(status)).into_response());
    }
    Ok(
        Json(service_blocking(move || service.corpus_grep_response(&request)).await?)
            .into_response(),
    )
}

async fn search_job_v2(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
) -> Result<Json<crate::self_host::app::SearchJobStatus>, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:corpus").await?;
    Ok(Json(
        service_blocking(move || service.search_job_status(&job_id)).await?,
    ))
}

async fn projection_grep(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(projection_id): Path<String>,
    Json(request): Json<lethe_api::api::grep::GrepRequest>,
) -> Result<Json<ResponseEnvelope<lethe_api::api::grep::GrepResponse>>, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:corpus").await?;
    ensure_projection_corpus(&projection_id)?;
    Ok(Json(
        service_blocking(move || service.corpus_grep_response(&request)).await?,
    ))
}

async fn projection_record_detail(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path((projection_id, record_id)): Path<(String, String)>,
    Query(query): Query<ReadQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    match projection_id.as_str() {
        "proj:person-page" => {
            authorize_headers_all_blocking(&service, &headers, &["read:persons", "read:timeline"])
                .await?;
            let response = service_blocking(move || {
                service.person_detail_response(
                    &record_id,
                    query.mode.as_deref(),
                    query.pin.as_deref(),
                )
            })
            .await?;
            Ok(Json(serde_json::to_value(response)?))
        }
        "proj:corpus" => {
            authorize_headers_blocking(&service, &headers, "read:corpus").await?;
            let response =
                service_blocking(move || service.corpus_record_response(&record_id)).await?;
            Ok(Json(serde_json::to_value(response)?))
        }
        _ => Err(ApiError::not_found()),
    }
}

async fn projection_thread(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path((projection_id, thread_ts)): Path<(String, String)>,
) -> Result<Json<ResponseEnvelope<lethe_api::api::grep::ThreadResponse>>, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:corpus").await?;
    ensure_projection_corpus(&projection_id)?;
    Ok(Json(
        service_blocking(move || service.corpus_thread_response(&thread_ts)).await?,
    ))
}

async fn projection_resolve_link(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path(projection_id): Path<String>,
    Json(request): Json<lethe_api::api::grep::ResolveLinkRequest>,
) -> Result<Json<ResponseEnvelope<lethe_api::api::grep::ResolveLinkResponse>>, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:corpus").await?;
    ensure_projection_corpus(&projection_id)?;
    Ok(Json(
        service_blocking(move || service.resolve_link_response(&request)).await?,
    ))
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
    authorize_headers_blocking(&service, &headers, "read:answer-log").await?;
    ensure_projection_answer_log(&projection_id)?;
    Ok(Json(
        service_blocking(move || service.prior_qa_search_response(&request)).await?,
    ))
}

async fn projection_record_slides(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path((projection_id, record_id)): Path<(String, String)>,
    Query(query): Query<ReadQuery>,
) -> Result<Json<ResponseEnvelope<serde_json::Value>>, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:timeline").await?;
    ensure_projection_person_page(&projection_id)?;
    Ok(Json(
        service_blocking(move || {
            service.person_slides_response(&record_id, query.mode.as_deref(), query.pin.as_deref())
        })
        .await?,
    ))
}

async fn projection_record_messages(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path((projection_id, record_id)): Path<(String, String)>,
    Query(query): Query<ReadQuery>,
) -> Result<Json<ResponseEnvelope<serde_json::Value>>, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:timeline").await?;
    ensure_projection_person_page(&projection_id)?;
    Ok(Json(
        service_blocking(move || {
            service.person_messages_response(
                &record_id,
                query.mode.as_deref(),
                query.pin.as_deref(),
            )
        })
        .await?,
    ))
}

async fn projection_record_timeline(
    State(service): State<AppService>,
    headers: HeaderMap,
    Path((projection_id, record_id)): Path<(String, String)>,
    Query(query): Query<ReadQuery>,
) -> Result<Json<ResponseEnvelope<serde_json::Value>>, ApiError> {
    authorize_headers_blocking(&service, &headers, "read:timeline").await?;
    ensure_projection_person_page(&projection_id)?;
    Ok(Json(
        service_blocking(move || {
            service.person_timeline_response(
                &record_id,
                query.mode.as_deref(),
                query.pin.as_deref(),
            )
        })
        .await?,
    ))
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

    fn bad_request(detail: &str) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            body: ErrorResponse::bad_request(detail),
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

    fn bad_request_with_details(
        code: &'static str,
        detail: String,
        details: serde_json::Value,
    ) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            body: ErrorResponse {
                error: code.to_owned(),
                detail: Some(detail),
                details: Some(details),
                retry_after: None,
            },
        }
    }

    fn payload_too_large(actual_bytes: Option<usize>) -> Self {
        Self {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            body: ErrorResponse {
                error: "body_too_large".to_owned(),
                detail: Some(format!(
                    "request body exceeds configured maximum {IMPORT_REQUEST_BODY_LIMIT_BYTES} bytes"
                )),
                details: Some(serde_json::json!({
                    "actual_bytes": actual_bytes,
                    "max_bytes": IMPORT_REQUEST_BODY_LIMIT_BYTES,
                })),
                retry_after: None,
            },
        }
    }

    fn source_blob_too_large(max_bytes: usize) -> Self {
        Self {
            status: StatusCode::PAYLOAD_TOO_LARGE,
            body: ErrorResponse {
                error: "blob_too_large".to_owned(),
                detail: Some(format!(
                    "source blob body exceeds configured maximum {max_bytes} bytes"
                )),
                details: Some(serde_json::json!({"max_bytes": max_bytes})),
                retry_after: None,
            },
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
            SelfHostError::ImportConcurrencyLimit { maximum } => Self {
                status: StatusCode::TOO_MANY_REQUESTS,
                body: ErrorResponse {
                    error: "import_concurrency_limit".to_owned(),
                    detail: Some(format!(
                        "concurrent import limit {maximum} is currently full"
                    )),
                    details: Some(serde_json::json!({"maximum": maximum})),
                    retry_after: Some(1),
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
            SelfHostError::Storage(
                lethe_storage_api::StorageError::OperationalIdempotencyCollision(idempotency_key),
            ) => Self::conflict(
                "operational_idempotency_collision",
                serde_json::json!({"idempotency_key": idempotency_key}),
            ),
            SelfHostError::Storage(
                lethe_storage_api::StorageError::OperationalEventIdCollision(event_id),
            ) => Self::conflict(
                "operational_event_id_collision",
                serde_json::json!({"event_id": event_id}),
            ),
            SelfHostError::Storage(lethe_storage_api::StorageError::CutoverAdmissionDenied(
                detail,
            )) => Self {
                status: StatusCode::UNAUTHORIZED,
                body: ErrorResponse {
                    error: "authorization_failed".to_owned(),
                    detail: Some(detail),
                    details: None,
                    retry_after: None,
                },
            },
            SelfHostError::Storage(lethe_storage_api::StorageError::CutoverConflict(detail)) => {
                Self::conflict("cutover_conflict", serde_json::json!({"detail": detail}))
            }
            SelfHostError::Storage(lethe_storage_api::StorageError::CutoverRollbackRefused(
                detail,
            )) => Self::conflict(
                "cutover_rollback_refused",
                serde_json::json!({"detail": detail, "action": "forward_fix"}),
            ),
            SelfHostError::Storage(lethe_storage_api::StorageError::Invariant(detail)) => {
                Self::unprocessable_entity(
                    "operational_event_invalid",
                    serde_json::json!({"detail": detail}),
                )
            }
            SelfHostError::History(HistoryError::NotFound(_)) => Self::not_found(),
            SelfHostError::History(HistoryError::UnresolvedOwnership(detail)) => Self::conflict(
                "history_ownership_unresolved",
                serde_json::json!({"detail": detail}),
            ),
            SelfHostError::History(HistoryError::ManifestMismatch { expected, actual }) => {
                Self::conflict(
                    "history_manifest_mismatch",
                    serde_json::json!({"expected": expected, "actual": actual}),
                )
            }
            SelfHostError::History(HistoryError::SourceIdentityCollision(identity)) => {
                Self::conflict(
                    "history_source_identity_collision",
                    serde_json::json!({"identity": identity}),
                )
            }
            SelfHostError::History(HistoryError::Invariant(detail)) => {
                Self::unprocessable_entity("history_invalid", serde_json::json!({"detail": detail}))
            }
            SelfHostError::History(HistoryError::InvalidCursor(detail)) => {
                Self::unprocessable_entity(
                    "HistoryCursorInvalid",
                    serde_json::json!({"detail": detail}),
                )
            }
            SelfHostError::History(HistoryError::CursorStale {
                cursor_source,
                current_source,
            }) => Self::unprocessable_entity(
                "HistoryCursorStale",
                serde_json::json!({
                    "cursor_source": cursor_source,
                    "current_source": current_source,
                }),
            ),
            SelfHostError::History(HistoryError::ResultTooLarge { required, maximum }) => {
                Self::unprocessable_entity(
                    "HistoryResultTooLarge",
                    serde_json::json!({
                        "required": required,
                        "maximum": maximum,
                    }),
                )
            }
            SelfHostError::Ingestion(detail) => Self {
                status: StatusCode::BAD_REQUEST,
                body: ErrorResponse::bad_request(&detail),
            },
            SelfHostError::IngestionRequest {
                code,
                detail,
                details,
            } => Self::bad_request_with_details(code, detail, details),
            SelfHostError::AtomicPageRejected { failures } => Self::unprocessable_entity(
                "atomic_page_rejected",
                serde_json::json!({"failures": failures}),
            ),
            SelfHostError::AtomicPageTransient => Self {
                status: StatusCode::SERVICE_UNAVAILABLE,
                body: ErrorResponse {
                    error: "atomic_page_transient".to_owned(),
                    detail: Some(
                        "atomic page storage transaction did not commit; retry the exact page"
                            .to_owned(),
                    ),
                    details: None,
                    retry_after: Some(1),
                },
            },
            SelfHostError::SourceBlobValidation { code, details } => {
                Self::unprocessable_entity(code, details)
            }
            SelfHostError::SourceExportValidation { code, details } => {
                Self::unprocessable_entity(code, details)
            }
            SelfHostError::SourceExportUnavailable { code, details } => Self {
                status: StatusCode::SERVICE_UNAVAILABLE,
                body: ErrorResponse {
                    error: code.to_owned(),
                    detail: Some(
                        "source Observation export could not reach a safe page boundary".to_owned(),
                    ),
                    details: Some(details),
                    retry_after: None,
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

    #[test]
    fn v2_request_errors_expose_structured_limit_details() {
        let error = ApiError::from(SelfHostError::IngestionRequest {
            code: "draft_count_exceeded",
            detail: "draft count 11 exceeds configured maximum 10".to_owned(),
            details: serde_json::json!({"actual": 11, "maximum": 10}),
        });

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert_eq!(error.body.error, "draft_count_exceeded");
        assert_eq!(
            error.body.details,
            Some(serde_json::json!({"actual": 11, "maximum": 10}))
        );
    }

    #[test]
    fn import_concurrency_limit_maps_to_immediate_429() {
        let error = ApiError::from(SelfHostError::ImportConcurrencyLimit { maximum: 2 });

        assert_eq!(error.status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(error.body.error, "import_concurrency_limit");
        assert_eq!(error.body.details, Some(serde_json::json!({"maximum": 2})));
        assert_eq!(error.body.retry_after, Some(1));
    }

    #[test]
    fn page_limit_error_uses_the_frozen_machine_code() {
        let error = ApiError::from(SelfHostError::IngestionRequest {
            code: "page_limit_exceeded",
            detail: "person projection page limit 501 must be between 1 and 500".to_owned(),
            details: serde_json::json!({
                "resource": "person projection",
                "actual": 501,
                "maximum": 500,
            }),
        });

        assert_eq!(error.status, StatusCode::BAD_REQUEST);
        assert_eq!(error.body.error, "page_limit_exceeded");
        assert_eq!(
            error
                .body
                .details
                .as_ref()
                .and_then(|details| details.get("maximum")),
            Some(&serde_json::json!(500))
        );
    }
}
