use axum::body::Bytes;
use axum::extract::State;
use axum::http::header::{AUTHORIZATION, WWW_AUTHENTICATE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::{DateTime, Utc};
use lethe_api::api::envelope::ErrorResponse;
use lethe_api::api::grep::GrepOrder;
use ring::signature;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::BTreeSet;

use crate::self_host::app::SupplementalWriteRequest;
use crate::self_host::app::{AppService, CorpusSourceTypeSummary, SelfHostError};
use crate::self_host::config::{JsonWebKey, JsonWebKeySet, McpOAuthConfig};
use crate::self_host::mcp_contract::{
    ClaimQueueArguments, GetRecordArguments, GetThreadArguments, SearchDecisionsArguments,
    SearchLakeArguments, SearchLakeOrder,
};
use lethe_projection_claim_queue::ClaimState;

const MCP_RESPONSE_LIMIT: usize = 20;
const MCP_DEFAULT_LIMIT: usize = 20;
const SEARCH_LAKE_METADATA_REMOVED_FIELDS: &[&str] = &[
    "observation_id",
    "schema",
    "source_system",
    "source_type",
    "thread_key",
    "session_id",
    "parent_session_id",
    "is_sidechain",
    "message_id",
    "parent_message_id",
];
const GET_RECORD_DESCRIPTION: &str =
    "Fetch one projected corpus record by record_id; returns RecordNotFound if absent.";
const GET_THREAD_DESCRIPTION: &str = "Fetch projected thread context for a corpus record or thread key. limit defaults to 20 and is capped at 20 for MCP response safety; use cursor from the previous response to continue.";
const CLAIM_QUEUE_DESCRIPTION: &str = "List folded claim groups from the claim queue projection, filterable by state. limit is capped at 20 for MCP response safety.";
const SEARCH_DECISIONS_DESCRIPTION: &str = "Search the folded decision ledger with supersedes resolution. Query syntax: one term uses partial match; space, tab, or fullwidth-space separated terms require every term to appear in any order (AND). limit is capped at 20 for MCP response safety.";
const WRITE_SUPPLEMENTAL_DESCRIPTION: &str = "Write one supplemental record as post-processing for an observation already ingested into the lake. This is not for live self-enrichment during the same conversation. Kinds with anchor_required=true require resolved anchors; system-event kinds with anchor_required=false require payload.origin.";
const MCP_READ_SCOPE: &str = "mcp:read";
const MCP_WRITE_SUPPLEMENTAL_SCOPE: &str = "write:supplemental";
const MCP_AUTH_CHALLENGE_SCOPE: &str = "mcp:read write:supplemental";

#[derive(Debug, Clone)]
struct VerifiedToken {
    scopes: Vec<String>,
}

impl VerifiedToken {
    fn has_scope(&self, required: &str) -> bool {
        self.scopes
            .iter()
            .any(|scope| scope == "*" || scope == required)
    }
}

#[derive(Clone)]
struct McpState {
    service: AppService,
    oauth: McpOAuthConfig,
}

impl McpState {
    fn new(service: AppService) -> Self {
        let oauth = service.mcp_oauth_config();
        Self { service, oauth }
    }

    fn verify_authorization(&self, headers: &HeaderMap) -> Result<VerifiedToken, McpHttpError> {
        let Some(header) = headers.get(AUTHORIZATION) else {
            return Err(McpHttpError::invalid_token(
                &self.oauth.protected_resource_metadata_url,
                "missing bearer token",
            ));
        };
        let raw = header.to_str().map_err(|_| {
            McpHttpError::invalid_token(
                &self.oauth.protected_resource_metadata_url,
                "invalid authorization header",
            )
        })?;
        let token = raw.strip_prefix("Bearer ").ok_or_else(|| {
            McpHttpError::invalid_token(
                &self.oauth.protected_resource_metadata_url,
                "authorization must use Bearer token",
            )
        })?;
        verify_jwt(token, &self.oauth).map_err(|error| {
            McpHttpError::invalid_token(
                &self.oauth.protected_resource_metadata_url,
                &error.to_string(),
            )
        })
    }
}

pub fn build_mcp_router(service: AppService) -> Router {
    Router::new()
        .route(
            "/.well-known/oauth-protected-resource",
            get(protected_resource_metadata),
        )
        .route(
            "/.well-known/oauth-protected-resource/mcp",
            get(protected_resource_metadata),
        )
        .route("/mcp", post(mcp_post))
        .with_state(McpState::new(service))
}

async fn protected_resource_metadata(State(state): State<McpState>) -> Json<Value> {
    Json(json!({
        "resource": state.oauth.resource_url,
        "authorization_servers": [state.oauth.issuer],
        "issuer": state.oauth.issuer,
        "bearer_methods_supported": ["header"],
        "scopes_supported": [MCP_READ_SCOPE, MCP_WRITE_SUPPLEMENTAL_SCOPE],
    }))
}

async fn mcp_post(
    State(state): State<McpState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, McpHttpError> {
    let token = state.verify_authorization(&headers)?;
    let request: JsonRpcRequest = serde_json::from_slice(&body)
        .map_err(|error| McpHttpError::bad_request(format!("invalid JSON-RPC request: {error}")))?;
    if request.jsonrpc != "2.0" {
        return Err(McpHttpError::bad_request(
            "JSON-RPC version must be 2.0".to_owned(),
        ));
    }
    let Some(id) = request.id.clone() else {
        return Ok(StatusCode::ACCEPTED.into_response());
    };
    let response = match handle_json_rpc(&state, &token, request).await {
        Ok(result) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "result": result,
        }),
        Err(error) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": {
                "code": error.code,
                "message": error.message,
            }
        }),
    };
    Ok(Json(response).into_response())
}

async fn handle_json_rpc(
    state: &McpState,
    token: &VerifiedToken,
    request: JsonRpcRequest,
) -> Result<Value, JsonRpcAppError> {
    match request.method.as_str() {
        "initialize" => Ok(json!({
            "protocolVersion": "2025-03-26",
            "capabilities": { "tools": {} },
            "serverInfo": {
                "name": "lethe-mcp-read-port",
                "version": env!("CARGO_PKG_VERSION"),
            }
        })),
        "tools/list" => Ok(tools_list_result(
            &state.service.corpus_source_type_summaries()?,
        )),
        "tools/call" => {
            let params: ToolCallParams = parse_params(request.params)?;
            call_tool(
                &state.service,
                &state.oauth.protected_resource_metadata_url,
                token,
                params,
            )
        }
        other => Err(JsonRpcAppError::method_not_found(format!(
            "unsupported MCP method: {other}"
        ))),
    }
}

fn tools_list_result(source_types: &[CorpusSourceTypeSummary]) -> Value {
    json!({
        "tools": [
            tool_definition(
                "search_lake",
                &search_lake_description(source_types),
                json!({
                    "type": "object",
                    "required": ["query"],
                    "additionalProperties": false,
                    "properties": {
                        "query": { "type": "string" },
                        "source_types": {
                            "type": "array",
                            "items": { "type": "string" },
                            "description": "Optional source_type filter. Unknown values are rejected with the valid source_type list."
                        },
                        "from": {
                            "type": "string",
                            "format": "date-time",
                            "description": "Inclusive lower timestamp bound as ISO 8601/RFC3339, e.g. 2026-07-01T00:00:00Z."
                        },
                        "to": {
                            "type": "string",
                            "format": "date-time",
                            "description": "Inclusive upper timestamp bound as ISO 8601/RFC3339, e.g. 2026-07-08T23:59:59Z."
                        },
                        "order": {
                            "type": "string",
                            "enum": ["newest_first", "oldest_first"],
                            "description": "Result time order. Default is newest_first, matching existing behavior."
                        },
                        "limit": { "type": "integer", "minimum": 1, "maximum": 20 },
                        "cursor": { "type": "string" }
                    }
                })
            ),
            tool_definition(
                "get_record",
                GET_RECORD_DESCRIPTION,
                json!({
                    "type": "object",
                    "required": ["record_id"],
                    "additionalProperties": false,
                    "properties": {
                        "record_id": { "type": "string" }
                    }
                })
            ),
            tool_definition(
                "get_thread",
                GET_THREAD_DESCRIPTION,
                json!({
                    "type": "object",
                    "required": ["record_id"],
                    "additionalProperties": false,
                    "properties": {
                        "record_id": { "type": "string" },
                        "limit": { "type": "integer", "minimum": 1, "maximum": 20 },
                        "cursor": { "type": "string" }
                    }
                })
            ),
            tool_definition(
                "claim_queue",
                CLAIM_QUEUE_DESCRIPTION,
                json!({
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "state": {
                            "type": "string",
                            "enum": ["open", "dispatched", "verified", "refuted", "inconclusive", "terminated", "parked"]
                        },
                        "verification_mode": { "type": "string", "enum": ["check", "generate"] },
                        "limit": { "type": "integer", "minimum": 1, "maximum": 20 },
                        "cursor": { "type": "string" }
                    }
                })
            ),
            tool_definition(
                "search_decisions",
                SEARCH_DECISIONS_DESCRIPTION,
                json!({
                    "type": "object",
                    "required": ["query"],
                    "additionalProperties": false,
                    "properties": {
                        "query": { "type": "string" },
                        "limit": { "type": "integer", "minimum": 1, "maximum": 20 }
                    }
                })
            ),
            write_tool_definition(
                "write_supplemental",
                WRITE_SUPPLEMENTAL_DESCRIPTION,
                json!({
                    "type": "object",
                    "required": ["id", "kind", "derived_from", "payload", "created_by", "mutability"],
                    "additionalProperties": false,
                    "properties": {
                        "id": { "type": "string", "pattern": "^sup:[0-9a-fA-F-]{36}$" },
                        "kind": { "type": "string" },
                        "derived_from": {
                            "type": "object",
                            "additionalProperties": false,
                            "properties": {
                                "observations": { "type": "array", "items": { "type": "string" } },
                                "blobs": { "type": "array", "items": { "type": "string" } },
                                "supplementals": { "type": "array", "items": { "type": "string" } }
                            }
                        },
                        "payload": { "type": "object" },
                        "created_by": { "type": "string" },
                        "mutability": { "type": "string", "enum": ["append_only", "managed_cache"] },
                        "model_version": { "type": "string" },
                        "consent_metadata": { "type": "object" },
                        "lineage": { "type": "string" }
                    }
                })
            )
        ]
    })
}

fn search_lake_description(source_types: &[CorpusSourceTypeSummary]) -> String {
    let valid_source_types = valid_source_types(source_types)
        .into_iter()
        .collect::<Vec<_>>()
        .join(", ");
    let available_source_types = if source_types.is_empty() {
        "none currently present".to_owned()
    } else {
        source_types
            .iter()
            .map(|source| format!("{} ({})", source.source_type, source.records))
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "Search projected corpus text across lake records; returns record IDs and hit-centered snippets. Query syntax: one term works as before; space, tab, or fullwidth-space separated terms require every term to appear in any order (AND). Terms may be Rust regex; invalid regex terms in compound queries are matched literally. Optional from/to are ISO 8601/RFC3339 timestamps, e.g. from=\"2026-07-01T00:00:00Z\". order is newest_first (default) or oldest_first. Valid source_types: {valid_source_types}. Currently available source_types with record counts: {available_source_types}. limit is capped at 20 for MCP response safety; snippets are capped at 240 chars and matched_ranges at 20 per record. matched_ranges start/end are UTF-8 byte offsets in the searched text."
    )
}

fn tool_definition(name: &str, description: &str, input_schema: Value) -> Value {
    let security_schemes = oauth2_security_schemes(MCP_READ_SCOPE);
    json!({
        "name": name,
        "description": description,
        "inputSchema": input_schema,
        "securitySchemes": security_schemes.clone(),
        "_meta": {
            "securitySchemes": security_schemes
        },
        "annotations": {
            "readOnlyHint": true,
            "destructiveHint": false,
            "idempotentHint": true,
            "openWorldHint": false
        }
    })
}

fn write_tool_definition(name: &str, description: &str, input_schema: Value) -> Value {
    let security_schemes = oauth2_security_schemes(MCP_WRITE_SUPPLEMENTAL_SCOPE);
    json!({
        "name": name,
        "description": description,
        "inputSchema": input_schema,
        "securitySchemes": security_schemes.clone(),
        "_meta": {
            "securitySchemes": security_schemes
        },
        "annotations": {
            "readOnlyHint": false,
            "destructiveHint": false,
            "idempotentHint": false,
            "openWorldHint": false
        }
    })
}

fn oauth2_security_schemes(scope: &str) -> Value {
    json!([{ "type": "oauth2", "scopes": [scope] }])
}

fn call_tool(
    service: &AppService,
    metadata_url: &str,
    token: &VerifiedToken,
    params: ToolCallParams,
) -> Result<Value, JsonRpcAppError> {
    match params.name.as_str() {
        "search_lake" => {
            if let Some(result) = missing_scope_result(token, MCP_READ_SCOPE, metadata_url) {
                return Ok(result);
            }
            let args: SearchLakeArguments = parse_arguments(params.arguments)?;
            ensure_not_blank("query", &args.query)?;
            let limit = mcp_limit(args.limit)?;
            let requested_source_types = args.source_types.clone();
            validate_time_range(args.from.as_ref(), args.to.as_ref())?;
            let (response, source_type_summaries) = service
                .corpus_grep_response_with_source_summaries(&lethe_api::api::grep::GrepRequest {
                    pattern: args.query,
                    filters: lethe_api::api::grep::GrepFilters {
                        types: args.source_types,
                        from: args.from,
                        to: args.to,
                        ..lethe_api::api::grep::GrepFilters::default()
                    },
                    order: args.order.map(search_lake_order).unwrap_or_default(),
                    limit: Some(limit.effective_limit),
                    cursor: args.cursor,
                    ..lethe_api::api::grep::GrepRequest::default()
                })?;
            validate_source_types(&requested_source_types, &source_type_summaries)?;
            let response = mcp_search_lake_response(response);
            tool_result_with_limit_and_available_source_types(
                response,
                limit,
                &source_type_summaries,
            )
        }
        "get_record" => {
            if let Some(result) = missing_scope_result(token, MCP_READ_SCOPE, metadata_url) {
                return Ok(result);
            }
            let args: GetRecordArguments = parse_arguments(params.arguments)?;
            ensure_not_blank("record_id", &args.record_id)?;
            let response = service.corpus_record_response(&args.record_id)?;
            tool_result(response)
        }
        "get_thread" => {
            if let Some(result) = missing_scope_result(token, MCP_READ_SCOPE, metadata_url) {
                return Ok(result);
            }
            let args: GetThreadArguments = parse_arguments(params.arguments)?;
            ensure_not_blank("record_id", &args.record_id)?;
            let limit = mcp_limit(args.limit)?;
            let response = service.corpus_thread_response_paged(
                &args.record_id,
                limit.effective_limit,
                args.cursor.as_deref(),
            )?;
            tool_result_with_limit(response, limit)
        }
        "claim_queue" => {
            if let Some(result) = missing_scope_result(token, MCP_READ_SCOPE, metadata_url) {
                return Ok(result);
            }
            let args: ClaimQueueArguments = parse_arguments(params.arguments)?;
            let limit = mcp_limit(args.limit)?;
            let response = service.claim_queue_response_filtered(
                parse_claim_state(args.state.as_deref())?,
                args.verification_mode.as_deref(),
                None,
                limit.effective_limit,
                args.cursor.as_deref(),
            )?;
            tool_result_with_limit(response, limit)
        }
        "search_decisions" => {
            if let Some(result) = missing_scope_result(token, MCP_READ_SCOPE, metadata_url) {
                return Ok(result);
            }
            let args: SearchDecisionsArguments = parse_arguments(params.arguments)?;
            ensure_not_blank("query", &args.query)?;
            let limit = mcp_limit(args.limit)?;
            let response = service
                .decision_search_response(Some(args.query.as_str()), limit.effective_limit)?;
            tool_result_with_limit(response, limit)
        }
        "write_supplemental" => {
            if let Some(result) =
                missing_scope_result(token, MCP_WRITE_SUPPLEMENTAL_SCOPE, metadata_url)
            {
                return Ok(result);
            }
            let args: SupplementalWriteRequest = parse_arguments(params.arguments)?;
            let response = service.write_supplemental(args)?;
            tool_result(WriteSupplementalResult { record: response })
        }
        other => Err(JsonRpcAppError::invalid_params(format!(
            "unknown tool: {other}"
        ))),
    }
}

#[derive(Debug, Serialize)]
struct WriteSupplementalResult {
    record: lethe_core::domain::SupplementalRecord,
}

fn missing_scope_result(
    token: &VerifiedToken,
    required: &str,
    metadata_url: &str,
) -> Option<Value> {
    if token.has_scope(required) {
        None
    } else {
        Some(authorization_required_tool_result(metadata_url, required))
    }
}

fn authorization_required_tool_result(metadata_url: &str, required: &str) -> Value {
    let challenge = format!(
        "Bearer resource_metadata=\"{metadata_url}\", error=\"insufficient_scope\", error_description=\"Token lacks required scope {required}\", scope=\"{required}\""
    );
    json!({
        "content": [
            {
                "type": "text",
                "text": format!("Authentication required: token lacks required scope {required}.")
            }
        ],
        "_meta": {
            "mcp/www_authenticate": [challenge]
        },
        "isError": true
    })
}

fn parse_claim_state(value: Option<&str>) -> Result<Option<ClaimState>, JsonRpcAppError> {
    value
        .map(|raw| {
            serde_json::from_value::<ClaimState>(json!(raw)).map_err(|_| {
                JsonRpcAppError::invalid_params(format!("invalid claim state filter: {raw}"))
            })
        })
        .transpose()
}

fn search_lake_order(order: SearchLakeOrder) -> GrepOrder {
    match order {
        SearchLakeOrder::NewestFirst => GrepOrder::DateDesc,
        SearchLakeOrder::OldestFirst => GrepOrder::DateAsc,
    }
}

fn validate_source_types(
    requested: &[String],
    source_type_summaries: &[CorpusSourceTypeSummary],
) -> Result<(), JsonRpcAppError> {
    let valid = valid_source_types(source_type_summaries);
    let unknown = requested
        .iter()
        .filter(|source_type| !valid.contains(*source_type))
        .cloned()
        .collect::<Vec<_>>();
    if unknown.is_empty() {
        return Ok(());
    }
    let unknown = unknown
        .iter()
        .map(|source_type| format!("'{source_type}'"))
        .collect::<Vec<_>>()
        .join(", ");
    let valid = valid.into_iter().collect::<Vec<_>>().join(", ");
    Err(JsonRpcAppError::invalid_params(format!(
        "unknown source_type(s): {unknown}. Valid source_types: {valid}"
    )))
}

fn validate_time_range(
    from: Option<&DateTime<Utc>>,
    to: Option<&DateTime<Utc>>,
) -> Result<(), JsonRpcAppError> {
    if matches!((from, to), (Some(from), Some(to)) if from > to) {
        return Err(JsonRpcAppError::invalid_params(
            "from must be earlier than or equal to to".to_owned(),
        ));
    }
    Ok(())
}

fn valid_source_types(source_type_summaries: &[CorpusSourceTypeSummary]) -> BTreeSet<String> {
    let mut source_types = lethe_projection_corpus::supported_source_types()
        .iter()
        .map(|source_type| (*source_type).to_owned())
        .collect::<BTreeSet<_>>();
    source_types.extend(
        source_type_summaries
            .iter()
            .map(|source| source.source_type.clone()),
    );
    source_types
}

fn mcp_search_lake_response(
    mut response: lethe_api::api::envelope::ResponseEnvelope<lethe_api::api::grep::GrepResponse>,
) -> lethe_api::api::envelope::ResponseEnvelope<lethe_api::api::grep::GrepResponse> {
    for matched in &mut response.data.matches {
        trim_search_lake_match_metadata(matched);
    }
    response
}

fn trim_search_lake_match_metadata(matched: &mut lethe_api::api::grep::GrepMatch) {
    if let Value::Object(metadata) = &mut matched.metadata {
        for field in SEARCH_LAKE_METADATA_REMOVED_FIELDS {
            metadata.remove(*field);
        }
    }
}

fn tool_result<T: Serialize>(value: T) -> Result<Value, JsonRpcAppError> {
    let structured = serde_json::to_value(value)
        .map_err(|error| JsonRpcAppError::internal(error.to_string()))?;
    let text = serde_json::to_string_pretty(&structured)
        .map_err(|error| JsonRpcAppError::internal(error.to_string()))?;
    Ok(json!({
        "content": [
            { "type": "text", "text": text }
        ],
        "structuredContent": structured,
        "isError": false,
    }))
}

#[derive(Debug, Clone, Copy, Serialize)]
struct McpLimitInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    requested_limit: Option<usize>,
    effective_limit: usize,
    max_limit: usize,
    limit_clamped: bool,
}

fn mcp_limit(requested_limit: Option<usize>) -> Result<McpLimitInfo, JsonRpcAppError> {
    let requested = requested_limit.unwrap_or(MCP_DEFAULT_LIMIT);
    if requested == 0 {
        return Err(JsonRpcAppError::invalid_params(format!(
            "limit must be between 1 and {MCP_RESPONSE_LIMIT}"
        )));
    }
    let effective_limit = requested.min(MCP_RESPONSE_LIMIT);
    Ok(McpLimitInfo {
        requested_limit,
        effective_limit,
        max_limit: MCP_RESPONSE_LIMIT,
        limit_clamped: requested > MCP_RESPONSE_LIMIT,
    })
}

fn tool_result_with_limit<T: Serialize>(
    value: T,
    limit: McpLimitInfo,
) -> Result<Value, JsonRpcAppError> {
    tool_result_with_limit_meta(value, limit, Vec::new())
}

fn tool_result_with_limit_and_available_source_types<T: Serialize>(
    value: T,
    limit: McpLimitInfo,
    source_types: &[CorpusSourceTypeSummary],
) -> Result<Value, JsonRpcAppError> {
    tool_result_with_limit_meta(
        value,
        limit,
        vec![(
            "lethe/available_source_types",
            serde_json::to_value(source_types)
                .map_err(|error| JsonRpcAppError::internal(error.to_string()))?,
        )],
    )
}

fn tool_result_with_limit_meta<T: Serialize>(
    value: T,
    limit: McpLimitInfo,
    extra_meta: Vec<(&'static str, Value)>,
) -> Result<Value, JsonRpcAppError> {
    let structured = serde_json::to_value(value)
        .map_err(|error| JsonRpcAppError::internal(error.to_string()))?;
    let text = serde_json::to_string_pretty(&structured)
        .map_err(|error| JsonRpcAppError::internal(error.to_string()))?;
    let mut meta = serde_json::Map::new();
    meta.insert(
        "lethe/response_limit".to_owned(),
        serde_json::to_value(limit)
            .map_err(|error| JsonRpcAppError::internal(error.to_string()))?,
    );
    for (key, value) in extra_meta {
        meta.insert(key.to_owned(), value);
    }
    Ok(json!({
        "content": [
            { "type": "text", "text": text }
        ],
        "structuredContent": structured,
        "_meta": meta,
        "isError": false,
    }))
}

fn parse_params<T: for<'de> Deserialize<'de>>(params: Option<Value>) -> Result<T, JsonRpcAppError> {
    serde_json::from_value(params.unwrap_or_else(|| json!({})))
        .map_err(|error| JsonRpcAppError::invalid_params(error.to_string()))
}

fn parse_arguments<T: for<'de> Deserialize<'de>>(
    arguments: Option<Value>,
) -> Result<T, JsonRpcAppError> {
    serde_json::from_value(arguments.unwrap_or_else(|| json!({})))
        .map_err(|error| JsonRpcAppError::invalid_params(error.to_string()))
}

fn ensure_not_blank(name: &str, value: &str) -> Result<(), JsonRpcAppError> {
    if value.trim().is_empty() {
        Err(JsonRpcAppError::invalid_params(format!(
            "{name} must not be blank"
        )))
    } else {
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    jsonrpc: String,
    #[serde(default)]
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct ToolCallParams {
    name: String,
    #[serde(default)]
    arguments: Option<Value>,
}

struct JsonRpcAppError {
    code: i64,
    message: String,
}

impl JsonRpcAppError {
    fn invalid_params(message: String) -> Self {
        Self {
            code: -32602,
            message,
        }
    }

    fn method_not_found(message: String) -> Self {
        Self {
            code: -32601,
            message,
        }
    }

    fn not_found(message: String) -> Self {
        Self {
            code: -32004,
            message,
        }
    }

    fn internal(message: String) -> Self {
        Self {
            code: -32000,
            message,
        }
    }
}

impl From<SelfHostError> for JsonRpcAppError {
    fn from(value: SelfHostError) -> Self {
        match value {
            SelfHostError::NotFound(detail) => Self::not_found(format!("RecordNotFound: {detail}")),
            SelfHostError::ProjectionStale(detail) => {
                Self::internal(format!("ProjectionStale: {detail}"))
            }
            SelfHostError::SearchIndexUnavailable { code, detail } => {
                Self::internal(format!("{code}: {detail}"))
            }
            SelfHostError::ReadMode(detail) => Self::invalid_params(detail),
            SelfHostError::IngestionRequest { code, detail, .. } => {
                Self::invalid_params(format!("{code}: {detail}"))
            }
            SelfHostError::SupplementalValidation { code, detail } => {
                Self::invalid_params(format!("SupplementalValidation:{code}: {detail}"))
            }
            SelfHostError::SupplementalConflict { code, detail } => {
                Self::internal(format!("SupplementalConflict:{code}: {detail}"))
            }
            other => Self::internal(other.to_string()),
        }
    }
}

#[cfg(test)]
mod search_index_error_tests {
    use super::*;

    #[test]
    fn search_index_unavailable_maps_to_explicit_json_rpc_internal_error() {
        let error = JsonRpcAppError::from(SelfHostError::SearchIndexUnavailable {
            code: "search_index_failed",
            detail: "checksum validation failed".to_owned(),
        });

        assert_eq!(error.code, -32000);
        assert_eq!(
            error.message,
            "search_index_failed: checksum validation failed"
        );
    }
}

struct McpHttpError {
    status: StatusCode,
    body: ErrorResponse,
    authenticate: Option<String>,
}

impl McpHttpError {
    fn invalid_token(metadata_url: &str, detail: &str) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            body: ErrorResponse {
                error: "InvalidToken".to_owned(),
                detail: Some(detail.to_owned()),
                details: None,
                retry_after: None,
            },
            authenticate: Some(format!(
                "Bearer error=\"invalid_token\", resource_metadata=\"{metadata_url}\", scope=\"{MCP_AUTH_CHALLENGE_SCOPE}\""
            )),
        }
    }

    fn bad_request(detail: String) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            body: ErrorResponse::bad_request(&detail),
            authenticate: None,
        }
    }
}

impl IntoResponse for McpHttpError {
    fn into_response(self) -> Response {
        let mut response = (self.status, Json(self.body)).into_response();
        if let Some(authenticate) = self.authenticate
            && let Ok(value) = HeaderValue::from_str(&authenticate)
        {
            response.headers_mut().insert(WWW_AUTHENTICATE, value);
        }
        response
    }
}

#[derive(Debug, thiserror::Error)]
enum JwtError {
    #[error("JWT must contain header, claims, and signature")]
    WrongPartCount,
    #[error("JWT header is invalid: {0}")]
    InvalidHeader(String),
    #[error("JWT claims are invalid: {0}")]
    InvalidClaims(String),
    #[error("JWT uses unsupported algorithm {0}")]
    UnsupportedAlgorithm(String),
    #[error("JWT kid is required")]
    MissingKid,
    #[error("JWT key {0} was not found")]
    UnknownKey(String),
    #[error("JWT key {0} does not match algorithm {1}")]
    KeyAlgorithmMismatch(String, String),
    #[error("JWT signature is invalid")]
    InvalidSignature,
    #[error("JWT issuer mismatch")]
    IssuerMismatch,
    #[error("JWT audience mismatch")]
    AudienceMismatch,
    #[error("JWT is expired")]
    Expired,
    #[error("JWT contains no authorization grants")]
    MissingAuthorizationGrant,
}

#[derive(Debug, Deserialize)]
struct JwtHeader {
    alg: String,
    #[serde(default)]
    kid: Option<String>,
}

#[derive(Debug, Deserialize)]
struct JwtClaims {
    iss: String,
    exp: i64,
    aud: AudienceClaim,
    #[serde(default)]
    scope: Option<ScopeClaim>,
    #[serde(default)]
    permissions: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum AudienceClaim {
    Single(String),
    Multiple(Vec<String>),
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum ScopeClaim {
    SpaceDelimited(String),
    List(Vec<String>),
}

impl ScopeClaim {
    fn into_scopes(self) -> Vec<String> {
        match self {
            Self::SpaceDelimited(value) => value
                .split_whitespace()
                .filter(|scope| !scope.is_empty())
                .map(ToOwned::to_owned)
                .collect(),
            Self::List(values) => values
                .into_iter()
                .map(|scope| scope.trim().to_owned())
                .filter(|scope| !scope.is_empty())
                .collect(),
        }
    }
}

impl AudienceClaim {
    fn contains(&self, expected: &str) -> bool {
        match self {
            Self::Single(value) => value == expected,
            Self::Multiple(values) => values.iter().any(|value| value == expected),
        }
    }
}

fn verify_jwt(token: &str, oauth: &McpOAuthConfig) -> Result<VerifiedToken, JwtError> {
    let parts = token.split('.').collect::<Vec<_>>();
    if parts.len() != 3 {
        return Err(JwtError::WrongPartCount);
    }
    let header_bytes =
        decode_base64url(parts[0]).map_err(|error| JwtError::InvalidHeader(error.to_string()))?;
    let claims_bytes =
        decode_base64url(parts[1]).map_err(|error| JwtError::InvalidClaims(error.to_string()))?;
    let signature = decode_base64url(parts[2]).map_err(|_| JwtError::InvalidSignature)?;
    let header: JwtHeader = serde_json::from_slice(&header_bytes)
        .map_err(|error| JwtError::InvalidHeader(error.to_string()))?;
    let claims: JwtClaims = serde_json::from_slice(&claims_bytes)
        .map_err(|error| JwtError::InvalidClaims(error.to_string()))?;
    let kid = header.kid.as_deref().ok_or(JwtError::MissingKid)?;
    let jwk = find_jwk(&oauth.jwks, kid)?;
    let signing_input = format!("{}.{}", parts[0], parts[1]);
    verify_signature(jwk, &header.alg, signing_input.as_bytes(), &signature)?;
    if claims.iss != oauth.issuer {
        return Err(JwtError::IssuerMismatch);
    }
    if claims.exp <= Utc::now().timestamp() {
        return Err(JwtError::Expired);
    }
    if !claims.aud.contains(&oauth.audience) {
        return Err(JwtError::AudienceMismatch);
    }
    Ok(VerifiedToken {
        scopes: authorization_grants(claims.scope, claims.permissions)?,
    })
}

fn authorization_grants(
    scope: Option<ScopeClaim>,
    permissions: Vec<String>,
) -> Result<Vec<String>, JwtError> {
    let mut grants = scope.map(ScopeClaim::into_scopes).unwrap_or_default();
    grants.extend(
        permissions
            .into_iter()
            .map(|permission| permission.trim().to_owned())
            .filter(|permission| !permission.is_empty()),
    );
    grants.sort();
    grants.dedup();
    if grants.is_empty() {
        Err(JwtError::MissingAuthorizationGrant)
    } else {
        Ok(grants)
    }
}

fn find_jwk<'a>(jwks: &'a JsonWebKeySet, kid: &str) -> Result<&'a JsonWebKey, JwtError> {
    jwks.keys
        .iter()
        .find(|key| key.kid == kid)
        .ok_or_else(|| JwtError::UnknownKey(kid.to_owned()))
}

fn verify_signature(
    jwk: &JsonWebKey,
    alg: &str,
    signing_input: &[u8],
    jwt_signature: &[u8],
) -> Result<(), JwtError> {
    if jwk.alg.as_deref().is_some_and(|key_alg| key_alg != alg) {
        return Err(JwtError::KeyAlgorithmMismatch(
            jwk.kid.clone(),
            alg.to_owned(),
        ));
    }
    match alg {
        "ES256" => verify_es256(jwk, signing_input, jwt_signature),
        "RS256" => verify_rs256(jwk, signing_input, jwt_signature),
        other => Err(JwtError::UnsupportedAlgorithm(other.to_owned())),
    }
}

fn verify_es256(
    jwk: &JsonWebKey,
    signing_input: &[u8],
    jwt_signature: &[u8],
) -> Result<(), JwtError> {
    if jwk.kty != "EC" || jwk.crv.as_deref() != Some("P-256") {
        return Err(JwtError::KeyAlgorithmMismatch(
            jwk.kid.clone(),
            "ES256".to_owned(),
        ));
    }
    let x = decode_jwk_part(jwk.x.as_deref())?;
    let y = decode_jwk_part(jwk.y.as_deref())?;
    if x.len() != 32 || y.len() != 32 {
        return Err(JwtError::InvalidSignature);
    }
    let mut public_key = Vec::with_capacity(65);
    public_key.push(0x04);
    public_key.extend_from_slice(&x);
    public_key.extend_from_slice(&y);
    signature::UnparsedPublicKey::new(&signature::ECDSA_P256_SHA256_FIXED, public_key)
        .verify(signing_input, jwt_signature)
        .map_err(|_| JwtError::InvalidSignature)
}

fn verify_rs256(
    jwk: &JsonWebKey,
    signing_input: &[u8],
    jwt_signature: &[u8],
) -> Result<(), JwtError> {
    if jwk.kty != "RSA" {
        return Err(JwtError::KeyAlgorithmMismatch(
            jwk.kid.clone(),
            "RS256".to_owned(),
        ));
    }
    let n = decode_jwk_part(jwk.n.as_deref())?;
    let e = decode_jwk_part(jwk.e.as_deref())?;
    signature::RsaPublicKeyComponents { n: &n, e: &e }
        .verify(
            &signature::RSA_PKCS1_2048_8192_SHA256,
            signing_input,
            jwt_signature,
        )
        .map_err(|_| JwtError::InvalidSignature)
}

fn decode_jwk_part(value: Option<&str>) -> Result<Vec<u8>, JwtError> {
    let value = value.ok_or(JwtError::InvalidSignature)?;
    decode_base64url(value).map_err(|_| JwtError::InvalidSignature)
}

fn decode_base64url(value: &str) -> Result<Vec<u8>, base64::DecodeError> {
    URL_SAFE_NO_PAD.decode(value.as_bytes())
}
