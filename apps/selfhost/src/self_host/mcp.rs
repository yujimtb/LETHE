use axum::body::Bytes;
use axum::extract::State;
use axum::http::header::{AUTHORIZATION, WWW_AUTHENTICATE};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use chrono::Utc;
use lethe_api::api::envelope::ErrorResponse;
use ring::signature;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::self_host::app::SupplementalWriteRequest;
use crate::self_host::app::{AppService, SelfHostError};
use crate::self_host::config::{JsonWebKey, JsonWebKeySet, McpOAuthConfig};
use crate::self_host::mcp_contract::{
    ClaimQueueArguments, GetRecordArguments, GetThreadArguments, SearchDecisionsArguments,
    SearchLakeArguments,
};
use lethe_projection_claim_queue::ClaimState;

const SEARCH_LAKE_DESCRIPTION: &str =
    "Search projected corpus text across lake records; returns record IDs and snippets.";
const GET_RECORD_DESCRIPTION: &str =
    "Fetch one projected corpus record by record_id; returns RecordNotFound if absent.";
const GET_THREAD_DESCRIPTION: &str =
    "Fetch projected thread context for a corpus record or thread key.";
const CLAIM_QUEUE_DESCRIPTION: &str =
    "List folded claim groups from the claim queue projection, filterable by state.";
const SEARCH_DECISIONS_DESCRIPTION: &str =
    "Search the folded decision ledger with supersedes resolution.";
const WRITE_SUPPLEMENTAL_DESCRIPTION: &str = "Write one supplemental record as post-processing for an observation already ingested into the lake. This is not for live self-enrichment during the same conversation. Kinds with anchor_required=true require resolved anchors; system-event kinds with anchor_required=false require payload.origin.";

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
        .route("/mcp", post(mcp_post))
        .with_state(McpState::new(service))
}

async fn protected_resource_metadata(State(state): State<McpState>) -> Json<Value> {
    Json(json!({
        "resource": state.oauth.resource_url,
        "authorization_servers": [state.oauth.issuer],
        "issuer": state.oauth.issuer,
        "bearer_methods_supported": ["header"],
        "scopes_supported": ["mcp:read", "write:supplemental"],
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
    let response = match handle_json_rpc(&state.service, &token, request).await {
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
    service: &AppService,
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
        "tools/list" => Ok(tools_list_result()),
        "tools/call" => {
            let params: ToolCallParams = parse_params(request.params)?;
            call_tool(service, token, params)
        }
        other => Err(JsonRpcAppError::method_not_found(format!(
            "unsupported MCP method: {other}"
        ))),
    }
}

fn tools_list_result() -> Value {
    json!({
        "tools": [
            tool_definition(
                "search_lake",
                SEARCH_LAKE_DESCRIPTION,
                json!({
                    "type": "object",
                    "required": ["query"],
                    "additionalProperties": false,
                    "properties": {
                        "query": { "type": "string" },
                        "source_types": { "type": "array", "items": { "type": "string" } },
                        "limit": { "type": "integer", "minimum": 1 },
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
                        "record_id": { "type": "string" }
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
                        "limit": { "type": "integer", "minimum": 1 },
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
                        "limit": { "type": "integer", "minimum": 1 }
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

fn tool_definition(name: &str, description: &str, input_schema: Value) -> Value {
    json!({
        "name": name,
        "description": description,
        "inputSchema": input_schema,
        "annotations": {
            "readOnlyHint": true,
            "destructiveHint": false,
            "idempotentHint": true,
            "openWorldHint": false
        }
    })
}

fn write_tool_definition(name: &str, description: &str, input_schema: Value) -> Value {
    json!({
        "name": name,
        "description": description,
        "inputSchema": input_schema,
        "annotations": {
            "readOnlyHint": false,
            "destructiveHint": false,
            "idempotentHint": false,
            "openWorldHint": false
        }
    })
}

fn call_tool(
    service: &AppService,
    token: &VerifiedToken,
    params: ToolCallParams,
) -> Result<Value, JsonRpcAppError> {
    match params.name.as_str() {
        "search_lake" => {
            ensure_scope(token, "mcp:read")?;
            let args: SearchLakeArguments = parse_arguments(params.arguments)?;
            ensure_not_blank("query", &args.query)?;
            let response = service.corpus_grep_response(&lethe_api::api::grep::GrepRequest {
                pattern: args.query,
                filters: lethe_api::api::grep::GrepFilters {
                    types: args.source_types,
                    ..lethe_api::api::grep::GrepFilters::default()
                },
                limit: args.limit,
                cursor: args.cursor,
                ..lethe_api::api::grep::GrepRequest::default()
            })?;
            tool_result(response)
        }
        "get_record" => {
            ensure_scope(token, "mcp:read")?;
            let args: GetRecordArguments = parse_arguments(params.arguments)?;
            ensure_not_blank("record_id", &args.record_id)?;
            let response = service.corpus_record_response(&args.record_id)?;
            tool_result(response)
        }
        "get_thread" => {
            ensure_scope(token, "mcp:read")?;
            let args: GetThreadArguments = parse_arguments(params.arguments)?;
            ensure_not_blank("record_id", &args.record_id)?;
            let response = service.corpus_thread_response(&args.record_id)?;
            tool_result(response)
        }
        "claim_queue" => {
            ensure_scope(token, "mcp:read")?;
            let args: ClaimQueueArguments = parse_arguments(params.arguments)?;
            let response = service.claim_queue_response_filtered(
                parse_claim_state(args.state.as_deref())?,
                args.verification_mode.as_deref(),
                None,
                args.limit.unwrap_or(20),
                args.cursor.as_deref(),
            )?;
            tool_result(response)
        }
        "search_decisions" => {
            ensure_scope(token, "mcp:read")?;
            let args: SearchDecisionsArguments = parse_arguments(params.arguments)?;
            ensure_not_blank("query", &args.query)?;
            let response = service
                .decision_search_response(Some(args.query.as_str()), args.limit.unwrap_or(20))?;
            tool_result(response)
        }
        "write_supplemental" => {
            ensure_scope(token, "write:supplemental")?;
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

fn ensure_scope(token: &VerifiedToken, required: &str) -> Result<(), JsonRpcAppError> {
    if token.has_scope(required) {
        Ok(())
    } else {
        Err(JsonRpcAppError::permission_denied(format!(
            "token lacks required scope {required}"
        )))
    }
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

    fn permission_denied(message: String) -> Self {
        Self {
            code: -32003,
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
            SelfHostError::ReadMode(detail) => Self::invalid_params(detail),
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
                "Bearer error=\"invalid_token\", resource_metadata=\"{metadata_url}\""
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
        if let Some(authenticate) = self.authenticate {
            if let Ok(value) = HeaderValue::from_str(&authenticate) {
                response.headers_mut().insert(WWW_AUTHENTICATE, value);
            }
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
    scope: ScopeClaim,
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
            Self::List(values) => values,
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
        scopes: claims.scope.into_scopes(),
    })
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
