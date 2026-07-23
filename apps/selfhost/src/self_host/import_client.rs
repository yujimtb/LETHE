use crate::self_host::app::{BulkImportSessionReport, ImportOutcome, ImportReport, ImportSummary};
use lethe_adapter_api::idempotency::{CANONICAL_JSON_META_KEY, OBJECT_ID_META_KEY, identity_key};
use lethe_adapter_api::traits::ObservationDraft;
use std::str::FromStr;

pub const API_VERSION_ENV: &str = "LETHE_INGEST_API_VERSION";
pub const ADMISSION_GENERATION_ENV: &str = "LETHE_ADMISSION_GENERATION";

const OBSERVATION_V1_PATH: &str = "/api/import/observation-drafts";
const OBSERVATION_V2_PATH: &str = "/api/v2/import/observation-drafts";
const ADMISSION_GENERATION_HEADER: &str = "X-LETHE-Admission-Generation";

/// import API returns HTTP 429 with error code `import_concurrency_limit` when the
/// shared import concurrency permit is full. That condition is transient, so the
/// client retries up to this many times, honoring the server's `retry_after` hint.
const IMPORT_CONCURRENCY_RETRY_LIMIT: u32 = 30;
const IMPORT_CONCURRENCY_DEFAULT_RETRY_AFTER_SECS: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ImportApiVersion {
    V1,
    V2,
}

impl FromStr for ImportApiVersion {
    type Err = ImportClientError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "1" => Ok(Self::V1),
            "2" => Ok(Self::V2),
            _ => Err(ImportClientError::InvalidApiVersion(value.to_owned())),
        }
    }
}

impl std::fmt::Display for ImportApiVersion {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::V1 => "1",
            Self::V2 => "2",
        })
    }
}

#[derive(Debug, Clone)]
pub struct ImportApiConfig {
    pub base_url: String,
    pub api_token_env: String,
    pub api_version: ImportApiVersion,
    pub admission_generation: Option<u64>,
}

#[derive(Debug, thiserror::Error)]
pub enum ImportClientError {
    #[error("{0} must not be blank")]
    BlankField(&'static str),
    #[error(
        "missing environment variable {0}. Set {0} to an API token with write:observations, or pass --api-token-env=<name> for the variable you already set"
    )]
    MissingTokenEnv(String),
    #[error("unsupported import API version {0}; expected 1 or 2")]
    InvalidApiVersion(String),
    #[error("{0} requires a value")]
    MissingOptionValue(&'static str),
    #[error("admission generation must be a positive integer: {0}")]
    InvalidAdmissionGeneration(String),
    #[error("admission generation is required when --api-version=2 is selected")]
    MissingAdmissionGeneration,
    #[error("v2 identity cannot be derived for draft {index}: {detail}")]
    InvalidV2Identity { index: usize, detail: String },
    #[error("v2 observation import response violates its contract: {0}")]
    InvalidV2Response(String),
    #[error(
        "v2 observation import rejected {rejected} item(s) (ingested={ingested}, duplicates={duplicates}, quarantined={quarantined}, rejected={rejected}): {detail}"
    )]
    V2Rejected {
        report: Box<ImportReport>,
        ingested: usize,
        duplicates: usize,
        quarantined: usize,
        rejected: usize,
        detail: String,
    },
    #[error("HTTP client error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("import API rejected request with {status}: {body}")]
    Api {
        status: reqwest::StatusCode,
        body: String,
    },
}

pub struct ImportApiClient {
    base_url: String,
    api_token: String,
    api_version: ImportApiVersion,
    admission_generation: Option<u64>,
    http: reqwest::blocking::Client,
}

impl ImportApiConfig {
    pub fn connect(self) -> Result<ImportApiClient, ImportClientError> {
        require_non_blank("base_url", &self.base_url)?;
        require_non_blank("api_token_env", &self.api_token_env)?;
        let api_token = std::env::var(&self.api_token_env)
            .map_err(|_| ImportClientError::MissingTokenEnv(self.api_token_env.clone()))?;
        require_non_blank("api_token", &api_token)?;
        if self.api_version == ImportApiVersion::V2 && self.admission_generation.is_none() {
            return Err(ImportClientError::MissingAdmissionGeneration);
        }
        if self.admission_generation == Some(0) {
            return Err(ImportClientError::InvalidAdmissionGeneration(
                "0".to_owned(),
            ));
        }
        Ok(ImportApiClient {
            base_url: self.base_url.trim_end_matches('/').to_owned(),
            api_token,
            api_version: self.api_version,
            admission_generation: self.admission_generation,
            http: reqwest::blocking::Client::new(),
        })
    }
}

pub fn resolve_api_version(cli_value: Option<&str>) -> Result<ImportApiVersion, ImportClientError> {
    if let Some(value) = cli_value {
        return value.parse();
    }
    match std::env::var(API_VERSION_ENV) {
        Ok(value) => value.parse(),
        Err(std::env::VarError::NotPresent) => Ok(ImportApiVersion::V1),
        Err(std::env::VarError::NotUnicode(_)) => Err(ImportClientError::InvalidApiVersion(
            format!("{API_VERSION_ENV} is not valid UTF-8"),
        )),
    }
}

pub fn resolve_admission_generation(
    cli_value: Option<&str>,
) -> Result<Option<u64>, ImportClientError> {
    let value = if let Some(value) = cli_value {
        Some(value.to_owned())
    } else {
        match std::env::var(ADMISSION_GENERATION_ENV) {
            Ok(value) => Some(value),
            Err(std::env::VarError::NotPresent) => None,
            Err(std::env::VarError::NotUnicode(_)) => {
                return Err(ImportClientError::InvalidAdmissionGeneration(format!(
                    "{ADMISSION_GENERATION_ENV} is not valid UTF-8"
                )));
            }
        }
    };
    let Some(value) = value else {
        return Ok(None);
    };
    let generation = value
        .parse::<u64>()
        .map_err(|_| ImportClientError::InvalidAdmissionGeneration(value.clone()))?;
    if generation == 0 {
        return Err(ImportClientError::InvalidAdmissionGeneration(value));
    }
    Ok(Some(generation))
}

pub fn normalize_import_option_args(
    args: impl IntoIterator<Item = String>,
) -> Result<Vec<String>, ImportClientError> {
    let mut normalized = Vec::new();
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        let option = match arg.as_str() {
            "--api-version" => Some("--api-version="),
            "--admission-generation" => Some("--admission-generation="),
            _ => None,
        };
        let Some(option) = option else {
            normalized.push(arg);
            continue;
        };
        let value = args
            .next()
            .ok_or_else(|| ImportClientError::MissingOptionValue(option.trim_end_matches('=')))?;
        normalized.push(format!("{option}{value}"));
    }
    Ok(normalized)
}

impl ImportApiClient {
    pub fn begin_bulk_import_session(&self) -> Result<BulkImportSessionReport, ImportClientError> {
        let response = self
            .http
            .post(format!("{}/api/import/bulk-sessions/begin", self.base_url))
            .bearer_auth(&self.api_token)
            .send()?;
        decode_response(response)
    }

    pub fn ingest_observation_drafts(
        &self,
        drafts: Vec<ObservationDraft>,
        source_instance_id: &str,
    ) -> Result<ImportReport, ImportClientError> {
        self.send_observation_drafts(drafts, source_instance_id, None)
    }

    pub fn ingest_observation_drafts_in_session(
        &self,
        drafts: Vec<ObservationDraft>,
        source_instance_id: &str,
        bulk_session_id: &str,
    ) -> Result<ImportReport, ImportClientError> {
        require_non_blank("bulk_session_id", bulk_session_id)?;
        self.send_observation_drafts(drafts, source_instance_id, Some(bulk_session_id))
    }

    pub fn end_bulk_import_session(
        &self,
        bulk_session_id: &str,
    ) -> Result<BulkImportSessionReport, ImportClientError> {
        require_non_blank("bulk_session_id", bulk_session_id)?;
        let response = self
            .http
            .post(format!(
                "{}/api/import/bulk-sessions/{bulk_session_id}/end",
                self.base_url
            ))
            .bearer_auth(&self.api_token)
            .send()?;
        decode_response(response)
    }

    fn send_observation_drafts(
        &self,
        drafts: Vec<ObservationDraft>,
        source_instance_id: &str,
        bulk_session_id: Option<&str>,
    ) -> Result<ImportReport, ImportClientError> {
        require_non_blank("source_instance_id", source_instance_id)?;
        let expected_result_count = drafts.len();
        let drafts = match self.api_version {
            ImportApiVersion::V1 => drafts,
            ImportApiVersion::V2 => prepare_v2_drafts(drafts, source_instance_id)?,
        };
        let request = ImportObservationDraftsRequest {
            source_instance_id: source_instance_id.to_owned(),
            bulk_session_id: bulk_session_id.map(str::to_owned),
            drafts,
        };
        let path = match self.api_version {
            ImportApiVersion::V1 => OBSERVATION_V1_PATH,
            ImportApiVersion::V2 => OBSERVATION_V2_PATH,
        };
        let mut request_builder = self
            .http
            .post(format!("{}{path}", self.base_url))
            .bearer_auth(&self.api_token);
        if let Some(generation) = self.admission_generation {
            request_builder = request_builder.header(ADMISSION_GENERATION_HEADER, generation);
        }
        let response = send_with_concurrency_retry(request_builder.json(&request))?;
        let report = decode_response(response)?;
        match self.api_version {
            ImportApiVersion::V1 => Ok(report),
            ImportApiVersion::V2 => finalize_v2_report(report, expected_result_count),
        }
    }
}

#[derive(Debug, serde::Serialize)]
struct ImportObservationDraftsRequest {
    source_instance_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    bulk_session_id: Option<String>,
    drafts: Vec<ObservationDraft>,
}

/// Sends `request_builder`, retrying on HTTP 429 (`import_concurrency_limit`) up to
/// [`IMPORT_CONCURRENCY_RETRY_LIMIT`] times. Each retry resends the identical request
/// (safe because requests carry an idempotency key) after sleeping for the server's
/// `retry_after` hint (default 1s if absent). Non-429 responses, and 429s once the
/// retry budget is exhausted, are returned as-is for `decode_response` to classify.
fn send_with_concurrency_retry(
    request_builder: reqwest::blocking::RequestBuilder,
) -> Result<reqwest::blocking::Response, ImportClientError> {
    let mut builder = request_builder;
    let mut attempt = 0_u32;
    loop {
        let retry_builder = builder.try_clone();
        let response = builder.send()?;
        if response.status() != reqwest::StatusCode::TOO_MANY_REQUESTS
            || attempt >= IMPORT_CONCURRENCY_RETRY_LIMIT
        {
            return Ok(response);
        }
        let Some(next_builder) = retry_builder else {
            return Ok(response);
        };
        attempt += 1;
        let retry_after_secs = retry_after_secs_from_response(response)?;
        eprintln!(
            "import API returned 429 import_concurrency_limit; retrying in {retry_after_secs}s (attempt {attempt}/{IMPORT_CONCURRENCY_RETRY_LIMIT})"
        );
        std::thread::sleep(std::time::Duration::from_secs(retry_after_secs));
        builder = next_builder;
    }
}

fn retry_after_secs_from_response(
    response: reqwest::blocking::Response,
) -> Result<u64, ImportClientError> {
    let body = response.text()?;
    let retry_after = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|value| value.get("retry_after")?.as_u64())
        .unwrap_or(IMPORT_CONCURRENCY_DEFAULT_RETRY_AFTER_SECS);
    Ok(retry_after)
}

fn decode_response<T: serde::de::DeserializeOwned>(
    response: reqwest::blocking::Response,
) -> Result<T, ImportClientError> {
    let status = response.status();
    if !status.is_success() {
        return Err(ImportClientError::Api {
            status,
            body: response.text()?,
        });
    }
    Ok(response.json()?)
}

fn require_non_blank(name: &'static str, value: &str) -> Result<(), ImportClientError> {
    if value.trim().is_empty() {
        Err(ImportClientError::BlankField(name))
    } else {
        Ok(())
    }
}

fn prepare_v2_drafts(
    drafts: Vec<ObservationDraft>,
    source_instance_id: &str,
) -> Result<Vec<ObservationDraft>, ImportClientError> {
    drafts
        .into_iter()
        .enumerate()
        .map(|(index, mut draft)| {
            let meta =
                draft
                    .meta
                    .as_object()
                    .ok_or_else(|| ImportClientError::InvalidV2Identity {
                        index,
                        detail: "meta must be an object".to_owned(),
                    })?;
            let object_id = meta
                .get(OBJECT_ID_META_KEY)
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| ImportClientError::InvalidV2Identity {
                    index,
                    detail: "meta.object_id is required".to_owned(),
                })?;
            let canonical_json = meta
                .get(CANONICAL_JSON_META_KEY)
                .and_then(serde_json::Value::as_str)
                .filter(|value| !value.trim().is_empty())
                .ok_or_else(|| ImportClientError::InvalidV2Identity {
                    index,
                    detail: "meta.canonical_json is required".to_owned(),
                })?;
            draft.idempotency_key = identity_key(source_instance_id, object_id, canonical_json);
            Ok(draft)
        })
        .collect()
}

fn finalize_v2_report(
    mut report: ImportReport,
    expected_result_count: usize,
) -> Result<ImportReport, ImportClientError> {
    if report.results.len() != expected_result_count {
        return Err(ImportClientError::InvalidV2Response(format!(
            "results contains {} items, expected {}",
            report.results.len(),
            expected_result_count
        )));
    }

    let mut summary = ImportSummary::default();
    for result in &report.results {
        match result.outcome {
            ImportOutcome::Ingested => {
                if result.observation_id.is_none() {
                    return Err(ImportClientError::InvalidV2Response(format!(
                        "ingested result for client_ref={} is missing observation_id",
                        result.client_ref
                    )));
                }
                summary.ingested += 1;
            }
            ImportOutcome::Duplicate => {
                if result.existing_id.is_none()
                    || result.error_code.as_deref() != Some("duplicate.existing_id")
                {
                    return Err(ImportClientError::InvalidV2Response(format!(
                        "duplicate result for client_ref={} must contain existing_id and error_code=duplicate.existing_id",
                        result.client_ref
                    )));
                }
                summary.duplicates += 1;
            }
            ImportOutcome::Quarantined => {
                if result.ticket.is_none() {
                    return Err(ImportClientError::InvalidV2Response(format!(
                        "quarantined result for client_ref={} is missing ticket",
                        result.client_ref
                    )));
                }
                summary.quarantined += 1;
            }
            ImportOutcome::Rejected => {
                if result.error_code.is_none() || result.reason.is_none() {
                    return Err(ImportClientError::InvalidV2Response(format!(
                        "rejected result for client_ref={} is missing error_code or reason",
                        result.client_ref
                    )));
                }
                summary.rejected += 1;
            }
        }
    }

    report.ingested = summary.ingested;
    report.duplicates = summary.duplicates;
    report.quarantined = summary.quarantined;
    report.rejected = summary.rejected;
    report.summary = summary.clone();

    if summary.rejected > 0 {
        let detail = report
            .results
            .iter()
            .filter(|result| result.outcome == ImportOutcome::Rejected)
            .map(|result| {
                format!(
                    "client_ref={} error_code={} reason={}",
                    result.client_ref,
                    result
                        .error_code
                        .as_deref()
                        .expect("validated rejected result error_code"),
                    result
                        .reason
                        .as_deref()
                        .expect("validated rejected result reason")
                )
            })
            .collect::<Vec<_>>()
            .join("; ");
        return Err(ImportClientError::V2Rejected {
            report: Box::new(report),
            ingested: summary.ingested,
            duplicates: summary.duplicates,
            quarantined: summary.quarantined,
            rejected: summary.rejected,
            detail,
        });
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use lethe_adapter_api::idempotency::identity_key;
    use lethe_adapter_claude::claude::importer::ClaudeAiImporter;
    use lethe_core::domain::SemVer;
    use serde_json::Value;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::{Mutex, OnceLock};
    use std::thread::{self, JoinHandle};

    const TEST_TOKEN_ENV: &str = "LETHE_IMPORT_CLIENT_TEST_TOKEN";

    fn env_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn adapter_drafts(count: usize) -> Vec<ObservationDraft> {
        let messages = (0..count)
            .map(|index| {
                serde_json::json!({
                    "uuid": format!("message-{index}"),
                    "sender": "human",
                    "text": format!("message {index}"),
                    "created_at": format!("2026-07-01T00:0{index}:00Z")
                })
            })
            .collect::<Vec<_>>();
        let export = serde_json::json!({
            "conversations": [{"uuid": "conversation-1", "messages": messages}]
        });
        ClaudeAiImporter::new(SemVer::new("1.0.0"))
            .import_json_str(&export.to_string())
            .unwrap()
    }

    fn spawn_http_response(response_body: String) -> (String, JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let handle = thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let request = read_request(&mut stream);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            stream.write_all(response.as_bytes()).unwrap();
            request
        });
        (address, handle)
    }

    /// Serves one canned `(status, body)` response per accepted connection, in order,
    /// then returns the raw request text captured for each connection.
    fn spawn_http_responses(responses: Vec<(u16, String)>) -> (String, JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let handle = thread::spawn(move || {
            let mut requests = Vec::new();
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().unwrap();
                requests.push(read_request(&mut stream));
                let reason = match status {
                    200 => "OK",
                    429 => "Too Many Requests",
                    other => panic!("unsupported status in test helper: {other}"),
                };
                let response = format!(
                    "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                stream.write_all(response.as_bytes()).unwrap();
            }
            requests
        });
        (address, handle)
    }

    fn read_request(stream: &mut TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 4096];
        loop {
            let read = stream.read(&mut chunk).unwrap();
            assert!(read > 0, "request ended before its body was read");
            bytes.extend_from_slice(&chunk[..read]);
            let Some(header_end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") else {
                continue;
            };
            let headers = String::from_utf8_lossy(&bytes[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.strip_prefix("Content-Length:")
                        .or_else(|| line.strip_prefix("content-length:"))
                })
                .map(str::trim)
                .and_then(|value| value.parse::<usize>().ok())
                .unwrap();
            if bytes.len() >= header_end + 4 + content_length {
                return String::from_utf8(bytes).unwrap();
            }
        }
    }

    fn set_test_token() {
        unsafe { std::env::set_var(TEST_TOKEN_ENV, "test-token") };
    }

    fn test_config(base_url: String, api_version: ImportApiVersion) -> ImportApiConfig {
        ImportApiConfig {
            base_url,
            api_token_env: TEST_TOKEN_ENV.to_owned(),
            api_version,
            admission_generation: (api_version == ImportApiVersion::V2).then_some(7),
        }
    }

    #[test]
    fn v1_default_wire_contract_keeps_endpoint_key_and_header_unchanged() {
        let _guard = env_lock();
        set_test_token();
        let drafts = adapter_drafts(1);
        let original_key = drafts[0].idempotency_key.clone();
        let (base_url, server) = spawn_http_response(
            serde_json::json!({"ingested": 1, "duplicates": 0, "quarantined": 0}).to_string(),
        );

        let report = test_config(base_url, ImportApiVersion::V1)
            .connect()
            .unwrap()
            .ingest_observation_drafts(drafts, "claude-personal")
            .unwrap();
        let request = server.join().unwrap();
        let request_body = request.split("\r\n\r\n").nth(1).unwrap();
        let body: Value = serde_json::from_str(request_body).unwrap();

        assert!(request.starts_with("POST /api/import/observation-drafts HTTP/1.1"));
        assert!(
            !request
                .to_ascii_lowercase()
                .contains("x-lethe-admission-generation:")
        );
        assert_eq!(body["drafts"][0]["idempotency_key"], original_key.as_str());
        assert_eq!(report.ingested, 1);
    }

    #[test]
    fn v2_rewrites_actual_adapter_identity_and_sends_generation_header() {
        let _guard = env_lock();
        set_test_token();
        let drafts = adapter_drafts(1);
        let original_key = drafts[0].idempotency_key.clone();
        let meta = drafts[0].meta.as_object().unwrap();
        let object_id = meta[OBJECT_ID_META_KEY].as_str().unwrap();
        let canonical_json = meta[CANONICAL_JSON_META_KEY].as_str().unwrap();
        let expected_key = identity_key("claude-personal", object_id, canonical_json);
        let (base_url, server) = spawn_http_response(
            serde_json::json!({
                "ingested": 1,
                "duplicates": 0,
                "quarantined": 0,
                "rejected": 0,
                "results": [{
                    "client_ref": "0",
                    "outcome": "ingested",
                    "observation_id": "obs:test"
                }]
            })
            .to_string(),
        );

        let report = test_config(base_url, ImportApiVersion::V2)
            .connect()
            .unwrap()
            .ingest_observation_drafts(drafts, "claude-personal")
            .unwrap();
        let request = server.join().unwrap();
        let request_body = request.split("\r\n\r\n").nth(1).unwrap();
        let body: Value = serde_json::from_str(request_body).unwrap();

        assert!(request.starts_with("POST /api/v2/import/observation-drafts HTTP/1.1"));
        assert!(
            request
                .lines()
                .any(|line| line.eq_ignore_ascii_case("x-lethe-admission-generation: 7"))
        );
        assert_eq!(body["drafts"][0]["idempotency_key"], expected_key.as_str());
        assert_ne!(body["drafts"][0]["idempotency_key"], original_key.as_str());
        assert_eq!(report.ingested, 1);
    }

    #[test]
    fn v2_rejected_item_returns_reported_error_after_aggregating_all_results() {
        let _guard = env_lock();
        set_test_token();
        let response = serde_json::json!({
            "ingested": 1,
            "duplicates": 1,
            "quarantined": 1,
            "rejected": 1,
            "results": [
                {"client_ref": "0", "outcome": "ingested", "observation_id": "obs:one"},
                {"client_ref": "1", "outcome": "duplicate", "existing_id": "obs:two", "error_code": "duplicate.existing_id"},
                {"client_ref": "2", "outcome": "quarantined", "ticket": {"id": "ticket-1", "reason": "review"}},
                {"client_ref": "3", "outcome": "rejected", "error_code": "schema_validation", "failure_class": "validation", "reason": "invalid payload"}
            ]
        });
        let (base_url, server) = spawn_http_response(response.to_string());
        let error = test_config(base_url, ImportApiVersion::V2)
            .connect()
            .unwrap()
            .ingest_observation_drafts(adapter_drafts(4), "claude-personal")
            .unwrap_err();
        let display = error.to_string();
        let ImportClientError::V2Rejected { report, .. } = error else {
            panic!("expected rejected v2 report, got {error:?}");
        };

        server.join().unwrap();
        assert_eq!(report.ingested, 1);
        assert_eq!(report.duplicates, 1);
        assert_eq!(report.quarantined, 1);
        assert_eq!(report.rejected, 1);
        assert!(display.contains("client_ref=3"));
        assert!(display.contains("schema_validation"));
    }

    #[test]
    fn v2_requires_admission_generation() {
        let _guard = env_lock();
        set_test_token();
        let result = ImportApiConfig {
            base_url: "http://127.0.0.1:1".to_owned(),
            api_token_env: TEST_TOKEN_ENV.to_owned(),
            api_version: ImportApiVersion::V2,
            admission_generation: None,
        }
        .connect();
        let error = match result {
            Ok(_) => panic!("v2 connection without generation must fail"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            ImportClientError::MissingAdmissionGeneration
        ));
    }

    #[test]
    fn retries_after_429_import_concurrency_limit_then_succeeds() {
        let _guard = env_lock();
        set_test_token();
        let drafts = adapter_drafts(1);
        let (base_url, server) = spawn_http_responses(vec![
            (
                429,
                serde_json::json!({
                    "error": "import_concurrency_limit",
                    "detail": "concurrent import limit 2 is currently full",
                    "details": {"maximum": 2},
                    "retry_after": 0
                })
                .to_string(),
            ),
            (
                200,
                serde_json::json!({"ingested": 1, "duplicates": 0, "quarantined": 0}).to_string(),
            ),
        ]);

        let report = test_config(base_url, ImportApiVersion::V1)
            .connect()
            .unwrap()
            .ingest_observation_drafts(drafts, "claude-personal")
            .unwrap();

        let requests = server.join().unwrap();
        assert_eq!(
            requests.len(),
            2,
            "client must resend the identical request after a 429"
        );
        assert_eq!(requests[0], requests[1], "retry must resend the same body");
        assert_eq!(report.ingested, 1);
    }

    #[test]
    fn gives_up_after_max_retries_and_surfaces_429_error() {
        let _guard = env_lock();
        set_test_token();
        let drafts = adapter_drafts(1);
        let attempts = (IMPORT_CONCURRENCY_RETRY_LIMIT + 1) as usize;
        let responses = std::iter::repeat((
            429,
            serde_json::json!({
                "error": "import_concurrency_limit",
                "detail": "concurrent import limit 2 is currently full",
                "details": {"maximum": 2},
                "retry_after": 0
            })
            .to_string(),
        ))
        .take(attempts)
        .collect();
        let (base_url, server) = spawn_http_responses(responses);

        let error = test_config(base_url, ImportApiVersion::V1)
            .connect()
            .unwrap()
            .ingest_observation_drafts(drafts, "claude-personal")
            .unwrap_err();

        let requests = server.join().unwrap();
        assert_eq!(
            requests.len(),
            attempts,
            "client must exhaust the retry budget before giving up"
        );
        match error {
            ImportClientError::Api { status, .. } => {
                assert_eq!(status, reqwest::StatusCode::TOO_MANY_REQUESTS);
            }
            other => panic!("expected ImportClientError::Api(429), got {other:?}"),
        }
    }

    #[test]
    fn common_api_flags_accept_space_and_equals_forms() {
        let normalized = normalize_import_option_args([
            "--api-version".to_owned(),
            "2".to_owned(),
            "--admission-generation=7".to_owned(),
        ])
        .unwrap();
        assert_eq!(
            normalized,
            vec![
                "--api-version=2".to_owned(),
                "--admission-generation=7".to_owned()
            ]
        );
    }
}
