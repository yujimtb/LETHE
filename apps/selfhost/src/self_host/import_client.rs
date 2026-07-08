use crate::self_host::app::ImportReport;
use lethe_adapter_api::traits::ObservationDraft;

#[derive(Debug, Clone)]
pub struct ImportApiConfig {
    pub base_url: String,
    pub api_token_env: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ImportClientError {
    #[error("{0} must not be blank")]
    BlankField(&'static str),
    #[error(
        "missing environment variable {0}. Set {0} to an API token with write:observations, or pass --api-token-env=<name> for the variable you already set"
    )]
    MissingTokenEnv(String),
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
    http: reqwest::blocking::Client,
}

impl ImportApiConfig {
    pub fn connect(self) -> Result<ImportApiClient, ImportClientError> {
        require_non_blank("base_url", &self.base_url)?;
        require_non_blank("api_token_env", &self.api_token_env)?;
        let api_token = std::env::var(&self.api_token_env)
            .map_err(|_| ImportClientError::MissingTokenEnv(self.api_token_env.clone()))?;
        require_non_blank("api_token", &api_token)?;
        Ok(ImportApiClient {
            base_url: self.base_url.trim_end_matches('/').to_owned(),
            api_token,
            http: reqwest::blocking::Client::new(),
        })
    }
}

impl ImportApiClient {
    pub fn ingest_observation_drafts(
        &self,
        drafts: Vec<ObservationDraft>,
        source_instance_id: &str,
    ) -> Result<ImportReport, ImportClientError> {
        require_non_blank("source_instance_id", source_instance_id)?;
        let request = ImportObservationDraftsRequest {
            source_instance_id: source_instance_id.to_owned(),
            drafts,
        };
        let response = self
            .http
            .post(format!("{}/api/import/observation-drafts", self.base_url))
            .bearer_auth(&self.api_token)
            .json(&request)
            .send()?;
        let status = response.status();
        if !status.is_success() {
            return Err(ImportClientError::Api {
                status,
                body: response.text()?,
            });
        }
        Ok(response.json()?)
    }
}

#[derive(Debug, serde::Serialize)]
struct ImportObservationDraftsRequest {
    source_instance_id: String,
    drafts: Vec<ObservationDraft>,
}

fn require_non_blank(name: &'static str, value: &str) -> Result<(), ImportClientError> {
    if value.trim().is_empty() {
        Err(ImportClientError::BlankField(name))
    } else {
        Ok(())
    }
}
