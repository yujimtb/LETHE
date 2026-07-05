use std::collections::HashSet;
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use lethe_projection_corpus::{CorpusConfig, CorpusMode};
use lethe_runtime::runtime::partition::RoutingKeyOrder;
use serde::Deserialize;

#[derive(Debug, Clone)]
pub struct SelfHostConfig {
    pub bind_addr: String,
    pub mcp_bind_addr: String,
    pub mcp_oauth: McpOAuthConfig,
    pub database_path: PathBuf,
    pub blob_dir: PathBuf,
    pub secret_encryption_key: [u8; 32],
    pub poll_interval: Duration,
    pub routing_key_order: RoutingKeyOrder,
    pub api_tokens: Vec<ApiTokenConfig>,
    pub resource_limits: ResourceLimits,
    pub corpus: CorpusProjectionConfig,
    pub slack_sources: Vec<SlackConfig>,
    pub google_sources: Vec<GoogleConfig>,
    pub slide_analysis_limit: Option<usize>,
    pub slide_ai: Option<SlideAiConfig>,
    pub supplemental: SupplementalConfig,
}

#[derive(Debug, Clone)]
pub struct ApiTokenConfig {
    pub token: SecretString,
    pub scopes: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct McpOAuthConfig {
    pub resource_url: String,
    pub protected_resource_metadata_url: String,
    pub issuer: String,
    pub audience: String,
    pub jwks_path: PathBuf,
    pub jwks: JsonWebKeySet,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct JsonWebKeySet {
    pub keys: Vec<JsonWebKey>,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct JsonWebKey {
    pub kty: String,
    pub kid: String,
    #[serde(default)]
    pub alg: Option<String>,
    #[serde(default)]
    pub crv: Option<String>,
    #[serde(default)]
    pub x: Option<String>,
    #[serde(default)]
    pub y: Option<String>,
    #[serde(default)]
    pub n: Option<String>,
    #[serde(default)]
    pub e: Option<String>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Result<Self, ConfigError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ConfigError::Invalid("secret must not be blank".to_owned()));
        }
        Ok(Self(value))
    }

    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SecretString([redacted])")
    }
}

#[derive(Debug, Clone)]
pub struct ResourceLimits {
    pub max_blob_bytes: usize,
    pub max_payload_bytes: usize,
    pub max_sync_items: usize,
    pub max_page_size: usize,
    pub max_leaf_observations: usize,
    pub retention_days: u32,
}

#[derive(Debug, Clone)]
pub struct CorpusProjectionConfig {
    pub mode: CorpusMode,
}

impl CorpusProjectionConfig {
    pub fn projector_config(&self) -> CorpusConfig {
        CorpusConfig {
            mode: self.mode,
            ..CorpusConfig::default()
        }
    }
}

#[derive(Debug, Clone)]
pub struct SlackConfig {
    pub id: String,
    pub bot_token: SecretString,
    pub thread_token: SecretString,
    pub channel_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct GoogleConfig {
    pub id: String,
    pub access_token: Option<SecretString>,
    pub client_id: Option<SecretString>,
    pub client_secret: Option<SecretString>,
    pub refresh_token: Option<SecretString>,
    pub presentation_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SlideAiConfig {
    pub api_key: SecretString,
    pub model: String,
}

#[derive(Debug, Clone)]
pub struct SupplementalConfig {
    pub reject_unregistered_kinds: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("missing environment variable {0}")]
    MissingEnv(String),
    #[error("failed to read config {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("invalid TOML config: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("invalid config: {0}")]
    Invalid(String),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    server: ServerFileConfig,
    mcp: McpFileConfig,
    storage: StorageFileConfig,
    routing: RoutingFileConfig,
    runtime: RuntimeFileConfig,
    limits: LimitsFileConfig,
    corpus: CorpusFileConfig,
    api_tokens: Vec<ApiTokenFileConfig>,
    sources: SourcesFileConfig,
    supplemental: SupplementalFileConfig,
    #[serde(default)]
    derivation: Option<DerivationFileConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ServerFileConfig {
    bind_addr: String,
    mcp_bind_addr: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct McpFileConfig {
    resource_url: String,
    protected_resource_metadata_url: String,
    oauth_issuer: String,
    oauth_audience: String,
    oauth_jwks_path: PathBuf,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct StorageFileConfig {
    database_path: PathBuf,
    blob_dir: PathBuf,
    encryption_key_env: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RoutingFileConfig {
    key_order: RoutingKeyOrder,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RuntimeFileConfig {
    poll_seconds: u64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct LimitsFileConfig {
    max_blob_bytes: usize,
    max_payload_bytes: usize,
    max_sync_items: usize,
    max_page_size: usize,
    max_leaf_observations: usize,
    retention_days: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CorpusFileConfig {
    mode: CorpusMode,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ApiTokenFileConfig {
    token_env: String,
    scopes: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SourcesFileConfig {
    slack: Vec<SlackFileConfig>,
    google_slides: Vec<GoogleFileConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SlackFileConfig {
    id: String,
    bot_token_env: String,
    thread_token_env: String,
    channel_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GoogleFileConfig {
    id: String,
    access_token_env: Option<String>,
    client_id_env: Option<String>,
    client_secret_env: Option<String>,
    refresh_token_env: Option<String>,
    presentation_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DerivationFileConfig {
    gemini_api_key_env: String,
    gemini_model: String,
    slide_analysis_limit: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SupplementalFileConfig {
    reject_unregistered_kinds: bool,
}

impl SelfHostConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let path = required_env("LETHE_CONFIG_PATH")?;
        Self::from_file(Path::new(&path))
    }

    pub fn from_file(path: &Path) -> Result<Self, ConfigError> {
        let source = fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let raw: FileConfig = toml::from_str(&source)?;
        raw.validate()?;
        let jwks_source =
            fs::read_to_string(&raw.mcp.oauth_jwks_path).map_err(|source| ConfigError::Read {
                path: raw.mcp.oauth_jwks_path.clone(),
                source,
            })?;
        let jwks: JsonWebKeySet = serde_json::from_str(&jwks_source)
            .map_err(|error| ConfigError::Invalid(format!("invalid MCP OAuth JWKS: {error}")))?;
        validate_jwks(&jwks)?;

        let api_tokens = raw
            .api_tokens
            .into_iter()
            .map(|token| {
                Ok(ApiTokenConfig {
                    token: SecretString::new(required_env(&token.token_env)?)?,
                    scopes: token.scopes,
                })
            })
            .collect::<Result<Vec<_>, ConfigError>>()?;

        let slack_sources = raw
            .sources
            .slack
            .into_iter()
            .map(|source| {
                Ok(SlackConfig {
                    id: source.id,
                    bot_token: SecretString::new(required_env(&source.bot_token_env)?)?,
                    thread_token: SecretString::new(required_env(&source.thread_token_env)?)?,
                    channel_ids: source.channel_ids,
                })
            })
            .collect::<Result<Vec<_>, ConfigError>>()?;

        let google_sources = raw
            .sources
            .google_slides
            .into_iter()
            .map(|source| {
                let access_token = optional_secret(source.access_token_env.as_deref())?;
                let client_id = optional_secret(source.client_id_env.as_deref())?;
                let client_secret = optional_secret(source.client_secret_env.as_deref())?;
                let refresh_token = optional_secret(source.refresh_token_env.as_deref())?;
                if access_token.is_none()
                    && (client_id.is_none() || client_secret.is_none() || refresh_token.is_none())
                {
                    return Err(ConfigError::Invalid(format!(
                        "google source {} requires access_token_env or client_id_env/client_secret_env/refresh_token_env",
                        source.id
                    )));
                }
                Ok(GoogleConfig {
                    id: source.id,
                    access_token,
                    client_id,
                    client_secret,
                    refresh_token,
                    presentation_ids: source.presentation_ids,
                })
            })
            .collect::<Result<Vec<_>, ConfigError>>()?;

        let (slide_analysis_limit, slide_ai) = match raw.derivation {
            Some(derivation) => (
                Some(derivation.slide_analysis_limit),
                Some(SlideAiConfig {
                    api_key: SecretString::new(required_env(&derivation.gemini_api_key_env)?)?,
                    model: derivation.gemini_model,
                }),
            ),
            None => (None, None),
        };

        Ok(Self {
            bind_addr: raw.server.bind_addr,
            mcp_bind_addr: raw.server.mcp_bind_addr,
            mcp_oauth: McpOAuthConfig {
                resource_url: raw.mcp.resource_url,
                protected_resource_metadata_url: raw.mcp.protected_resource_metadata_url,
                issuer: raw.mcp.oauth_issuer,
                audience: raw.mcp.oauth_audience,
                jwks_path: raw.mcp.oauth_jwks_path,
                jwks,
            },
            database_path: raw.storage.database_path,
            blob_dir: raw.storage.blob_dir,
            secret_encryption_key: parse_encryption_key(&required_env(
                &raw.storage.encryption_key_env,
            )?)?,
            poll_interval: Duration::from_secs(raw.runtime.poll_seconds),
            routing_key_order: raw.routing.key_order,
            api_tokens,
            resource_limits: ResourceLimits {
                max_blob_bytes: raw.limits.max_blob_bytes,
                max_payload_bytes: raw.limits.max_payload_bytes,
                max_sync_items: raw.limits.max_sync_items,
                max_page_size: raw.limits.max_page_size,
                max_leaf_observations: raw.limits.max_leaf_observations,
                retention_days: raw.limits.retention_days,
            },
            corpus: CorpusProjectionConfig {
                mode: raw.corpus.mode,
            },
            slack_sources,
            google_sources,
            slide_analysis_limit,
            slide_ai,
            supplemental: SupplementalConfig {
                reject_unregistered_kinds: raw.supplemental.reject_unregistered_kinds,
            },
        })
    }
}

impl FileConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        require_non_empty("server.bind_addr", &self.server.bind_addr)?;
        require_non_empty("server.mcp_bind_addr", &self.server.mcp_bind_addr)?;
        let bind_addr = parse_socket_addr("server.bind_addr", &self.server.bind_addr)?;
        let mcp_bind_addr = parse_socket_addr("server.mcp_bind_addr", &self.server.mcp_bind_addr)?;
        if bind_addr.port() == mcp_bind_addr.port() {
            return Err(ConfigError::Invalid(
                "server.bind_addr and server.mcp_bind_addr must use different ports".to_owned(),
            ));
        }
        require_non_empty("mcp.resource_url", &self.mcp.resource_url)?;
        require_non_empty(
            "mcp.protected_resource_metadata_url",
            &self.mcp.protected_resource_metadata_url,
        )?;
        require_non_empty("mcp.oauth_issuer", &self.mcp.oauth_issuer)?;
        require_non_empty("mcp.oauth_audience", &self.mcp.oauth_audience)?;
        if self.mcp.oauth_jwks_path.as_os_str().is_empty() {
            return Err(ConfigError::Invalid(
                "mcp.oauth_jwks_path must not be blank".to_owned(),
            ));
        }
        reject_header_control_chars(
            "mcp.protected_resource_metadata_url",
            &self.mcp.protected_resource_metadata_url,
        )?;
        require_positive("runtime.poll_seconds", self.runtime.poll_seconds as usize)?;
        require_positive("limits.max_blob_bytes", self.limits.max_blob_bytes)?;
        require_positive("limits.max_payload_bytes", self.limits.max_payload_bytes)?;
        require_positive("limits.max_sync_items", self.limits.max_sync_items)?;
        require_positive("limits.max_page_size", self.limits.max_page_size)?;
        require_positive(
            "limits.max_leaf_observations",
            self.limits.max_leaf_observations,
        )?;
        require_positive("limits.retention_days", self.limits.retention_days as usize)?;
        if matches!(self.corpus.mode, CorpusMode::PersonalAllText) {
            let has_corpus_reader = self.api_tokens.iter().any(|token| {
                token
                    .scopes
                    .iter()
                    .any(|scope| scope == "*" || scope == "read:corpus")
            });
            if !has_corpus_reader {
                return Err(ConfigError::Invalid(
                    "corpus.mode = personal_all_text requires an api token with read:corpus scope"
                        .to_owned(),
                ));
            }
        }
        if self.api_tokens.is_empty() {
            return Err(ConfigError::Invalid(
                "api_tokens must contain at least one entry".to_owned(),
            ));
        }
        if !self.sources.google_slides.is_empty() && self.derivation.is_none() {
            return Err(ConfigError::Invalid(
                "derivation is required when google_slides sources are configured".to_owned(),
            ));
        }
        if let Some(derivation) = &self.derivation {
            require_non_empty(
                "derivation.gemini_api_key_env",
                &derivation.gemini_api_key_env,
            )?;
            require_non_empty("derivation.gemini_model", &derivation.gemini_model)?;
            require_positive(
                "derivation.slide_analysis_limit",
                derivation.slide_analysis_limit,
            )?;
        }
        let mut ids = HashSet::new();
        for id in self
            .sources
            .slack
            .iter()
            .map(|source| &source.id)
            .chain(self.sources.google_slides.iter().map(|source| &source.id))
        {
            require_non_empty("source.id", id)?;
            if !ids.insert(id) {
                return Err(ConfigError::Invalid(format!(
                    "duplicate source instance id: {id}"
                )));
            }
        }
        for token in &self.api_tokens {
            require_non_empty("api_tokens.token_env", &token.token_env)?;
            if token.scopes.is_empty() {
                return Err(ConfigError::Invalid(
                    "api token scopes must not be empty".to_owned(),
                ));
            }
        }
        for source in &self.sources.slack {
            if source.channel_ids.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "slack source {} has no channel_ids",
                    source.id
                )));
            }
        }
        for source in &self.sources.google_slides {
            if source.presentation_ids.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "google source {} has no presentation_ids",
                    source.id
                )));
            }
        }
        Ok(())
    }
}

fn required_env(name: &str) -> Result<String, ConfigError> {
    let value = env::var(name).map_err(|_| ConfigError::MissingEnv(name.to_owned()))?;
    require_non_empty(name, &value)?;
    Ok(value)
}

fn optional_secret(name: Option<&str>) -> Result<Option<SecretString>, ConfigError> {
    name.map(|name| required_env(name).and_then(SecretString::new))
        .transpose()
}

fn require_non_empty(name: &str, value: &str) -> Result<(), ConfigError> {
    if value.trim().is_empty() {
        Err(ConfigError::Invalid(format!("{name} must not be blank")))
    } else {
        Ok(())
    }
}

fn require_positive(name: &str, value: usize) -> Result<(), ConfigError> {
    if value == 0 {
        Err(ConfigError::Invalid(format!("{name} must be positive")))
    } else {
        Ok(())
    }
}

fn parse_socket_addr(name: &str, value: &str) -> Result<SocketAddr, ConfigError> {
    value
        .parse::<SocketAddr>()
        .map_err(|error| ConfigError::Invalid(format!("{name} must be a socket address: {error}")))
}

fn reject_header_control_chars(name: &str, value: &str) -> Result<(), ConfigError> {
    if value.chars().any(|ch| ch == '\r' || ch == '\n') {
        Err(ConfigError::Invalid(format!(
            "{name} must not contain CR/LF characters"
        )))
    } else if !value.is_ascii() {
        Err(ConfigError::Invalid(format!("{name} must be ASCII")))
    } else {
        Ok(())
    }
}

fn validate_jwks(jwks: &JsonWebKeySet) -> Result<(), ConfigError> {
    if jwks.keys.is_empty() {
        return Err(ConfigError::Invalid(
            "MCP OAuth JWKS must contain at least one key".to_owned(),
        ));
    }
    let mut kids = HashSet::new();
    for key in &jwks.keys {
        require_non_empty("mcp JWKS key.kid", &key.kid)?;
        if !kids.insert(key.kid.as_str()) {
            return Err(ConfigError::Invalid(format!(
                "duplicate MCP OAuth JWKS kid: {}",
                key.kid
            )));
        }
        match key.kty.as_str() {
            "EC" => {
                if key.crv.as_deref() != Some("P-256")
                    || key.x.as_deref().is_none()
                    || key.y.as_deref().is_none()
                {
                    return Err(ConfigError::Invalid(format!(
                        "MCP OAuth JWKS EC key {} must be P-256 with x and y",
                        key.kid
                    )));
                }
                let x = validate_base64url_part("mcp JWKS key.x", key.x.as_deref().unwrap())?;
                let y = validate_base64url_part("mcp JWKS key.y", key.y.as_deref().unwrap())?;
                if x.len() != 32 || y.len() != 32 {
                    return Err(ConfigError::Invalid(format!(
                        "MCP OAuth JWKS EC key {} x and y must be 32 bytes",
                        key.kid
                    )));
                }
            }
            "RSA" => {
                if key.n.as_deref().is_none() || key.e.as_deref().is_none() {
                    return Err(ConfigError::Invalid(format!(
                        "MCP OAuth JWKS RSA key {} must contain n and e",
                        key.kid
                    )));
                }
                validate_base64url_part("mcp JWKS key.n", key.n.as_deref().unwrap())?;
                validate_base64url_part("mcp JWKS key.e", key.e.as_deref().unwrap())?;
            }
            other => {
                return Err(ConfigError::Invalid(format!(
                    "unsupported MCP OAuth JWKS key type: {other}"
                )));
            }
        }
    }
    Ok(())
}

fn validate_base64url_part(name: &str, value: &str) -> Result<Vec<u8>, ConfigError> {
    URL_SAFE_NO_PAD
        .decode(value.as_bytes())
        .map_err(|error| ConfigError::Invalid(format!("{name} is not valid base64url: {error}")))
}

fn parse_encryption_key(value: &str) -> Result<[u8; 32], ConfigError> {
    let decoded = hex::decode(value)
        .map_err(|error| ConfigError::Invalid(format!("invalid encryption key hex: {error}")))?;
    decoded.try_into().map_err(|_| {
        ConfigError::Invalid("storage encryption key must be exactly 32 bytes".to_owned())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_string_debug_redacts_value() {
        let secret = SecretString::new("super-secret-token").unwrap();
        let debug = format!("{secret:?}");
        assert!(!debug.contains("super-secret-token"));
        assert!(debug.contains("redacted"));
    }

    #[test]
    fn duplicate_source_ids_are_rejected() {
        let raw: FileConfig = toml::from_str(
            r#"
            [server]
            bind_addr = "127.0.0.1:8080"
            mcp_bind_addr = "127.0.0.1:8090"
            [mcp]
            resource_url = "https://mcp.example.test/mcp"
            protected_resource_metadata_url = "https://mcp.example.test/.well-known/oauth-protected-resource"
            oauth_issuer = "https://issuer.example.test/"
            oauth_audience = "lethe-mcp"
            oauth_jwks_path = "mcp-jwks.json"
            [storage]
            database_path = "data/lethe.sqlite3"
            blob_dir = "data/blobs"
            encryption_key_env = "ENCRYPTION_KEY"
            [routing]
            key_order = "month_year_source_container_published"
            [runtime]
            poll_seconds = 60
            [limits]
            max_blob_bytes = 1
            max_payload_bytes = 1
            max_sync_items = 1
            max_page_size = 1
            max_leaf_observations = 1
            retention_days = 30
            [corpus]
            mode = "workspace_filtered"
            [supplemental]
            reject_unregistered_kinds = true
            [[api_tokens]]
            token_env = "TOKEN"
            scopes = ["read"]
            [sources]
            [[sources.slack]]
            id = "same"
            bot_token_env = "BOT"
            thread_token_env = "THREAD"
            channel_ids = ["C1"]
            [[sources.google_slides]]
            id = "same"
            access_token_env = "GOOGLE"
            presentation_ids = ["P1"]
            [derivation]
            gemini_api_key_env = "GEMINI"
            gemini_model = "model"
            slide_analysis_limit = 1
            "#,
        )
        .unwrap();

        assert!(raw.validate().is_err());
    }

    #[test]
    fn empty_sources_are_allowed_for_import_only_instance() {
        let raw: FileConfig = toml::from_str(
            r#"
            [server]
            bind_addr = "127.0.0.1:8080"
            mcp_bind_addr = "127.0.0.1:8090"
            [mcp]
            resource_url = "https://mcp.example.test/mcp"
            protected_resource_metadata_url = "https://mcp.example.test/.well-known/oauth-protected-resource"
            oauth_issuer = "https://issuer.example.test/"
            oauth_audience = "lethe-mcp"
            oauth_jwks_path = "mcp-jwks.json"
            [storage]
            database_path = "data/lethe.sqlite3"
            blob_dir = "data/blobs"
            encryption_key_env = "ENCRYPTION_KEY"
            [routing]
            key_order = "year_month_source_container_published"
            [runtime]
            poll_seconds = 60
            [limits]
            max_blob_bytes = 1
            max_payload_bytes = 1
            max_sync_items = 1
            max_page_size = 1
            max_leaf_observations = 1
            retention_days = 3650
            [corpus]
            mode = "personal_all_text"
            [supplemental]
            reject_unregistered_kinds = true
            [[api_tokens]]
            token_env = "TOKEN"
            scopes = ["admin:health", "read:corpus"]
            [sources]
            slack = []
            google_slides = []
            "#,
        )
        .unwrap();

        assert!(raw.validate().is_ok());
    }

    #[test]
    fn mcp_listener_must_not_share_internal_api_port() {
        let raw: FileConfig = toml::from_str(
            r#"
            [server]
            bind_addr = "127.0.0.1:8080"
            mcp_bind_addr = "127.0.0.1:8080"
            [mcp]
            resource_url = "https://mcp.example.test/mcp"
            protected_resource_metadata_url = "https://mcp.example.test/.well-known/oauth-protected-resource"
            oauth_issuer = "https://issuer.example.test/"
            oauth_audience = "lethe-mcp"
            oauth_jwks_path = "mcp-jwks.json"
            [storage]
            database_path = "data/lethe.sqlite3"
            blob_dir = "data/blobs"
            encryption_key_env = "ENCRYPTION_KEY"
            [routing]
            key_order = "year_month_source_container_published"
            [runtime]
            poll_seconds = 60
            [limits]
            max_blob_bytes = 1
            max_payload_bytes = 1
            max_sync_items = 1
            max_page_size = 1
            max_leaf_observations = 1
            retention_days = 3650
            [corpus]
            mode = "personal_all_text"
            [supplemental]
            reject_unregistered_kinds = true
            [[api_tokens]]
            token_env = "TOKEN"
            scopes = ["read:corpus"]
            [sources]
            slack = []
            google_slides = []
            "#,
        )
        .unwrap();

        let error = raw.validate().unwrap_err().to_string();
        assert!(error.contains("different ports"));
    }

    #[test]
    fn google_sources_require_derivation_config() {
        let raw: FileConfig = toml::from_str(
            r#"
            [server]
            bind_addr = "127.0.0.1:8080"
            mcp_bind_addr = "127.0.0.1:8090"
            [mcp]
            resource_url = "https://mcp.example.test/mcp"
            protected_resource_metadata_url = "https://mcp.example.test/.well-known/oauth-protected-resource"
            oauth_issuer = "https://issuer.example.test/"
            oauth_audience = "lethe-mcp"
            oauth_jwks_path = "mcp-jwks.json"
            [storage]
            database_path = "data/lethe.sqlite3"
            blob_dir = "data/blobs"
            encryption_key_env = "ENCRYPTION_KEY"
            [routing]
            key_order = "month_year_source_container_published"
            [runtime]
            poll_seconds = 60
            [limits]
            max_blob_bytes = 1
            max_payload_bytes = 1
            max_sync_items = 1
            max_page_size = 1
            max_leaf_observations = 1
            retention_days = 30
            [corpus]
            mode = "workspace_filtered"
            [supplemental]
            reject_unregistered_kinds = true
            [[api_tokens]]
            token_env = "TOKEN"
            scopes = ["read"]
            [sources]
            slack = []
            [[sources.google_slides]]
            id = "gslides"
            access_token_env = "GOOGLE"
            presentation_ids = ["P1"]
            "#,
        )
        .unwrap();

        assert!(raw.validate().is_err());
    }
}
