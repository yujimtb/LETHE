use std::collections::{BTreeMap, HashSet};
use std::env;
use std::fs;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use lethe_core::domain::DataSpaceId;
use lethe_projection_corpus::{CorpusConfig, CorpusMode};
use lethe_registry::registry::{ChannelKind, ChannelRecord};
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
    pub operational_ledger: OperationalLedgerConfig,
    pub poll_interval: Duration,
    pub routing_key_order: RoutingKeyOrder,
    pub api_tokens: Vec<ApiTokenConfig>,
    pub resource_limits: ResourceLimits,
    pub corpus: CorpusProjectionConfig,
    pub freshness: FreshnessConfig,
    pub ops: OpsConfig,
    pub channels: Vec<ChannelRecord>,
    pub slack_sources: Vec<SlackConfig>,
    pub google_sources: Vec<GoogleConfig>,
    pub slide_analysis_limit: Option<usize>,
    pub slide_ai: Option<SlideAiConfig>,
    pub supplemental: SupplementalConfig,
}

#[derive(Debug, Clone)]
pub enum OperationalLedgerConfig {
    Sqlite {
        data_space_id: DataSpaceId,
        database_path: PathBuf,
        blob_dir: PathBuf,
        secret_encryption_key: [u8; 32],
    },
    Postgres {
        data_space_id: DataSpaceId,
        dsn: SecretString,
        schema: String,
        role: String,
    },
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
    pub max_search_job_workers: usize,
    pub max_leaf_observations: usize,
    pub retention_days: u32,
}

#[derive(Debug, Clone)]
pub struct CorpusProjectionConfig {
    pub mode: CorpusMode,
    pub index_dir: PathBuf,
    pub writer_heap_bytes: usize,
    pub rebuild_page_size: usize,
}

pub const MIN_CORPUS_INDEX_WRITER_HEAP_BYTES: usize = 15_000_000;

#[derive(Debug, Clone)]
pub struct FreshnessConfig {
    pub threshold_seconds: BTreeMap<String, i64>,
}

#[derive(Debug, Clone)]
pub struct OpsConfig {
    pub backfill_nightly_budget_items: usize,
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
    pub mention_user_ids: Vec<String>,
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
    operational_ledger: OperationalLedgerFileConfig,
    routing: RoutingFileConfig,
    runtime: RuntimeFileConfig,
    limits: LimitsFileConfig,
    corpus: CorpusFileConfig,
    freshness: FreshnessFileConfig,
    ops: OpsFileConfig,
    channels: Vec<ChannelFileConfig>,
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
#[serde(tag = "backend", rename_all = "snake_case", deny_unknown_fields)]
enum OperationalLedgerFileConfig {
    Sqlite {
        data_space_id: String,
        database_path: PathBuf,
        blob_dir: PathBuf,
        encryption_key_env: String,
    },
    Postgres {
        data_space_id: String,
        dsn_env: String,
        schema: String,
        role: String,
    },
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
    max_search_job_workers: usize,
    max_leaf_observations: usize,
    retention_days: u32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CorpusFileConfig {
    mode: CorpusMode,
    index_dir: PathBuf,
    writer_heap_bytes: usize,
    rebuild_page_size: usize,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct FreshnessFileConfig {
    threshold_seconds: BTreeMap<String, i64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct OpsFileConfig {
    backfill_nightly_budget_items: usize,
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
    mention_user_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ChannelFileConfig {
    id: String,
    kind: ChannelKind,
    source_instance_id: String,
    external_id: String,
    connection_ref: String,
    default_consent_scope: String,
    reply_slo_seconds: u64,
    freshness_threshold_seconds: u64,
    break_glass_channel: bool,
    break_glass_senders: Vec<String>,
    enabled: bool,
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
                    mention_user_ids: source.mention_user_ids,
                })
            })
            .collect::<Result<Vec<_>, ConfigError>>()?;

        let channels = raw
            .channels
            .into_iter()
            .map(|channel| ChannelRecord {
                id: channel.id,
                kind: channel.kind,
                source_instance_id: channel.source_instance_id,
                external_id: channel.external_id,
                connection_ref: channel.connection_ref,
                default_consent_scope: channel.default_consent_scope,
                reply_slo_seconds: channel.reply_slo_seconds,
                freshness_threshold_seconds: channel.freshness_threshold_seconds,
                break_glass_channel: channel.break_glass_channel,
                break_glass_senders: channel.break_glass_senders,
                enabled: channel.enabled,
            })
            .collect::<Vec<_>>();

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
        let operational_ledger = match raw.operational_ledger {
            OperationalLedgerFileConfig::Sqlite {
                data_space_id,
                database_path,
                blob_dir,
                encryption_key_env,
            } => OperationalLedgerConfig::Sqlite {
                data_space_id: DataSpaceId::new(data_space_id),
                database_path,
                blob_dir,
                secret_encryption_key: parse_encryption_key(&required_env(&encryption_key_env)?)?,
            },
            OperationalLedgerFileConfig::Postgres {
                data_space_id,
                dsn_env,
                schema,
                role,
            } => OperationalLedgerConfig::Postgres {
                data_space_id: DataSpaceId::new(data_space_id),
                dsn: SecretString::new(required_env(&dsn_env)?)?,
                schema,
                role,
            },
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
            operational_ledger,
            poll_interval: Duration::from_secs(raw.runtime.poll_seconds),
            routing_key_order: raw.routing.key_order,
            api_tokens,
            resource_limits: ResourceLimits {
                max_blob_bytes: raw.limits.max_blob_bytes,
                max_payload_bytes: raw.limits.max_payload_bytes,
                max_sync_items: raw.limits.max_sync_items,
                max_page_size: raw.limits.max_page_size,
                max_search_job_workers: raw.limits.max_search_job_workers,
                max_leaf_observations: raw.limits.max_leaf_observations,
                retention_days: raw.limits.retention_days,
            },
            corpus: CorpusProjectionConfig {
                mode: raw.corpus.mode,
                index_dir: raw.corpus.index_dir,
                writer_heap_bytes: raw.corpus.writer_heap_bytes,
                rebuild_page_size: raw.corpus.rebuild_page_size,
            },
            freshness: FreshnessConfig {
                threshold_seconds: raw.freshness.threshold_seconds,
            },
            ops: OpsConfig {
                backfill_nightly_budget_items: raw.ops.backfill_nightly_budget_items,
            },
            channels,
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
            "limits.max_search_job_workers",
            self.limits.max_search_job_workers,
        )?;
        require_positive(
            "limits.max_leaf_observations",
            self.limits.max_leaf_observations,
        )?;
        require_positive("limits.retention_days", self.limits.retention_days as usize)?;
        self.operational_ledger.validate()?;
        self.corpus.validate()?;
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
        for required_scope in [
            "read:operational",
            "write:operational",
            "read:history",
            "write:history",
        ] {
            let present = self.api_tokens.iter().any(|token| {
                token
                    .scopes
                    .iter()
                    .any(|scope| scope == "*" || scope == required_scope)
            });
            if !present {
                return Err(ConfigError::Invalid(format!(
                    "operational ledger requires an api token with {required_scope} scope"
                )));
            }
        }
        if self.freshness.threshold_seconds.is_empty() {
            return Err(ConfigError::Invalid(
                "freshness.threshold_seconds must not be empty".to_owned(),
            ));
        }
        for (source_id, seconds) in &self.freshness.threshold_seconds {
            require_non_empty("freshness.threshold_seconds key", source_id)?;
            if *seconds <= 0 {
                return Err(ConfigError::Invalid(format!(
                    "freshness threshold for {source_id} must be positive"
                )));
            }
        }
        require_positive(
            "ops.backfill_nightly_budget_items",
            self.ops.backfill_nightly_budget_items,
        )?;
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
            if source.mention_user_ids.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "slack source {} has no mention_user_ids",
                    source.id
                )));
            }
        }
        validate_channels(&self.channels)?;
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

impl OperationalLedgerFileConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        match self {
            Self::Sqlite {
                data_space_id,
                database_path,
                blob_dir,
                encryption_key_env,
            } => {
                require_non_empty("operational_ledger.data_space_id", data_space_id)?;
                require_non_empty(
                    "operational_ledger.database_path",
                    &database_path.to_string_lossy(),
                )?;
                require_non_empty("operational_ledger.blob_dir", &blob_dir.to_string_lossy())?;
                require_non_empty("operational_ledger.encryption_key_env", encryption_key_env)
            }
            Self::Postgres {
                data_space_id,
                dsn_env,
                schema,
                role,
            } => {
                require_non_empty("operational_ledger.data_space_id", data_space_id)?;
                require_non_empty("operational_ledger.dsn_env", dsn_env)?;
                require_postgres_identifier("operational_ledger.schema", schema)?;
                require_postgres_identifier("operational_ledger.role", role)
            }
        }
    }
}

fn require_postgres_identifier(field: &str, value: &str) -> Result<(), ConfigError> {
    require_non_empty(field, value)?;
    let valid = value.chars().enumerate().all(|(index, ch)| {
        ch == '_' || ch.is_ascii_lowercase() || (index > 0 && ch.is_ascii_digit())
    });
    if valid {
        Ok(())
    } else {
        Err(ConfigError::Invalid(format!(
            "{field} must be a lowercase PostgreSQL identifier"
        )))
    }
}

impl CorpusFileConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.index_dir.as_os_str().is_empty()
            || self.index_dir.to_string_lossy().trim().is_empty()
        {
            return Err(ConfigError::Invalid(
                "corpus.index_dir must not be blank".to_owned(),
            ));
        }
        if self.writer_heap_bytes < MIN_CORPUS_INDEX_WRITER_HEAP_BYTES {
            return Err(ConfigError::Invalid(format!(
                "corpus.writer_heap_bytes must be at least {MIN_CORPUS_INDEX_WRITER_HEAP_BYTES}"
            )));
        }
        require_positive("corpus.rebuild_page_size", self.rebuild_page_size)
    }
}

fn validate_channels(channels: &[ChannelFileConfig]) -> Result<(), ConfigError> {
    let mut ids = HashSet::new();
    let mut keys = HashSet::new();
    for channel in channels {
        require_non_empty("channels.id", &channel.id)?;
        require_non_empty("channels.source_instance_id", &channel.source_instance_id)?;
        require_non_empty("channels.external_id", &channel.external_id)?;
        require_non_empty("channels.connection_ref", &channel.connection_ref)?;
        require_non_empty(
            "channels.default_consent_scope",
            &channel.default_consent_scope,
        )?;
        require_positive(
            "channels.reply_slo_seconds",
            channel.reply_slo_seconds as usize,
        )?;
        require_positive(
            "channels.freshness_threshold_seconds",
            channel.freshness_threshold_seconds as usize,
        )?;
        if !ids.insert(channel.id.as_str()) {
            return Err(ConfigError::Invalid(format!(
                "duplicate channel id: {}",
                channel.id
            )));
        }
        let key = format!(
            "{}:{}:{}",
            channel.kind, channel.source_instance_id, channel.external_id
        );
        if !keys.insert(key.clone()) {
            return Err(ConfigError::Invalid(format!(
                "duplicate channel lookup key: {key}"
            )));
        }
    }
    Ok(())
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

    fn corpus_file_config(
        index_dir: &str,
        writer_heap_bytes: usize,
        rebuild_page_size: usize,
    ) -> CorpusFileConfig {
        CorpusFileConfig {
            mode: CorpusMode::WorkspaceFiltered,
            index_dir: PathBuf::from(index_dir),
            writer_heap_bytes,
            rebuild_page_size,
        }
    }

    #[test]
    fn secret_string_debug_redacts_value() {
        let secret = SecretString::new("super-secret-token").unwrap();
        let debug = format!("{secret:?}");
        assert!(!debug.contains("super-secret-token"));
        assert!(debug.contains("redacted"));
    }

    #[test]
    fn corpus_index_settings_are_required() {
        let cases = [
            (
                "index_dir",
                r#"
                mode = "workspace_filtered"
                writer_heap_bytes = 33554432
                rebuild_page_size = 512
                "#,
            ),
            (
                "writer_heap_bytes",
                r#"
                mode = "workspace_filtered"
                index_dir = "data/corpus-index"
                rebuild_page_size = 512
                "#,
            ),
            (
                "rebuild_page_size",
                r#"
                mode = "workspace_filtered"
                index_dir = "data/corpus-index"
                writer_heap_bytes = 33554432
                "#,
            ),
        ];

        for (field, source) in cases {
            let error = toml::from_str::<CorpusFileConfig>(source).unwrap_err();
            assert!(
                error.to_string().contains(field),
                "missing {field} was not reported: {error}"
            );
        }
    }

    #[test]
    fn corpus_index_settings_reject_invalid_values() {
        let blank_dir = corpus_file_config("   ", 32 * 1024 * 1024, 512)
            .validate()
            .unwrap_err();
        assert!(
            blank_dir
                .to_string()
                .contains("index_dir must not be blank")
        );

        for writer_heap_bytes in [0, MIN_CORPUS_INDEX_WRITER_HEAP_BYTES - 1] {
            let error = corpus_file_config("data/corpus-index", writer_heap_bytes, 512)
                .validate()
                .unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("writer_heap_bytes must be at least 15000000")
            );
        }

        let zero_page = corpus_file_config("data/corpus-index", 32 * 1024 * 1024, 0)
            .validate()
            .unwrap_err();
        assert!(
            zero_page
                .to_string()
                .contains("rebuild_page_size must be positive")
        );
    }

    #[test]
    fn corpus_index_settings_accept_writer_minimum_and_positive_page() {
        corpus_file_config("data/corpus-index", MIN_CORPUS_INDEX_WRITER_HEAP_BYTES, 1)
            .validate()
            .unwrap();
    }

    #[test]
    fn shipped_config_files_include_valid_corpus_index_settings() {
        let configs = [
            (
                "config.example.toml",
                include_str!("../../../../config.example.toml"),
            ),
            (
                "deploy/personal-lake/config.toml",
                include_str!("../../../../deploy/personal-lake/config.toml"),
            ),
            (
                "deploy/personal-lake/config.host.toml",
                include_str!("../../../../deploy/personal-lake/config.host.toml"),
            ),
        ];

        for (name, source) in configs {
            let config = toml::from_str::<FileConfig>(source)
                .unwrap_or_else(|error| panic!("{name} must parse: {error}"));
            config
                .validate()
                .unwrap_or_else(|error| panic!("{name} must validate: {error}"));
        }
    }

    #[test]
    fn duplicate_source_ids_are_rejected() {
        let raw: FileConfig = toml::from_str(
            r#"
            channels = []
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
            [operational_ledger]
            backend = "sqlite"
            data_space_id = "space:test"
            database_path = "data/operational.sqlite3"
            blob_dir = "data/operational-blobs"
            encryption_key_env = "OPERATIONAL_ENCRYPTION_KEY"
            [routing]
            key_order = "month_year_source_container_published"
            [runtime]
            poll_seconds = 60
            [limits]
            max_blob_bytes = 1
            max_payload_bytes = 1
            max_sync_items = 1
            max_page_size = 1
            max_search_job_workers = 2
            max_leaf_observations = 1
            retention_days = 30
            [corpus]
            mode = "workspace_filtered"
            index_dir = "data/corpus-index"
            writer_heap_bytes = 33554432
            rebuild_page_size = 512
            [freshness.threshold_seconds]
            "sys:slack" = 129600
            [ops]
            backfill_nightly_budget_items = 1000
            [supplemental]
            reject_unregistered_kinds = true
            [[api_tokens]]
            token_env = "TOKEN"
            scopes = ["read", "read:operational", "write:operational", "read:history", "write:history"]
            [sources]
            [[sources.slack]]
            id = "same"
            bot_token_env = "BOT"
            thread_token_env = "THREAD"
            channel_ids = ["C1"]
            mention_user_ids = ["U123"]
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
            channels = []
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
            [operational_ledger]
            backend = "sqlite"
            data_space_id = "space:test"
            database_path = "data/operational.sqlite3"
            blob_dir = "data/operational-blobs"
            encryption_key_env = "OPERATIONAL_ENCRYPTION_KEY"
            [routing]
            key_order = "year_month_source_container_published"
            [runtime]
            poll_seconds = 60
            [limits]
            max_blob_bytes = 1
            max_payload_bytes = 1
            max_sync_items = 1
            max_page_size = 1
            max_search_job_workers = 2
            max_leaf_observations = 1
            retention_days = 3650
            [corpus]
            mode = "personal_all_text"
            index_dir = "data/corpus-index"
            writer_heap_bytes = 33554432
            rebuild_page_size = 512
            [freshness.threshold_seconds]
            "sys:slack" = 129600
            [ops]
            backfill_nightly_budget_items = 1000
            [supplemental]
            reject_unregistered_kinds = true
            [[api_tokens]]
            token_env = "TOKEN"
            scopes = ["admin:health", "read:corpus", "read:operational", "write:operational", "read:history", "write:history"]
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
            channels = []
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
            [operational_ledger]
            backend = "sqlite"
            data_space_id = "space:test"
            database_path = "data/operational.sqlite3"
            blob_dir = "data/operational-blobs"
            encryption_key_env = "OPERATIONAL_ENCRYPTION_KEY"
            [routing]
            key_order = "year_month_source_container_published"
            [runtime]
            poll_seconds = 60
            [limits]
            max_blob_bytes = 1
            max_payload_bytes = 1
            max_sync_items = 1
            max_page_size = 1
            max_search_job_workers = 2
            max_leaf_observations = 1
            retention_days = 3650
            [corpus]
            mode = "personal_all_text"
            index_dir = "data/corpus-index"
            writer_heap_bytes = 33554432
            rebuild_page_size = 512
            [freshness.threshold_seconds]
            "sys:slack" = 129600
            [ops]
            backfill_nightly_budget_items = 1000
            [supplemental]
            reject_unregistered_kinds = true
            [[api_tokens]]
            token_env = "TOKEN"
            scopes = ["read:corpus", "read:operational", "write:operational", "read:history", "write:history"]
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
            channels = []
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
            [operational_ledger]
            backend = "sqlite"
            data_space_id = "space:test"
            database_path = "data/operational.sqlite3"
            blob_dir = "data/operational-blobs"
            encryption_key_env = "OPERATIONAL_ENCRYPTION_KEY"
            [routing]
            key_order = "month_year_source_container_published"
            [runtime]
            poll_seconds = 60
            [limits]
            max_blob_bytes = 1
            max_payload_bytes = 1
            max_sync_items = 1
            max_page_size = 1
            max_search_job_workers = 2
            max_leaf_observations = 1
            retention_days = 30
            [corpus]
            mode = "workspace_filtered"
            index_dir = "data/corpus-index"
            writer_heap_bytes = 33554432
            rebuild_page_size = 512
            [freshness.threshold_seconds]
            "sys:slack" = 129600
            [ops]
            backfill_nightly_budget_items = 1000
            [supplemental]
            reject_unregistered_kinds = true
            [[api_tokens]]
            token_env = "TOKEN"
            scopes = ["read", "read:operational", "write:operational", "read:history", "write:history"]
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
