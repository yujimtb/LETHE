use std::env;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct SelfHostConfig {
    pub bind_addr: String,
    pub public_base_url: Option<String>,
    pub database_path: PathBuf,
    pub blob_dir: PathBuf,
    pub poll_interval: Duration,
    pub api_tokens: Vec<ApiTokenConfig>,
    pub resource_limits: ResourceLimits,
    pub sources: Vec<SourceInstanceConfig>,
    pub slack: SlackConfig,
    pub google: GoogleConfig,
    pub slide_analysis_limit: usize,
    pub slide_ai: Option<SlideAiConfig>,
    pub notion: Option<NotionWritebackConfig>,
}

#[derive(Debug, Clone)]
pub struct ApiTokenConfig {
    pub token: SecretString,
    pub scopes: Vec<String>,
}

#[derive(Clone, PartialEq, Eq)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Result<Self, ConfigError> {
        let value = value.into();
        if value.trim().is_empty() {
            return Err(ConfigError::InvalidEnv {
                name: "secret",
                message: "must not be blank".to_string(),
            });
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
}

#[derive(Debug, Clone)]
pub struct SourceInstanceConfig {
    pub id: String,
    pub adapter: String,
    pub credential_ref: String,
    pub cursor_key: String,
    pub settings: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct SlackConfig {
    pub bot_token: String,
    pub thread_token: Option<String>,
    pub channel_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct GoogleConfig {
    pub access_token: Option<String>,
    pub client_id: Option<String>,
    pub client_secret: Option<String>,
    pub refresh_token: Option<String>,
    pub presentation_ids: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct SlideAiConfig {
    pub api_key: String,
    pub model: String,
}

/// Configuration for Notion write-back adapter.
#[derive(Debug, Clone)]
pub struct NotionWritebackConfig {
    /// Notion integration token.
    pub token: String,
    /// Target Notion database ID.
    pub database_id: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("missing environment variable {0}")]
    MissingEnv(&'static str),
    #[error("invalid environment variable {name}: {message}")]
    InvalidEnv { name: &'static str, message: String },
    #[error("google credentials require either LETHE_GOOGLE_ACCESS_TOKEN or the trio LETHE_GOOGLE_CLIENT_ID, LETHE_GOOGLE_CLIENT_SECRET, LETHE_GOOGLE_REFRESH_TOKEN")]
    MissingGoogleCredentials,
}

impl SelfHostConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        let _ = dotenvy::dotenv();

        let bind_addr = env::var("LETHE_BIND_ADDR").unwrap_or_else(|_| "127.0.0.1:8080".to_string());
        let public_base_url = env::var("LETHE_PUBLIC_BASE_URL")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .map(|value| normalize_public_base_url("LETHE_PUBLIC_BASE_URL", value))
            .transpose()?;
        let database_path =
            PathBuf::from(env::var("LETHE_DATABASE_PATH").unwrap_or_else(|_| "./data/lethe.sqlite3".to_string()));
        let blob_dir =
            PathBuf::from(env::var("LETHE_BLOB_DIR").unwrap_or_else(|_| "./data/blobs".to_string()));
        let poll_interval = Duration::from_secs(parse_u64_env("LETHE_POLL_SECONDS", 300)?);
        let api_tokens = parse_api_tokens_env("LETHE_API_TOKENS")?;
        let resource_limits = ResourceLimits {
            max_blob_bytes: parse_usize_env("LETHE_MAX_BLOB_BYTES", 10 * 1024 * 1024)?,
            max_payload_bytes: parse_usize_env("LETHE_MAX_PAYLOAD_BYTES", 1024 * 1024)?,
            max_sync_items: parse_usize_env("LETHE_MAX_SYNC_ITEMS", 10_000)?,
            max_page_size: parse_usize_env("LETHE_MAX_PAGE_SIZE", 100)?,
        };

        let slack = SlackConfig {
            bot_token: required_env("LETHE_SLACK_BOT_TOKEN")?,
            thread_token: env::var("LETHE_SLACK_THREAD_TOKEN")
                .ok()
                .filter(|v| !v.trim().is_empty()),
            channel_ids: parse_csv_env("LETHE_SLACK_CHANNEL_IDS", true)?,
        };

        let google = GoogleConfig {
            access_token: env::var("LETHE_GOOGLE_ACCESS_TOKEN").ok().filter(|v| !v.trim().is_empty()),
            client_id: env::var("LETHE_GOOGLE_CLIENT_ID").ok().filter(|v| !v.trim().is_empty()),
            client_secret: env::var("LETHE_GOOGLE_CLIENT_SECRET").ok().filter(|v| !v.trim().is_empty()),
            refresh_token: env::var("LETHE_GOOGLE_REFRESH_TOKEN").ok().filter(|v| !v.trim().is_empty()),
            presentation_ids: parse_csv_env("LETHE_GOOGLE_PRESENTATION_IDS", true)?,
        };
        let slide_analysis_limit = parse_usize_env("LETHE_GOOGLE_SLIDE_ANALYSIS_LIMIT", 10)?;
        let sources = source_instances_from_legacy_config(&slack, &google);

        if google.access_token.is_none()
            && (google.client_id.is_none()
                || google.client_secret.is_none()
                || google.refresh_token.is_none())
        {
            return Err(ConfigError::MissingGoogleCredentials);
        }

        let notion = match (
            env::var("LETHE_NOTION_TOKEN").ok().filter(|v| !v.trim().is_empty()),
            env::var("LETHE_NOTION_DATABASE_ID").ok().filter(|v| !v.trim().is_empty()),
        ) {
            (Some(token), Some(database_id)) => Some(NotionWritebackConfig { token, database_id }),
            _ => None,
        };

        let slide_ai = env::var("LETHE_GEMINI_API_KEY")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .map(|api_key| SlideAiConfig {
                api_key,
                model: env::var("LETHE_GEMINI_MODEL")
                    .ok()
                    .filter(|v| !v.trim().is_empty())
                    .unwrap_or_else(|| "gemini-3-flash-preview".to_string()),
            });

        Ok(Self {
            bind_addr,
            public_base_url,
            database_path,
            blob_dir,
            poll_interval,
            api_tokens,
            resource_limits,
            sources,
            slack,
            google,
            slide_analysis_limit,
            slide_ai,
            notion,
        })
    }
}

fn parse_api_tokens_env(name: &'static str) -> Result<Vec<ApiTokenConfig>, ConfigError> {
    let raw = required_env(name)?;
    let mut tokens = Vec::new();
    for entry in raw.split(',').map(str::trim).filter(|entry| !entry.is_empty()) {
        let (token, scopes) = entry.split_once(':').ok_or_else(|| ConfigError::InvalidEnv {
            name,
            message: "entries must use token:scope+scope format".to_string(),
        })?;
        let scopes = scopes
            .split('+')
            .map(str::trim)
            .filter(|scope| !scope.is_empty())
            .map(ToOwned::to_owned)
            .collect::<Vec<_>>();
        if scopes.is_empty() {
            return Err(ConfigError::InvalidEnv {
                name,
                message: "token entry must include at least one scope".to_string(),
            });
        }
        tokens.push(ApiTokenConfig {
            token: SecretString::new(token.to_string())?,
            scopes,
        });
    }
    if tokens.is_empty() {
        return Err(ConfigError::InvalidEnv {
            name,
            message: "must include at least one token".to_string(),
        });
    }
    Ok(tokens)
}

fn source_instances_from_legacy_config(
    slack: &SlackConfig,
    google: &GoogleConfig,
) -> Vec<SourceInstanceConfig> {
    let mut sources = Vec::new();
    for channel_id in &slack.channel_ids {
        sources.push(SourceInstanceConfig {
            id: format!("slack:{channel_id}"),
            adapter: "slack".to_string(),
            credential_ref: "env:LETHE_SLACK_BOT_TOKEN".to_string(),
            cursor_key: format!("slack:{channel_id}:oldest_ts"),
            settings: serde_json::json!({ "channel_id": channel_id }),
        });
    }
    for presentation_id in &google.presentation_ids {
        sources.push(SourceInstanceConfig {
            id: format!("gslides:{presentation_id}"),
            adapter: "gslides".to_string(),
            credential_ref: "env:LETHE_GOOGLE_ACCESS_TOKEN".to_string(),
            cursor_key: format!("gslides:{presentation_id}:revision"),
            settings: serde_json::json!({ "presentation_id": presentation_id }),
        });
    }
    sources
}

fn required_env(name: &'static str) -> Result<String, ConfigError> {
    env::var(name).map_err(|_| ConfigError::MissingEnv(name))
}

fn parse_u64_env(name: &'static str, default: u64) -> Result<u64, ConfigError> {
    match env::var(name) {
        Ok(raw) => raw.parse::<u64>().map_err(|err| ConfigError::InvalidEnv {
            name,
            message: err.to_string(),
        }),
        Err(_) => Ok(default),
    }
}

fn normalize_public_base_url(name: &'static str, raw: String) -> Result<String, ConfigError> {
    let normalized = raw.trim().trim_end_matches('/').to_string();
    if normalized.starts_with("http://") || normalized.starts_with("https://") {
        Ok(normalized)
    } else {
        Err(ConfigError::InvalidEnv {
            name,
            message: "must start with http:// or https://".to_string(),
        })
    }
}

fn parse_csv_env(name: &'static str, required: bool) -> Result<Vec<String>, ConfigError> {
    match env::var(name) {
        Ok(raw) => {
            let values: Vec<String> = raw
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect();
            if required && values.is_empty() {
                return Err(ConfigError::InvalidEnv {
                    name,
                    message: "must contain at least one comma-separated value".to_string(),
                });
            }
            Ok(values)
        }
        Err(_) if required => Err(ConfigError::MissingEnv(name)),
        Err(_) => Ok(Vec::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_csv_splits_values() {
        unsafe {
            env::set_var("LETHE_SLACK_CHANNEL_IDS", "C1, C2 ,,C3");
        }
        let values = parse_csv_env("LETHE_SLACK_CHANNEL_IDS", true).unwrap();
        assert_eq!(values, vec!["C1", "C2", "C3"]);
    }

    #[test]
    fn normalize_public_base_url_trims_trailing_slash() {
        let normalized =
            normalize_public_base_url("LETHE_PUBLIC_BASE_URL", "https://example.com/base/".into())
                .unwrap();
        assert_eq!(normalized, "https://example.com/base");
    }

    #[test]
    fn normalize_public_base_url_rejects_non_http() {
        let err = normalize_public_base_url("LETHE_PUBLIC_BASE_URL", "ftp://example.com".into())
            .unwrap_err();
        assert!(matches!(err, ConfigError::InvalidEnv { .. }));
    }

    #[test]
    fn secret_string_debug_redacts_value() {
        let secret = SecretString::new("super-secret-token").unwrap();
        let debug = format!("{secret:?}");
        assert!(!debug.contains("super-secret-token"));
        assert!(debug.contains("redacted"));
    }
}

fn parse_usize_env(name: &'static str, default: usize) -> Result<usize, ConfigError> {
    match env::var(name) {
        Ok(raw) => raw.parse::<usize>().map_err(|err| ConfigError::InvalidEnv {
            name,
            message: err.to_string(),
        }),
        Err(_) => Ok(default),
    }
}
