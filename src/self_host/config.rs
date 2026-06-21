use std::env;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct SelfHostConfig {
    pub bind_addr: String,
    pub database_path: PathBuf,
    pub blob_dir: PathBuf,
    pub poll_interval: Duration,
    pub api_tokens: Vec<ApiTokenConfig>,
    pub resource_limits: ResourceLimits,
    pub slack: SlackConfig,
    pub google: GoogleConfig,
    pub slide_analysis_limit: usize,
    pub slide_ai: SlideAiConfig,
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
pub struct SlackConfig {
    pub bot_token: String,
    pub thread_token: String,
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

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("dotenv error: {0}")]
    Dotenv(#[from] dotenvy::Error),
    #[error("missing environment variable {0}")]
    MissingEnv(&'static str),
    #[error("invalid environment variable {name}: {message}")]
    InvalidEnv { name: &'static str, message: String },
    #[error(
        "google credentials require either LETHE_GOOGLE_ACCESS_TOKEN or the trio LETHE_GOOGLE_CLIENT_ID, LETHE_GOOGLE_CLIENT_SECRET, LETHE_GOOGLE_REFRESH_TOKEN"
    )]
    MissingGoogleCredentials,
}

impl SelfHostConfig {
    pub fn from_env() -> Result<Self, ConfigError> {
        dotenvy::dotenv()?;

        let bind_addr = required_env("LETHE_BIND_ADDR")?;
        let database_path = PathBuf::from(required_env("LETHE_DATABASE_PATH")?);
        let blob_dir = PathBuf::from(required_env("LETHE_BLOB_DIR")?);
        let poll_interval = Duration::from_secs(parse_u64_env("LETHE_POLL_SECONDS")?);
        let api_tokens = parse_api_tokens_env("LETHE_API_TOKENS")?;
        let resource_limits = ResourceLimits {
            max_blob_bytes: parse_usize_env("LETHE_MAX_BLOB_BYTES")?,
            max_payload_bytes: parse_usize_env("LETHE_MAX_PAYLOAD_BYTES")?,
            max_sync_items: parse_usize_env("LETHE_MAX_SYNC_ITEMS")?,
            max_page_size: parse_usize_env("LETHE_MAX_PAGE_SIZE")?,
        };

        let slack = SlackConfig {
            bot_token: required_env("LETHE_SLACK_BOT_TOKEN")?,
            thread_token: required_env("LETHE_SLACK_THREAD_TOKEN")?,
            channel_ids: parse_csv_env("LETHE_SLACK_CHANNEL_IDS")?,
        };

        let google = GoogleConfig {
            access_token: env::var("LETHE_GOOGLE_ACCESS_TOKEN")
                .ok()
                .filter(|v| !v.trim().is_empty()),
            client_id: env::var("LETHE_GOOGLE_CLIENT_ID")
                .ok()
                .filter(|v| !v.trim().is_empty()),
            client_secret: env::var("LETHE_GOOGLE_CLIENT_SECRET")
                .ok()
                .filter(|v| !v.trim().is_empty()),
            refresh_token: env::var("LETHE_GOOGLE_REFRESH_TOKEN")
                .ok()
                .filter(|v| !v.trim().is_empty()),
            presentation_ids: parse_csv_env("LETHE_GOOGLE_PRESENTATION_IDS")?,
        };
        let slide_analysis_limit = parse_usize_env("LETHE_GOOGLE_SLIDE_ANALYSIS_LIMIT")?;

        if google.access_token.is_none()
            && (google.client_id.is_none()
                || google.client_secret.is_none()
                || google.refresh_token.is_none())
        {
            return Err(ConfigError::MissingGoogleCredentials);
        }

        let slide_ai = SlideAiConfig {
            api_key: required_env("LETHE_GEMINI_API_KEY")?,
            model: required_env("LETHE_GEMINI_MODEL")?,
        };

        Ok(Self {
            bind_addr,
            database_path,
            blob_dir,
            poll_interval,
            api_tokens,
            resource_limits,
            slack,
            google,
            slide_analysis_limit,
            slide_ai,
        })
    }
}

fn parse_api_tokens_env(name: &'static str) -> Result<Vec<ApiTokenConfig>, ConfigError> {
    let raw = required_env(name)?;
    let mut tokens = Vec::new();
    for entry in raw
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
    {
        let (token, scopes) = entry
            .split_once(':')
            .ok_or_else(|| ConfigError::InvalidEnv {
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

fn required_env(name: &'static str) -> Result<String, ConfigError> {
    let value = env::var(name).map_err(|_| ConfigError::MissingEnv(name))?;
    if value.trim().is_empty() {
        return Err(ConfigError::InvalidEnv {
            name,
            message: "must not be blank".to_string(),
        });
    }
    Ok(value)
}

fn parse_u64_env(name: &'static str) -> Result<u64, ConfigError> {
    required_env(name)?
        .parse::<u64>()
        .map_err(|err| ConfigError::InvalidEnv {
            name,
            message: err.to_string(),
        })
}

fn parse_csv_env(name: &'static str) -> Result<Vec<String>, ConfigError> {
    let values: Vec<String> = required_env(name)?
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect();
    if values.is_empty() {
        return Err(ConfigError::InvalidEnv {
            name,
            message: "must contain at least one comma-separated value".to_string(),
        });
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_csv_splits_values() {
        unsafe {
            env::set_var("LETHE_SLACK_CHANNEL_IDS", "C1, C2 ,,C3");
        }
        let values = parse_csv_env("LETHE_SLACK_CHANNEL_IDS").unwrap();
        assert_eq!(values, vec!["C1", "C2", "C3"]);
    }

    #[test]
    fn secret_string_debug_redacts_value() {
        let secret = SecretString::new("super-secret-token").unwrap();
        let debug = format!("{secret:?}");
        assert!(!debug.contains("super-secret-token"));
        assert!(debug.contains("redacted"));
    }
}

fn parse_usize_env(name: &'static str) -> Result<usize, ConfigError> {
    required_env(name)?
        .parse::<usize>()
        .map_err(|err| ConfigError::InvalidEnv {
            name,
            message: err.to_string(),
        })
}
