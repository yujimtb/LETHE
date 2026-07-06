use std::env;
use std::fs;
use std::path::PathBuf;

use lethe_adapter_github::github::mapper::GitHubDumpMapper;
use lethe_core::domain::SemVer;
use lethe_selfhost::self_host::import_client::ImportApiConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = parse_options(env::args().skip(1))?;
    let json = fs::read_to_string(&options.dump_path)?;
    let mapper = GitHubDumpMapper::new(SemVer::new("1.0.0"));
    let drafts = mapper.import_json_str(&json)?;
    let report = ImportApiConfig {
        base_url: options.base_url,
        api_token_env: options.api_token_env,
    }
    .connect()?
    .ingest_observation_drafts(drafts, &options.source_instance)?;

    println!(
        "github import complete: ingested={}, duplicates={}, quarantined={}",
        report.ingested, report.duplicates, report.quarantined
    );
    Ok(())
}

struct CliOptions {
    dump_path: PathBuf,
    source_instance: String,
    base_url: String,
    api_token_env: String,
}

fn parse_options(
    args: impl Iterator<Item = String>,
) -> Result<CliOptions, Box<dyn std::error::Error>> {
    let mut dump_path = None;
    let mut source_instance = None;
    let mut base_url = None;
    let mut api_token_env = None;

    for arg in args {
        if let Some(raw) = arg.strip_prefix("--dump=") {
            dump_path = Some(PathBuf::from(raw));
        } else if let Some(raw) = arg.strip_prefix("--source-instance=") {
            if raw.trim().is_empty() {
                return Err("--source-instance must not be blank".into());
            }
            source_instance = Some(raw.to_owned());
        } else if let Some(raw) = arg.strip_prefix("--base-url=") {
            if raw.trim().is_empty() {
                return Err("--base-url must not be blank".into());
            }
            base_url = Some(raw.to_owned());
        } else if let Some(raw) = arg.strip_prefix("--api-token-env=") {
            if raw.trim().is_empty() {
                return Err("--api-token-env must not be blank".into());
            }
            api_token_env = Some(raw.to_owned());
        } else {
            return Err(format!("unknown argument: {arg}").into());
        }
    }

    Ok(CliOptions {
        dump_path: dump_path.ok_or("--dump=<path> is required")?,
        source_instance: source_instance.ok_or("--source-instance=<id> is required")?,
        base_url: base_url.ok_or("--base-url=<url> is required")?,
        api_token_env: api_token_env.ok_or("--api-token-env=<name> is required")?,
    })
}
