use std::env;
use std::path::PathBuf;

use lethe_adapter_coding_agent::claude_code::ClaudeCodeImporter;
use lethe_core::domain::SemVer;
use lethe_selfhost::self_host::import_client::ImportApiConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = parse_options(env::args().skip(1))?;
    let importer = ClaudeCodeImporter::new(SemVer::new("1.0.0"));
    let batch = importer.import_archive_root(&options.archive_root)?;

    for line in batch
        .audit
        .malformed_lines
        .iter()
        .chain(batch.audit.skipped_unknown_lines.iter())
    {
        eprintln!(
            "claude-code import audit: source={}, line={}, reason={}",
            line.path, line.line, line.reason
        );
    }

    let report = ImportApiConfig {
        base_url: options.base_url,
        api_token_env: options.api_token_env,
    }
    .connect()?
    .ingest_observation_drafts(batch.drafts, &options.source_instance)?;

    println!(
        "claude-code import complete: ingested={}, duplicates={}, quarantined={}, files={}, lines={}, observed={}, skipped_malformed={}, skipped_unknown={}, excluded_known={}, excluded_tool_results={}",
        report.ingested,
        report.duplicates,
        report.quarantined,
        batch.audit.files_read,
        batch.audit.lines_read,
        report.ingested + report.duplicates + report.quarantined,
        batch.audit.malformed_lines.len(),
        batch.audit.skipped_unknown_lines.len(),
        batch.audit.excluded_known_lines,
        batch.audit.excluded_tool_result_lines
    );
    Ok(())
}

struct CliOptions {
    archive_root: PathBuf,
    source_instance: String,
    base_url: String,
    api_token_env: String,
}

fn parse_options(
    args: impl Iterator<Item = String>,
) -> Result<CliOptions, Box<dyn std::error::Error>> {
    let mut archive_root = None;
    let mut source_instance = None;
    let mut base_url = None;
    let mut api_token_env = None;

    for arg in args {
        if let Some(raw) = arg.strip_prefix("--archive-root=") {
            archive_root = Some(PathBuf::from(raw));
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
        archive_root: archive_root.ok_or("--archive-root=<path> is required")?,
        source_instance: source_instance.ok_or("--source-instance=<id> is required")?,
        base_url: base_url.ok_or("--base-url=<url> is required")?,
        api_token_env: api_token_env.ok_or("--api-token-env=<name> is required")?,
    })
}
