use std::env;
use std::path::PathBuf;

use lethe_adapter_coding_agent::claude_code::ClaudeCodeImporter;
use lethe_core::domain::SemVer;
use lethe_selfhost::self_host::app::AppService;
use lethe_selfhost::self_host::config::SelfHostConfig;

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

    let config = SelfHostConfig::from_env()?;
    let service = AppService::bootstrap(config)?;
    let report = service.ingest_observation_drafts(batch.drafts, &options.source_instance)?;

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
}

fn parse_options(
    args: impl Iterator<Item = String>,
) -> Result<CliOptions, Box<dyn std::error::Error>> {
    let mut archive_root = None;
    let mut source_instance = None;

    for arg in args {
        if let Some(raw) = arg.strip_prefix("--archive-root=") {
            archive_root = Some(PathBuf::from(raw));
        } else if let Some(raw) = arg.strip_prefix("--source-instance=") {
            if raw.trim().is_empty() {
                return Err("--source-instance must not be blank".into());
            }
            source_instance = Some(raw.to_owned());
        } else {
            return Err(format!("unknown argument: {arg}").into());
        }
    }

    Ok(CliOptions {
        archive_root: archive_root.ok_or("--archive-root=<path> is required")?,
        source_instance: source_instance.ok_or("--source-instance=<id> is required")?,
    })
}
