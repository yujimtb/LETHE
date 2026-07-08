use std::env;
use std::path::PathBuf;

use lethe_adapter_coding_agent::codex::CodexImporter;
use lethe_core::domain::SemVer;
use lethe_selfhost::self_host::import_client::ImportApiConfig;

const HELP: &str = "\
Import Codex archive JSONL files into LETHE through the online import API.

Usage: lethe-import-codex --archive=<path> --source-instance=<id> --base-url=<url> --api-token-env=<name>

Required arguments:
  --archive=<path>          Archive working copy containing codex/sessions/
  --source-instance=<id>    Stable source instance id, for example codex-personal
  --base-url=<url>          LETHE internal API base URL
  --api-token-env=<name>    Environment variable that holds the API token

Required environment:
  The variable named by --api-token-env must be set to a token with write:observations.

Example:
  lethe-import-codex --archive=D:\\archive --source-instance=codex-personal --base-url=http://127.0.0.1:8080 --api-token-env=LETHE_API_WRITE_TOKEN
";

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if help_requested(&args) {
        print!("{HELP}");
        return Ok(());
    }

    let options = parse_options(args.into_iter())?;
    let importer = CodexImporter::new(SemVer::new("1.0.0"));
    let batch = importer.import_archive_path(&options.archive_path)?;
    let report = ImportApiConfig {
        base_url: options.base_url,
        api_token_env: options.api_token_env,
    }
    .connect()?
    .ingest_observation_drafts(batch.drafts, &options.source_instance)?;

    println!(
        "codex import complete: ingested={}, duplicates={}, quarantined={}, files={}, transcripts={}, skipped_malformed={}, skipped_unknown={}, excluded_known={}",
        report.ingested,
        report.duplicates,
        report.quarantined,
        batch.audit.files_read,
        batch.audit.transcripts_read,
        batch.audit.malformed_lines.len(),
        batch.audit.skipped_unknown_lines.len(),
        batch.audit.excluded_known_lines
    );
    Ok(())
}

fn help_requested(args: &[String]) -> bool {
    args.iter().any(|arg| arg == "--help" || arg == "-h")
}

struct CliOptions {
    archive_path: PathBuf,
    source_instance: String,
    base_url: String,
    api_token_env: String,
}

fn parse_options(
    args: impl Iterator<Item = String>,
) -> Result<CliOptions, Box<dyn std::error::Error>> {
    let mut archive_path = None;
    let mut source_instance = None;
    let mut base_url = None;
    let mut api_token_env = None;

    for arg in args {
        if let Some(raw) = arg.strip_prefix("--archive=") {
            archive_path = Some(PathBuf::from(raw));
        } else if let Some(raw) = arg.strip_prefix("--source-instance=") {
            if raw.trim().is_empty() {
                return Err(
                    "--source-instance must not be blank. Pass --source-instance=<id>.".into(),
                );
            }
            source_instance = Some(raw.to_owned());
        } else if let Some(raw) = arg.strip_prefix("--base-url=") {
            if raw.trim().is_empty() {
                return Err("--base-url must not be blank. Pass --base-url=<url>.".into());
            }
            base_url = Some(raw.to_owned());
        } else if let Some(raw) = arg.strip_prefix("--api-token-env=") {
            if raw.trim().is_empty() {
                return Err(
                    "--api-token-env must not be blank. Pass --api-token-env=<name>.".into(),
                );
            }
            api_token_env = Some(raw.to_owned());
        } else {
            return Err(format!("unknown argument: {arg}. Run with --help for usage.").into());
        }
    }

    Ok(CliOptions {
        archive_path: archive_path.ok_or_else(|| {
            missing_argument("--archive=<path>", "Pass --archive=D:\\path\\to\\archive.")
        })?,
        source_instance: source_instance.ok_or_else(|| {
            missing_argument(
                "--source-instance=<id>",
                "Pass --source-instance=codex-personal.",
            )
        })?,
        base_url: base_url.ok_or_else(|| {
            missing_argument("--base-url=<url>", "Pass --base-url=http://127.0.0.1:8080.")
        })?,
        api_token_env: api_token_env.ok_or_else(|| {
            missing_argument(
                "--api-token-env=<name>",
                "Pass --api-token-env=LETHE_API_WRITE_TOKEN and set that environment variable.",
            )
        })?,
    })
}

fn missing_argument(name: &str, fix: &str) -> Box<dyn std::error::Error> {
    format!("missing required argument {name}. {fix} Run with --help for usage.").into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn help_flags_are_detected() {
        assert!(help_requested(&["--help".to_owned()]));
        assert!(help_requested(&["-h".to_owned()]));
        assert!(HELP.contains("Import Codex"));
        assert!(HELP.contains("--archive=<path>"));
        assert!(HELP.contains("--api-token-env=<name>"));
    }
}
