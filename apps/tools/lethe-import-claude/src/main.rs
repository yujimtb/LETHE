use std::env;
use std::fs;
use std::path::PathBuf;

use lethe_adapter_claude::claude::importer::ClaudeAiImporter;
use lethe_core::domain::SemVer;
use lethe_selfhost::self_host::import_client::{
    ImportApiConfig, ImportApiVersion, normalize_import_option_args, resolve_admission_generation,
    resolve_api_version,
};

const HELP: &str = "\
Import a claude.ai export zip into LETHE through the online import API.

Usage: lethe-import-claude --zip=<path> --source-instance=<id> --base-url=<url> --api-token-env=<name> [--api-version=<1|2>] [--admission-generation=<int>]

Required arguments:
  --zip=<path>              claude.ai export zip
  --source-instance=<id>    Stable source instance id, for example claude-personal
  --base-url=<url>          LETHE internal API base URL
  --api-token-env=<name>    Environment variable that holds the API token
  --api-version=<1|2>      Import API version; defaults to 1
  --admission-generation=<int>
                            Required for API version 2; sent as the admission header

Required environment:
  The variable named by --api-token-env must be set to a token with write:observations.
  LETHE_INGEST_API_VERSION may provide --api-version (default: 1).
  LETHE_ADMISSION_GENERATION may provide --admission-generation.

Example:
  lethe-import-claude --zip=C:\\exports\\claude.zip --source-instance=claude-personal --base-url=http://127.0.0.1:8080 --api-token-env=LETHE_API_WRITE_TOKEN
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
    let bytes = fs::read(&options.zip_path)?;
    let importer = ClaudeAiImporter::new(SemVer::new("1.0.0"));
    let drafts = importer.import_zip(&bytes)?;
    let report = ImportApiConfig {
        base_url: options.base_url,
        api_token_env: options.api_token_env,
        api_version: options.api_version,
        admission_generation: options.admission_generation,
    }
    .connect()?
    .ingest_observation_drafts(drafts, &options.source_instance)?;

    println!(
        "claude import complete: ingested={}, duplicates={}, quarantined={}",
        report.ingested, report.duplicates, report.quarantined
    );
    Ok(())
}

fn help_requested(args: &[String]) -> bool {
    args.iter().any(|arg| arg == "--help" || arg == "-h")
}

struct CliOptions {
    zip_path: PathBuf,
    source_instance: String,
    base_url: String,
    api_token_env: String,
    api_version: ImportApiVersion,
    admission_generation: Option<u64>,
}

fn parse_options(
    args: impl Iterator<Item = String>,
) -> Result<CliOptions, Box<dyn std::error::Error>> {
    let mut zip_path = None;
    let mut source_instance = None;
    let mut base_url = None;
    let mut api_token_env = None;
    let mut api_version = None;
    let mut admission_generation = None;

    for arg in normalize_import_option_args(args)? {
        if let Some(raw) = arg.strip_prefix("--zip=") {
            zip_path = Some(PathBuf::from(raw));
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
        } else if let Some(raw) = arg.strip_prefix("--api-version=") {
            if raw.trim().is_empty() {
                return Err("--api-version must not be blank. Pass --api-version=1 or 2.".into());
            }
            api_version = Some(raw.to_owned());
        } else if let Some(raw) = arg.strip_prefix("--admission-generation=") {
            if raw.trim().is_empty() {
                return Err(
                    "--admission-generation must not be blank. Pass --admission-generation=<int>."
                        .into(),
                );
            }
            admission_generation = Some(raw.to_owned());
        } else {
            return Err(format!("unknown argument: {arg}. Run with --help for usage.").into());
        }
    }

    let api_version = resolve_api_version(api_version.as_deref())?;
    let admission_generation = resolve_admission_generation(admission_generation.as_deref())?;
    Ok(CliOptions {
        zip_path: zip_path.ok_or_else(|| {
            missing_argument(
                "--zip=<path>",
                "Pass --zip=C:\\path\\to\\claude-export.zip.",
            )
        })?,
        source_instance: source_instance.ok_or_else(|| {
            missing_argument(
                "--source-instance=<id>",
                "Pass --source-instance=claude-personal.",
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
        api_version,
        admission_generation,
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
        assert!(HELP.contains("Import a claude.ai"));
        assert!(HELP.contains("--zip=<path>"));
        assert!(HELP.contains("--api-token-env=<name>"));
    }
}
