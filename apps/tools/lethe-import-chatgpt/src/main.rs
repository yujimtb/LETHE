use std::env;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use lethe_adapter_chatgpt::{ChatGptImportFilter, ChatGptImporter};
use lethe_core::domain::SemVer;
use lethe_selfhost::self_host::import_client::{
    ImportApiConfig, ImportApiVersion, normalize_import_option_args, resolve_admission_generation,
    resolve_api_version,
};

const HELP: &str = "\
Import ChatGPT export files into LETHE through the online import API.

Usage: lethe-import-chatgpt --archive-root=<path> --source-instance=<id> --base-url=<url> --api-token-env=<name> [--api-version=<1|2>] [--admission-generation=<int>] [--backfill] [--from=<rfc3339>] [--to=<rfc3339>] [--conversation-id=<id>] [--json]

Required arguments:
  --archive-root=<path>     Archive working copy containing chatgpt/ JSON files
  --source-instance=<id>    Stable source instance id, for example chatgpt-personal
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
  lethe-import-chatgpt --archive-root=D:\\archive --source-instance=chatgpt-personal --base-url=http://127.0.0.1:8080 --api-token-env=LETHE_API_WRITE_TOKEN --backfill
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
    let importer = ChatGptImporter::new(SemVer::new("1.0.0"));
    let batch = importer.import_archive_root(&options.archive_root, &options.filter)?;

    for skipped in &batch.audit.skipped_records {
        eprintln!(
            "chatgpt import audit: source={}, conversation={:?}, message={:?}, reason={}",
            skipped.path, skipped.conversation_id, skipped.message_id, skipped.reason
        );
    }

    let report = ImportApiConfig {
        base_url: options.base_url,
        api_token_env: options.api_token_env,
        api_version: options.api_version,
        admission_generation: options.admission_generation,
    }
    .connect()?
    .ingest_observation_drafts(batch.drafts, &options.source_instance)?;
    let quarantined = report.quarantined + batch.audit.skipped_records.len();

    if options.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "kind": "chatgpt",
                "ingested": report.ingested,
                "duplicates": report.duplicates,
                "quarantined": quarantined,
                "files": batch.audit.files_read,
                "conversations": batch.audit.conversations_read,
                "messages_seen": batch.audit.messages_seen,
                "skipped_records": batch.audit.skipped_records.len(),
                "backfill": options.filter.backfill
            }))?
        );
    } else {
        println!(
            "chatgpt import complete: ingested={}, duplicates={}, quarantined={}, files={}, conversations={}, messages_seen={}, skipped_records={}, backfill={}",
            report.ingested,
            report.duplicates,
            quarantined,
            batch.audit.files_read,
            batch.audit.conversations_read,
            batch.audit.messages_seen,
            batch.audit.skipped_records.len(),
            options.filter.backfill
        );
    }
    Ok(())
}

fn help_requested(args: &[String]) -> bool {
    args.iter().any(|arg| arg == "--help" || arg == "-h")
}

struct CliOptions {
    archive_root: PathBuf,
    source_instance: String,
    base_url: String,
    api_token_env: String,
    api_version: ImportApiVersion,
    admission_generation: Option<u64>,
    filter: ChatGptImportFilter,
    json: bool,
}

fn parse_options(
    args: impl Iterator<Item = String>,
) -> Result<CliOptions, Box<dyn std::error::Error>> {
    let mut archive_root = None;
    let mut source_instance = None;
    let mut base_url = None;
    let mut api_token_env = None;
    let mut api_version = None;
    let mut admission_generation = None;
    let mut filter = ChatGptImportFilter::default();
    let mut json = false;

    for arg in normalize_import_option_args(args)? {
        if let Some(raw) = arg.strip_prefix("--archive-root=") {
            archive_root = Some(PathBuf::from(raw));
        } else if let Some(raw) = arg.strip_prefix("--source-instance=") {
            require_non_blank("--source-instance", raw)?;
            source_instance = Some(raw.to_owned());
        } else if let Some(raw) = arg.strip_prefix("--base-url=") {
            require_non_blank("--base-url", raw)?;
            base_url = Some(raw.to_owned());
        } else if let Some(raw) = arg.strip_prefix("--api-token-env=") {
            require_non_blank("--api-token-env", raw)?;
            api_token_env = Some(raw.to_owned());
        } else if let Some(raw) = arg.strip_prefix("--api-version=") {
            require_non_blank("--api-version", raw)?;
            api_version = Some(raw.to_owned());
        } else if let Some(raw) = arg.strip_prefix("--admission-generation=") {
            require_non_blank("--admission-generation", raw)?;
            admission_generation = Some(raw.to_owned());
        } else if let Some(raw) = arg.strip_prefix("--from=") {
            filter.from = Some(parse_rfc3339("--from", raw)?);
        } else if let Some(raw) = arg.strip_prefix("--to=") {
            filter.to = Some(parse_rfc3339("--to", raw)?);
        } else if let Some(raw) = arg.strip_prefix("--conversation-id=") {
            require_non_blank("--conversation-id", raw)?;
            filter.conversation_ids.insert(raw.to_owned());
        } else if arg == "--backfill" {
            filter.backfill = true;
        } else if arg == "--json" {
            json = true;
        } else {
            return Err(format!("unknown argument: {arg}. Run with --help for usage.").into());
        }
    }
    let api_version = resolve_api_version(api_version.as_deref())?;
    let admission_generation = resolve_admission_generation(admission_generation.as_deref())?;
    Ok(CliOptions {
        archive_root: archive_root.ok_or_else(|| {
            missing_argument(
                "--archive-root=<path>",
                "Pass --archive-root=D:\\path\\to\\archive.",
            )
        })?,
        source_instance: source_instance.ok_or_else(|| {
            missing_argument(
                "--source-instance=<id>",
                "Pass --source-instance=chatgpt-personal.",
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
        filter,
        json,
    })
}

fn parse_rfc3339(name: &str, raw: &str) -> Result<DateTime<Utc>, Box<dyn std::error::Error>> {
    require_non_blank(name, raw)?;
    Ok(DateTime::parse_from_rfc3339(raw)
        .map_err(|error| {
            format!(
                "{name} must be an RFC3339 timestamp, for example 2026-07-01T00:00:00Z: {error}"
            )
        })?
        .to_utc())
}

fn require_non_blank(name: &str, raw: &str) -> Result<(), Box<dyn std::error::Error>> {
    if raw.trim().is_empty() {
        Err(format!("{name} must not be blank. Pass {name}=<value>.").into())
    } else {
        Ok(())
    }
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
        assert!(HELP.contains("Import ChatGPT"));
        assert!(HELP.contains("--archive-root=<path>"));
        assert!(HELP.contains("--api-token-env=<name>"));
    }
}
