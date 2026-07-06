use std::env;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use lethe_adapter_chatgpt::{ChatGptImportFilter, ChatGptImporter};
use lethe_core::domain::SemVer;
use lethe_selfhost::self_host::import_client::ImportApiConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = parse_options(env::args().skip(1))?;
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

struct CliOptions {
    archive_root: PathBuf,
    source_instance: String,
    base_url: String,
    api_token_env: String,
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
    let mut filter = ChatGptImportFilter::default();
    let mut json = false;

    for arg in args {
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
            return Err(format!("unknown argument: {arg}").into());
        }
    }
    Ok(CliOptions {
        archive_root: archive_root.ok_or("--archive-root=<path> is required")?,
        source_instance: source_instance.ok_or("--source-instance=<id> is required")?,
        base_url: base_url.ok_or("--base-url=<url> is required")?,
        api_token_env: api_token_env.ok_or("--api-token-env=<name> is required")?,
        filter,
        json,
    })
}

fn parse_rfc3339(name: &str, raw: &str) -> Result<DateTime<Utc>, Box<dyn std::error::Error>> {
    require_non_blank(name, raw)?;
    Ok(DateTime::parse_from_rfc3339(raw)?.to_utc())
}

fn require_non_blank(name: &str, raw: &str) -> Result<(), Box<dyn std::error::Error>> {
    if raw.trim().is_empty() {
        Err(format!("{name} must not be blank").into())
    } else {
        Ok(())
    }
}
