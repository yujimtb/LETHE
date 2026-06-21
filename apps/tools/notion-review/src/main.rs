use std::env;

use lethe_selfhost::self_host::app::AppService;
use lethe_selfhost::self_host::config::SelfHostConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = parse_options(env::args().skip(1))?;
    let config = SelfHostConfig::from_env()?;
    let service = AppService::bootstrap(config)?;

    let report = service.notion_review_sync(options.limit, options.refresh_data)?;

    println!(
        "Synced {} Notion page(s) after cleaning up {} existing page(s).",
        report.notion_synced, report.cleaned_up
    );
    if options.refresh_data {
        println!(
            "Data sync: slack_ingested={}, google_ingested={}, slide_analyses={}, duplicates={}",
            report.sync_report.slack_ingested,
            report.sync_report.google_ingested,
            report.sync_report.slide_analyses,
            report.sync_report.duplicates
        );
    } else {
        println!("Data sync skipped; used persisted local snapshot.");
    }

    for candidate in &report.candidates {
        println!(
            "[candidate #{rank}] {title} | entity={entity} | person={person} | last_activity={last_activity:?} | slide={slide}",
            rank = candidate.rank,
            title = candidate.title,
            entity = candidate.entity_id,
            person = candidate.person_id,
            last_activity = candidate.last_activity,
            slide = candidate.source_document_id,
        );
    }

    for write in &report.writes {
        println!(
            "[write #{rank}] {title} | cleaned_existing={cleaned} | external_id={external_id} | url={url}",
            rank = write.rank,
            title = write.title,
            cleaned = write.cleaned_existing_page,
            external_id = write.external_id,
            url = write.url.as_deref().unwrap_or(""),
        );
    }

    Ok(())
}

struct CliOptions {
    limit: usize,
    refresh_data: bool,
}

fn parse_options(
    args: impl Iterator<Item = String>,
) -> Result<CliOptions, Box<dyn std::error::Error>> {
    let mut limit = 3usize;
    let mut refresh_data = false;
    for arg in args {
        if let Some(raw) = arg.strip_prefix("--limit=") {
            limit = raw.parse::<usize>()?;
        } else if arg == "--refresh-data" {
            refresh_data = true;
        }
    }
    Ok(CliOptions {
        limit: limit.max(1),
        refresh_data,
    })
}
