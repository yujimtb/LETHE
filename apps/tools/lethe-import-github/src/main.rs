use std::env;
use std::fs;
use std::path::PathBuf;

use lethe_adapter_github::github::mapper::GitHubDumpMapper;
use lethe_core::domain::SemVer;
use lethe_selfhost::self_host::app::AppService;
use lethe_selfhost::self_host::config::SelfHostConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = parse_options(env::args().skip(1))?;
    let json = fs::read_to_string(&options.dump_path)?;
    let mapper = GitHubDumpMapper::new(SemVer::new("1.0.0"));
    let drafts = mapper.import_json_str(&json)?;
    let config = SelfHostConfig::from_env()?;
    let service = AppService::bootstrap(config)?;
    let report = service.ingest_observation_drafts(drafts, &options.source_instance)?;

    println!(
        "github import complete: ingested={}, duplicates={}, quarantined={}",
        report.ingested, report.duplicates, report.quarantined
    );
    Ok(())
}

struct CliOptions {
    dump_path: PathBuf,
    source_instance: String,
}

fn parse_options(
    args: impl Iterator<Item = String>,
) -> Result<CliOptions, Box<dyn std::error::Error>> {
    let mut dump_path = None;
    let mut source_instance = None;

    for arg in args {
        if let Some(raw) = arg.strip_prefix("--dump=") {
            dump_path = Some(PathBuf::from(raw));
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
        dump_path: dump_path.ok_or("--dump=<path> is required")?,
        source_instance: source_instance.ok_or("--source-instance=<id> is required")?,
    })
}
