use std::env;
use std::io::{self, Write};
use std::path::PathBuf;

use lethe::attribute_inventory::{
    AttributeInventoryState, InventoryCommand, parse_inventory_command, read_alias_catalog,
    summarize_documents, write_json_file,
};
use lethe::self_host::app::AppService;
use lethe::self_host::config::SelfHostConfig;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = parse_options(env::args().skip(1))?;
    let config = SelfHostConfig::from_env()?;
    let service = AppService::bootstrap(config)?;

    if options.refresh_data {
        let report = service.sync_without_notion_writeback()?;
        println!(
            "Data sync refreshed: slack_ingested={}, google_ingested={}, slide_analyses={}, duplicates={}",
            report.slack_ingested, report.google_ingested, report.slide_analyses, report.duplicates
        );
    } else {
        println!("Using persisted local snapshot.");
    }

    let documents = service.attribute_inventory_documents()?;
    let selected_documents = documents
        .into_iter()
        .skip(options.offset)
        .take(options.limit.unwrap_or(usize::MAX))
        .collect::<Vec<_>>();
    let mut state = AttributeInventoryState::load_or_default(&options.state_path)?;

    if let Some(path) = &options.import_aliases_path {
        let catalog = read_alias_catalog(path)?;
        state.merge_alias_catalog(catalog);
        state.save(&options.state_path)?;
        println!("Imported alias catalog: {}", path.display());
    }

    let total_candidates = selected_documents
        .iter()
        .map(|document| document.candidates.len())
        .sum::<usize>();
    let mut pending = selected_documents
        .iter()
        .flat_map(|document| document.candidates.iter())
        .filter(|candidate| !state.is_reviewed(candidate))
        .count();

    println!(
        "Loaded {} unique label(s), {} pending review(s).",
        total_candidates, pending
    );
    println!("State file: {}", options.state_path.display());

    if let Some(path) = &options.export_discovered_path {
        let discovered = summarize_documents(&selected_documents);
        write_json_file(path, &discovered)?;
        println!("Exported discovered attribute groups: {}", path.display());
    }

    if let Some(path) = &options.export_aliases_path {
        let catalog = state.export_alias_catalog();
        write_json_file(path, &catalog)?;
        println!("Exported alias catalog: {}", path.display());
    }

    if (options.export_discovered_path.is_some()
        || options.export_aliases_path.is_some()
        || options.import_aliases_path.is_some())
        && !options.interactive
    {
        return Ok(());
    }

    let mut reviewed_this_run = 0usize;

    'documents: for (document_index, document) in selected_documents.iter().enumerate() {
        let pending_in_document = document
            .candidates
            .iter()
            .filter(|candidate| !state.is_reviewed(candidate))
            .count();
        if pending_in_document == 0 {
            continue;
        }

        println!();
        println!(
            "=== Document {}/{} ===",
            document_index + 1,
            selected_documents.len()
        );
        println!("Name: {}", document.display_name);
        println!("Person ID: {}", document.person_id);
        if let Some(source_document_id) = &document.source_document_id {
            println!("Source document: {}", source_document_id);
        }
        if let Some(source_canonical_uri) = &document.source_canonical_uri {
            println!("Source URL: {}", source_canonical_uri);
        }
        println!("Pending in document: {}", pending_in_document);

        for candidate in &document.candidates {
            if state.is_reviewed(candidate) {
                continue;
            }

            loop {
                let suggestions = state.suggestions_for(candidate);
                println!();
                println!("Label [{} pending]", pending);
                println!("  Label:    {}", candidate.value);
                println!(
                    "  Students: {} ({})",
                    candidate.student_count,
                    candidate
                        .student_names
                        .iter()
                        .take(5)
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                if let Some(ai_key) = &candidate.ai_property_key {
                    println!("  AI key:   {}", ai_key);
                }
                if !candidate.example_values.is_empty() {
                    println!(
                        "  Examples: {}",
                        candidate
                            .example_values
                            .iter()
                            .take(3)
                            .cloned()
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                }
                if suggestions.is_empty() {
                    println!("  Suggestions: none");
                } else {
                    println!("  Suggestions:");
                    for (index, suggestion) in suggestions.iter().enumerate() {
                        let paths = if suggestion.source_paths.is_empty() {
                            String::new()
                        } else {
                            format!(" [{}]", suggestion.source_paths.join(", "))
                        };
                        println!(
                            "    {}. {} ({}) score={}{}",
                            index + 1,
                            suggestion.display_name,
                            suggestion.id,
                            suggestion.score,
                            paths
                        );
                    }
                }
                println!("  Commands: <number> | e <attribute-id> | n <new name> | s | q | ?");
                print!("review> ");
                io::stdout().flush()?;

                let mut input = String::new();
                io::stdin().read_line(&mut input)?;
                match parse_inventory_command(&input) {
                    Ok(InventoryCommand::Help) => {
                        println!("  <number> maps to a suggestion.");
                        println!("  e <attribute-id> maps to an existing known attribute.");
                        println!("  n <new name> creates a new attribute and maps this value.");
                        println!("  s skips this candidate for now.");
                        println!("  q saves and exits.");
                    }
                    Ok(InventoryCommand::PickSuggestion(index)) => {
                        let Some(suggestion) = suggestions.get(index - 1) else {
                            println!("No suggestion #{index}.");
                            continue;
                        };
                        match state.review_existing(candidate, &suggestion.id) {
                            Ok(()) => {
                                state.save(&options.state_path)?;
                                reviewed_this_run += 1;
                                pending = pending.saturating_sub(1);
                                break;
                            }
                            Err(message) => {
                                println!("{message}");
                            }
                        }
                    }
                    Ok(InventoryCommand::MapExisting(attribute_id)) => {
                        match state.review_existing(candidate, &attribute_id) {
                            Ok(()) => {
                                state.save(&options.state_path)?;
                                reviewed_this_run += 1;
                                pending = pending.saturating_sub(1);
                                break;
                            }
                            Err(message) => {
                                println!("{message}");
                            }
                        }
                    }
                    Ok(InventoryCommand::CreateNew(display_name)) => {
                        match state.review_new(candidate, &display_name) {
                            Ok(attribute_id) => {
                                println!("Created attribute: {} ({})", display_name, attribute_id);
                                state.save(&options.state_path)?;
                                reviewed_this_run += 1;
                                pending = pending.saturating_sub(1);
                                break;
                            }
                            Err(message) => {
                                println!("{message}");
                            }
                        }
                    }
                    Ok(InventoryCommand::Skip) => {
                        state.review_skip(candidate);
                        state.save(&options.state_path)?;
                        reviewed_this_run += 1;
                        pending = pending.saturating_sub(1);
                        break;
                    }
                    Ok(InventoryCommand::Quit) => {
                        state.save(&options.state_path)?;
                        break 'documents;
                    }
                    Err(message) => {
                        println!("{message}");
                    }
                }
            }
        }
    }

    println!();
    println!(
        "Review session complete. Reviewed {} candidate(s) this run. Known attributes: {}. Remaining pending: {}.",
        reviewed_this_run,
        state.known_attributes.len(),
        pending
    );
    Ok(())
}

struct CliOptions {
    refresh_data: bool,
    state_path: PathBuf,
    offset: usize,
    limit: Option<usize>,
    export_discovered_path: Option<PathBuf>,
    export_aliases_path: Option<PathBuf>,
    import_aliases_path: Option<PathBuf>,
    interactive: bool,
}

fn parse_options(
    args: impl Iterator<Item = String>,
) -> Result<CliOptions, Box<dyn std::error::Error>> {
    let mut refresh_data = false;
    let mut state_path = PathBuf::from(".\\data\\attribute_inventory_state.json");
    let mut offset = 0usize;
    let mut limit = None;
    let mut export_discovered_path = None;
    let mut export_aliases_path = None;
    let mut import_aliases_path = None;
    let mut interactive = false;

    for arg in args {
        if arg == "--refresh-data" {
            refresh_data = true;
        } else if arg == "--interactive" {
            interactive = true;
        } else if let Some(raw) = arg.strip_prefix("--state=") {
            state_path = PathBuf::from(raw);
        } else if let Some(raw) = arg.strip_prefix("--offset=") {
            offset = raw.parse::<usize>()?;
        } else if let Some(raw) = arg.strip_prefix("--limit=") {
            limit = Some(raw.parse::<usize>()?);
        } else if let Some(raw) = arg.strip_prefix("--export-discovered=") {
            export_discovered_path = Some(PathBuf::from(raw));
        } else if let Some(raw) = arg.strip_prefix("--export-aliases=") {
            export_aliases_path = Some(PathBuf::from(raw));
        } else if let Some(raw) = arg.strip_prefix("--import-aliases=") {
            import_aliases_path = Some(PathBuf::from(raw));
        }
    }

    Ok(CliOptions {
        refresh_data,
        state_path,
        offset,
        limit,
        export_discovered_path,
        export_aliases_path,
        import_aliases_path,
        interactive,
    })
}
