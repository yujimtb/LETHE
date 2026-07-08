use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use regex::Regex;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub const HELP: &str = "\
Extract and verify OpenSpec requirement coverage evidence.

Usage: lethe-review-harness <extract|generate|verify|diff> [options]

Required arguments:
  extract  --spec-root=<path>
  generate --spec-root=<path> --evidence-root=<path> --tasks-root=<path>
  verify   --spec-root=<path> --evidence-root=<path> --tasks-root=<path>
  diff     --base=<path> --head=<path>

Required environment:
  none

Example:
  lethe-review-harness verify --spec-root=openspec\\specs --evidence-root=. --tasks-root=openspec\\changes
";

#[derive(Debug, Error)]
pub enum HarnessError {
    #[error("failed to access {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: io::Error,
    },
    #[error("failed to parse JSON from {path}: {source}")]
    JsonRead {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("failed to serialize JSON: {0}")]
    JsonWrite(#[from] serde_json::Error),
    #[error("invalid command: {0}. Run with --help for usage")]
    InvalidCommand(String),
    #[error("missing required argument: {0}. Run with --help for usage")]
    MissingArgument(String),
    #[error("missing requirement ID at {source_path}:{line}: {text}")]
    MissingRequirementId {
        source_path: String,
        line: usize,
        text: String,
    },
    #[error("invalid requirement ID `{id}` at {source_path}:{line}")]
    InvalidRequirementId {
        source_path: String,
        line: usize,
        id: String,
    },
    #[error("invalid evidence ID `{id}` at {source_path}:{line}")]
    InvalidEvidenceId {
        source_path: String,
        line: usize,
        id: String,
    },
    #[error("unknown evidence reference(s): {0}")]
    UnknownEvidenceReferences(String),
    #[error("uncovered requirement(s): {0}")]
    UncoveredRequirements(String),
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Requirement {
    pub id: String,
    pub source: String,
    pub line: usize,
    pub text: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceKind {
    Automated,
    Manual,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct Evidence {
    pub requirement_id: String,
    pub kind: EvidenceKind,
    pub source: String,
    pub line: usize,
    pub detail: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CoverageRow {
    pub requirement_id: String,
    pub judgement: CoverageJudgement,
    pub evidence: Vec<Evidence>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoverageJudgement {
    Covered,
    Uncovered,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CoverageMatrix {
    pub requirements: Vec<Requirement>,
    pub evidence: Vec<Evidence>,
    pub rows: Vec<CoverageRow>,
}

#[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct EvidenceKey {
    pub requirement_id: String,
    pub kind: EvidenceKind,
    pub source: String,
    pub line: usize,
    pub detail: String,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct CoverageDiff {
    pub has_diff: bool,
    pub message: String,
    pub new_requirements: Vec<String>,
    pub new_evidence: Vec<EvidenceKey>,
    pub lost_evidence: Vec<EvidenceKey>,
}

#[derive(Debug, Serialize)]
struct ExtractionReport {
    requirements: Vec<Requirement>,
}

#[derive(Clone)]
struct RequirementContext {
    id: Option<String>,
    invalid_id: Option<(String, usize)>,
}

pub fn run_cli(args: impl IntoIterator<Item = String>) -> Result<String, HarnessError> {
    let args = args.into_iter().collect::<Vec<_>>();
    let Some(command) = args.first() else {
        return Err(HarnessError::MissingArgument("command".to_string()));
    };
    if command == "--help" || command == "-h" {
        return Ok(HELP.to_owned());
    }
    let options = parse_options(&args[1..])?;

    match command.as_str() {
        "extract" => {
            let spec_root = required_path(&options, "--spec-root")?;
            let requirements = extract_requirements(&spec_root)?;
            to_json(&ExtractionReport { requirements })
        }
        "generate" => {
            let spec_root = required_path(&options, "--spec-root")?;
            let evidence_root = required_path(&options, "--evidence-root")?;
            let tasks_root = required_path(&options, "--tasks-root")?;
            let matrix = generate_matrix(&spec_root, &evidence_root, &tasks_root)?;
            to_json(&matrix)
        }
        "verify" => {
            let spec_root = required_path(&options, "--spec-root")?;
            let evidence_root = required_path(&options, "--evidence-root")?;
            let tasks_root = required_path(&options, "--tasks-root")?;
            let matrix = verify_matrix(&spec_root, &evidence_root, &tasks_root)?;
            to_json(&matrix)
        }
        "diff" => {
            let base = read_matrix(&required_path(&options, "--base")?)?;
            let head = read_matrix(&required_path(&options, "--head")?)?;
            to_json(&diff_matrices(&base, &head))
        }
        unknown => Err(HarnessError::InvalidCommand(unknown.to_string())),
    }
}

pub fn extract_requirements(spec_root: &Path) -> Result<Vec<Requirement>, HarnessError> {
    let mut requirements = BTreeMap::<String, Requirement>::new();
    for path in collect_files(spec_root, is_markdown_file, &[])? {
        parse_spec_file(&path, &mut requirements)?;
    }
    Ok(requirements.into_values().collect())
}

pub fn collect_automated_evidence(root: &Path) -> Result<Vec<Evidence>, HarnessError> {
    let mut evidence = Vec::new();
    for path in collect_files(root, is_test_code_candidate, &[".git", "data", "target"])? {
        let source = display_path(&path);
        let content = read_to_string(&path)?;
        for (index, line) in content.lines().enumerate() {
            if !is_comment_line(line) {
                continue;
            }
            evidence.extend(parse_evidence_line(
                line,
                "covers:",
                EvidenceKind::Automated,
                &source,
                index + 1,
            )?);
        }
    }
    evidence.sort();
    Ok(evidence)
}

pub fn collect_manual_evidence(root: &Path) -> Result<Vec<Evidence>, HarnessError> {
    let mut evidence = Vec::new();
    for path in collect_files(root, is_tasks_file, &[".git", "data", "target"])? {
        let source = display_path(&path);
        let content = read_to_string(&path)?;
        for (index, line) in content.lines().enumerate() {
            evidence.extend(parse_evidence_line(
                line,
                "manual evidence:",
                EvidenceKind::Manual,
                &source,
                index + 1,
            )?);
        }
    }
    evidence.sort();
    Ok(evidence)
}

pub fn generate_matrix(
    spec_root: &Path,
    evidence_root: &Path,
    tasks_root: &Path,
) -> Result<CoverageMatrix, HarnessError> {
    let requirements = extract_requirements(spec_root)?;
    let mut evidence = collect_automated_evidence(evidence_root)?;
    evidence.extend(collect_manual_evidence(tasks_root)?);
    evidence.sort();

    validate_evidence_references(&requirements, &evidence)?;

    let mut evidence_by_requirement = BTreeMap::<String, Vec<Evidence>>::new();
    for item in &evidence {
        evidence_by_requirement
            .entry(item.requirement_id.clone())
            .or_default()
            .push(item.clone());
    }

    let rows = requirements
        .iter()
        .map(|requirement| {
            let evidence = evidence_by_requirement
                .remove(&requirement.id)
                .unwrap_or_default();
            let judgement = if evidence.is_empty() {
                CoverageJudgement::Uncovered
            } else {
                CoverageJudgement::Covered
            };
            CoverageRow {
                requirement_id: requirement.id.clone(),
                judgement,
                evidence,
            }
        })
        .collect::<Vec<_>>();

    Ok(CoverageMatrix {
        requirements,
        evidence,
        rows,
    })
}

pub fn verify_matrix(
    spec_root: &Path,
    evidence_root: &Path,
    tasks_root: &Path,
) -> Result<CoverageMatrix, HarnessError> {
    let matrix = generate_matrix(spec_root, evidence_root, tasks_root)?;
    let uncovered = matrix
        .rows
        .iter()
        .filter(|row| row.judgement == CoverageJudgement::Uncovered)
        .map(|row| row.requirement_id.clone())
        .collect::<Vec<_>>();
    if uncovered.is_empty() {
        Ok(matrix)
    } else {
        Err(HarnessError::UncoveredRequirements(uncovered.join(", ")))
    }
}

pub fn diff_matrices(base: &CoverageMatrix, head: &CoverageMatrix) -> CoverageDiff {
    let base_requirements = base
        .requirements
        .iter()
        .map(|requirement| requirement.id.clone())
        .collect::<BTreeSet<_>>();
    let head_requirements = head
        .requirements
        .iter()
        .map(|requirement| requirement.id.clone())
        .collect::<BTreeSet<_>>();
    let base_evidence = evidence_keys(base).collect::<BTreeSet<_>>();
    let head_evidence = evidence_keys(head).collect::<BTreeSet<_>>();

    let new_requirements = head_requirements
        .difference(&base_requirements)
        .cloned()
        .collect::<Vec<_>>();
    let new_evidence = head_evidence
        .difference(&base_evidence)
        .cloned()
        .collect::<Vec<_>>();
    let lost_evidence = base_evidence
        .difference(&head_evidence)
        .cloned()
        .collect::<Vec<_>>();
    let has_diff =
        !new_requirements.is_empty() || !new_evidence.is_empty() || !lost_evidence.is_empty();
    let message = if has_diff {
        "coverage diff exists".to_string()
    } else {
        "no coverage diff exists".to_string()
    };

    CoverageDiff {
        has_diff,
        message,
        new_requirements,
        new_evidence,
        lost_evidence,
    }
}

fn parse_spec_file(
    path: &Path,
    requirements: &mut BTreeMap<String, Requirement>,
) -> Result<(), HarnessError> {
    let source = display_path(path);
    let content = read_to_string(path)?;
    let mut context = RequirementContext {
        id: None,
        invalid_id: None,
    };
    let mut in_scenario = false;

    for (index, line) in content.lines().enumerate() {
        let line_number = index + 1;
        if let Some(title) = line.trim_start().strip_prefix("### Requirement:") {
            context = parse_requirement_context(title, line_number);
            in_scenario = false;
            continue;
        }
        if line.trim_start().starts_with("#### Scenario:") {
            in_scenario = true;
            continue;
        }
        if in_scenario {
            continue;
        }

        if !line.contains("SHALL") {
            continue;
        }

        let ids = valid_ids(line);
        if ids.is_empty() {
            if let Some((invalid_id, invalid_line)) = context.invalid_id.clone() {
                return Err(HarnessError::InvalidRequirementId {
                    source_path: source,
                    line: invalid_line,
                    id: invalid_id,
                });
            }
            if let Some(invalid_id) = first_malformed_id(line) {
                return Err(HarnessError::InvalidRequirementId {
                    source_path: source,
                    line: line_number,
                    id: invalid_id,
                });
            }
            let Some(id) = context.id.clone() else {
                return Err(HarnessError::MissingRequirementId {
                    source_path: source,
                    line: line_number,
                    text: line.trim().to_string(),
                });
            };
            upsert_requirement(requirements, id, &source, line_number, line);
        } else {
            if let Some(invalid_id) = first_malformed_id(line) {
                return Err(HarnessError::InvalidRequirementId {
                    source_path: source,
                    line: line_number,
                    id: invalid_id,
                });
            }
            for id in ids {
                upsert_requirement(requirements, id, &source, line_number, line);
            }
        }
    }

    Ok(())
}

fn parse_requirement_context(title: &str, line_number: usize) -> RequirementContext {
    let ids = valid_ids(title);
    if let Some(id) = ids.into_iter().next() {
        return RequirementContext {
            id: Some(id),
            invalid_id: None,
        };
    }
    RequirementContext {
        id: None,
        invalid_id: first_malformed_id(title).map(|id| (id, line_number)),
    }
}

fn upsert_requirement(
    requirements: &mut BTreeMap<String, Requirement>,
    id: String,
    source: &str,
    line: usize,
    text: &str,
) {
    let normalized = text.trim().to_string();
    requirements
        .entry(id.clone())
        .and_modify(|requirement| {
            if !requirement.text.contains(&normalized) {
                requirement.text.push(' ');
                requirement.text.push_str(&normalized);
            }
        })
        .or_insert_with(|| Requirement {
            id,
            source: source.to_string(),
            line,
            text: normalized,
        });
}

fn parse_evidence_line(
    line: &str,
    marker: &str,
    kind: EvidenceKind,
    source: &str,
    line_number: usize,
) -> Result<Vec<Evidence>, HarnessError> {
    if !line.contains(marker) {
        return Ok(Vec::new());
    }
    let Some(marker_index) = line.find(marker) else {
        return Ok(Vec::new());
    };
    let tail = &line[marker_index + marker.len()..];
    let ids = valid_ids(tail);
    if ids.is_empty() {
        let invalid_id = tail
            .split(|character: char| {
                character.is_whitespace() || character == ',' || character == ';'
            })
            .find(|token| !token.is_empty())
            .unwrap_or("")
            .to_string();
        return Err(HarnessError::InvalidEvidenceId {
            source_path: source.to_string(),
            line: line_number,
            id: invalid_id,
        });
    }

    Ok(ids
        .into_iter()
        .map(|requirement_id| Evidence {
            requirement_id,
            kind: kind.clone(),
            source: source.to_string(),
            line: line_number,
            detail: line.trim().to_string(),
        })
        .collect())
}

fn validate_evidence_references(
    requirements: &[Requirement],
    evidence: &[Evidence],
) -> Result<(), HarnessError> {
    let known = requirements
        .iter()
        .map(|requirement| requirement.id.as_str())
        .collect::<BTreeSet<_>>();
    let unknown = evidence
        .iter()
        .filter(|item| !known.contains(item.requirement_id.as_str()))
        .map(|item| format!("{} at {}:{}", item.requirement_id, item.source, item.line))
        .collect::<Vec<_>>();
    if unknown.is_empty() {
        Ok(())
    } else {
        Err(HarnessError::UnknownEvidenceReferences(unknown.join(", ")))
    }
}

fn read_matrix(path: &Path) -> Result<CoverageMatrix, HarnessError> {
    let content = read_to_string(path)?;
    serde_json::from_str(&content).map_err(|source| HarnessError::JsonRead {
        path: display_path(path),
        source,
    })
}

fn evidence_keys(matrix: &CoverageMatrix) -> impl Iterator<Item = EvidenceKey> + '_ {
    matrix.evidence.iter().map(|evidence| EvidenceKey {
        requirement_id: evidence.requirement_id.clone(),
        kind: evidence.kind.clone(),
        source: evidence.source.clone(),
        line: evidence.line,
        detail: evidence.detail.clone(),
    })
}

fn to_json(value: &impl Serialize) -> Result<String, HarnessError> {
    let mut output = serde_json::to_string_pretty(value)?;
    output.push('\n');
    Ok(output)
}

fn parse_options(args: &[String]) -> Result<BTreeMap<String, PathBuf>, HarnessError> {
    let mut options = BTreeMap::new();
    let mut index = 0usize;
    while index < args.len() {
        let arg = &args[index];
        if !arg.starts_with("--") {
            return Err(HarnessError::InvalidCommand(format!(
                "unexpected positional argument `{arg}`"
            )));
        }
        if let Some((key, value)) = arg.split_once('=') {
            insert_option(&mut options, key, value)?;
            index += 1;
        } else {
            let key = arg.as_str();
            let Some(value) = args.get(index + 1) else {
                return Err(HarnessError::MissingArgument(option_name(key).to_string()));
            };
            if value.starts_with("--") {
                return Err(HarnessError::MissingArgument(option_name(key).to_string()));
            }
            insert_option(&mut options, key, value)?;
            index += 2;
        }
    }
    Ok(options)
}

fn insert_option(
    options: &mut BTreeMap<String, PathBuf>,
    key: &str,
    value: &str,
) -> Result<(), HarnessError> {
    if options
        .insert(key.to_string(), PathBuf::from(value))
        .is_some()
    {
        return Err(HarnessError::InvalidCommand(format!(
            "duplicate argument `{key}`"
        )));
    }
    Ok(())
}

fn required_path(
    options: &BTreeMap<String, PathBuf>,
    key: &'static str,
) -> Result<PathBuf, HarnessError> {
    options
        .get(key)
        .cloned()
        .ok_or(HarnessError::MissingArgument(option_name(key).to_string()))
}

fn option_name(key: &str) -> &str {
    key.strip_prefix("--").unwrap_or(key)
}

fn collect_files(
    root: &Path,
    matcher: fn(&Path) -> bool,
    skip_dirs: &[&str],
) -> Result<Vec<PathBuf>, HarnessError> {
    let metadata = fs::metadata(root).map_err(|source| HarnessError::Io {
        path: display_path(root),
        source,
    })?;
    let mut files = Vec::new();
    if metadata.is_file() {
        if matcher(root) {
            files.push(root.to_path_buf());
        }
    } else {
        visit_dir(root, matcher, skip_dirs, &mut files)?;
    }
    files.sort_by_key(|path| display_path(path));
    Ok(files)
}

fn visit_dir(
    dir: &Path,
    matcher: fn(&Path) -> bool,
    skip_dirs: &[&str],
    files: &mut Vec<PathBuf>,
) -> Result<(), HarnessError> {
    let mut entries = fs::read_dir(dir)
        .map_err(|source| HarnessError::Io {
            path: display_path(dir),
            source,
        })?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|source| HarnessError::Io {
            path: display_path(dir),
            source,
        })?;
    entries.sort_by_key(|entry| display_path(&entry.path()));

    for entry in entries {
        let path = entry.path();
        let metadata = entry.metadata().map_err(|source| HarnessError::Io {
            path: display_path(&path),
            source,
        })?;
        if metadata.is_dir() {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("");
            if skip_dirs.contains(&name) {
                continue;
            }
            visit_dir(&path, matcher, skip_dirs, files)?;
        } else if metadata.is_file() && matcher(&path) {
            files.push(path);
        }
    }
    Ok(())
}

fn read_to_string(path: &Path) -> Result<String, HarnessError> {
    fs::read_to_string(path).map_err(|source| HarnessError::Io {
        path: display_path(path),
        source,
    })
}

fn is_markdown_file(path: &Path) -> bool {
    has_extension(path, "md")
}

fn is_tasks_file(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name == "tasks.md")
}

fn is_test_code_candidate(path: &Path) -> bool {
    ["rs", "py", "ts", "tsx", "js", "jsx"]
        .iter()
        .any(|extension| has_extension(path, extension))
}

fn has_extension(path: &Path, expected: &str) -> bool {
    path.extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension == expected)
}

fn is_comment_line(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with("//") || trimmed.starts_with('#')
}

fn valid_ids(text: &str) -> Vec<String> {
    valid_id_regex()
        .find_iter(text)
        .map(|match_| match_.as_str().to_string())
        .collect()
}

fn first_malformed_id(text: &str) -> Option<String> {
    malformed_id_regex()
        .find_iter(text)
        .map(|match_| match_.as_str().to_string())
        .find(|candidate| !valid_id_regex().is_match(candidate))
}

fn valid_id_regex() -> Regex {
    Regex::new(r"\b[A-Z][A-Z0-9]+-[0-9]{2}\b").expect("valid requirement ID regex")
}

fn malformed_id_regex() -> Regex {
    Regex::new(r"\b[A-Za-z][A-Za-z0-9]*-[0-9]+\b").expect("malformed requirement ID regex")
}

fn display_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    // covers: RVH-01
    #[test]
    fn extracts_requirement_ids_from_spec_delta() {
        let fixture = TempFixture::new();
        fixture.write(
            "specs/review-harness/spec.md",
            "## ADDED Requirements\n\n### Requirement: RVH-01 Parse specs\nThe system SHALL parse spec deltas.\n\n#### Scenario: Valid\n- **WHEN** a spec exists\n- **THEN** it is parsed\n",
        );

        let requirements = extract_requirements(&fixture.path("specs")).unwrap();

        assert_eq!(requirements.len(), 1);
        assert_eq!(requirements[0].id, "RVH-01");
        assert!(requirements[0].text.contains("SHALL parse"));
    }

    // covers: RVH-01
    #[test]
    fn rejects_shall_without_requirement_id() {
        let fixture = TempFixture::new();
        fixture.write(
            "specs/review-harness/spec.md",
            "## ADDED Requirements\n\n### Requirement: Parse specs\nThe system SHALL parse spec deltas.\n",
        );

        let error = extract_requirements(&fixture.path("specs")).unwrap_err();

        assert!(matches!(error, HarnessError::MissingRequirementId { .. }));
    }

    // covers: RVH-01
    #[test]
    fn rejects_malformed_requirement_id() {
        let fixture = TempFixture::new();
        fixture.write(
            "specs/review-harness/spec.md",
            "## ADDED Requirements\n\n### Requirement: RVH-1 Parse specs\nThe system SHALL parse spec deltas.\n",
        );

        let error = extract_requirements(&fixture.path("specs")).unwrap_err();

        assert!(matches!(error, HarnessError::InvalidRequirementId { .. }));
    }

    // covers: RVH-01
    #[test]
    fn cli_extract_emits_stable_json() {
        let fixture = TempFixture::new();
        fixture.write(
            "specs/review-harness/spec.md",
            "## ADDED Requirements\n\n### Requirement: RVH-01 Parse specs\nThe system SHALL parse spec deltas.\n",
        );

        let output = run_cli(vec![
            "extract".to_string(),
            "--spec-root".to_string(),
            fixture.path("specs").to_string_lossy().to_string(),
        ])
        .unwrap();

        let json: serde_json::Value = serde_json::from_str(&output).unwrap();
        assert_eq!(json["requirements"][0]["id"], "RVH-01");
    }

    // covers: RVH-01
    #[test]
    fn cli_help_emits_usage() {
        let output = run_cli(vec!["--help".to_string()]).unwrap();

        assert!(output.contains("Extract and verify"));
        assert!(output.contains("Required arguments"));
        assert!(output.contains("--spec-root=<path>"));
    }

    // covers: RVH-02
    #[test]
    fn detects_automated_coverage_annotations_from_comments() {
        let fixture = TempFixture::new();
        fixture.write(
            "tests/review_harness.rs",
            "// covers: RVH-02\n#[test]\nfn verifies_matrix() {}\n",
        );

        let evidence = collect_automated_evidence(fixture.root()).unwrap();

        assert_eq!(evidence.len(), 1);
        assert_eq!(evidence[0].requirement_id, "RVH-02");
        assert_eq!(evidence[0].kind, EvidenceKind::Automated);
    }

    // covers: RVH-02
    #[test]
    fn verify_fails_for_uncovered_requirements() {
        let fixture = TempFixture::new();
        fixture.write(
            "specs/review-harness/spec.md",
            "## ADDED Requirements\n\n### Requirement: RVH-02 Matrix\nThe system SHALL fail when uncovered.\n",
        );
        fixture.write("tasks.md", "- [ ] no evidence\n");

        let error =
            verify_matrix(&fixture.path("specs"), fixture.root(), fixture.root()).unwrap_err();

        assert!(matches!(error, HarnessError::UncoveredRequirements(_)));
    }

    // covers: RVH-02
    #[test]
    fn generate_fails_for_unknown_evidence_reference() {
        let fixture = TempFixture::new();
        fixture.write(
            "specs/review-harness/spec.md",
            "## ADDED Requirements\n\n### Requirement: RVH-02 Matrix\nThe system SHALL fail on unknown evidence.\n",
        );
        fixture.write(
            "tests/review_harness.rs",
            &format!("// cov{} UNKNOWN-01\n#[test]\nfn stray() {{}}\n", "ers:"),
        );

        let error =
            generate_matrix(&fixture.path("specs"), fixture.root(), fixture.root()).unwrap_err();

        assert!(matches!(error, HarnessError::UnknownEvidenceReferences(_)));
    }

    // covers: RVH-03
    #[test]
    fn reports_matrix_diff_in_stable_sets() {
        let base = matrix_with(
            vec![requirement("RVH-01")],
            vec![evidence("RVH-01", "tests/a.rs", 1)],
        );
        let head = matrix_with(
            vec![requirement("RVH-01"), requirement("RVH-03")],
            vec![evidence("RVH-03", "tests/b.rs", 2)],
        );

        let diff = diff_matrices(&base, &head);

        assert!(diff.has_diff);
        assert_eq!(diff.new_requirements, vec!["RVH-03"]);
        assert_eq!(diff.new_evidence.len(), 1);
        assert_eq!(diff.lost_evidence.len(), 1);
    }

    // covers: RVH-03
    #[test]
    fn reports_explicit_no_diff() {
        let matrix = matrix_with(
            vec![requirement("RVH-03")],
            vec![evidence("RVH-03", "tests/b.rs", 2)],
        );

        let diff = diff_matrices(&matrix, &matrix);

        assert!(!diff.has_diff);
        assert_eq!(diff.message, "no coverage diff exists");
    }

    fn requirement(id: &str) -> Requirement {
        Requirement {
            id: id.to_string(),
            source: "spec.md".to_string(),
            line: 1,
            text: "The system SHALL be covered.".to_string(),
        }
    }

    fn evidence(id: &str, source: &str, line: usize) -> Evidence {
        Evidence {
            requirement_id: id.to_string(),
            kind: EvidenceKind::Automated,
            source: source.to_string(),
            line,
            detail: format!("// covers: {id}"),
        }
    }

    fn matrix_with(requirements: Vec<Requirement>, evidence: Vec<Evidence>) -> CoverageMatrix {
        let rows = requirements
            .iter()
            .map(|requirement| {
                let row_evidence = evidence
                    .iter()
                    .filter(|item| item.requirement_id == requirement.id)
                    .cloned()
                    .collect::<Vec<_>>();
                let judgement = if row_evidence.is_empty() {
                    CoverageJudgement::Uncovered
                } else {
                    CoverageJudgement::Covered
                };
                CoverageRow {
                    requirement_id: requirement.id.clone(),
                    judgement,
                    evidence: row_evidence,
                }
            })
            .collect();
        CoverageMatrix {
            requirements,
            evidence,
            rows,
        }
    }

    struct TempFixture {
        root: PathBuf,
    }

    impl TempFixture {
        fn new() -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let root = std::env::temp_dir().join(format!(
                "lethe-review-harness-{}-{nanos}",
                std::process::id()
            ));
            fs::create_dir_all(&root).unwrap();
            Self { root }
        }

        fn root(&self) -> &Path {
            &self.root
        }

        fn path(&self, relative: &str) -> PathBuf {
            self.root.join(relative)
        }

        fn write(&self, relative: &str, content: &str) {
            let path = self.path(relative);
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent).unwrap();
            }
            fs::write(path, content).unwrap();
        }
    }

    impl Drop for TempFixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }
}
