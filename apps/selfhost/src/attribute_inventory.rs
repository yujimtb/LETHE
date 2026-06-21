use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use chrono::{DateTime, Utc};
use regex::Regex;
use serde::{Deserialize, Serialize};

use crate::self_host::app::ProjectionSnapshot;
pub use lethe_profile_model::{AttributeAliasCatalog, AttributeAliasDefinition};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributeInventoryDocument {
    pub document_key: String,
    pub person_id: String,
    pub display_name: String,
    pub source_document_id: Option<String>,
    pub source_canonical_uri: Option<String>,
    pub last_activity: Option<DateTime<Utc>>,
    pub candidates: Vec<AttributeCandidate>,
}

/// A candidate label found across student profiles.
///
/// In label-inventory mode each candidate represents a unique **attribute
/// label** (e.g. "出身地", "呼ばれたい名前") rather than a property value.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttributeCandidate {
    pub candidate_key: String,
    pub attribute_path: String,
    pub source_path: String,
    pub label: String,
    pub value: String,
    pub value_kind: CandidateValueKind,
    /// How many students use this label.
    #[serde(default)]
    pub student_count: usize,
    /// Names of students who use this label (for display).
    #[serde(default)]
    pub student_names: Vec<String>,
    /// Sample property values for context (e.g. "東京", "佐野ちゃん").
    #[serde(default)]
    pub example_values: Vec<String>,
    /// The AI-normalized property key, if known (e.g. "Nickname", "Birthplace").
    #[serde(default)]
    pub ai_property_key: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CandidateValueKind {
    ShortText,
    LongText,
    Url,
}

/// A raw label–value pair extracted from bio_text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BioLabel {
    pub label: String,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributeInventoryState {
    pub version: u32,
    pub known_attributes: Vec<KnownAttribute>,
    pub reviewed_candidates: BTreeMap<String, ReviewedCandidate>,
}

impl Default for AttributeInventoryState {
    fn default() -> Self {
        Self {
            version: 1,
            known_attributes: Vec::new(),
            reviewed_candidates: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnownAttribute {
    pub id: String,
    pub display_name: String,
    pub source_paths: Vec<String>,
    pub examples: Vec<String>,
    pub review_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewedCandidate {
    pub candidate_key: String,
    pub action: ReviewAction,
    pub attribute_id: Option<String>,
    pub attribute_path: String,
    pub source_path: String,
    pub value: String,
    pub reviewed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReviewAction {
    Existing,
    New,
    Skipped,
}

#[derive(Debug, Clone)]
pub struct AttributeSuggestion {
    pub id: String,
    pub display_name: String,
    pub score: usize,
    pub source_paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredAttributeGroup {
    pub attribute_path: String,
    pub label: String,
    pub value_kind: CandidateValueKind,
    pub occurrence_count: usize,
    pub document_count: usize,
    pub source_paths: Vec<String>,
    pub example_values: Vec<String>,
    /// Which students have this label.
    #[serde(default)]
    pub student_names: Vec<String>,
    /// AI-normalized property key, if mapped.
    #[serde(default)]
    pub ai_property_key: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InventoryCommand {
    PickSuggestion(usize),
    MapExisting(String),
    CreateNew(String),
    Skip,
    Quit,
    Help,
}

pub fn build_inventory_documents(snapshot: &ProjectionSnapshot) -> Vec<AttributeInventoryDocument> {
    // Collect per-student label data, then aggregate into deduplicated candidates.
    struct StudentLabels {
        display_name: String,
        labels: Vec<BioLabel>,
        ai_keys: Vec<(String, String)>, // (ai_key, example_value)
    }

    let mut all_students: Vec<StudentLabels> = Vec::new();

    for person in &snapshot.person_page.profiles {
        let Some(frontend) = person.frontend_profile.as_ref() else {
            continue;
        };
        let bio_text = frontend.profile.bio_text.as_deref().unwrap_or("");
        let labels = extract_bio_labels(bio_text);

        // Collect AI property keys with their values for cross-reference.
        let ai_keys = extract_ai_property_keys(&frontend.profile.properties);

        all_students.push(StudentLabels {
            display_name: person.display_name.clone(),
            labels,
            ai_keys,
        });
    }

    // Aggregate: group by normalized label text.
    // Key = normalized label, Value = aggregated candidate info
    struct LabelAgg {
        raw_label: String, // first-seen raw form
        student_names: Vec<String>,
        example_values: BTreeSet<String>,
        ai_property_key: Option<String>,
    }

    let mut label_map: BTreeMap<String, LabelAgg> = BTreeMap::new();

    for student in &all_students {
        // Track which normalized labels this student contributed (dedup per student).
        let mut seen_in_student: BTreeSet<String> = BTreeSet::new();

        for bio_label in &student.labels {
            let norm = normalize_label_text(&bio_label.label);
            if norm.is_empty() || seen_in_student.contains(&norm) {
                continue;
            }
            seen_in_student.insert(norm.clone());

            let agg = label_map.entry(norm).or_insert_with(|| LabelAgg {
                raw_label: bio_label.label.clone(),
                student_names: Vec::new(),
                example_values: BTreeSet::new(),
                ai_property_key: None,
            });
            agg.student_names.push(student.display_name.clone());
            if !bio_label.value.is_empty() && agg.example_values.len() < 5 {
                agg.example_values.insert(bio_label.value.clone());
            }
        }

        // Also register AI property keys — these serve as "label" evidence.
        for (ai_key, example_val) in &student.ai_keys {
            let norm = normalize_label_text(ai_key);
            if norm.is_empty() {
                continue;
            }
            if let Some(agg) = label_map.get_mut(&norm) {
                // Attach AI key to an existing bio label group.
                if agg.ai_property_key.is_none() {
                    agg.ai_property_key = Some(ai_key.clone());
                }
                if !example_val.is_empty() && agg.example_values.len() < 5 {
                    agg.example_values.insert(example_val.clone());
                }
            }
            // For AI keys without a matching bio label we also inject a
            // candidate if no student wrote a matching raw label.  This
            // ensures properties that only appear via the AI extraction
            // are still surfaced (e.g. if some students don't write an
            // explicit label).
            if !label_map.contains_key(&norm) && !seen_in_student.contains(&norm) {
                seen_in_student.insert(norm.clone());
                let agg = label_map.entry(norm).or_insert_with(|| LabelAgg {
                    raw_label: ai_key.clone(),
                    student_names: Vec::new(),
                    example_values: BTreeSet::new(),
                    ai_property_key: Some(ai_key.clone()),
                });
                agg.student_names.push(student.display_name.clone());
                if !example_val.is_empty() && agg.example_values.len() < 5 {
                    agg.example_values.insert(example_val.clone());
                }
            }
        }
    }

    // Build candidates sorted by student count (most common first).
    let mut candidates: Vec<AttributeCandidate> = label_map
        .into_iter()
        .map(|(norm, agg)| {
            let student_count = agg.student_names.len();
            AttributeCandidate {
                candidate_key: format!("label:{norm}"),
                attribute_path: norm.clone(),
                source_path: format!("{} student(s)", student_count),
                label: agg.raw_label.clone(),
                value: agg.raw_label,
                value_kind: CandidateValueKind::ShortText,
                student_count,
                student_names: agg.student_names,
                example_values: agg.example_values.into_iter().collect(),
                ai_property_key: agg.ai_property_key,
            }
        })
        .collect();

    candidates.sort_by(|a, b| {
        b.student_count
            .cmp(&a.student_count)
            .then(a.label.cmp(&b.label))
    });

    // Return a single aggregate document (all students combined).
    vec![AttributeInventoryDocument {
        document_key: "all-labels".into(),
        person_id: "aggregate".into(),
        display_name: format!("All Students ({} profiles)", all_students.len()),
        source_document_id: None,
        source_canonical_uri: None,
        last_activity: None,
        candidates,
    }]
}

/// Extract `label：value` pairs from bio_text.
///
/// Handles full-width `：` and half-width `:` separators.  Skips labels that
/// look like time stamps (e.g. `12:34`) or URLs.
pub fn extract_bio_labels(bio_text: &str) -> Vec<BioLabel> {
    let re = Regex::new(r"(?m)^\s*([^\n:：]{1,30}?)\s*[：:]\s*(.*)").unwrap();
    let mut results = Vec::new();
    for caps in re.captures_iter(bio_text) {
        let raw_label = caps[1].trim().to_string();
        let raw_value = caps[2].trim().to_string();

        // Skip labels that look like numeric timestamps or single chars.
        if raw_label.is_empty() || raw_label.chars().count() < 2 {
            continue;
        }
        if raw_label.chars().all(|c| c.is_ascii_digit() || c == '.') {
            continue;
        }
        // Skip if it looks like a URL fragment (e.g. "https")
        if raw_label.eq_ignore_ascii_case("http") || raw_label.eq_ignore_ascii_case("https") {
            continue;
        }
        results.push(BioLabel {
            label: raw_label,
            value: raw_value,
        });
    }
    results
}

/// Extract AI property keys and their (first) values from a `StudentProperties`
/// object serialized as JSON.
fn extract_ai_property_keys(
    properties: &lethe_profile_model::StudentProperties,
) -> Vec<(String, String)> {
    let json = serde_json::to_value(properties).unwrap_or_default();
    let Some(obj) = json.as_object() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for (key, val) in obj {
        let example = match val {
            serde_json::Value::String(s) if !s.is_empty() => s.clone(),
            serde_json::Value::Array(arr) => arr
                .iter()
                .filter_map(|v| v.as_str())
                .next()
                .unwrap_or("")
                .to_string(),
            _ => String::new(),
        };
        // Only include keys that have non-null/non-empty values.
        if !example.is_empty() {
            out.push((key.clone(), example));
        }
    }
    out
}

/// Normalize a label text for grouping (lowercase ASCII, collapse whitespace).
fn normalize_label_text(label: &str) -> String {
    label
        .trim()
        .chars()
        .map(|c| {
            if c.is_ascii_uppercase() {
                c.to_ascii_lowercase()
            } else {
                c
            }
        })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

impl AttributeInventoryState {
    pub fn load_or_default(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let raw = fs::read_to_string(path)?;
        Ok(serde_json::from_str(&raw)?)
    }

    pub fn save(&self, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let json = serde_json::to_string_pretty(self)?;
        fs::write(path, json)?;
        Ok(())
    }

    pub fn is_reviewed(&self, candidate: &AttributeCandidate) -> bool {
        self.reviewed_candidates
            .contains_key(&candidate.candidate_key)
    }

    pub fn review_existing(
        &mut self,
        candidate: &AttributeCandidate,
        attribute_id: &str,
    ) -> Result<(), String> {
        let Some(attribute) = self
            .known_attributes
            .iter_mut()
            .find(|attribute| attribute.id == attribute_id)
        else {
            return Err(format!("unknown attribute id: {attribute_id}"));
        };
        absorb_candidate(attribute, candidate);
        self.reviewed_candidates.insert(
            candidate.candidate_key.clone(),
            ReviewedCandidate {
                candidate_key: candidate.candidate_key.clone(),
                action: ReviewAction::Existing,
                attribute_id: Some(attribute_id.to_string()),
                attribute_path: candidate.attribute_path.clone(),
                source_path: candidate.source_path.clone(),
                value: candidate.value.clone(),
                reviewed_at: Utc::now(),
            },
        );
        Ok(())
    }

    pub fn review_new(
        &mut self,
        candidate: &AttributeCandidate,
        display_name: &str,
    ) -> Result<String, String> {
        let id = normalize_attribute_id(display_name);
        if id.is_empty() {
            return Err(
                "attribute name must contain at least one alphanumeric character".to_string(),
            );
        }
        if self
            .known_attributes
            .iter()
            .any(|attribute| attribute.id == id)
        {
            return Err(format!("attribute id already exists: {id}"));
        }
        let mut attribute = KnownAttribute {
            id: id.clone(),
            display_name: display_name.trim().to_string(),
            source_paths: Vec::new(),
            examples: Vec::new(),
            review_count: 0,
        };
        absorb_candidate(&mut attribute, candidate);
        self.known_attributes.push(attribute);
        self.known_attributes.sort_by(|left, right| {
            left.display_name
                .cmp(&right.display_name)
                .then(left.id.cmp(&right.id))
        });
        self.reviewed_candidates.insert(
            candidate.candidate_key.clone(),
            ReviewedCandidate {
                candidate_key: candidate.candidate_key.clone(),
                action: ReviewAction::New,
                attribute_id: Some(id.clone()),
                attribute_path: candidate.attribute_path.clone(),
                source_path: candidate.source_path.clone(),
                value: candidate.value.clone(),
                reviewed_at: Utc::now(),
            },
        );
        Ok(id)
    }

    pub fn review_skip(&mut self, candidate: &AttributeCandidate) {
        self.reviewed_candidates.insert(
            candidate.candidate_key.clone(),
            ReviewedCandidate {
                candidate_key: candidate.candidate_key.clone(),
                action: ReviewAction::Skipped,
                attribute_id: None,
                attribute_path: candidate.attribute_path.clone(),
                source_path: candidate.source_path.clone(),
                value: candidate.value.clone(),
                reviewed_at: Utc::now(),
            },
        );
    }

    pub fn suggestions_for(&self, candidate: &AttributeCandidate) -> Vec<AttributeSuggestion> {
        let candidate_norm = normalize_label_text(&candidate.value);
        let mut suggestions = self
            .known_attributes
            .iter()
            .filter_map(|attribute| {
                let mut score = 0usize;
                // Exact label match in source_paths (aliases).
                if attribute
                    .source_paths
                    .iter()
                    .any(|alias| normalize_label_text(alias) == candidate_norm)
                {
                    score += 100;
                }
                // Display name similarity.
                if normalize_label_text(&attribute.display_name) == candidate_norm {
                    score += 90;
                }
                // Partial overlap in label text (shared characters).
                if score == 0 {
                    let attr_norm = normalize_label_text(&attribute.display_name);
                    if !candidate_norm.is_empty()
                        && !attr_norm.is_empty()
                        && (candidate_norm.contains(&attr_norm)
                            || attr_norm.contains(&candidate_norm))
                    {
                        score += 50;
                    }
                }
                // Check if AI key matches any alias.
                if let Some(ai_key) = &candidate.ai_property_key {
                    let ai_norm = normalize_label_text(ai_key);
                    if attribute
                        .source_paths
                        .iter()
                        .any(|p| normalize_label_text(p) == ai_norm)
                    {
                        score += 70;
                    }
                }
                if score == 0 {
                    return None;
                }
                Some(AttributeSuggestion {
                    id: attribute.id.clone(),
                    display_name: attribute.display_name.clone(),
                    score,
                    source_paths: attribute.source_paths.clone(),
                })
            })
            .collect::<Vec<_>>();
        suggestions.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then(left.display_name.cmp(&right.display_name))
                .then(left.id.cmp(&right.id))
        });
        suggestions.truncate(5);
        suggestions
    }

    pub fn export_alias_catalog(&self) -> AttributeAliasCatalog {
        AttributeAliasCatalog {
            version: self.version,
            attributes: self
                .known_attributes
                .iter()
                .map(|attribute| AttributeAliasDefinition {
                    id: attribute.id.clone(),
                    display_name: attribute.display_name.clone(),
                    aliases: attribute.source_paths.clone(),
                    examples: attribute.examples.clone(),
                })
                .collect(),
        }
    }

    pub fn merge_alias_catalog(&mut self, catalog: AttributeAliasCatalog) {
        for entry in catalog.attributes {
            if let Some(existing) = self
                .known_attributes
                .iter_mut()
                .find(|attribute| attribute.id == entry.id)
            {
                let mut aliases = existing
                    .source_paths
                    .iter()
                    .cloned()
                    .collect::<BTreeSet<_>>();
                aliases.extend(entry.aliases.iter().cloned());
                existing.source_paths = aliases.into_iter().collect();

                let mut examples = existing.examples.iter().cloned().collect::<BTreeSet<_>>();
                examples.extend(entry.examples.iter().cloned());
                existing.examples = examples.into_iter().take(5).collect();
                if existing.display_name.trim().is_empty() {
                    existing.display_name = entry.display_name;
                }
            } else {
                self.known_attributes.push(KnownAttribute {
                    id: entry.id,
                    display_name: entry.display_name,
                    source_paths: entry.aliases,
                    examples: entry.examples,
                    review_count: 0,
                });
            }
        }
        self.known_attributes.sort_by(|left, right| {
            left.display_name
                .cmp(&right.display_name)
                .then(left.id.cmp(&right.id))
        });
    }
}

pub fn summarize_documents(
    documents: &[AttributeInventoryDocument],
) -> Vec<DiscoveredAttributeGroup> {
    let mut groups = BTreeMap::<String, DiscoveredAttributeGroup>::new();
    for document in documents {
        for candidate in &document.candidates {
            let group = groups
                .entry(candidate.attribute_path.clone())
                .or_insert_with(|| DiscoveredAttributeGroup {
                    attribute_path: candidate.attribute_path.clone(),
                    label: candidate.label.clone(),
                    value_kind: candidate.value_kind,
                    occurrence_count: 0,
                    document_count: candidate.student_count,
                    source_paths: Vec::new(),
                    example_values: Vec::new(),
                    student_names: candidate.student_names.clone(),
                    ai_property_key: candidate.ai_property_key.clone(),
                });
            group.occurrence_count += 1;
            if !group.source_paths.contains(&candidate.label) {
                group.source_paths.push(candidate.label.clone());
            }
            for ev in &candidate.example_values {
                if !group.example_values.contains(ev) && group.example_values.len() < 5 {
                    group.example_values.push(ev.clone());
                }
            }
        }
    }
    let mut result: Vec<_> = groups.into_values().collect();
    result.sort_by(|a, b| {
        b.document_count
            .cmp(&a.document_count)
            .then(a.label.cmp(&b.label))
    });
    result
}

pub fn write_json_file<T: Serialize>(
    path: &Path,
    value: &T,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let raw = serde_json::to_string_pretty(value)?;
    fs::write(path, raw)?;
    Ok(())
}

pub fn read_alias_catalog(
    path: &Path,
) -> Result<AttributeAliasCatalog, Box<dyn std::error::Error>> {
    let raw = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&raw)?)
}

fn absorb_candidate(attribute: &mut KnownAttribute, candidate: &AttributeCandidate) {
    // Store the label text itself as a source alias.
    let mut source_paths = attribute
        .source_paths
        .iter()
        .cloned()
        .collect::<BTreeSet<_>>();
    source_paths.insert(candidate.value.clone());
    attribute.source_paths = source_paths.into_iter().collect();

    // Store sample property values (from the candidate's example_values) as examples.
    let mut examples = attribute.examples.iter().cloned().collect::<BTreeSet<_>>();
    for ev in &candidate.example_values {
        examples.insert(ev.clone());
    }
    attribute.examples = examples.into_iter().take(5).collect();
    attribute.review_count += 1;
}

fn normalize_attribute_id(value: &str) -> String {
    let mut id = String::new();
    let mut last_was_dash = false;
    for ch in value.trim().chars() {
        if ch.is_alphanumeric() {
            for lowered in ch.to_lowercase() {
                id.push(lowered);
            }
            last_was_dash = false;
        } else if !last_was_dash && !id.is_empty() {
            id.push('-');
            last_was_dash = true;
        }
    }
    id.trim_matches('-').to_string()
}

pub fn parse_inventory_command(input: &str) -> Result<InventoryCommand, String> {
    let trimmed = input.trim();
    if trimmed.is_empty() || trimmed == "?" || trimmed.eq_ignore_ascii_case("help") {
        return Ok(InventoryCommand::Help);
    }
    if trimmed.eq_ignore_ascii_case("s") || trimmed.eq_ignore_ascii_case("skip") {
        return Ok(InventoryCommand::Skip);
    }
    if trimmed.eq_ignore_ascii_case("q") || trimmed.eq_ignore_ascii_case("quit") {
        return Ok(InventoryCommand::Quit);
    }
    if let Ok(index) = trimmed.parse::<usize>() {
        if index == 0 {
            return Err("suggestion index starts at 1".to_string());
        }
        return Ok(InventoryCommand::PickSuggestion(index));
    }
    if let Some(value) = trimmed.strip_prefix("e ") {
        let id = value.trim();
        if id.is_empty() {
            return Err("usage: e <attribute-id>".to_string());
        }
        return Ok(InventoryCommand::MapExisting(id.to_string()));
    }
    if let Some(value) = trimmed.strip_prefix("n ") {
        let display_name = value.trim();
        if display_name.is_empty() {
            return Err("usage: n <attribute display name>".to_string());
        }
        return Ok(InventoryCommand::CreateNew(display_name.to_string()));
    }
    Err("commands: <number>, e <attribute-id>, n <name>, s, q, ?".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use lethe_core::domain::EntityRef;
    use lethe_profile_model::{StudentProfile, StudentProperties};
    use lethe_projection_person::person_page::types::{
        FrontendProfile, PersonPageOutput, PersonProfile,
    };

    #[test]
    fn extract_bio_labels_parses_colon_separated_pairs() {
        let bio = "呼ばれたい名前：佐野ちゃん\n出身地：東京\n生年月日：2007/02/16\n趣味・特技：旅行、写真";
        let labels = extract_bio_labels(bio);
        assert_eq!(labels.len(), 4);
        assert_eq!(labels[0].label, "呼ばれたい名前");
        assert_eq!(labels[0].value, "佐野ちゃん");
        assert_eq!(labels[1].label, "出身地");
        assert_eq!(labels[1].value, "東京");
        assert_eq!(labels[2].label, "生年月日");
        assert_eq!(labels[3].label, "趣味・特技");
        assert_eq!(labels[3].value, "旅行、写真");
    }

    #[test]
    fn extract_bio_labels_handles_half_width_colon() {
        let bio = "Nickname: メンディー\nMBTI: INTJ";
        let labels = extract_bio_labels(bio);
        assert!(
            labels
                .iter()
                .any(|l| l.label == "Nickname" && l.value == "メンディー")
        );
        assert!(
            labels
                .iter()
                .any(|l| l.label == "MBTI" && l.value == "INTJ")
        );
    }

    #[test]
    fn extract_bio_labels_skips_timestamps_and_urls() {
        let bio = "12:34\nhttps://example.com\n出身地：東京";
        let labels = extract_bio_labels(bio);
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].label, "出身地");
    }

    #[test]
    fn build_inventory_documents_extracts_labels_not_values() {
        let snapshot = ProjectionSnapshot {
            identity: Default::default(),
            person_page: PersonPageOutput {
                profiles: vec![
                    make_person(
                        "Student A",
                        "呼ばれたい名前：なっしー\n出身地：東京",
                        StudentProperties {
                            nickname: Some("なっしー".into()),
                            birthplace: Some("東京".into()),
                            ..StudentProperties::default()
                        },
                    ),
                    make_person(
                        "Student B",
                        "あだ名：メンディー\n出身地：大阪",
                        StudentProperties {
                            nickname: Some("メンディー".into()),
                            birthplace: Some("大阪".into()),
                            ..StudentProperties::default()
                        },
                    ),
                ],
                ..PersonPageOutput::default()
            },
            built_at: Utc::now(),
            lineage: ProjectionSnapshot::default().lineage,
        };

        let docs = build_inventory_documents(&snapshot);
        assert_eq!(docs.len(), 1, "should produce a single aggregate document");
        let doc = &docs[0];

        // Should have label candidates, not value candidates.
        // "出身地" appears in both students → student_count = 2
        let birthplace = doc.candidates.iter().find(|c| c.value == "出身地");
        assert!(birthplace.is_some(), "should have 出身地 label");
        assert_eq!(birthplace.unwrap().student_count, 2);

        // "呼ばれたい名前" appears in 1 student only
        let nickname_a = doc.candidates.iter().find(|c| c.value == "呼ばれたい名前");
        assert!(nickname_a.is_some(), "should have 呼ばれたい名前 label");
        assert_eq!(nickname_a.unwrap().student_count, 1);

        // "あだ名" appears in 1 student
        let nickname_b = doc.candidates.iter().find(|c| c.value == "あだ名");
        assert!(nickname_b.is_some(), "should have あだ名 label");
        assert_eq!(nickname_b.unwrap().student_count, 1);

        // Raw values like "東京" or "なっしー" should NOT appear as candidate labels.
        assert!(
            !doc.candidates.iter().any(|c| c.value == "東京"),
            "property values should not be candidates"
        );
        assert!(
            !doc.candidates.iter().any(|c| c.value == "なっしー"),
            "property values should not be candidates"
        );
    }

    #[test]
    fn suggestions_match_labels_by_alias() {
        let candidate = AttributeCandidate {
            candidate_key: "label:あだ名".into(),
            attribute_path: "あだ名".into(),
            source_path: "1 student(s)".into(),
            label: "あだ名".into(),
            value: "あだ名".into(),
            value_kind: CandidateValueKind::ShortText,
            student_count: 1,
            student_names: vec!["Test".into()],
            example_values: vec!["メンディー".into()],
            ai_property_key: Some("Nickname".into()),
        };
        let state = AttributeInventoryState {
            version: 1,
            known_attributes: vec![KnownAttribute {
                id: "nickname".into(),
                display_name: "ニックネーム".into(),
                source_paths: vec!["呼ばれたい名前".into(), "Nickname".into()],
                examples: vec!["なっしー".into()],
                review_count: 1,
            }],
            reviewed_candidates: BTreeMap::new(),
        };

        let suggestions = state.suggestions_for(&candidate);
        assert!(
            !suggestions.is_empty(),
            "should suggest ニックネーム via AI key match"
        );
        assert_eq!(suggestions[0].id, "nickname");
    }

    #[test]
    fn parse_inventory_commands_supports_review_actions() {
        assert_eq!(
            parse_inventory_command("1").unwrap(),
            InventoryCommand::PickSuggestion(1)
        );
        assert_eq!(
            parse_inventory_command("e hobbies").unwrap(),
            InventoryCommand::MapExisting("hobbies".into())
        );
        assert_eq!(
            parse_inventory_command("n Favorite Food").unwrap(),
            InventoryCommand::CreateNew("Favorite Food".into())
        );
        assert_eq!(
            parse_inventory_command("s").unwrap(),
            InventoryCommand::Skip
        );
        assert_eq!(
            parse_inventory_command("q").unwrap(),
            InventoryCommand::Quit
        );
    }

    #[test]
    fn summarize_groups_labels_by_normalized_text() {
        let documents = vec![AttributeInventoryDocument {
            document_key: "all-labels".into(),
            person_id: "aggregate".into(),
            display_name: "All Students".into(),
            source_document_id: None,
            source_canonical_uri: None,
            last_activity: None,
            candidates: vec![
                AttributeCandidate {
                    candidate_key: "label:出身地".into(),
                    attribute_path: "出身地".into(),
                    source_path: "3 student(s)".into(),
                    label: "出身地".into(),
                    value: "出身地".into(),
                    value_kind: CandidateValueKind::ShortText,
                    student_count: 3,
                    student_names: vec!["A".into(), "B".into(), "C".into()],
                    example_values: vec!["東京".into(), "大阪".into()],
                    ai_property_key: Some("Birthplace".into()),
                },
                AttributeCandidate {
                    candidate_key: "label:趣味".into(),
                    attribute_path: "趣味".into(),
                    source_path: "2 student(s)".into(),
                    label: "趣味".into(),
                    value: "趣味".into(),
                    value_kind: CandidateValueKind::ShortText,
                    student_count: 2,
                    student_names: vec!["A".into(), "B".into()],
                    example_values: vec!["旅行".into()],
                    ai_property_key: Some("Hobbies".into()),
                },
            ],
        }];

        let groups = summarize_documents(&documents);
        assert_eq!(groups.len(), 2);
        // Sorted by document_count (student_count) desc.
        assert_eq!(groups[0].label, "出身地");
        assert_eq!(groups[0].document_count, 3);
        assert_eq!(groups[1].label, "趣味");
    }

    fn make_person(name: &str, bio: &str, props: StudentProperties) -> PersonProfile {
        PersonProfile {
            person_id: EntityRef::new(format!("person:{name}")),
            display_name: name.into(),
            self_intro_text: None,
            self_intro_slide_id: None,
            self_intro_thumbnail: None,
            identities: Vec::new(),
            source_count: 1,
            last_activity: None,
            profile_updated_at: Utc::now(),
            frontend_profile: Some(FrontendProfile {
                source_document_id: format!("doc:{name}"),
                source_canonical_uri: None,
                thumbnail_ref: None,
                thumbnail_url: None,
                profile: StudentProfile {
                    email: None,
                    generated_email: None,
                    name: name.into(),
                    bio_text: Some(bio.into()),
                    profile_pic: None,
                    gallery_images: Vec::new(),
                    properties: props,
                    attributes: Vec::new(),
                    source_slide_object_id: None,
                    source_document_id: None,
                    source_canonical_uri: None,
                    thumbnail_blob_ref: None,
                    thumbnail_url: None,
                    companion_to_slide_object_id: None,
                },
            }),
        }
    }
}
