use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::self_host::app::ProjectionSnapshot;

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AttributeCandidate {
    pub candidate_key: String,
    pub attribute_path: String,
    pub source_path: String,
    pub label: String,
    pub value: String,
    pub value_kind: CandidateValueKind,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CandidateValueKind {
    ShortText,
    LongText,
    Url,
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
pub struct AttributeAliasCatalog {
    pub version: u32,
    pub attributes: Vec<AttributeAliasDefinition>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributeAliasDefinition {
    pub id: String,
    pub display_name: String,
    pub aliases: Vec<String>,
    pub examples: Vec<String>,
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
    let mut documents = snapshot
        .person_page
        .profiles
        .iter()
        .filter_map(|person| {
            let frontend = person.frontend_profile.as_ref()?;
            let source_document_id = Some(frontend.source_document_id.clone());
            let source_canonical_uri = frontend.source_canonical_uri.clone();
            let document_key = source_document_id
                .clone()
                .unwrap_or_else(|| person.person_id.as_str().to_string());
            let payload = serde_json::json!({
                "person": {
                    "display_name": person.display_name,
                    "self_intro_text": person.self_intro_text,
                },
                "profile": frontend.profile,
            });
            let mut candidates = Vec::new();
            collect_string_candidates(&payload, None, None, &mut candidates);
            candidates.sort_by(|left, right| {
                left.source_path
                    .cmp(&right.source_path)
                    .then(left.value.cmp(&right.value))
            });
            Some(AttributeInventoryDocument {
                document_key,
                person_id: person.person_id.as_str().to_string(),
                display_name: person.display_name.clone(),
                source_document_id,
                source_canonical_uri,
                last_activity: person.last_activity,
                candidates,
            })
        })
        .collect::<Vec<_>>();

    documents.sort_by(|left, right| {
        right
            .last_activity
            .cmp(&left.last_activity)
            .then(left.display_name.cmp(&right.display_name))
            .then(left.person_id.cmp(&right.person_id))
    });
    documents
}

fn collect_string_candidates(
    value: &serde_json::Value,
    source_path: Option<&str>,
    attribute_path: Option<&str>,
    out: &mut Vec<AttributeCandidate>,
) {
    match value {
        serde_json::Value::Null => {}
        serde_json::Value::String(raw) => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return;
            }
            let source_path = source_path.unwrap_or("").to_string();
            let attribute_path = attribute_path.unwrap_or("").to_string();
            if is_metadata_path(&attribute_path) {
                return;
            }
            out.push(AttributeCandidate {
                candidate_key: format!("{source_path}={trimmed}"),
                label: label_for_attribute_path(&attribute_path),
                value: trimmed.to_string(),
                value_kind: classify_value_kind(trimmed),
                source_path,
                attribute_path,
            });
        }
        serde_json::Value::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                let next_source = match source_path {
                    Some(base) if !base.is_empty() => format!("{base}[{index}]"),
                    _ => format!("[{index}]"),
                };
                let next_attribute = match attribute_path {
                    Some(base) if !base.is_empty() => format!("{base}[]"),
                    _ => "[]".to_string(),
                };
                collect_string_candidates(item, Some(&next_source), Some(&next_attribute), out);
            }
        }
        serde_json::Value::Object(object) => {
            for (key, item) in object {
                let next_source = append_object_segment(source_path, key);
                let next_attribute = append_object_segment(attribute_path, &canonicalize_segment(key));
                collect_string_candidates(item, Some(&next_source), Some(&next_attribute), out);
            }
        }
        serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
}

/// Paths that represent system metadata rather than student attributes.
const METADATA_PATHS: &[&str] = &[
    "profile.source_slide_object_id",
    "profile.source_document_id",
    "profile.source_canonical_uri",
    "profile.thumbnail_blob_ref",
    "profile.thumbnail_url",
    "profile.companion_to_slide_object_id",
    "profile.profile_pic.url",
    "profile.gallery_images[].url",
];

fn is_metadata_path(attribute_path: &str) -> bool {
    METADATA_PATHS.iter().any(|&pattern| attribute_path == pattern)
}

fn append_object_segment(base: Option<&str>, key: &str) -> String {
    match base {
        Some(prefix) if !prefix.is_empty() => format!("{prefix}.{key}"),
        _ => key.to_string(),
    }
}

fn canonicalize_segment(key: &str) -> String {
    let mut normalized = String::new();
    let mut last_was_separator = false;
    let mut previous_was_lowercase = false;
    for ch in key.chars() {
        if ch.is_ascii_alphanumeric() {
            if ch.is_ascii_uppercase() && previous_was_lowercase && !normalized.ends_with('_') {
                normalized.push('_');
            }
            normalized.push(ch.to_ascii_lowercase());
            last_was_separator = false;
            previous_was_lowercase = ch.is_ascii_lowercase();
        } else if !last_was_separator && !normalized.is_empty() {
            normalized.push('_');
            last_was_separator = true;
            previous_was_lowercase = false;
        }
    }
    normalized.trim_matches('_').to_string()
}

fn classify_value_kind(value: &str) -> CandidateValueKind {
    if value.starts_with("http://") || value.starts_with("https://") {
        CandidateValueKind::Url
    } else if value.len() > 120 || value.contains('\n') {
        CandidateValueKind::LongText
    } else {
        CandidateValueKind::ShortText
    }
}

fn label_for_attribute_path(attribute_path: &str) -> String {
    let leaf = attribute_path
        .rsplit('.')
        .next()
        .unwrap_or(attribute_path)
        .replace("[]", "")
        .replace('_', " ");
    let mut chars = leaf.chars();
    match chars.next() {
        Some(first) => format!("{}{}", first.to_ascii_uppercase(), chars.collect::<String>()),
        None => "Value".to_string(),
    }
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
        self.reviewed_candidates.contains_key(&candidate.candidate_key)
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
            return Err("attribute name must contain at least one alphanumeric character".to_string());
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
        self.known_attributes
            .sort_by(|left, right| left.display_name.cmp(&right.display_name).then(left.id.cmp(&right.id)));
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
        let candidate_leaf = normalized_leaf(&candidate.attribute_path);
        let mut suggestions = self
            .known_attributes
            .iter()
            .filter_map(|attribute| {
                let mut score = 0usize;
                if attribute
                    .source_paths
                    .iter()
                    .any(|path| path == &candidate.attribute_path)
                {
                    score += 100;
                }
                if normalized_leaf(&attribute.id) == candidate_leaf {
                    score += 80;
                }
                if normalized_leaf(&attribute.display_name) == candidate_leaf {
                    score += 75;
                }
                if attribute
                    .source_paths
                    .iter()
                    .any(|path| normalized_leaf(path) == candidate_leaf)
                {
                    score += 60;
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
                let mut aliases = existing.source_paths.iter().cloned().collect::<BTreeSet<_>>();
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
        self.known_attributes
            .sort_by(|left, right| left.display_name.cmp(&right.display_name).then(left.id.cmp(&right.id)));
    }
}

pub fn summarize_documents(documents: &[AttributeInventoryDocument]) -> Vec<DiscoveredAttributeGroup> {
    let mut groups = BTreeMap::<String, DiscoveredAttributeGroup>::new();
    let mut document_sets = BTreeMap::<String, BTreeSet<String>>::new();
    for document in documents {
        for candidate in &document.candidates {
            let group = groups
                .entry(candidate.attribute_path.clone())
                .or_insert_with(|| DiscoveredAttributeGroup {
                    attribute_path: candidate.attribute_path.clone(),
                    label: candidate.label.clone(),
                    value_kind: candidate.value_kind,
                    occurrence_count: 0,
                    document_count: 0,
                    source_paths: Vec::new(),
                    example_values: Vec::new(),
                });
            group.occurrence_count += 1;
            if !group.source_paths.contains(&candidate.source_path) {
                group.source_paths.push(candidate.source_path.clone());
            }
            if !group.example_values.contains(&candidate.value) && group.example_values.len() < 8 {
                group.example_values.push(candidate.value.clone());
            }
            document_sets
                .entry(candidate.attribute_path.clone())
                .or_default()
                .insert(document.document_key.clone());
        }
    }
    for (path, document_ids) in document_sets {
        if let Some(group) = groups.get_mut(&path) {
            group.document_count = document_ids.len();
        }
    }
    groups.into_values().collect()
}

pub fn write_json_file<T: Serialize>(path: &Path, value: &T) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let raw = serde_json::to_string_pretty(value)?;
    fs::write(path, raw)?;
    Ok(())
}

pub fn read_alias_catalog(path: &Path) -> Result<AttributeAliasCatalog, Box<dyn std::error::Error>> {
    let raw = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&raw)?)
}

fn absorb_candidate(attribute: &mut KnownAttribute, candidate: &AttributeCandidate) {
    let mut source_paths = attribute.source_paths.iter().cloned().collect::<BTreeSet<_>>();
    source_paths.insert(candidate.attribute_path.clone());
    attribute.source_paths = source_paths.into_iter().collect();

    let mut examples = attribute.examples.iter().cloned().collect::<BTreeSet<_>>();
    examples.insert(candidate.value.clone());
    attribute.examples = examples.into_iter().take(5).collect();
    attribute.review_count += 1;
}

fn normalize_attribute_id(value: &str) -> String {
    let mut id = String::new();
    let mut last_was_dash = false;
    for ch in value.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            id.push(ch.to_ascii_lowercase());
            last_was_dash = false;
        } else if !last_was_dash && !id.is_empty() {
            id.push('-');
            last_was_dash = true;
        }
    }
    id.trim_matches('-').to_string()
}

fn normalized_leaf(value: &str) -> String {
    value
        .rsplit('.')
        .next()
        .unwrap_or(value)
        .replace("[]", "")
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric())
        .map(|ch| ch.to_ascii_lowercase())
        .collect()
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
    use crate::domain::EntityRef;
    use crate::person_page::types::{FrontendProfile, PersonPageOutput, PersonProfile};
    use crate::slide_analysis::types::{GalleryImage, ProfilePic, StudentProfile, StudentProperties};

    #[test]
    fn build_inventory_documents_flattens_dynamic_strings() {
        let snapshot = ProjectionSnapshot {
            identity: Default::default(),
            person_page: PersonPageOutput {
                profiles: vec![PersonProfile {
                    person_id: EntityRef::new("person:test"),
                    display_name: "Test User".into(),
                    self_intro_text: Some("Hello world".into()),
                    self_intro_slide_id: None,
                    self_intro_thumbnail: None,
                    identities: Vec::new(),
                    source_count: 1,
                    last_activity: None,
                    profile_updated_at: Utc::now(),
                    frontend_profile: Some(FrontendProfile {
                        source_document_id: "document:gslides:test#slide:1".into(),
                        source_canonical_uri: Some("https://example.com/slide".into()),
                        thumbnail_ref: None,
                        thumbnail_url: None,
                        profile: StudentProfile {
                            email: Some("test@example.com".into()),
                            generated_email: None,
                            name: "Test User".into(),
                            bio_text: Some("Longer biography".into()),
                            profile_pic: Some(ProfilePic {
                                coordinates: None,
                                description: Some("Portrait".into()),
                                url: Some("https://example.com/p.png".into()),
                            }),
                            gallery_images: vec![GalleryImage {
                                coordinates: None,
                                description: Some("Club activity".into()),
                                url: None,
                            }],
                            properties: StudentProperties {
                                hobbies: vec!["Basketball".into()],
                                ..StudentProperties::default()
                            },
                            attributes: vec!["friendly".into()],
                            source_slide_object_id: None,
                            source_document_id: Some("document:gslides:test#slide:1".into()),
                            source_canonical_uri: Some("https://example.com/slide".into()),
                            thumbnail_blob_ref: None,
                            thumbnail_url: None,
                            companion_to_slide_object_id: None,
                        },
                    }),
                }],
                ..PersonPageOutput::default()
            },
            built_at: Utc::now(),
        };

        let docs = build_inventory_documents(&snapshot);
        let doc = &docs[0];
        assert!(doc
            .candidates
            .iter()
            .any(|candidate| candidate.attribute_path == "profile.properties.hobbies[]" && candidate.value == "Basketball"));
        assert!(doc
            .candidates
            .iter()
            .any(|candidate| candidate.attribute_path == "profile.gallery_images[].description" && candidate.value == "Club activity"));
        assert!(doc
            .candidates
            .iter()
            .any(|candidate| candidate.attribute_path == "person.self_intro_text" && candidate.value == "Hello world"));
        // Metadata paths must be excluded (M)
        assert!(!doc
            .candidates
            .iter()
            .any(|candidate| candidate.attribute_path == "profile.source_document_id"));
        assert!(!doc
            .candidates
            .iter()
            .any(|candidate| candidate.attribute_path == "profile.source_canonical_uri"));
        assert!(!doc
            .candidates
            .iter()
            .any(|candidate| candidate.attribute_path == "profile.profile_pic.url"));
    }

    #[test]
    fn state_suggests_exact_path_matches_first() {
        let candidate = AttributeCandidate {
            candidate_key: "k".into(),
            attribute_path: "profile.properties.hobbies[]".into(),
            source_path: "profile.properties.hobbies[0]".into(),
            label: "Hobbies".into(),
            value: "Basketball".into(),
            value_kind: CandidateValueKind::ShortText,
        };
        let state = AttributeInventoryState {
            version: 1,
            known_attributes: vec![
                KnownAttribute {
                    id: "hobbies".into(),
                    display_name: "Hobbies".into(),
                    source_paths: vec!["profile.properties.hobbies[]".into()],
                    examples: vec![],
                    review_count: 1,
                },
                KnownAttribute {
                    id: "likes".into(),
                    display_name: "Likes".into(),
                    source_paths: vec!["profile.properties.likes[]".into()],
                    examples: vec![],
                    review_count: 1,
                },
            ],
            reviewed_candidates: BTreeMap::new(),
        };

        let suggestions = state.suggestions_for(&candidate);
        assert_eq!(suggestions[0].id, "hobbies");
    }

    #[test]
    fn parse_inventory_commands_supports_review_actions() {
        assert_eq!(parse_inventory_command("1").unwrap(), InventoryCommand::PickSuggestion(1));
        assert_eq!(
            parse_inventory_command("e hobbies").unwrap(),
            InventoryCommand::MapExisting("hobbies".into())
        );
        assert_eq!(
            parse_inventory_command("n Favorite Food").unwrap(),
            InventoryCommand::CreateNew("Favorite Food".into())
        );
        assert_eq!(parse_inventory_command("s").unwrap(), InventoryCommand::Skip);
        assert_eq!(parse_inventory_command("q").unwrap(), InventoryCommand::Quit);
    }

    #[test]
    fn summarize_documents_groups_candidates_by_attribute_path() {
        let documents = vec![AttributeInventoryDocument {
            document_key: "doc-1".into(),
            person_id: "person:test".into(),
            display_name: "Test".into(),
            source_document_id: None,
            source_canonical_uri: None,
            last_activity: None,
            candidates: vec![
                AttributeCandidate {
                    candidate_key: "a".into(),
                    attribute_path: "profile.properties.hobbies[]".into(),
                    source_path: "profile.properties.Hobbies[0]".into(),
                    label: "Hobbies".into(),
                    value: "Basketball".into(),
                    value_kind: CandidateValueKind::ShortText,
                },
                AttributeCandidate {
                    candidate_key: "b".into(),
                    attribute_path: "profile.properties.hobbies[]".into(),
                    source_path: "profile.properties.Hobbies[1]".into(),
                    label: "Hobbies".into(),
                    value: "Piano".into(),
                    value_kind: CandidateValueKind::ShortText,
                },
            ],
        }];

        let groups = summarize_documents(&documents);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].attribute_path, "profile.properties.hobbies[]");
        assert_eq!(groups[0].occurrence_count, 2);
    }
}
