use super::*;

pub(super) fn build_page_blocks(
    profile: &StudentProfile,
    profile_pic: Option<&NotionMediaRef>,
    gallery_urls: &[(usize, NotionMediaRef)],
    source_url: Option<&str>,
) -> Vec<serde_json::Value> {
    let mut sections = Vec::new();

    if let Some(section) = build_bio_section(profile.bio_text.as_deref()) {
        sections.push(section);
    }
    if let Some(section) = build_about_section(&profile.properties, profile_pic) {
        sections.push(section);
    }
    if let Some(section) = build_highlights_section(&profile.properties) {
        sections.push(section);
    }
    if let Some(section) = build_toggle_section(&profile.properties) {
        sections.push(section);
    }
    if let Some(section) = build_gallery_section(profile, gallery_urls) {
        sections.push(section);
    }
    if let Some(section) =
        build_source_section(source_url.or(profile.source_canonical_uri.as_deref()))
    {
        sections.push(section);
    }

    interleave_dividers(sections)
}

pub(super) fn build_bio_section(bio_text: Option<&str>) -> Option<Vec<serde_json::Value>> {
    let bio_text = bio_text.map(str::trim).filter(|text| !text.is_empty())?;
    Some(vec![serde_json::json!({
        "object": "block",
        "type": "callout",
        "callout": {
            "icon": { "type": "emoji", "emoji": "💬" },
            "rich_text": [plain_rich_text(bio_text)],
            "color": "gray_background"
        }
    })])
}

pub(super) fn build_about_section(
    properties: &StudentProperties,
    profile_pic: Option<&NotionMediaRef>,
) -> Option<Vec<serde_json::Value>> {
    let rows = build_about_rows(properties);
    if rows.is_empty() && profile_pic.is_none() {
        return None;
    }

    let mut section = vec![heading_2_block("About")];
    if let Some(profile_pic) = profile_pic {
        section.push(serde_json::json!({
            "object": "block",
            "type": "column_list",
            "column_list": {
                "children": [
                    {
                        "object": "block",
                        "type": "column",
                        "column": { "children": [profile_pic.to_image_block()] }
                    },
                    {
                        "object": "block",
                        "type": "column",
                        "column": { "children": [about_table_block(&rows)] }
                    }
                ]
            }
        }));
    } else {
        section.push(about_table_block(&rows));
    }
    Some(section)
}

pub(super) fn build_about_rows(properties: &StudentProperties) -> Vec<(String, AboutValue)> {
    let mut rows = Vec::new();
    if let Some(value) = properties
        .nickname
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        rows.push(("呼び名".to_string(), AboutValue::Text(value.to_string())));
    }
    if let Some(value) = properties
        .birthplace
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        rows.push(("出身".to_string(), AboutValue::Text(value.to_string())));
    }
    if let Some(value) = properties.dob.as_deref().filter(|value| !value.is_empty()) {
        rows.push(("誕生日".to_string(), AboutValue::Text(value.to_string())));
    }
    if let Some(value) = properties
        .major
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        rows.push(("専攻".to_string(), AboutValue::Text(value.to_string())));
    }
    if let Some(value) = properties
        .affiliation
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        rows.push(("所属".to_string(), AboutValue::Text(value.to_string())));
    }
    if let Some(value) = properties.mbti.as_deref().filter(|value| !value.is_empty()) {
        rows.push(("MBTI".to_string(), AboutValue::Text(value.to_string())));
    }
    if let Some(value) = properties.sns.as_deref().filter(|value| !value.is_empty()) {
        rows.push((
            "SNS".to_string(),
            if value.starts_with("http://") || value.starts_with("https://") {
                AboutValue::Link(value.to_string())
            } else {
                AboutValue::Text(value.to_string())
            },
        ));
    }
    rows
}

pub(super) fn about_table_block(rows: &[(String, AboutValue)]) -> serde_json::Value {
    serde_json::json!({
        "object": "block",
        "type": "table",
        "table": {
            "table_width": 2,
            "has_column_header": false,
            "has_row_header": false,
            "children": rows.iter().map(|(label, value)| {
                serde_json::json!({
                    "object": "block",
                    "type": "table_row",
                    "table_row": {
                        "cells": [
                            [bold_rich_text(label)],
                            [about_value_rich_text(value)]
                        ]
                    }
                })
            }).collect::<Vec<_>>()
        }
    })
}

pub(super) fn build_highlights_section(
    properties: &StudentProperties,
) -> Option<Vec<serde_json::Value>> {
    let mut section = vec![heading_2_block("Highlights")];
    let mut has_content = false;
    for (emoji, label, values) in [
        ("🎯", "Hobbies", &properties.hobbies),
        ("🔍", "Interests", &properties.interests),
        ("❤️", "Likes", &properties.likes),
    ] {
        let Some(values) = combine_list_texts([values.clone()]) else {
            continue;
        };
        has_content = true;
        section.push(serde_json::json!({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [
                    bold_rich_text(&format!("{emoji} {label}: ")),
                    plain_rich_text(&values)
                ]
            }
        }));
    }
    has_content.then_some(section)
}

pub(super) fn build_toggle_section(
    properties: &StudentProperties,
) -> Option<Vec<serde_json::Value>> {
    let mut toggles = Vec::new();
    for (emoji, title, value) in [
        ("🚀", "New Challenges", properties.new_challenges.as_deref()),
        ("💡", "Ask Me About", properties.ask_me_about.as_deref()),
        ("🔄", "Turning Point", properties.turning_point.as_deref()),
        ("💭", "BTW", properties.btw.as_deref()),
        ("✉️", "Message", properties.message.as_deref()),
        ("🙅", "Dislikes", properties.dislikes.as_deref()),
    ] {
        let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
            continue;
        };
        toggles.push(serde_json::json!({
            "object": "block",
            "type": "toggle",
            "toggle": {
                "rich_text": [bold_rich_text(&format!("{emoji} {title}"))],
                "children": [{
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [plain_rich_text(value)]
                    }
                }]
            }
        }));
    }
    (!toggles.is_empty()).then_some(toggles)
}

pub(super) fn build_gallery_section(
    profile: &StudentProfile,
    gallery_urls: &[(usize, NotionMediaRef)],
) -> Option<Vec<serde_json::Value>> {
    let gallery_urls = &gallery_urls[..gallery_urls.len().min(9)];
    if gallery_urls.is_empty() {
        return None;
    }

    let mut section = vec![heading_2_block("Gallery")];
    for chunk in gallery_urls.chunks(3) {
        let mut columns = Vec::new();
        let mut single_column_children = None;
        for (index, media) in chunk {
            let Some(image) = profile.gallery_images.get(*index) else {
                continue;
            };
            let mut children = vec![media.to_image_block()];
            if let Some(description) = image
                .description
                .as_deref()
                .filter(|value| !value.trim().is_empty())
            {
                children.push(serde_json::json!({
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "rich_text": [{
                            "type": "text",
                            "text": { "content": truncate_rich_text_content(description) },
                            "annotations": { "italic": true, "color": "gray" }
                        }]
                    }
                }));
            }
            if chunk.len() == 1 {
                single_column_children = Some(children);
                continue;
            }
            columns.push(serde_json::json!({
                "object": "block",
                "type": "column",
                "column": { "children": children }
            }));
        }
        if let Some(children) = single_column_children {
            section.extend(children);
            continue;
        }
        if !columns.is_empty() {
            section.push(serde_json::json!({
                "object": "block",
                "type": "column_list",
                "column_list": { "children": columns }
            }));
        }
    }
    Some(section)
}

pub(super) fn build_source_section(source_url: Option<&str>) -> Option<Vec<serde_json::Value>> {
    let source_url = source_url
        .map(str::trim)
        .filter(|value| !value.is_empty())?;
    Some(vec![
        heading_2_block("Source"),
        serde_json::json!({
            "object": "block",
            "type": "bookmark",
            "bookmark": {
                "url": source_url,
                "caption": [plain_rich_text("Google Slides — 自己紹介スライド原本")]
            }
        }),
    ])
}

pub(super) fn interleave_dividers(sections: Vec<Vec<serde_json::Value>>) -> Vec<serde_json::Value> {
    let mut blocks = Vec::new();
    for (index, section) in sections.into_iter().enumerate() {
        if index > 0 {
            blocks.push(serde_json::json!({
                "object": "block",
                "type": "divider",
                "divider": {}
            }));
        }
        blocks.extend(section);
    }
    blocks
}

pub(super) fn heading_2_block(title: &str) -> serde_json::Value {
    serde_json::json!({
        "object": "block",
        "type": "heading_2",
        "heading_2": {
            "rich_text": [plain_rich_text(title)]
        }
    })
}

pub(super) fn plain_rich_text(content: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "text",
        "text": { "content": truncate_rich_text_content(content) }
    })
}

pub(super) fn bold_rich_text(content: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "text",
        "text": { "content": truncate_rich_text_content(content) },
        "annotations": { "bold": true }
    })
}

pub(super) fn about_value_rich_text(value: &AboutValue) -> serde_json::Value {
    match value {
        AboutValue::Text(text) => plain_rich_text(text),
        AboutValue::Link(url) => serde_json::json!({
            "type": "text",
            "text": {
                "content": truncate_rich_text_content(url),
                "link": { "url": url }
            }
        }),
    }
}

pub(super) fn truncate_rich_text_content(content: &str) -> String {
    const MAX_CHARS: usize = 2000;
    let mut chars = content.chars();
    let truncated = chars.by_ref().take(MAX_CHARS).collect::<String>();
    if chars.next().is_some() {
        let mut shortened = truncated
            .chars()
            .take(MAX_CHARS.saturating_sub(1))
            .collect::<String>();
        shortened.push('…');
        shortened
    } else {
        truncated
    }
}

pub(super) enum AboutValue {
    Text(String),
    Link(String),
}

pub(super) fn json_text(value: &serde_json::Value) -> Option<String> {
    value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub(super) fn json_list_values(value: Option<&serde_json::Value>) -> Vec<String> {
    let Some(value) = value else {
        return Vec::new();
    };
    if let Some(array) = value.as_array() {
        array
            .iter()
            .filter_map(|item| item.as_str())
            .map(str::trim)
            .filter(|item| !item.is_empty())
            .map(str::to_string)
            .collect()
    } else {
        json_text(value).into_iter().collect()
    }
}

pub(super) fn json_list_text(value: Option<&serde_json::Value>) -> Option<String> {
    combine_list_texts([json_list_values(value)])
}

pub(super) fn combine_list_texts<const N: usize>(groups: [Vec<String>; N]) -> Option<String> {
    let mut seen = HashSet::new();
    let mut merged = Vec::new();
    for value in groups.into_iter().flatten() {
        let key = normalize_property_name(&value);
        if key.is_empty() || !seen.insert(key) {
            continue;
        }
        merged.push(value);
    }
    if merged.is_empty() {
        None
    } else {
        Some(merged.join(", "))
    }
}

pub(super) fn load_attribute_alias_catalog() -> Option<AttributeAliasCatalog> {
    let path = PathBuf::from("data").join("attribute_alias_catalog.json");
    let bytes = fs::read(path).ok()?;
    serde_json::from_slice(&bytes).ok()
}

pub(super) fn catalog_property_candidates(attribute: &AttributeAliasDefinition) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut candidates = Vec::new();
    for candidate in std::iter::once(attribute.display_name.as_str())
        .chain(std::iter::once(attribute.id.as_str()))
        .chain(attribute.aliases.iter().map(String::as_str))
    {
        let normalized = normalize_property_name(candidate);
        if normalized.is_empty() || !seen.insert(normalized) {
            continue;
        }
        candidates.push(candidate.to_string());
    }
    candidates
}

pub(super) fn catalog_attribute_value(
    attribute: &AttributeAliasDefinition,
    payload: &serde_json::Value,
    props: &serde_json::Value,
) -> Option<String> {
    match attribute.id.as_str() {
        "mbti" => json_text(&props["MBTI"]),
        "sns" => json_text(&props["SNS"]),
        "カレッジで挑戦したいこと" => json_text(&props["New Challenges"]),
        "カレッジ生に聞いてみたいこと" => json_text(&props["Ask Me About"]),
        "ハッシュタグ" => json_list_text(props.get("Hashtags")),
        "その他" => json_list_text(payload.get("attributes")),
        "一言" => json_text(&props["Message"]),
        "人生の転換期" => json_text(&props["Turning Point"]),
        "余談" => json_text(&props["BTW"]),
        "出身地" => json_text(&props["Birthplace"]),
        "呼ばれたい名前" => json_text(&props["Nickname"]),
        "好きなもの" => json_list_text(props.get("Likes")),
        "嫌いなもの" => json_text(&props["Dislikes"]),
        "専攻-興味分野" => combine_list_texts([
            json_list_values(props.get("Major")),
            json_list_values(props.get("Interests")),
        ]),
        "所属" => json_text(&props["Affiliation"]),
        "氏名" => payload
            .get("name")
            .and_then(|value| value.as_str())
            .map(ToOwned::to_owned),
        "生年月日" => json_text(&props["DoB"]),
        "趣味-特技" => json_list_text(props.get("Hobbies")),
        _ => None,
    }
}

pub(super) fn normalize_property_name(value: &str) -> String {
    value
        .chars()
        .filter(|ch| ch.is_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

pub(super) fn metadata_pointer<'a>(
    payload: &'a serde_json::Value,
    field: &str,
) -> Option<&'a serde_json::Value> {
    payload.pointer(&format!("/_lethe/{field}"))
}

pub(super) fn metadata_str<'a>(payload: &'a serde_json::Value, field: &str) -> Option<&'a str> {
    metadata_pointer(payload, field).and_then(|value| value.as_str())
}

pub(super) fn metadata_value(payload: &serde_json::Value, field: &str) -> Option<String> {
    metadata_str(payload, field).map(ToOwned::to_owned)
}

pub(super) fn metadata_bool(payload: &serde_json::Value, field: &str) -> Option<bool> {
    metadata_pointer(payload, field).and_then(|value| value.as_bool())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
