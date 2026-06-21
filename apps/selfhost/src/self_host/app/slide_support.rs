pub(super) fn ranked_self_intro_slide_indices(
    presentation: &lethe_adapter_gslides::gslides::client::PresentationNative,
    limit: usize,
) -> Vec<usize> {
    let mut ranked = presentation
        .slides
        .iter()
        .enumerate()
        .map(|(index, slide)| {
            (
                index,
                score_self_intro_slide(slide, index, presentation.slides.len()),
            )
        })
        .collect::<Vec<_>>();

    ranked.sort_by(|left, right| right.1.cmp(&left.1).then(left.0.cmp(&right.0)));

    ranked
        .into_iter()
        .take(limit.min(presentation.slides.len()))
        .map(|(index, _)| index)
        .collect()
}

pub(super) fn score_self_intro_slide(
    slide: &lethe_adapter_gslides::gslides::client::SlideNative,
    index: usize,
    total_slides: usize,
) -> i32 {
    let fragments = extract_slide_text_fragments(slide);
    if fragments.is_empty() {
        return 0;
    }

    let text = fragments.join("\n").to_lowercase();
    let mut score = 0i32;

    if find_first_email(&fragments).is_some() {
        score += 8;
    }

    score += keyword_score(
        &text,
        &[
            "自己紹介",
            "self intro",
            "self-introduction",
            "about me",
            "profile",
            "プロフィール",
            "my name",
            "名前",
        ],
        6,
    );
    score += keyword_score(
        &text,
        &[
            "nickname",
            "ニックネーム",
            "mbti",
            "birthplace",
            "出身",
            "hobby",
            "hobbies",
            "趣味",
            "interest",
            "interests",
            "好き",
            "likes",
            "dislikes",
            "所属",
            "affiliation",
            "major",
            "学部",
            "学科",
            "message",
            "challenge",
            "turning point",
            "ask me",
        ],
        2,
    );
    score += keyword_score(&text, &["私", "ぼく", "僕", "俺", "i am", "i'm"], 1);
    score -= keyword_score(
        &text,
        &[
            "agenda",
            "project",
            "summary",
            "overview",
            "roadmap",
            "schedule",
            "目次",
            "進捗",
            "研究計画",
            "team",
        ],
        2,
    );

    if fragments.len() >= 3 {
        score += 2;
    }
    if slide_has_image_elements(slide) {
        score += 2;
    }

    let early_bonus = (total_slides.saturating_sub(index)).min(3) as i32;
    score + early_bonus
}

pub(super) fn keyword_score(text: &str, keywords: &[&str], weight: i32) -> i32 {
    keywords
        .iter()
        .filter(|keyword| text.contains(**keyword))
        .count() as i32
        * weight
}

pub(super) fn extract_slide_text_fragments(
    slide: &lethe_adapter_gslides::gslides::client::SlideNative,
) -> Vec<String> {
    let mut fragments = Vec::new();
    for element in &slide.page_elements {
        collect_slide_text_values(element, None, &mut fragments);
    }

    let mut deduped = Vec::new();
    for fragment in fragments {
        let trimmed = fragment.trim();
        if trimmed.is_empty() {
            continue;
        }
        if !deduped.iter().any(|existing: &String| existing == trimmed) {
            deduped.push(trimmed.to_string());
        }
    }
    deduped
}

pub(super) fn collect_slide_text_values(
    value: &serde_json::Value,
    key: Option<&str>,
    fragments: &mut Vec<String>,
) {
    match value {
        serde_json::Value::Object(map) => {
            for (child_key, child_value) in map {
                collect_slide_text_values(child_value, Some(child_key.as_str()), fragments);
            }
        }
        serde_json::Value::Array(values) => {
            for child in values {
                collect_slide_text_values(child, key, fragments);
            }
        }
        serde_json::Value::String(text)
            if matches!(key, Some("content") | Some("description") | Some("title")) =>
        {
            let trimmed = text.trim();
            if !trimmed.is_empty() {
                fragments.push(trimmed.to_string());
            }
        }
        _ => {}
    }
}

pub(super) fn slide_has_image_elements(
    slide: &lethe_adapter_gslides::gslides::client::SlideNative,
) -> bool {
    slide.page_elements.iter().any(|element| {
        element.get("image").is_some()
            || element
                .get("shape")
                .and_then(|shape| shape.get("shapeType"))
                .and_then(|value| value.as_str())
                == Some("RECTANGLE")
    })
}

pub(super) fn find_first_email(fragments: &[String]) -> Option<String> {
    fragments.iter().find_map(|fragment| {
        fragment
            .split_whitespace()
            .map(|token| {
                token.trim_matches(|ch: char| {
                    matches!(ch, '<' | '>' | '(' | ')' | '[' | ']' | ',' | ';')
                })
            })
            .find(|token| token.contains('@') && token.contains('.'))
            .map(|token| token.to_lowercase())
    })
}

#[cfg(test)]
pub(super) fn infer_profile_name_from_fragments(fragments: &[String]) -> Option<String> {
    fragments.iter().find_map(|fragment| {
        let trimmed = fragment.trim();
        if trimmed.is_empty() || trimmed.contains('@') || trimmed.len() > 40 {
            return None;
        }
        let lowered = trimmed.to_lowercase();
        if [
            "自己紹介",
            "self intro",
            "profile",
            "about me",
            "nickname",
            "mbti",
        ]
        .iter()
        .any(|keyword| lowered.contains(keyword))
        {
            return None;
        }
        Some(trimmed.to_string())
    })
}
