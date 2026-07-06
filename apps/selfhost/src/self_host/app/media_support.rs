use super::*;

pub(super) fn analysis_record_needs_refresh(
    record: &lethe_core::domain::SupplementalRecord,
    analysis_model: &str,
) -> bool {
    !analysis_record_is_rich(record) || record.model_version.as_deref() != Some(analysis_model)
}

pub(super) fn should_merge_companion_slide(
    primary: &StudentProfile,
    companion: &StudentProfile,
    observation: &Observation,
) -> bool {
    if !profile_has_content(companion) {
        return false;
    }

    if companion
        .email
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
    {
        return false;
    }

    let deck_title = observation
        .payload
        .get("title")
        .and_then(|value| value.as_str())
        .unwrap_or_default();
    let primary_name = normalize_profile_name(&primary.name);
    let companion_name = normalize_profile_name(&companion.name);

    companion_name.is_empty()
        || companion_name == normalize_profile_name(deck_title)
        || (!primary_name.is_empty() && companion_name == primary_name)
}

pub(super) fn profile_has_content(profile: &StudentProfile) -> bool {
    profile.has_meaningful_content() || profile.thumbnail_url.is_some()
}

pub(super) fn normalize_profile_name(value: &str) -> String {
    value
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect::<String>()
        .to_lowercase()
}

pub(super) fn merge_companion_profile(primary: &mut StudentProfile, companion: &StudentProfile) {
    if let Some(companion_thumbnail_url) = &companion.thumbnail_url {
        let description = companion
            .bio_text
            .clone()
            .or_else(|| {
                companion
                    .profile_pic
                    .as_ref()
                    .and_then(|pic| pic.description.clone())
            })
            .or_else(|| Some("Continuation slide".to_string()));
        primary.gallery_images.push(GalleryImage {
            coordinates: None,
            description,
            url: Some(companion_thumbnail_url.clone()),
        });
    }

    primary
        .gallery_images
        .extend(companion.gallery_images.clone());

    if let Some(companion_bio) = companion
        .bio_text
        .as_ref()
        .map(|text| text.trim())
        .filter(|text| !text.is_empty())
    {
        match primary.bio_text.as_mut() {
            Some(primary_bio) if !primary_bio.contains(companion_bio) => {
                primary_bio.push_str("\n\n");
                primary_bio.push_str(companion_bio);
            }
            None => primary.bio_text = Some(companion_bio.to_string()),
            _ => {}
        }
    }

    if primary.profile_pic.is_none() {
        primary.profile_pic = companion.profile_pic.clone();
    }

    merge_optional_field(
        &mut primary.properties.nickname,
        &companion.properties.nickname,
    );
    merge_optional_field(
        &mut primary.properties.birthplace,
        &companion.properties.birthplace,
    );
    merge_optional_field(&mut primary.properties.dob, &companion.properties.dob);
    merge_optional_field(&mut primary.properties.major, &companion.properties.major);
    merge_optional_field(
        &mut primary.properties.affiliation,
        &companion.properties.affiliation,
    );
    merge_optional_field(&mut primary.properties.mbti, &companion.properties.mbti);
    merge_optional_field(&mut primary.properties.sns, &companion.properties.sns);
    merge_optional_field(
        &mut primary.properties.dislikes,
        &companion.properties.dislikes,
    );
    merge_optional_field(
        &mut primary.properties.new_challenges,
        &companion.properties.new_challenges,
    );
    merge_optional_field(
        &mut primary.properties.ask_me_about,
        &companion.properties.ask_me_about,
    );
    merge_optional_field(
        &mut primary.properties.turning_point,
        &companion.properties.turning_point,
    );
    merge_optional_field(&mut primary.properties.btw, &companion.properties.btw);
    merge_optional_field(
        &mut primary.properties.message,
        &companion.properties.message,
    );

    append_distinct_strings(
        &mut primary.properties.hobbies,
        &companion.properties.hobbies,
    );
    append_distinct_strings(
        &mut primary.properties.interests,
        &companion.properties.interests,
    );
    append_distinct_strings(&mut primary.properties.likes, &companion.properties.likes);
    append_distinct_strings(
        &mut primary.properties.hashtags,
        &companion.properties.hashtags,
    );
    append_distinct_strings(&mut primary.attributes, &companion.attributes);
}

pub(super) fn resolve_slide_image_urls(
    presentation: &lethe_adapter_gslides::gslides::client::PresentationNative,
    slide: &lethe_adapter_gslides::gslides::client::SlideNative,
    profile: &mut StudentProfile,
) {
    let Some(page_size) = presentation.page_size.as_ref() else {
        return;
    };
    if page_size.width_emu <= 0 || page_size.height_emu <= 0 {
        return;
    }

    let mut available_images = slide_image_candidates(slide);
    if available_images.is_empty() {
        return;
    }

    if let Some(profile_pic) = profile.profile_pic.as_mut() {
        if let Some(coordinates) = profile_pic.coordinates.as_ref() {
            let target = normalize_coordinate_target(coordinates, page_size);
            if let Some(matched) = find_nearest_slide_image(target, &available_images) {
                let matched_object_id = matched.object_id.clone();
                let matched_url = apply_rotation_to_google_image_url(
                    &matched.content_url,
                    matched.rotation_degrees,
                );
                profile_pic.url = Some(matched_url);
                available_images.retain(|image| image.object_id != matched_object_id);
            }
        } else if profile_pic.url.is_none() {
            let first_image = available_images.remove(0);
            profile_pic.url = Some(apply_rotation_to_google_image_url(
                &first_image.content_url,
                first_image.rotation_degrees,
            ));
        }
    }

    for gallery_image in &mut profile.gallery_images {
        if gallery_image
            .url
            .as_deref()
            .is_some_and(|url| url.starts_with("http"))
        {
            continue;
        }
        let Some(coordinates) = gallery_image.coordinates.as_ref() else {
            continue;
        };
        let target = normalize_coordinate_target(coordinates, page_size);
        let Some(matched) = find_nearest_slide_image(target, &available_images) else {
            continue;
        };
        let matched_object_id = matched.object_id.clone();
        let matched_url =
            apply_rotation_to_google_image_url(&matched.content_url, matched.rotation_degrees);
        gallery_image.url = Some(matched_url);
        available_images.retain(|image| image.object_id != matched_object_id);
    }

    if profile.profile_pic.is_none() && !available_images.is_empty() {
        let first_image = available_images.remove(0);
        profile.profile_pic = Some(ProfilePic {
            coordinates: None,
            description: None,
            url: Some(apply_rotation_to_google_image_url(
                &first_image.content_url,
                first_image.rotation_degrees,
            )),
        });
    }
}

pub(super) fn slide_image_candidates(
    slide: &lethe_adapter_gslides::gslides::client::SlideNative,
) -> Vec<SlideImageCandidate> {
    slide
        .page_elements
        .iter()
        .enumerate()
        .filter_map(|(z_index, element)| slide_image_candidate_from_element(element, z_index))
        .collect()
}

pub(super) fn slide_image_candidate_from_element(
    element: &serde_json::Value,
    z_index: usize,
) -> Option<SlideImageCandidate> {
    let image = element.get("image")?;
    let content_url = image.get("contentUrl")?.as_str()?.to_string();
    let object_id = element.get("objectId")?.as_str()?.to_string();
    let size = element.get("size")?;
    let width = size
        .get("width")
        .and_then(|value| value.get("magnitude"))
        .and_then(serde_json::Value::as_f64)?;
    let height = size
        .get("height")
        .and_then(|value| value.get("magnitude"))
        .and_then(serde_json::Value::as_f64)?;
    let transform = element.get("transform")?;
    let translate_x = transform
        .get("translateX")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or_default();
    let translate_y = transform
        .get("translateY")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or_default();
    let scale_x = transform
        .get("scaleX")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or(1.0);
    let scale_y = transform
        .get("scaleY")
        .and_then(serde_json::Value::as_f64)
        .unwrap_or(1.0);
    let rotation_degrees = image
        .get("imageProperties")
        .and_then(|value| value.get("cropProperties"))
        .map(|_| 0)
        .unwrap_or(0);

    Some(SlideImageCandidate {
        object_id,
        content_url,
        center_x: translate_x + (width * scale_x.abs() / 2.0),
        center_y: translate_y + (height * scale_y.abs() / 2.0),
        z_index,
        rotation_degrees,
    })
}

pub(super) fn normalize_coordinate_target(
    coordinates: &ImageCoordinates,
    page_size: &lethe_adapter_gslides::gslides::client::PageSize,
) -> (f64, f64) {
    let x_pct = normalize_selection_percent(coordinates.x);
    let y_pct = normalize_selection_percent(coordinates.y);
    (
        (x_pct / 100.0) * page_size.width_emu as f64,
        (y_pct / 100.0) * page_size.height_emu as f64,
    )
}

pub(super) fn normalize_selection_percent(value: f64) -> f64 {
    if value <= 100.0 {
        value.max(0.0)
    } else if value <= 1000.0 {
        (value / 10.0).max(0.0)
    } else {
        100.0
    }
}

pub(super) fn find_nearest_slide_image(
    target: (f64, f64),
    candidates: &[SlideImageCandidate],
) -> Option<&SlideImageCandidate> {
    if candidates.is_empty() {
        return None;
    }
    let mut with_distance = candidates
        .iter()
        .map(|candidate| {
            let dx = candidate.center_x - target.0;
            let dy = candidate.center_y - target.1;
            let distance = (dx * dx + dy * dy).sqrt();
            (candidate, distance)
        })
        .collect::<Vec<_>>();
    let min_distance = with_distance
        .iter()
        .map(|(_, distance)| *distance)
        .fold(f64::INFINITY, f64::min);
    let tolerance = 50.0;
    with_distance.retain(|(_, distance)| *distance <= min_distance + tolerance);
    with_distance.sort_by(|left, right| {
        right
            .0
            .z_index
            .cmp(&left.0.z_index)
            .then_with(|| left.1.total_cmp(&right.1))
    });
    with_distance
        .into_iter()
        .map(|(candidate, _)| candidate)
        .next()
}

pub(super) fn apply_rotation_to_google_image_url(url: &str, rotation_degrees: i32) -> String {
    if rotation_degrees == 0 || !url.contains("googleusercontent.com") {
        return url.to_string();
    }
    let mut parts = url.splitn(2, '?');
    let mut base = parts.next().unwrap_or_default().to_string();
    let query = parts
        .next()
        .map(|value| format!("?{value}"))
        .unwrap_or_default();
    if base.contains('=') {
        base.push_str(&format!("-r{rotation_degrees}"));
    } else {
        base.push_str(&format!("=r{rotation_degrees}"));
    }
    format!("{base}{query}")
}

pub(super) fn merge_optional_field(target: &mut Option<String>, source: &Option<String>) {
    if target
        .as_ref()
        .is_some_and(|value| !value.trim().is_empty())
    {
        return;
    }
    *target = source.clone();
}

pub(super) fn append_distinct_strings(target: &mut Vec<String>, source: &[String]) {
    for value in source {
        if !target.contains(value) {
            target.push(value.clone());
        }
    }
}

pub(super) fn ensure_profile_identifier(profile: &mut StudentProfile, slide_object_id: &str) {
    if profile
        .email
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
        || profile
            .generated_email
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
    {
        return;
    }

    let fallback = slide_object_id
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() {
                ch.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    profile.generated_email = Some(format!("slide-{fallback}@hlab.college"));
}

pub(super) fn find_slide_analysis_record<'a>(
    records: &'a [lethe_core::domain::SupplementalRecord],
    presentation_id: &str,
    slide_object_id: &str,
) -> Option<&'a lethe_core::domain::SupplementalRecord> {
    records.iter().find(|record| {
        if record.kind != "slide-analysis" {
            return false;
        }
        let Ok(profile) = serde_json::from_value::<StudentProfile>(record.payload.clone()) else {
            return false;
        };
        profile.source_document_id.as_deref()
            == Some(&format!(
                "document:gslides:{presentation_id}#slide:{slide_object_id}"
            ))
            || profile.source_slide_object_id.as_deref() == Some(slide_object_id)
    })
}

pub(super) fn analysis_record_is_rich(record: &lethe_core::domain::SupplementalRecord) -> bool {
    let Ok(mut profile) = serde_json::from_value::<StudentProfile>(record.payload.clone()) else {
        return false;
    };
    profile.normalize_in_place();
    profile.has_meaningful_content()
}

pub(super) fn audit_kind_for_scope(scope: &str) -> AuditEventKind {
    match scope {
        "admin:sync" | "write:observations" | "write:supplemental" => {
            AuditEventKind::WriteExecution
        }
        "read:persons" | "read:timeline" => AuditEventKind::ReadRestricted,
        _ => AuditEventKind::ReadRestricted,
    }
}
