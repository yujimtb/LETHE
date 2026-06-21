use super::*;

pub(super) fn heuristic_profile_for_slide(
    observation: &Observation,
    slide: &lethe_adapter_gslides::gslides::client::SlideNative,
) -> Option<StudentProfile> {
    let fragments = extract_slide_text_fragments(slide);
    let email = find_first_email(&fragments);
    let name = infer_profile_name_from_fragments(&fragments).unwrap_or_else(|| {
        observation
            .payload
            .get("title")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown")
            .to_string()
    });
    let bio_lines = fragments
        .into_iter()
        .filter(|fragment| {
            let trimmed = fragment.trim();
            !trimmed.is_empty() && Some(trimmed) != email.as_deref() && trimmed != name
        })
        .take(6)
        .collect::<Vec<_>>();

    let mut profile = StudentProfile {
        email,
        generated_email: None,
        name,
        bio_text: (!bio_lines.is_empty()).then(|| bio_lines.join("\n")),
        profile_pic: None,
        gallery_images: vec![],
        properties: Default::default(),
        attributes: vec![],
        source_slide_object_id: None,
        source_document_id: None,
        source_canonical_uri: None,
        thumbnail_blob_ref: None,
        thumbnail_url: None,
        companion_to_slide_object_id: None,
    };
    profile.normalize_in_place();
    Some(profile)
}

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
        .and_then(|_| Some(0))
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

pub(super) fn ranked_notion_write_candidates_from_snapshot(
    snapshot: &ProjectionSnapshot,
    limit: usize,
    public_base_url: Option<&str>,
    source_image_index: &NotionSourceImageIndex,
) -> Vec<RankedNotionWriteCandidate> {
    let mut ranked = snapshot
        .person_page
        .profiles
        .iter()
        .filter_map(|person| {
            snapshot
                .person_page
                .activities
                .iter()
                .find(|activity| activity.person_id == person.person_id)?;
            let frontend = person.frontend_profile.as_ref()?;
            let write_record = notion_write_record_for_person(
                person,
                frontend,
                snapshot.built_at,
                public_base_url,
                source_image_index.get(frontend.source_document_id.as_str()),
            )?;

            Some(RankedNotionWriteCandidate {
                preview: NotionReviewCandidate {
                    rank: 0,
                    person_id: person.person_id.as_str().to_string(),
                    display_name: person.display_name.clone(),
                    entity_id: write_record.entity_id.clone(),
                    title: write_record.title.clone(),
                    last_activity: person.last_activity,
                    source_document_id: frontend.source_document_id.clone(),
                    source_canonical_uri: frontend.source_canonical_uri.clone(),
                },
                write_record,
            })
        })
        .collect::<Vec<_>>();

    ranked.sort_by(|left, right| {
        right
            .preview
            .last_activity
            .cmp(&left.preview.last_activity)
            .then(left.preview.display_name.cmp(&right.preview.display_name))
            .then(left.preview.entity_id.cmp(&right.preview.entity_id))
    });

    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    for mut candidate in ranked {
        if !seen.insert(candidate.preview.entity_id.clone()) {
            continue;
        }
        if deduped.len() >= limit {
            break;
        }
        candidate.preview.rank = deduped.len() + 1;
        deduped.push(candidate);
    }

    deduped
}

pub(super) fn notion_write_record_for_person(
    person: &PersonProfile,
    frontend: &FrontendProfile,
    synced_at: DateTime<Utc>,
    public_base_url: Option<&str>,
    source_images: Option<&Vec<NotionSourceImageCandidate>>,
) -> Option<WriteRecord> {
    let profile = frontend.profile.clone();
    let entity_id = profile
        .email
        .as_deref()
        .or(profile.generated_email.as_deref())
        .or(profile.source_document_id.as_deref())?
        .to_string();
    let title = notion_title_for_profile(&profile, frontend);
    let mut payload = serde_json::to_value(profile).ok()?;
    let payload_object = payload.as_object_mut()?;
    if let Some(stable_thumbnail_url) =
        notion_thumbnail_url_for_profile(&frontend.profile, public_base_url)
    {
        payload_object.insert(
            "thumbnail_url".to_string(),
            serde_json::Value::String(stable_thumbnail_url),
        );
    }
    payload_object.insert(
        "_lethe".to_string(),
        serde_json::json!({
            "person_id": person.person_id.as_str(),
            "projection_version": PERSON_PAGE_NOTION_PROJECTION_VERSION,
            "last_synced_at": synced_at.to_rfc3339(),
            "source_slide_url": frontend.source_canonical_uri,
            "status": "Done",
            "visibility": true,
        }),
    );
    if let Some(source_images) = source_images {
        payload_object.insert(
            "_lethe_source_images".to_string(),
            serde_json::to_value(source_images).ok()?,
        );
    }
    Some(WriteRecord {
        entity_id,
        title,
        payload,
        external_id: None,
    })
}

pub(super) fn notion_thumbnail_url_for_profile(
    profile: &StudentProfile,
    public_base_url: Option<&str>,
) -> Option<String> {
    stable_google_slides_thumbnail_url(profile)
        .or_else(|| stable_public_blob_url(public_base_url, profile.thumbnail_blob_ref.as_deref()))
        .or_else(|| profile.thumbnail_url.clone())
}

pub(super) fn stable_public_blob_url(
    public_base_url: Option<&str>,
    blob_ref: Option<&str>,
) -> Option<String> {
    let base_url = public_base_url?;
    let hash = blob_ref_sha256(blob_ref?)?;
    Some(format!("{base_url}/public/blobs/{hash}"))
}

pub(super) fn stable_google_slides_thumbnail_url(profile: &StudentProfile) -> Option<String> {
    let (presentation_id, parsed_slide_object_id) =
        parse_google_slide_document_id(profile.source_document_id.as_deref()?)?;
    let slide_object_id = profile
        .source_slide_object_id
        .as_deref()
        .unwrap_or(parsed_slide_object_id);
    Some(format!(
        "https://docs.google.com/presentation/d/{presentation_id}/export/png?id={presentation_id}&pageid={slide_object_id}"
    ))
}

pub(super) fn parse_google_slide_document_id(value: &str) -> Option<(&str, &str)> {
    let rest = value.strip_prefix("document:gslides:")?;
    let (presentation_id, slide_object_id) = rest.split_once("#slide:")?;
    if presentation_id.is_empty() || slide_object_id.is_empty() {
        None
    } else {
        Some((presentation_id, slide_object_id))
    }
}

pub(super) fn build_notion_source_image_index(
    observations: &[Observation],
    blobs: &mut BlobStore,
    google_client: &impl GoogleSlidesClient,
    persistence: &SqlitePersistence,
) -> NotionSourceImageIndex {
    let mut index = HashMap::new();

    for observation in observations {
        if !observation
            .subject
            .as_str()
            .starts_with("document:gslides:")
        {
            continue;
        }
        let Some(blob_ref) = observation
            .payload
            .get("native")
            .and_then(|value| value.get("blobRef"))
            .and_then(serde_json::Value::as_str)
            .map(BlobRef::new)
        else {
            continue;
        };
        let Some(blob_bytes) = blobs.get(&blob_ref) else {
            continue;
        };
        let Ok(presentation) = serde_json::from_slice::<
            lethe_adapter_gslides::gslides::client::PresentationNative,
        >(blob_bytes) else {
            continue;
        };

        let page_size = page_size_for_presentation(&presentation);
        let pptx_slide_images = page_size.and_then(|page_size| {
            pptx_slide_images_for_presentation(
                &presentation.presentation_id,
                page_size,
                google_client,
            )
        });

        for (slide_index, slide) in presentation.slides.iter().enumerate() {
            let candidates =
                if let (Some(slides), Some(page_size)) = (pptx_slide_images.as_ref(), page_size) {
                    slides
                        .get(slide_index)
                        .map(|pptx_images| {
                            notion_source_image_candidates_from_pptx_slide(
                                slide,
                                pptx_images,
                                page_size,
                                blobs,
                                persistence,
                            )
                        })
                        .unwrap_or_else(|| {
                            notion_source_image_candidates_from_slide(
                                &presentation,
                                slide,
                                blobs,
                                google_client,
                                persistence,
                            )
                        })
                } else {
                    notion_source_image_candidates_from_slide(
                        &presentation,
                        slide,
                        blobs,
                        google_client,
                        persistence,
                    )
                };
            if candidates.is_empty() {
                continue;
            }
            index.insert(
                format!(
                    "document:gslides:{}#slide:{}",
                    presentation.presentation_id, slide.object_id
                ),
                candidates,
            );
        }
    }

    index
}

pub(super) fn build_targeted_notion_source_image_index(
    observations: &[Observation],
    source_document_ids: &[String],
    blobs: &mut BlobStore,
    google_client: &impl GoogleSlidesClient,
    persistence: &SqlitePersistence,
) -> NotionSourceImageIndex {
    let mut observation_by_presentation = HashMap::new();
    for observation in observations {
        if observation
            .subject
            .as_str()
            .starts_with("document:gslides:")
        {
            observation_by_presentation
                .insert(observation.subject.as_str().to_string(), observation);
        }
    }

    let mut index = HashMap::new();
    let mut pptx_slide_images_by_presentation =
        HashMap::<String, Option<Vec<Vec<PptxSlideImageCandidate>>>>::new();
    for source_document_id in source_document_ids {
        let Some((presentation_id, slide_object_id)) =
            parse_google_slide_document_id(source_document_id)
        else {
            continue;
        };
        let Some(observation) =
            observation_by_presentation.get(&format!("document:gslides:{presentation_id}"))
        else {
            continue;
        };
        let Some(blob_ref) = observation
            .payload
            .get("native")
            .and_then(|value| value.get("blobRef"))
            .and_then(serde_json::Value::as_str)
            .map(BlobRef::new)
        else {
            continue;
        };
        let Some(blob_bytes) = blobs.get(&blob_ref) else {
            continue;
        };
        let Ok(presentation) = serde_json::from_slice::<
            lethe_adapter_gslides::gslides::client::PresentationNative,
        >(blob_bytes) else {
            continue;
        };
        let Some(page_size) = page_size_for_presentation(&presentation) else {
            continue;
        };
        let Some((slide_index, slide)) = presentation
            .slides
            .iter()
            .enumerate()
            .find(|(_, slide)| slide.object_id == slide_object_id)
        else {
            continue;
        };

        if !pptx_slide_images_by_presentation.contains_key(presentation_id) {
            pptx_slide_images_by_presentation.insert(
                presentation_id.to_string(),
                pptx_slide_images_for_presentation(presentation_id, page_size, google_client),
            );
        }
        let pptx_candidates = pptx_slide_images_by_presentation
            .get(presentation_id)
            .and_then(|slides| slides.as_ref())
            .and_then(|slides| {
                slides.get(slide_index).map(|pptx_images| {
                    notion_source_image_candidates_from_pptx_slide(
                        slide,
                        pptx_images,
                        page_size,
                        blobs,
                        persistence,
                    )
                })
            });

        let candidates = pptx_candidates.unwrap_or_else(|| {
            eprintln!(
                "falling back to direct source image download for {}#slide:{}",
                presentation_id, slide_object_id
            );
            notion_source_image_candidates_from_slide(
                &presentation,
                slide,
                blobs,
                google_client,
                persistence,
            )
        });
        if !candidates.is_empty() {
            index.insert(
                format!("document:gslides:{presentation_id}#slide:{slide_object_id}"),
                candidates,
            );
        } else {
            let _ = slide_index;
        }
    }
    index
}

pub(super) fn page_size_for_presentation(
    presentation: &lethe_adapter_gslides::gslides::client::PresentationNative,
) -> Option<&lethe_adapter_gslides::gslides::client::PageSize> {
    let page_size = presentation.page_size.as_ref()?;
    if page_size.width_emu <= 0 || page_size.height_emu <= 0 {
        None
    } else {
        Some(page_size)
    }
}

pub(super) fn notion_source_image_candidates_from_slide(
    presentation: &lethe_adapter_gslides::gslides::client::PresentationNative,
    slide: &lethe_adapter_gslides::gslides::client::SlideNative,
    blobs: &mut BlobStore,
    google_client: &impl GoogleSlidesClient,
    persistence: &SqlitePersistence,
) -> Vec<NotionSourceImageCandidate> {
    let Some(page_size) = presentation.page_size.as_ref() else {
        return Vec::new();
    };
    if page_size.width_emu <= 0 || page_size.height_emu <= 0 {
        return Vec::new();
    }

    slide
        .page_elements
        .iter()
        .filter_map(|element| {
            notion_source_image_candidate_from_element(
                element,
                page_size,
                blobs,
                google_client,
                persistence,
            )
        })
        .collect()
}

pub(super) fn notion_source_image_candidates_from_pptx_slide(
    slide: &lethe_adapter_gslides::gslides::client::SlideNative,
    pptx_images: &[PptxSlideImageCandidate],
    page_size: &lethe_adapter_gslides::gslides::client::PageSize,
    blobs: &mut BlobStore,
    persistence: &SqlitePersistence,
) -> Vec<NotionSourceImageCandidate> {
    let native_images = slide_image_candidates(slide);
    pptx_images
        .iter()
        .enumerate()
        .filter_map(|(index, pptx_image)| {
            let matched_native = match_native_slide_image_candidate(
                &native_images,
                pptx_image.center_x_pct,
                pptx_image.center_y_pct,
                page_size,
            );
            let blob_ref = persistence.persist_blob(&pptx_image.bytes).ok()?;
            blobs.put(&pptx_image.bytes);
            Some(NotionSourceImageCandidate {
                object_id: matched_native
                    .map(|candidate| candidate.object_id.clone())
                    .unwrap_or_else(|| format!("pptx-image-{index}")),
                source_url: matched_native
                    .map(|candidate| {
                        apply_rotation_to_google_image_url(
                            &candidate.content_url,
                            candidate.rotation_degrees,
                        )
                    })
                    .unwrap_or_default(),
                blob_ref: blob_ref.as_str().to_string(),
                center_x_pct: pptx_image.center_x_pct,
                center_y_pct: pptx_image.center_y_pct,
            })
        })
        .collect()
}

pub(super) fn notion_source_image_candidate_from_element(
    element: &serde_json::Value,
    page_size: &lethe_adapter_gslides::gslides::client::PageSize,
    blobs: &mut BlobStore,
    google_client: &impl GoogleSlidesClient,
    persistence: &SqlitePersistence,
) -> Option<NotionSourceImageCandidate> {
    let object_id = element.get("objectId")?.as_str()?.to_string();
    let content_url = element
        .get("image")
        .and_then(|value| value.get("contentUrl"))
        .and_then(serde_json::Value::as_str)?
        .to_string();
    let bytes = match google_client.download_bytes(&content_url) {
        Ok(bytes) => bytes,
        Err(err) => {
            eprintln!("source image download failed for {}: {}", object_id, err);
            return None;
        }
    };
    let blob_ref = persistence.persist_blob(&bytes).ok()?;
    blobs.put(&bytes);
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

    let center_x = translate_x + (width * scale_x.abs() / 2.0);
    let center_y = translate_y + (height * scale_y.abs() / 2.0);

    Some(NotionSourceImageCandidate {
        object_id,
        source_url: content_url,
        blob_ref: blob_ref.as_str().to_string(),
        center_x_pct: (center_x / page_size.width_emu as f64) * 100.0,
        center_y_pct: (center_y / page_size.height_emu as f64) * 100.0,
    })
}

pub(super) fn match_native_slide_image_candidate<'a>(
    candidates: &'a [SlideImageCandidate],
    center_x_pct: f64,
    center_y_pct: f64,
    page_size: &lethe_adapter_gslides::gslides::client::PageSize,
) -> Option<&'a SlideImageCandidate> {
    candidates.iter().min_by(|left, right| {
        let left_x = (left.center_x / page_size.width_emu as f64) * 100.0;
        let left_y = (left.center_y / page_size.height_emu as f64) * 100.0;
        let right_x = (right.center_x / page_size.width_emu as f64) * 100.0;
        let right_y = (right.center_y / page_size.height_emu as f64) * 100.0;
        squared_distance(left_x, left_y, center_x_pct, center_y_pct)
            .total_cmp(&squared_distance(
                right_x,
                right_y,
                center_x_pct,
                center_y_pct,
            ))
            .then_with(|| right.z_index.cmp(&left.z_index))
    })
}

pub(super) fn squared_distance(left_x: f64, left_y: f64, right_x: f64, right_y: f64) -> f64 {
    let dx = left_x - right_x;
    let dy = left_y - right_y;
    (dx * dx) + (dy * dy)
}

pub(super) fn parse_pptx_slide_images(
    pptx_bytes: &[u8],
    page_size: &lethe_adapter_gslides::gslides::client::PageSize,
) -> Result<Vec<Vec<PptxSlideImageCandidate>>, String> {
    const REL_NS: &str = "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
    let mut archive = zip::ZipArchive::new(Cursor::new(pptx_bytes))
        .map_err(|err| format!("failed to open pptx zip: {err}"))?;

    let mut slides = Vec::new();
    let mut slide_number = 1usize;
    loop {
        let slide_path = format!("ppt/slides/slide{slide_number}.xml");
        let rels_path = format!("ppt/slides/_rels/slide{slide_number}.xml.rels");
        if archive.by_name(&slide_path).is_err() {
            break;
        }
        let slide_xml = read_zip_entry(&mut archive, &slide_path)?;
        let rels_xml = read_zip_entry(&mut archive, &rels_path)?;
        let rels_doc = roxmltree::Document::parse(&rels_xml)
            .map_err(|err| format!("failed to parse {rels_path}: {err}"))?;
        let rel_targets = rels_doc
            .descendants()
            .filter(|node| node.tag_name().name() == "Relationship")
            .filter_map(|node| {
                Some((
                    node.attribute("Id")?.to_string(),
                    resolve_zip_relative_path("ppt/slides", node.attribute("Target")?),
                ))
            })
            .collect::<HashMap<_, _>>();
        let slide_doc = roxmltree::Document::parse(&slide_xml)
            .map_err(|err| format!("failed to parse {slide_path}: {err}"))?;
        let mut images = Vec::new();
        for pic in slide_doc
            .descendants()
            .filter(|node| node.tag_name().name() == "pic")
        {
            let Some(embed_id) = pic
                .descendants()
                .find(|node| node.tag_name().name() == "blip")
                .and_then(|node| node.attribute((REL_NS, "embed")))
            else {
                continue;
            };
            let Some(target) = rel_targets.get(embed_id) else {
                continue;
            };
            let Some(xfrm) = pic
                .descendants()
                .find(|node| node.tag_name().name() == "xfrm")
            else {
                continue;
            };
            let Some(off) = xfrm.children().find(|node| node.tag_name().name() == "off") else {
                continue;
            };
            let Some(ext) = xfrm.children().find(|node| node.tag_name().name() == "ext") else {
                continue;
            };
            let x = off
                .attribute("x")
                .and_then(|value| value.parse::<f64>().ok())
                .unwrap_or_default();
            let y = off
                .attribute("y")
                .and_then(|value| value.parse::<f64>().ok())
                .unwrap_or_default();
            let cx = ext
                .attribute("cx")
                .and_then(|value| value.parse::<f64>().ok())
                .unwrap_or_default();
            let cy = ext
                .attribute("cy")
                .and_then(|value| value.parse::<f64>().ok())
                .unwrap_or_default();
            let bytes = read_zip_entry_bytes(&mut archive, target)?;
            images.push(PptxSlideImageCandidate {
                bytes,
                center_x_pct: ((x + cx / 2.0) / page_size.width_emu as f64) * 100.0,
                center_y_pct: ((y + cy / 2.0) / page_size.height_emu as f64) * 100.0,
            });
        }
        slides.push(images);
        slide_number += 1;
    }
    Ok(slides)
}

pub(super) fn pptx_slide_images_for_presentation(
    presentation_id: &str,
    page_size: &lethe_adapter_gslides::gslides::client::PageSize,
    google_client: &impl GoogleSlidesClient,
) -> Option<Vec<Vec<PptxSlideImageCandidate>>> {
    let pptx_bytes = match google_client.export_presentation_pptx(presentation_id) {
        Ok(bytes) => Some(bytes),
        Err(err) => {
            eprintln!(
                "failed to export presentation pptx for {}: {}; trying local manual fallback",
                presentation_id, err
            );
            let manual_path = manual_pptx_path_for_presentation(presentation_id);
            match std::fs::read(&manual_path) {
                Ok(bytes) => Some(bytes),
                Err(read_err) => {
                    eprintln!(
                        "failed to read manual pptx fallback {}: {}",
                        manual_path.display(),
                        read_err
                    );
                    None
                }
            }
        }
    }?;

    match parse_pptx_slide_images(&pptx_bytes, page_size) {
        Ok(slides) => Some(slides),
        Err(err) => {
            eprintln!(
                "failed to parse pptx slide images for {}: {}",
                presentation_id, err
            );
            None
        }
    }
}

pub(super) fn manual_pptx_path_for_presentation(presentation_id: &str) -> std::path::PathBuf {
    std::path::PathBuf::from("data")
        .join("manual_pptx")
        .join(format!("{presentation_id}.pptx"))
}

pub(super) fn read_zip_entry(
    archive: &mut zip::ZipArchive<Cursor<&[u8]>>,
    path: &str,
) -> Result<String, String> {
    let mut file = archive
        .by_name(path)
        .map_err(|err| format!("missing zip entry {path}: {err}"))?;
    let mut xml = String::new();
    file.read_to_string(&mut xml)
        .map_err(|err| format!("failed to read {path}: {err}"))?;
    Ok(xml)
}

pub(super) fn read_zip_entry_bytes(
    archive: &mut zip::ZipArchive<Cursor<&[u8]>>,
    path: &str,
) -> Result<Vec<u8>, String> {
    let mut file = archive
        .by_name(path)
        .map_err(|err| format!("missing zip entry {path}: {err}"))?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|err| format!("failed to read {path}: {err}"))?;
    Ok(bytes)
}

pub(super) fn resolve_zip_relative_path(base_dir: &str, target: &str) -> String {
    let mut segments = base_dir
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    for part in target.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                segments.pop();
            }
            value => segments.push(value.to_string()),
        }
    }
    segments.join("/")
}

pub(super) fn blob_ref_sha256(blob_ref: &str) -> Option<&str> {
    let hash = blob_ref.strip_prefix("blob:sha256:")?;
    if hash.len() == 64 && hash.chars().all(|ch| ch.is_ascii_hexdigit()) {
        Some(hash)
    } else {
        None
    }
}

pub(super) fn audit_kind_for_scope(scope: &str) -> AuditEventKind {
    match scope {
        "admin:sync" => AuditEventKind::WriteExecution,
        "read:persons" | "read:timeline" => AuditEventKind::ReadRestricted,
        _ => AuditEventKind::ReadRestricted,
    }
}

pub(super) fn notion_title_for_profile(
    profile: &StudentProfile,
    frontend: &FrontendProfile,
) -> String {
    if profile.name.trim().is_empty() {
        frontend
            .source_document_id
            .rsplit_once("#slide:")
            .map(|(_, slide_id)| slide_id.to_string())
            .unwrap_or_else(|| "Untitled Slide".to_string())
    } else {
        profile.name.clone()
    }
}
