use super::*;

fn fixture_client(person_id_property_name: &str) -> NotionClient {
    let properties = [
        ("Birthplace", "rich_text"),
        ("Major_interests", "rich_text"),
        ("Hashtag", "rich_text"),
        ("Source Slide URL", "url"),
        ("Last Synced At", "date"),
        ("Projection Version", "rich_text"),
        ("Status", "status"),
        ("Visibility", "checkbox"),
    ]
    .into_iter()
    .chain(std::iter::once((person_id_property_name, "rich_text")))
    .map(|(name, property_type)| {
        (
            name.to_string(),
            NotionProperty {
                property_type: property_type.to_string(),
            },
        )
    })
    .collect::<HashMap<_, _>>();
    NotionClient {
        http: Client::builder().build().unwrap(),
        config: NotionConfig::new("test-token", "test-db"),
        schema: DatabaseSchema {
            title_property: "Name".into(),
            email_property: Some("Email".into()),
            actual_names_by_normalized: [
                "Birthplace",
                "Major_interests",
                "Hashtag",
                "Source Slide URL",
                "Last Synced At",
                "Projection Version",
                "Status",
                "Visibility",
            ]
            .into_iter()
            .chain(std::iter::once(person_id_property_name))
            .map(|name| (normalize_property_name(name), name.to_string()))
            .collect(),
            properties,
        },
    }
}

fn sample_profile() -> StudentProfile {
    StudentProfile {
        email: Some("sayaka@example.com".into()),
        generated_email: None,
        name: "彦野 沙彩花".into(),
        bio_text: Some("自己紹介テキスト".into()),
        profile_pic: None,
        gallery_images: vec![],
        properties: StudentProperties::default(),
        attributes: vec![],
        source_slide_object_id: Some("slide-1".into()),
        source_document_id: Some("document:gslides:test#slide:slide-1".into()),
        source_canonical_uri: Some(
            "https://docs.google.com/presentation/d/test/edit#slide=id.slide-1".into(),
        ),
        thumbnail_blob_ref: None,
        thumbnail_url: None,
        companion_to_slide_object_id: None,
    }
}

fn heading_titles(blocks: &[serde_json::Value]) -> Vec<String> {
    blocks
        .iter()
        .filter(|block| block["type"] == "heading_2")
        .filter_map(|block| block["heading_2"]["rich_text"][0]["text"]["content"].as_str())
        .map(str::to_string)
        .collect()
}

fn count_block_type(value: &serde_json::Value, block_type: &str) -> usize {
    match value {
        serde_json::Value::Object(map) => {
            usize::from(map.get("type").and_then(|value| value.as_str()) == Some(block_type))
                + map
                    .values()
                    .map(|child| count_block_type(child, block_type))
                    .sum::<usize>()
        }
        serde_json::Value::Array(values) => values
            .iter()
            .map(|child| count_block_type(child, block_type))
            .sum(),
        _ => 0,
    }
}

#[test]
fn source_image_matching_supports_legacy_thousand_scale_coordinates() {
    let candidates = vec![
        SourceImageCandidate {
            object_id: "left".into(),
            source_url: "https://example.com/left.png".into(),
            blob_ref: "blob:sha256:left".into(),
            center_x_pct: 20.0,
            center_y_pct: 30.0,
        },
        SourceImageCandidate {
            object_id: "right".into(),
            source_url: "https://example.com/right.png".into(),
            blob_ref: "blob:sha256:right".into(),
            center_x_pct: 80.0,
            center_y_pct: 75.0,
        },
    ];
    let coordinates = ImageCoordinates { x: 800.0, y: 750.0 };

    let matched = match_source_image_candidate(&candidates, &coordinates).unwrap();

    assert_eq!(matched.object_id, "right");
}

#[test]
fn full_profile_produces_all_sections() {
    let mut profile = sample_profile();
    profile.profile_pic = Some(lethe_profile_model::ProfilePic {
        coordinates: None,
        description: Some("portrait".into()),
        url: Some("https://example.com/profile.png".into()),
    });
    profile.gallery_images = vec![lethe_profile_model::GalleryImage {
        coordinates: None,
        description: Some("猫の写真".into()),
        url: Some("https://example.com/gallery.png".into()),
    }];
    profile.properties.nickname = Some("さやか".into());
    profile.properties.birthplace = Some("栃木県".into());
    profile.properties.major = Some("電気工学".into());
    profile.properties.affiliation = Some("HLAB College".into());
    profile.properties.mbti = Some("ENFP".into());
    profile.properties.sns = Some("https://example.com/sns".into());
    profile.properties.hobbies = vec!["写真".into()];
    profile.properties.interests = vec!["エネルギー".into()];
    profile.properties.likes = vec!["コーンスープ".into()];
    profile.properties.new_challenges = Some("海外で学ぶ".into());

    let gallery = vec![(
        0usize,
        NotionMediaRef::External("https://example.com/gallery.png".into()),
    )];
    let blocks = build_page_blocks(
        &profile,
        Some(&NotionMediaRef::External(
            "https://example.com/profile.png".into(),
        )),
        &gallery,
        profile.source_canonical_uri.as_deref(),
    );

    assert_eq!(blocks.first().unwrap()["type"], "callout");
    assert_eq!(
        heading_titles(&blocks),
        vec!["About", "Highlights", "Gallery", "Source"]
    );
    assert!(blocks.iter().any(|block| block["type"] == "toggle"));
    assert!(blocks.iter().any(|block| block["type"] == "bookmark"));
}

#[test]
fn missing_hobbies_interests_likes_skips_highlights() {
    let profile = sample_profile();
    let blocks = build_page_blocks(&profile, None, &[], profile.source_canonical_uri.as_deref());
    assert!(
        !heading_titles(&blocks)
            .iter()
            .any(|title| title == "Highlights")
    );
}

#[test]
fn partial_toggles_only_present_fields() {
    let mut profile = sample_profile();
    profile.properties.new_challenges = Some("Rust を学ぶ".into());
    let blocks = build_page_blocks(&profile, None, &[], None);
    let toggles = blocks
        .iter()
        .filter(|block| block["type"] == "toggle")
        .count();
    assert_eq!(toggles, 1);
    assert!(
        blocks
            .iter()
            .any(|block| block.to_string().contains("New Challenges"))
    );
}

#[test]
fn gallery_respects_max_9() {
    let mut profile = sample_profile();
    profile.gallery_images = (0..12)
        .map(|index| lethe_profile_model::GalleryImage {
            coordinates: None,
            description: Some(format!("image-{index}")),
            url: Some(format!("https://example.com/{index}.png")),
        })
        .collect();
    let gallery = (0..12)
        .map(|index| {
            (
                index,
                NotionMediaRef::External(format!("https://example.com/{index}.png")),
            )
        })
        .collect::<Vec<_>>();
    let blocks = build_page_blocks(&profile, None, &gallery, None);
    let image_count = blocks
        .iter()
        .map(|block| count_block_type(block, "image"))
        .sum::<usize>();
    assert_eq!(image_count, 9);
}

#[test]
fn single_gallery_image_does_not_emit_column_list() {
    let mut profile = sample_profile();
    profile.gallery_images = vec![lethe_profile_model::GalleryImage {
        coordinates: None,
        description: Some("single".into()),
        url: Some("https://example.com/one.png".into()),
    }];
    let gallery = vec![(0usize, NotionMediaRef::FileUpload("upload-1".into()))];
    let blocks = build_page_blocks(&profile, None, &gallery, None);
    let gallery_section = build_gallery_section(&profile, &gallery).unwrap();
    assert!(blocks.iter().any(|block| block["type"] == "image"));
    assert!(
        !gallery_section
            .iter()
            .any(|block| block["type"] == "column_list")
    );
}

#[test]
fn dividers_not_orphaned() {
    let mut profile = sample_profile();
    profile.bio_text = None;
    profile.properties.hobbies = vec!["写真".into()];
    let blocks = build_page_blocks(&profile, None, &[], profile.source_canonical_uri.as_deref());
    assert_ne!(blocks.first().unwrap()["type"], "divider");
    assert_ne!(blocks.last().unwrap()["type"], "divider");
    for pair in blocks.windows(2) {
        assert!(!(pair[0]["type"] == "divider" && pair[1]["type"] == "divider"));
    }
}

#[test]
fn sns_url_becomes_link() {
    let mut profile = sample_profile();
    profile.properties.sns = Some("https://example.com/sns".into());
    let blocks = build_page_blocks(&profile, None, &[], None);
    let about = blocks
        .iter()
        .find(|block| block["type"] == "table")
        .unwrap();
    assert_eq!(
        about["table"]["children"][0]["table_row"]["cells"][1][0]["text"]["link"]["url"].as_str(),
        Some("https://example.com/sns")
    );
}

#[test]
fn about_table_skips_none_rows() {
    let mut profile = sample_profile();
    profile.properties.nickname = Some("さやか".into());
    let blocks = build_page_blocks(&profile, None, &[], None);
    let about = blocks
        .iter()
        .find(|block| block["type"] == "table")
        .unwrap();
    assert_eq!(about["table"]["children"].as_array().unwrap().len(), 1);
}

#[test]
fn build_property_updates_keeps_major_as_major_interests() {
    let payload = serde_json::json!({
        "properties": {
            "Major": "CS",
            "Interests": ["AI", "Robotics"],
            "Birthplace": "Tokyo",
        }
    });
    let props = fixture_client("LETHE Person ID").build_property_updates("田中太郎", &payload);
    assert_eq!(
        props["Major_interests"]["rich_text"][0]["text"]["content"].as_str(),
        Some("CS")
    );
    assert!(props.get("Birthplace").is_some());
    assert!(props.get("Name").is_some());
}

#[test]
fn build_property_updates_populates_metadata_without_attribute_fallbacks() {
    let payload = serde_json::json!({
        "attributes": ["AI", "ML"],
        "_lethe": {
            "person_id": "person:alice",
            "projection_version": "proj:person-page@0.1.0",
            "last_synced_at": "2026-03-28T11:00:00Z",
            "source_slide_url": "https://example.com/slide",
            "status": "Done",
            "visibility": true
        },
        "properties": {
            "Hashtags": ["#rust"],
            "Major": "CS"
        }
    });

    let props = fixture_client("LETHE Person ID").build_property_updates("田中太郎", &payload);

    assert_eq!(
        props["Hashtag"]["rich_text"][0]["text"]["content"].as_str(),
        Some("#rust")
    );
    assert_eq!(
        props["LETHE Person ID"]["rich_text"][0]["text"]["content"].as_str(),
        Some("person:alice")
    );
    assert_eq!(props["Status"]["status"]["name"].as_str(), Some("Done"));
    assert_eq!(props["Visibility"]["checkbox"].as_bool(), Some(true));
}

#[test]
fn build_property_updates_supports_japanese_alias_named_properties() {
    let mut client = fixture_client("LETHE Person ID");
    client.schema.actual_names_by_normalized.extend(
        [
            ("呼ばれたい名前", "呼ばれたい名前"),
            ("趣味特技", "趣味・特技"),
            ("好きなもの", "好きなもの"),
            ("カレッジで挑戦したいこと", "カレッジで挑戦したいこと"),
        ]
        .into_iter()
        .map(|(normalized, actual)| (normalized.to_string(), actual.to_string())),
    );
    client.schema.properties.extend(
        [
            ("呼ばれたい名前", "rich_text"),
            ("趣味・特技", "rich_text"),
            ("好きなもの", "rich_text"),
            ("カレッジで挑戦したいこと", "rich_text"),
        ]
        .into_iter()
        .map(|(name, property_type)| {
            (
                name.to_string(),
                NotionProperty {
                    property_type: property_type.to_string(),
                },
            )
        }),
    );

    let payload = serde_json::json!({
        "properties": {
            "Nickname": "さやか",
            "Hobbies": ["写真", "散歩"],
            "Likes": ["コーヒー"],
            "New Challenges": "もっと話す"
        }
    });

    let props = client.build_property_updates("田中太郎", &payload);

    assert_eq!(
        props["呼ばれたい名前"]["rich_text"][0]["text"]["content"].as_str(),
        Some("さやか")
    );
    assert_eq!(
        props["趣味・特技"]["rich_text"][0]["text"]["content"].as_str(),
        Some("写真, 散歩")
    );
    assert_eq!(
        props["好きなもの"]["rich_text"][0]["text"]["content"].as_str(),
        Some("コーヒー")
    );
    assert_eq!(
        props["カレッジで挑戦したいこと"]["rich_text"][0]["text"]["content"].as_str(),
        Some("もっと話す")
    );
}

#[test]
fn headers_reject_invalid_bearer_token() {
    let mut client = fixture_client("LETHE Person ID");
    client.config.token = "bad\r\ntoken".into();
    assert!(matches!(
        client.headers_for_version(&client.config.api_version),
        Err(AdapterError::AuthFailure { .. })
    ));
}

#[test]
fn headers_reject_invalid_api_version() {
    let mut client = fixture_client("LETHE Person ID");
    client.config.api_version = "bad\r\nversion".into();
    assert!(matches!(
        client.headers_for_version(&client.config.api_version),
        Err(AdapterError::Other(_))
    ));
}

#[test]
fn media_ref_file_upload_image_block_uses_file_upload_type() {
    let block = NotionMediaRef::FileUpload("upload-123".into()).to_image_block();
    assert_eq!(block["type"], "image");
    assert_eq!(block["image"]["type"], "file_upload");
    assert_eq!(block["image"]["file_upload"]["id"], "upload-123");
}

#[test]
fn media_ref_external_image_block_uses_external_type() {
    let block = NotionMediaRef::External("https://example.com/thumb.png".into()).to_image_block();
    assert_eq!(block["type"], "image");
    assert_eq!(block["image"]["type"], "external");
    assert_eq!(
        block["image"]["external"]["url"],
        "https://example.com/thumb.png"
    );
}
