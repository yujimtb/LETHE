//! Lane C Integration Tests — End-to-end: Observation → Identity → PersonPage → API
//!
//! Tests the full vertical slice: ingest observations, resolve identities,
//! build person pages, and verify API contracts.

use chrono::Utc;
use serde_json::json;

use lethe::api::envelope::ResponseEnvelope;
use lethe::api::health::HealthResponse;
use lethe::api::pagination::{paginate, PaginationParams};
use lethe::api::read_mode::ReadModeResolver;
use lethe::domain::*;
use lethe::governance::filter::FilteringGate;
use lethe::governance::types::{AccessScope, MaskStrategy, RestrictedFieldSpec};
use lethe::identity::projector::IdentityProjector;
use lethe::lake::store::LakeStore;
use lethe::person_page::projector::PersonPageProjector;
use lethe::projection::catalog::ProjectionCatalog;
use lethe::projection::runner::Projector;
use lethe::projection::spec::*;
use lethe::projection::BuildStatus;
use lethe::propagation::watermark::WatermarkStore;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn slack_observation(user_id: &str, email: &str, name: &str, text: &str, channel: &str, key: &str) -> Observation {
    Observation {
        id: Observation::new_id(),
        schema: SchemaRef::new("schema:slack-message"),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new("obs:slack-crawler"),
        source_system: Some(SourceSystemRef::new("sys:slack")),
        actor: None,
        authority_model: AuthorityModel::LakeAuthoritative,
        capture_model: CaptureModel::Event,
        subject: EntityRef::new(format!("message:slack:{key}")),
        target: None,
        payload: json!({
            "user_id": user_id,
            "user_name": name,
            "email": email,
            "text": text,
            "channel": channel,
            "channel_id": format!("chan:{channel}"),
            "channel_name": channel,
        }),
        attachments: vec![],
        published: Utc::now(),
        recorded_at: Utc::now(),
        consent: None,
        idempotency_key: IdempotencyKey::new(key),
        meta: json!({}),
    }
}

fn gslides_observation(editors: &[&str], owner: &str, title: &str, key: &str) -> Observation {
    Observation {
        id: Observation::new_id(),
        schema: SchemaRef::new("schema:workspace-object-snapshot"),
        schema_version: SemVer::new("1.0.0"),
        observer: ObserverRef::new("obs:gslides-crawler"),
        source_system: Some(SourceSystemRef::new("sys:google-slides")),
        actor: None,
        authority_model: AuthorityModel::SourceAuthoritative,
        capture_model: CaptureModel::Snapshot,
        subject: EntityRef::new(format!("document:gslide:{key}")),
        target: None,
        payload: json!({
            "title": title,
            "relations": {
                "editors": editors,
                "owner": owner,
            },
        }),
        attachments: vec![],
        published: Utc::now(),
        recorded_at: Utc::now(),
        consent: None,
        idempotency_key: IdempotencyKey::new(key),
        meta: json!({}),
    }
}

fn identity_spec() -> ProjectionSpec {
    ProjectionSpec {
        id: ProjectionRef::new("proj:identity-resolution"),
        name: "Identity Resolution".into(),
        version: SemVer::new("1.0.0"),
        kind: ProjectionKind::PureProjection,
        sources: vec![SourceDecl {
            source: SourceRef::Lake,
            filter_schemas: vec![],
            filter_derivations: vec![],
        }],
        read_modes: vec![ReadModePolicy {
            mode: ReadMode::OperationalLatest,
            source_policy: "lake-latest".into(),
        }],
        build: BuildSpec {
            build_type: "rust".into(),
            entrypoint: None,
            projector: "identity-resolution".into(),
        },
        outputs: vec![OutputSpec {
            format: "sql".into(),
            tables: vec![
                "resolved_persons".into(),
                "candidates".into(),
                "person_identifiers".into(),
            ],
        }],
        reconciliation: None,
        deterministic_in: vec![],
        gap_action: None,
        tags: vec!["identity".into()],
        description: Some("Cross-source identity resolution".into()),
        created_by: "system".into(),
    }
}

fn person_page_spec() -> ProjectionSpec {
    ProjectionSpec {
        id: ProjectionRef::new("proj:person-page"),
        name: "Person Page".into(),
        version: SemVer::new("1.0.0"),
        kind: ProjectionKind::PureProjection,
        sources: vec![
            SourceDecl {
                source: SourceRef::Lake,
                filter_schemas: vec![],
                filter_derivations: vec![],
            },
            SourceDecl {
                source: SourceRef::Projection {
                    id: ProjectionRef::new("proj:identity-resolution"),
                    version: "1.0.0".into(),
                },
                filter_schemas: vec![],
                filter_derivations: vec![],
            },
        ],
        read_modes: vec![ReadModePolicy {
            mode: ReadMode::OperationalLatest,
            source_policy: "lake-latest".into(),
        }],
        build: BuildSpec {
            build_type: "rust".into(),
            entrypoint: None,
            projector: "person-page".into(),
        },
        outputs: vec![OutputSpec {
            format: "sql".into(),
            tables: vec![
                "person_profiles".into(),
                "person_slides".into(),
                "person_messages".into(),
                "person_activity".into(),
            ],
        }],
        reconciliation: Some(ReconciliationPolicy::LakeFirst),
        deterministic_in: vec![],
        gap_action: None,
        tags: vec!["person-page".into()],
        description: Some("Person page projection".into()),
        created_by: "system".into(),
    }
}

// ---------------------------------------------------------------------------
// Integration: End-to-end pipeline
// ---------------------------------------------------------------------------

#[test]
fn end_to_end_observation_to_person_page() {
    let mut lake = LakeStore::new();

    let obs = vec![
        slack_observation("U100", "tanaka@example.jp", "田中太郎", "おはよう", "general", "s1"),
        slack_observation("U100", "tanaka@example.jp", "田中太郎", "会議開始", "project-a", "s2"),
        slack_observation("U200", "suzuki@example.jp", "鈴木花子", "了解です", "general", "s3"),
        gslides_observation(&["tanaka@example.jp"], "tanaka@example.jp", "田中の自己紹介", "g1"),
        gslides_observation(&["suzuki@example.jp", "tanaka@example.jp"], "suzuki@example.jp", "共同プレゼン", "g2"),
    ];

    for o in &obs {
        lake.append(o.clone()).unwrap();
    }
    assert_eq!(lake.len(), 5);

    // 2. Run identity resolution.
    let projector = IdentityProjector::new("1.0.0");
    let all_obs = lake.list();
    let identity_results = projector.project(all_obs);
    let identity = &identity_results[0];

    // tanaka (slack email match google) → 1 person.
    // suzuki (slack email match google) → 1 person.
    assert_eq!(identity.resolved_persons.len(), 2);

    let tanaka = identity
        .resolved_persons
        .iter()
        .find(|p| p.canonical_name == "田中太郎")
        .expect("田中太郎 should be resolved");
    assert!(tanaka.sources.contains(&"slack".to_string()));
    assert!(tanaka.sources.contains(&"google".to_string()));

    let suzuki = identity
        .resolved_persons
        .iter()
        .find(|p| p.canonical_name == "鈴木花子")
        .expect("鈴木花子 should be resolved");
    assert!(suzuki.sources.contains(&"slack".to_string()));
    assert!(suzuki.sources.contains(&"google".to_string()));

    // 3. Run person page projector.
    let pp_output = PersonPageProjector::project(identity, all_obs, &[]);

    assert_eq!(pp_output.profiles.len(), 2);
    assert_eq!(pp_output.activities.len(), 2);

    // Tanaka: 2 Slack messages + 2 GSlides (editor on g1 and g2).
    let tanaka_msgs: Vec<_> = pp_output
        .messages
        .iter()
        .filter(|m| m.person_id == tanaka.person_id)
        .collect();
    assert_eq!(tanaka_msgs.len(), 2);

    let tanaka_slides: Vec<_> = pp_output
        .slides
        .iter()
        .filter(|s| s.person_id == tanaka.person_id)
        .collect();
    assert_eq!(tanaka_slides.len(), 2);

    // Suzuki: 1 Slack message + 1 GSlide (editor+owner on g2).
    let suzuki_msgs: Vec<_> = pp_output
        .messages
        .iter()
        .filter(|m| m.person_id == suzuki.person_id)
        .collect();
    assert_eq!(suzuki_msgs.len(), 1);

    let suzuki_slides: Vec<_> = pp_output
        .slides
        .iter()
        .filter(|s| s.person_id == suzuki.person_id)
        .collect();
    assert_eq!(suzuki_slides.len(), 1);
}

// ---------------------------------------------------------------------------
// Replay Law: Same input → same output
// ---------------------------------------------------------------------------

#[test]
fn replay_law_identity_and_person_page() {
    let obs = vec![
        slack_observation("U100", "tanaka@example.jp", "田中太郎", "msg1", "ch1", "s1"),
        gslides_observation(&["tanaka@example.jp"], "tanaka@example.jp", "slide1", "g1"),
    ];

    let projector = IdentityProjector::new("1.0.0");
    let r1 = projector.project(&obs);
    let r2 = projector.project(&obs);

    assert_eq!(
        serde_json::to_value(&r1).unwrap(),
        serde_json::to_value(&r2).unwrap()
    );

    let pp1 = PersonPageProjector::project(&r1[0], &obs, &[]);
    let pp2 = PersonPageProjector::project(&r2[0], &obs, &[]);

    assert_eq!(
        serde_json::to_value(&pp1).unwrap(),
        serde_json::to_value(&pp2).unwrap()
    );
}

// ---------------------------------------------------------------------------
// Catalog & Propagation
// ---------------------------------------------------------------------------

#[test]
fn catalog_dag_with_identity_and_person_page() {
    let mut catalog = ProjectionCatalog::new();
    catalog.register(identity_spec()).unwrap();
    catalog.register(person_page_spec()).unwrap();

    let order = catalog.topological_order().unwrap();
    let id_pos = order
        .iter()
        .position(|r| r.as_str() == "proj:identity-resolution")
        .unwrap();
    let pp_pos = order
        .iter()
        .position(|r| r.as_str() == "proj:person-page")
        .unwrap();

    assert!(id_pos < pp_pos, "identity must build before person page");
}

#[test]
fn watermark_incremental_propagation() {
    let mut lake = LakeStore::new();
    let mut wm = WatermarkStore::new();
    let proj_id = ProjectionRef::new("proj:identity-resolution");

    let obs1 = slack_observation("U1", "a@b.com", "A", "msg", "ch", "s1");
    lake.append(obs1).unwrap();

    // Initial: no watermark state yet.
    let state = wm.get(&proj_id);
    assert!(state.is_none());

    // Initialize and check.
    let state = wm.get_or_init(&proj_id);
    assert_eq!(state.last_processed_position, 0);
    let lake_pos = lake.watermark().map(|w| w.position).unwrap_or(0);
    let pending = lake_pos - state.last_processed_position;
    assert_eq!(pending, 1);

    // After processing, advance watermark.
    wm.update(&proj_id, lake_pos, BuildStatus::Success);
    let state = wm.get(&proj_id).unwrap();
    assert_eq!(state.last_processed_position, 1);

    // Add more observations.
    let obs2 = slack_observation("U2", "c@d.com", "B", "msg2", "ch", "s2");
    lake.append(obs2).unwrap();

    let new_lake_pos = lake.watermark().map(|w| w.position).unwrap_or(0);
    let pending2 = new_lake_pos - state.last_processed_position;
    assert_eq!(pending2, 1);
}

// ---------------------------------------------------------------------------
// API Contract: Envelope, Pagination, ReadMode
// ---------------------------------------------------------------------------

#[test]
fn api_response_envelope_contract() {
    let obs = vec![
        slack_observation("U1", "test@example.com", "Test", "hello", "general", "s1"),
    ];

    let projector = IdentityProjector::new("1.0.0");
    let identity = &projector.project(&obs)[0];
    let pp = PersonPageProjector::project(identity, &obs, &[]);

    let metadata = lethe::api::envelope::ProjectionMetadata {
        projection_id: ProjectionRef::new("proj:person-page"),
        version: SemVer::new("1.0.0"),
        built_at: Utc::now(),
        read_mode: ReadMode::OperationalLatest,
        stale: false,
        lineage_ref: None,
    };

    let list_items: Vec<_> = pp
        .profiles
        .iter()
        .zip(pp.activities.iter())
        .map(|(p, a)| PersonPageProjector::to_list_item(p, a))
        .collect();

    let envelope = ResponseEnvelope {
        data: list_items,
        projection_metadata: metadata,
    };
    let json = serde_json::to_string(&envelope).unwrap();
    assert!(json.contains("proj:person-page"));
    assert!(json.contains("operational_latest"));
}

#[test]
fn api_pagination_over_person_list() {
    let obs = vec![
        slack_observation("U1", "a@e.com", "A", "msg", "ch", "s1"),
        slack_observation("U2", "b@e.com", "B", "msg", "ch", "s2"),
        slack_observation("U3", "c@e.com", "C", "msg", "ch", "s3"),
    ];

    let projector = IdentityProjector::new("1.0.0");
    let identity = &projector.project(&obs)[0];
    let pp = PersonPageProjector::project(identity, &obs, &[]);

    let list_items: Vec<_> = pp
        .profiles
        .iter()
        .zip(pp.activities.iter())
        .map(|(p, a)| PersonPageProjector::to_list_item(p, a))
        .collect();

    let params = PaginationParams {
        offset: 0,
        limit: 2,
        ..Default::default()
    };

    let (page, total) = paginate(&list_items, &params);
    assert_eq!(total, 3);
    assert_eq!(page.len(), 2);

    let params2 = PaginationParams {
        offset: 2,
        limit: 2,
        ..Default::default()
    };
    let (page2, _) = paginate(&list_items, &params2);
    assert_eq!(page2.len(), 1);
}

#[test]
fn api_read_mode_resolver_for_person_page() {
    let spec = person_page_spec();
    let mode = ReadModeResolver::resolve(&spec, None, None).unwrap();
    assert_eq!(mode, ReadMode::OperationalLatest);
}

// ---------------------------------------------------------------------------
// Filtering-before-Exposure
// ---------------------------------------------------------------------------

#[test]
fn filtering_before_exposure_masks_restricted_fields() {
    let obs = vec![
        slack_observation("U1", "secret@example.com", "Secret User", "private msg", "dm", "s1"),
    ];

    let projector = IdentityProjector::new("1.0.0");
    let identity = &projector.project(&obs)[0];
    let pp = PersonPageProjector::project(identity, &obs, &[]);

    // Serialize a profile to JSON.
    let profile_json = serde_json::to_value(&pp.profiles[0]).unwrap();

    // Define restricted fields.
    let restricted_specs = vec![RestrictedFieldSpec {
        field_path: "identities".into(),
        level: AccessScope::Restricted,
        mask_strategy: MaskStrategy::Exclude,
    }];

    let result = FilteringGate::filter(&profile_json, AccessScope::Internal, &restricted_specs);
    let identities = result.payload.get("identities");
    assert!(
        identities.is_none(),
        "identities should be excluded for internal scope"
    );

    // Restricted scope should see identities.
    let result2 = FilteringGate::filter(&profile_json, AccessScope::Restricted, &restricted_specs);
    let identities2 = result2.payload.get("identities");
    assert!(identities2.is_some(), "restricted scope should see identities");
}

// ---------------------------------------------------------------------------
// Health Endpoint
// ---------------------------------------------------------------------------

#[test]
fn health_endpoint_reflects_catalog_state() {
    let mut catalog = ProjectionCatalog::new();
    catalog.register(identity_spec()).unwrap();
    catalog.register(person_page_spec()).unwrap();

    catalog.set_health(
        &ProjectionRef::new("proj:identity-resolution"),
        ProjectionHealth::Healthy,
    );
    catalog.set_health(
        &ProjectionRef::new("proj:person-page"),
        ProjectionHealth::Healthy,
    );

    let health = HealthResponse::from_catalog(&catalog, "0.1.0");
    assert_eq!(health.status, "ok");
    assert_eq!(health.projections.len(), 2);

    // Break one projection.
    catalog.set_health(
        &ProjectionRef::new("proj:person-page"),
        ProjectionHealth::Broken,
    );
    let health2 = HealthResponse::from_catalog(&catalog, "0.1.0");
    assert_eq!(health2.status, "degraded");
}

// ---------------------------------------------------------------------------
// Scheduler: Full propagation cycle
// ---------------------------------------------------------------------------

#[test]
fn scheduler_propagates_through_dag() {
    let mut catalog = ProjectionCatalog::new();
    catalog.register(identity_spec()).unwrap();
    catalog.register(person_page_spec()).unwrap();

    let mut wm_store = WatermarkStore::new();
    let mut lake = LakeStore::new();

    // Ingest data.
    lake.append(slack_observation("U1", "a@b.com", "A", "hi", "ch", "s1"))
        .unwrap();

    let order = catalog.topological_order().unwrap();

    // Process in topological order.
    let lake_pos = lake.watermark().map(|w| w.position).unwrap_or(0);
    for proj_ref in &order {
        let state = wm_store.get_or_init(proj_ref);
        let current_pos = state.last_processed_position;
        let new_data = lake.since(current_pos);
        if !new_data.is_empty() {
            wm_store.update(proj_ref, lake_pos, BuildStatus::Success);
        }
    }

    // All watermarks should be at 1 now.
    let id_wm = wm_store.get(&ProjectionRef::new("proj:identity-resolution")).unwrap();
    let pp_wm = wm_store.get(&ProjectionRef::new("proj:person-page")).unwrap();
    assert_eq!(id_wm.last_processed_position, 1);
    assert_eq!(pp_wm.last_processed_position, 1);
}
