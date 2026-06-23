use lethe_core::domain::*;
use lethe_engine::projection::catalog::ProjectionCatalog;
use lethe_engine::projection::spec::*;
use lethe_registry::registry::{ObservationSchema, Observer, RegistryStore, SourceSystem};

pub fn seed_registry() -> RegistryStore {
    let mut registry = RegistryStore::new();

    registry
        .register_source_system(SourceSystem {
            id: SourceSystemRef::new("sys:slack"),
            name: "Slack".into(),
            provider: Some("Slack".into()),
            api_version: Some("v1".into()),
            source_class: SourceClass::ImmutableText,
        })
        .unwrap();
    registry
        .register_source_system(SourceSystem {
            id: SourceSystemRef::new("sys:google-slides"),
            name: "Google Slides".into(),
            provider: Some("Google".into()),
            api_version: Some("v1".into()),
            source_class: SourceClass::MutableMultimodal,
        })
        .unwrap();
    for (id, name, source_class) in [
        ("sys:google-docs", "Google Docs", SourceClass::MutableText),
        (
            "sys:google-sheets",
            "Google Sheets",
            SourceClass::MutableText,
        ),
        ("sys:google-forms", "Google Forms", SourceClass::MutableText),
        (
            "sys:google-drive",
            "Google Drive",
            SourceClass::MutableMultimodal,
        ),
    ] {
        registry
            .register_source_system(SourceSystem {
                id: SourceSystemRef::new(id),
                name: name.into(),
                provider: Some("Google".into()),
                api_version: Some("v1".into()),
                source_class,
            })
            .unwrap();
    }

    registry
        .register_observer(Observer {
            id: ObserverRef::new("obs:slack-crawler"),
            name: "Slack Crawler".into(),
            observer_type: ObserverType::Crawler,
            source_system: SourceSystemRef::new("sys:slack"),
            adapter_version: SemVer::new("1.0.0"),
            schemas: vec![
                SchemaRef::new("schema:slack-message"),
                SchemaRef::new("schema:slack-channel-snapshot"),
                SchemaRef::new("schema:observer-heartbeat"),
            ],
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            owner: "lethe".into(),
            trust_level: TrustLevel::Automated,
        })
        .unwrap();
    registry
        .register_source_system(SourceSystem {
            id: SourceSystemRef::new("sys:lethe-governance"),
            name: "LETHE Governance".into(),
            provider: Some("LETHE".into()),
            api_version: Some("v1".into()),
            source_class: SourceClass::ImmutableText,
        })
        .unwrap();
    registry
        .register_observer(Observer {
            id: ObserverRef::new("obs:consent-ledger"),
            name: "Consent Ledger".into(),
            observer_type: ObserverType::Human,
            source_system: SourceSystemRef::new("sys:lethe-governance"),
            adapter_version: SemVer::new("1.0.0"),
            schemas: vec![SchemaRef::new("schema:consent-decision")],
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            owner: "lethe-governance".into(),
            trust_level: TrustLevel::HumanVerified,
        })
        .unwrap();
    registry
        .register_observer(Observer {
            id: ObserverRef::new("obs:gslides-crawler"),
            name: "Google Slides Crawler".into(),
            observer_type: ObserverType::Crawler,
            source_system: SourceSystemRef::new("sys:google-slides"),
            adapter_version: SemVer::new("1.0.0"),
            schemas: vec![
                SchemaRef::new("schema:workspace-object-snapshot"),
                SchemaRef::new("schema:observer-heartbeat"),
            ],
            authority_model: AuthorityModel::SourceAuthoritative,
            capture_model: CaptureModel::Snapshot,
            owner: "lethe".into(),
            trust_level: TrustLevel::Automated,
        })
        .unwrap();
    for (observer_id, name, source_system) in [
        (
            "obs:gdocs-crawler",
            "Google Docs Crawler",
            "sys:google-docs",
        ),
        (
            "obs:gsheets-crawler",
            "Google Sheets Crawler",
            "sys:google-sheets",
        ),
        (
            "obs:gforms-crawler",
            "Google Forms Crawler",
            "sys:google-forms",
        ),
        (
            "obs:gdrive-crawler",
            "Google Drive Crawler",
            "sys:google-drive",
        ),
    ] {
        registry
            .register_observer(Observer {
                id: ObserverRef::new(observer_id),
                name: name.into(),
                observer_type: ObserverType::Crawler,
                source_system: SourceSystemRef::new(source_system),
                adapter_version: SemVer::new("1.0.0"),
                schemas: vec![
                    SchemaRef::new("schema:workspace-object-snapshot"),
                    SchemaRef::new("schema:observer-heartbeat"),
                ],
                authority_model: AuthorityModel::SourceAuthoritative,
                capture_model: CaptureModel::Snapshot,
                owner: "lethe".into(),
                trust_level: TrustLevel::Automated,
            })
            .unwrap();
    }

    registry
        .register_source_system(SourceSystem {
            id: SourceSystemRef::new("sys:lethe-internal"),
            name: "LETHE Internal".into(),
            provider: Some("LETHE".into()),
            api_version: None,
            source_class: SourceClass::ImmutableText,
        })
        .unwrap();
    registry
        .register_observer(Observer {
            id: ObserverRef::new("obs:slide-analysis-projector"),
            name: "Slide Analysis Projector".into(),
            observer_type: ObserverType::Bot,
            source_system: SourceSystemRef::new("sys:lethe-internal"),
            adapter_version: SemVer::new("1.0.0"),
            schemas: vec![SchemaRef::new("schema:slide-analysis-result")],
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            owner: "lethe".into(),
            trust_level: TrustLevel::Automated,
        })
        .unwrap();
    registry
        .register_observer(Observer {
            id: ObserverRef::new("obs:search-bot"),
            name: "Workspace Search Bot".into(),
            observer_type: ObserverType::Bot,
            source_system: SourceSystemRef::new("sys:lethe-internal"),
            adapter_version: SemVer::new("1.0.0"),
            schemas: vec![SchemaRef::new("schema:bot-answer-log")],
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            owner: "lethe".into(),
            trust_level: TrustLevel::Automated,
        })
        .unwrap();

    for schema in base_schemas() {
        registry.register_schema(schema).unwrap();
    }

    registry
}

pub fn seed_projection_catalog() -> ProjectionCatalog {
    let mut catalog = ProjectionCatalog::new();
    catalog.register(identity_spec()).unwrap();
    catalog.register(person_page_spec()).unwrap();
    catalog.register(slide_analysis_spec()).unwrap();
    catalog.register(corpus_spec()).unwrap();
    catalog.register(answer_log_spec()).unwrap();
    catalog.set_status(
        &ProjectionRef::new("proj:identity-resolution"),
        ProjectionStatus::Active,
    );
    catalog.set_status(
        &ProjectionRef::new("proj:person-page"),
        ProjectionStatus::Active,
    );
    catalog.set_status(
        &ProjectionRef::new("proj:slide-analysis"),
        ProjectionStatus::Active,
    );
    catalog.set_status(&ProjectionRef::new("proj:corpus"), ProjectionStatus::Active);
    catalog.set_status(
        &ProjectionRef::new("proj:answer-log"),
        ProjectionStatus::Active,
    );
    catalog
}

fn base_schemas() -> Vec<ObservationSchema> {
    vec![
        ObservationSchema {
            id: SchemaRef::new("schema:slack-message"),
            name: "Slack Message".into(),
            version: SemVer::new("1.0.0"),
            subject_type: EntityTypeRef::new("et:message"),
            target_type: None,
            payload_schema: serde_json::json!({"type": "object"}),
            source_contracts: vec![],
            attachment_config: None,
            registered_by: None,
            registered_at: None,
        },
        ObservationSchema {
            id: SchemaRef::new("schema:slack-channel-snapshot"),
            name: "Slack Channel Snapshot".into(),
            version: SemVer::new("1.0.0"),
            subject_type: EntityTypeRef::new("et:*"),
            target_type: None,
            payload_schema: serde_json::json!({"type": "object"}),
            source_contracts: vec![],
            attachment_config: None,
            registered_by: None,
            registered_at: None,
        },
        ObservationSchema {
            id: SchemaRef::new("schema:workspace-object-snapshot"),
            name: "Workspace Object Snapshot".into(),
            version: SemVer::new("1.0.0"),
            subject_type: EntityTypeRef::new("et:document"),
            target_type: None,
            payload_schema: serde_json::json!({"type": "object"}),
            source_contracts: vec![],
            attachment_config: None,
            registered_by: None,
            registered_at: None,
        },
        ObservationSchema {
            id: SchemaRef::new("schema:observer-heartbeat"),
            name: "Observer Heartbeat".into(),
            version: SemVer::new("1.0.0"),
            subject_type: EntityTypeRef::new("et:observer"),
            target_type: None,
            payload_schema: serde_json::json!({"type": "object"}),
            source_contracts: vec![],
            attachment_config: None,
            registered_by: None,
            registered_at: None,
        },
        ObservationSchema {
            id: SchemaRef::new("schema:bot-answer-log"),
            name: "Bot Answer Log".into(),
            version: SemVer::new("1.0.0"),
            subject_type: EntityTypeRef::new("et:answer-log"),
            target_type: None,
            payload_schema: serde_json::json!({
                "type": "object",
                "required": ["question", "answer", "ts"],
                "properties": {
                    "question": {"type": "string"},
                    "answer": {"type": "string"},
                    "citations": {"type": "array"},
                    "used_queries": {"type": "array"},
                    "asker": {"type": "string"},
                    "ts": {"type": "string"},
                    "model": {"type": "string"},
                    "usage": {"type": "object"},
                    "confidence": {"type": "string"},
                    "unknowns": {"type": "array"}
                }
            }),
            source_contracts: vec![],
            attachment_config: None,
            registered_by: None,
            registered_at: None,
        },
        ObservationSchema {
            id: SchemaRef::new("schema:slide-analysis-result"),
            name: "Slide Analysis Result".into(),
            version: SemVer::new("1.0.0"),
            subject_type: EntityTypeRef::new("et:person"),
            target_type: Some(EntityTypeRef::new("et:document")),
            payload_schema: serde_json::json!({"type": "object"}),
            source_contracts: vec![],
            attachment_config: None,
            registered_by: None,
            registered_at: None,
        },
        ObservationSchema {
            id: SchemaRef::new("schema:consent-decision"),
            name: "Consent Decision".into(),
            version: SemVer::new("1.0.0"),
            subject_type: EntityTypeRef::new("et:person"),
            target_type: None,
            payload_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "status": {
                        "type": "string",
                        "enum": ["unrestricted", "restricted_capture", "opted_out"]
                    },
                    "identifier": { "type": "string" },
                    "reason": { "type": "string" }
                },
                "required": ["status"],
                "additionalProperties": false
            }),
            source_contracts: vec![],
            attachment_config: None,
            registered_by: None,
            registered_at: None,
        },
    ]
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
            format: "json".into(),
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
        created_by: "self-host".into(),
    }
}

fn person_page_spec() -> ProjectionSpec {
    ProjectionSpec {
        id: ProjectionRef::new("proj:person-page"),
        name: "Person Page".into(),
        version: SemVer::new("1.0.0"),
        kind: ProjectionKind::CachedProjection,
        sources: vec![
            SourceDecl {
                source: SourceRef::Lake,
                filter_schemas: vec![],
                filter_derivations: vec![],
            },
            SourceDecl {
                source: SourceRef::Projection {
                    id: ProjectionRef::new("proj:identity-resolution"),
                    version: ">=1.0.0".into(),
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
            format: "json".into(),
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
        created_by: "self-host".into(),
    }
}

fn slide_analysis_spec() -> ProjectionSpec {
    ProjectionSpec {
        id: ProjectionRef::new("proj:slide-analysis"),
        name: "Slide Analysis".into(),
        version: SemVer::new("1.0.0"),
        kind: ProjectionKind::CachedProjection,
        sources: vec![
            SourceDecl {
                source: SourceRef::Lake,
                filter_schemas: vec![SchemaRef::new("schema:workspace-object-snapshot")],
                filter_derivations: vec![],
            },
            SourceDecl {
                source: SourceRef::Supplemental,
                filter_schemas: vec![],
                filter_derivations: vec!["slide-analysis".into()],
            },
        ],
        read_modes: vec![ReadModePolicy {
            mode: ReadMode::OperationalLatest,
            source_policy: "lake-latest".into(),
        }],
        build: BuildSpec {
            build_type: "rust".into(),
            entrypoint: None,
            projector: "slide-analysis".into(),
        },
        outputs: vec![OutputSpec {
            format: "json".into(),
            tables: vec!["slide_analysis_results".into()],
        }],
        reconciliation: Some(ReconciliationPolicy::LakeFirst),
        deterministic_in: vec![],
        gap_action: None,
        tags: vec!["slide-analysis".into()],
        description: Some("Analyse Google Slides into supplemental records".into()),
        created_by: "self-host".into(),
    }
}

fn corpus_spec() -> ProjectionSpec {
    ProjectionSpec {
        id: ProjectionRef::new("proj:corpus"),
        name: "Access Controlled Corpus".into(),
        version: SemVer::new("1.0.0"),
        kind: ProjectionKind::CachedProjection,
        sources: vec![SourceDecl {
            source: SourceRef::Lake,
            filter_schemas: vec![
                SchemaRef::new("schema:slack-message"),
                SchemaRef::new("schema:workspace-object-snapshot"),
            ],
            filter_derivations: vec![],
        }],
        read_modes: vec![ReadModePolicy {
            mode: ReadMode::OperationalLatest,
            source_policy: "lake-latest".into(),
        }],
        build: BuildSpec {
            build_type: "rust".into(),
            entrypoint: None,
            projector: "corpus".into(),
        },
        outputs: vec![OutputSpec {
            format: "json".into(),
            tables: vec!["corpus_records".into()],
        }],
        reconciliation: None,
        deterministic_in: vec![],
        gap_action: None,
        tags: vec!["workspace-search".into(), "corpus".into()],
        description: Some("Bot-visible access-controlled workspace corpus".into()),
        created_by: "self-host".into(),
    }
}

fn answer_log_spec() -> ProjectionSpec {
    ProjectionSpec {
        id: ProjectionRef::new("proj:answer-log"),
        name: "Answer Log".into(),
        version: SemVer::new("1.0.0"),
        kind: ProjectionKind::CachedProjection,
        sources: vec![SourceDecl {
            source: SourceRef::Lake,
            filter_schemas: vec![SchemaRef::new("schema:bot-answer-log")],
            filter_derivations: vec![],
        }],
        read_modes: vec![ReadModePolicy {
            mode: ReadMode::OperationalLatest,
            source_policy: "lake-latest".into(),
        }],
        build: BuildSpec {
            build_type: "rust".into(),
            entrypoint: None,
            projector: "answer-log".into(),
        },
        outputs: vec![OutputSpec {
            format: "json".into(),
            tables: vec!["answer_log_records".into()],
        }],
        reconciliation: None,
        deterministic_in: vec![],
        gap_action: None,
        tags: vec!["workspace-search".into(), "answer-log".into()],
        description: Some("Search bot prior answer log projection".into()),
        created_by: "self-host".into(),
    }
}
