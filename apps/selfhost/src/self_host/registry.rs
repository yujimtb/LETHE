use lethe_core::domain::*;
use lethe_engine::projection::catalog::ProjectionCatalog;
use lethe_engine::projection::spec::*;
use lethe_registry::registry::{
    ObservationSchema, Observer, RegistryStore, SourceSystem, base_supplemental_kind_schemas,
};

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
            id: SourceSystemRef::new("sys:gmail"),
            name: "Gmail".into(),
            provider: Some("Google".into()),
            api_version: Some("v1".into()),
            source_class: SourceClass::ImmutableText,
        })
        .unwrap();
    registry
        .register_source_system(SourceSystem {
            id: SourceSystemRef::new("sys:discord"),
            name: "Discord".into(),
            provider: Some("Discord".into()),
            api_version: Some("v10".into()),
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
    registry
        .register_source_system(SourceSystem {
            id: SourceSystemRef::new("sys:claude-ai"),
            name: "claude.ai".into(),
            provider: Some("Anthropic".into()),
            api_version: None,
            source_class: SourceClass::ImmutableText,
        })
        .unwrap();
    registry
        .register_source_system(SourceSystem {
            id: SourceSystemRef::new("sys:chatgpt"),
            name: "ChatGPT".into(),
            provider: Some("OpenAI".into()),
            api_version: None,
            source_class: SourceClass::ImmutableText,
        })
        .unwrap();
    registry
        .register_source_system(SourceSystem {
            id: SourceSystemRef::new("sys:claude-code"),
            name: "Claude Code".into(),
            provider: Some("Anthropic".into()),
            api_version: None,
            source_class: SourceClass::ImmutableText,
        })
        .unwrap();
    registry
        .register_source_system(SourceSystem {
            id: SourceSystemRef::new("sys:github"),
            name: "GitHub".into(),
            provider: Some("GitHub".into()),
            api_version: Some("v3".into()),
            source_class: SourceClass::ImmutableText,
        })
        .unwrap();
    registry
        .register_source_system(SourceSystem {
            id: SourceSystemRef::new("sys:codex"),
            name: "Codex".into(),
            provider: Some("OpenAI".into()),
            api_version: None,
            source_class: SourceClass::ImmutableText,
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
        .register_observer(Observer {
            id: ObserverRef::new("obs:gmail-importer"),
            name: "Gmail Importer".into(),
            observer_type: ObserverType::Crawler,
            source_system: SourceSystemRef::new("sys:gmail"),
            adapter_version: SemVer::new("1.0.0"),
            schemas: vec![SchemaRef::new("schema:gmail-message")],
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            owner: "lethe".into(),
            trust_level: TrustLevel::Automated,
        })
        .unwrap();
    registry
        .register_observer(Observer {
            id: ObserverRef::new("obs:discord-importer"),
            name: "Discord Importer".into(),
            observer_type: ObserverType::Crawler,
            source_system: SourceSystemRef::new("sys:discord"),
            adapter_version: SemVer::new("1.0.0"),
            schemas: vec![SchemaRef::new("schema:discord-message")],
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            owner: "lethe".into(),
            trust_level: TrustLevel::Automated,
        })
        .unwrap();
    registry
        .register_observer(Observer {
            id: ObserverRef::new("obs:claude-ai-importer"),
            name: "claude.ai Importer".into(),
            observer_type: ObserverType::Crawler,
            source_system: SourceSystemRef::new("sys:claude-ai"),
            adapter_version: SemVer::new("1.0.0"),
            schemas: vec![SchemaRef::new("schema:claude-message")],
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            owner: "lethe".into(),
            trust_level: TrustLevel::Automated,
        })
        .unwrap();
    registry
        .register_observer(Observer {
            id: ObserverRef::new("obs:chatgpt-importer"),
            name: "ChatGPT Importer".into(),
            observer_type: ObserverType::Crawler,
            source_system: SourceSystemRef::new("sys:chatgpt"),
            adapter_version: SemVer::new("1.0.0"),
            schemas: vec![SchemaRef::new("schema:chatgpt-message")],
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            owner: "lethe".into(),
            trust_level: TrustLevel::Automated,
        })
        .unwrap();
    registry
        .register_observer(Observer {
            id: ObserverRef::new("obs:github-importer"),
            name: "GitHub Importer".into(),
            observer_type: ObserverType::Crawler,
            source_system: SourceSystemRef::new("sys:github"),
            adapter_version: SemVer::new("1.0.0"),
            schemas: vec![SchemaRef::new("schema:github-event")],
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            owner: "lethe".into(),
            trust_level: TrustLevel::Automated,
        })
        .unwrap();
    registry
        .register_observer(Observer {
            id: ObserverRef::new("obs:claude-code-importer"),
            name: "Claude Code Importer".into(),
            observer_type: ObserverType::Crawler,
            source_system: SourceSystemRef::new("sys:claude-code"),
            adapter_version: SemVer::new("1.0.0"),
            schemas: vec![SchemaRef::new("schema:coding-agent-message")],
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            owner: "lethe".into(),
            trust_level: TrustLevel::Automated,
        })
        .unwrap();
    registry
        .register_observer(Observer {
            id: ObserverRef::new("obs:codex-importer"),
            name: "Codex Importer".into(),
            observer_type: ObserverType::Crawler,
            source_system: SourceSystemRef::new("sys:codex"),
            adapter_version: SemVer::new("1.0.0"),
            schemas: vec![SchemaRef::new("schema:coding-agent-message")],
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
    for schema in base_supplemental_kind_schemas() {
        registry.register_supplemental_kind_schema(schema).unwrap();
    }

    registry
}

pub fn seed_projection_catalog() -> ProjectionCatalog {
    let mut catalog = ProjectionCatalog::new();
    catalog.register(identity_spec()).unwrap();
    catalog.register(person_page_spec()).unwrap();
    catalog.register(slide_analysis_spec()).unwrap();
    catalog.register(corpus_spec()).unwrap();
    catalog.register(claim_queue_spec()).unwrap();
    catalog.register(freshness_spec()).unwrap();
    catalog.register(reply_slo_spec()).unwrap();
    catalog.register(break_glass_spec()).unwrap();
    catalog.register(resume_snapshot_spec()).unwrap();
    catalog.register(plan_state_spec()).unwrap();
    catalog.register(card_queue_spec()).unwrap();
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
        &ProjectionRef::new("proj:claim-queue"),
        ProjectionStatus::Active,
    );
    catalog.set_status(
        &ProjectionRef::new("proj:freshness"),
        ProjectionStatus::Active,
    );
    catalog.set_status(
        &ProjectionRef::new("proj:reply-slo"),
        ProjectionStatus::Active,
    );
    catalog.set_status(
        &ProjectionRef::new("proj:break-glass"),
        ProjectionStatus::Active,
    );
    catalog.set_status(
        &ProjectionRef::new("proj:resume-snapshot"),
        ProjectionStatus::Active,
    );
    catalog.set_status(
        &ProjectionRef::new("proj:plan-state"),
        ProjectionStatus::Active,
    );
    catalog.set_status(
        &ProjectionRef::new("proj:card-queue"),
        ProjectionStatus::Active,
    );
    catalog.set_status(
        &ProjectionRef::new("proj:answer-log"),
        ProjectionStatus::Active,
    );
    catalog
}

fn base_schemas() -> Vec<ObservationSchema> {
    vec![
        ObservationSchema {
            id: SchemaRef::new("schema:claude-message"),
            name: "claude.ai Message".into(),
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
            id: SchemaRef::new("schema:chatgpt-message"),
            name: "ChatGPT Message".into(),
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
            id: SchemaRef::new("schema:github-event"),
            name: "GitHub Event".into(),
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
            id: SchemaRef::new("schema:coding-agent-message"),
            name: "Coding Agent Message".into(),
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
            id: SchemaRef::new("schema:gmail-message"),
            name: "Gmail Message".into(),
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
            id: SchemaRef::new("schema:discord-message"),
            name: "Discord Message".into(),
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

fn claim_queue_spec() -> ProjectionSpec {
    ProjectionSpec {
        id: ProjectionRef::new("proj:claim-queue"),
        name: "Claim Queue".into(),
        version: SemVer::new("1.0.0"),
        kind: ProjectionKind::CachedProjection,
        sources: vec![SourceDecl {
            source: SourceRef::Supplemental,
            filter_schemas: vec![],
            filter_derivations: vec![
                "claim@1".into(),
                "claim-transition@1".into(),
                "verification-result@1".into(),
                "decision@1".into(),
            ],
        }],
        read_modes: vec![ReadModePolicy {
            mode: ReadMode::OperationalLatest,
            source_policy: "supplemental-latest".into(),
        }],
        build: BuildSpec {
            build_type: "rust".into(),
            entrypoint: None,
            projector: "claim-queue".into(),
        },
        outputs: vec![OutputSpec {
            format: "json".into(),
            tables: vec![
                "claim_queue_claims".into(),
                "claim_queue_groups".into(),
                "decision_views".into(),
                "claim_queue_audit_log".into(),
            ],
        }],
        reconciliation: None,
        deterministic_in: vec![],
        gap_action: None,
        tags: vec!["claim-queue".into(), "decisions".into()],
        description: Some("Deduplicated claim queue and decision ledger projection".into()),
        created_by: "self-host".into(),
    }
}

fn freshness_spec() -> ProjectionSpec {
    ProjectionSpec {
        id: ProjectionRef::new("proj:freshness"),
        name: "Freshness".into(),
        version: SemVer::new("1.0.0"),
        kind: ProjectionKind::CachedProjection,
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
            projector: "freshness".into(),
        },
        outputs: vec![OutputSpec {
            format: "json".into(),
            tables: vec!["source_freshness".into(), "missing_sources".into()],
        }],
        reconciliation: None,
        deterministic_in: vec![],
        gap_action: None,
        tags: vec!["freshness".into(), "ops".into()],
        description: Some("Per-source latest observation freshness".into()),
        created_by: "self-host".into(),
    }
}

fn reply_slo_spec() -> ProjectionSpec {
    ProjectionSpec {
        id: ProjectionRef::new("proj:reply-slo"),
        name: "Reply SLO".into(),
        version: SemVer::new("1.0.0"),
        kind: ProjectionKind::CachedProjection,
        sources: vec![
            SourceDecl {
                source: SourceRef::Lake,
                filter_schemas: vec![
                    SchemaRef::new("schema:slack-message"),
                    SchemaRef::new("schema:gmail-message"),
                    SchemaRef::new("schema:discord-message"),
                ],
                filter_derivations: vec![],
            },
            SourceDecl {
                source: SourceRef::Supplemental,
                filter_schemas: vec![],
                filter_derivations: vec!["reply-draft@1".into(), "send-record@1".into()],
            },
        ],
        read_modes: vec![ReadModePolicy {
            mode: ReadMode::OperationalLatest,
            source_policy: "lake-and-supplemental-latest".into(),
        }],
        build: BuildSpec {
            build_type: "rust".into(),
            entrypoint: None,
            projector: "reply-slo".into(),
        },
        outputs: vec![OutputSpec {
            format: "json".into(),
            tables: vec!["reply_slo_rows".into(), "reply_slo_overdue".into()],
        }],
        reconciliation: Some(ReconciliationPolicy::DualTrack),
        deterministic_in: vec![],
        gap_action: None,
        tags: vec!["reply".into(), "slo".into(), "ops".into()],
        description: Some("Reply SLO status by communication observation".into()),
        created_by: "self-host".into(),
    }
}

fn break_glass_spec() -> ProjectionSpec {
    ProjectionSpec {
        id: ProjectionRef::new("proj:break-glass"),
        name: "Break Glass".into(),
        version: SemVer::new("1.0.0"),
        kind: ProjectionKind::CachedProjection,
        sources: vec![SourceDecl {
            source: SourceRef::SourceNative {
                system: "registry:channels".into(),
                read_mode: ReadMode::OperationalLatest,
                fallback: None,
            },
            filter_schemas: vec![],
            filter_derivations: vec![],
        }],
        read_modes: vec![ReadModePolicy {
            mode: ReadMode::OperationalLatest,
            source_policy: "registry-latest".into(),
        }],
        build: BuildSpec {
            build_type: "rust".into(),
            entrypoint: None,
            projector: "break-glass".into(),
        },
        outputs: vec![OutputSpec {
            format: "json".into(),
            tables: vec!["break_glass_channels".into()],
        }],
        reconciliation: None,
        deterministic_in: vec![],
        gap_action: None,
        tags: vec!["break-glass".into(), "ops".into(), "communications".into()],
        description: Some("Communication channel break-glass whitelist".into()),
        created_by: "self-host".into(),
    }
}

fn resume_snapshot_spec() -> ProjectionSpec {
    ProjectionSpec {
        id: ProjectionRef::new("proj:resume-snapshot"),
        name: "Resume Snapshot".into(),
        version: SemVer::new("1.0.0"),
        kind: ProjectionKind::CachedProjection,
        sources: vec![SourceDecl {
            source: SourceRef::Supplemental,
            filter_schemas: vec![],
            filter_derivations: vec![
                "session-summary@1".into(),
                "parking@1".into(),
                "claim@1".into(),
            ],
        }],
        read_modes: vec![ReadModePolicy {
            mode: ReadMode::OperationalLatest,
            source_policy: "supplemental-latest".into(),
        }],
        build: BuildSpec {
            build_type: "rust".into(),
            entrypoint: None,
            projector: "resume-snapshot".into(),
        },
        outputs: vec![OutputSpec {
            format: "json".into(),
            tables: vec!["resume_project_cards".into()],
        }],
        reconciliation: None,
        deterministic_in: vec![],
        gap_action: None,
        tags: vec!["resume".into()],
        description: Some(
            "Project resume cards from session summaries, parking, and open claims".into(),
        ),
        created_by: "self-host".into(),
    }
}

fn plan_state_spec() -> ProjectionSpec {
    ProjectionSpec {
        id: ProjectionRef::new("proj:plan-state"),
        name: "Plan State".into(),
        version: SemVer::new("1.0.0"),
        kind: ProjectionKind::CachedProjection,
        sources: vec![SourceDecl {
            source: SourceRef::Supplemental,
            filter_schemas: vec![],
            filter_derivations: vec!["claim@1".into(), "parking@1".into(), "decision@1".into()],
        }],
        read_modes: vec![ReadModePolicy {
            mode: ReadMode::OperationalLatest,
            source_policy: "supplemental-latest".into(),
        }],
        build: BuildSpec {
            build_type: "rust".into(),
            entrypoint: None,
            projector: "plan-state".into(),
        },
        outputs: vec![OutputSpec {
            format: "json".into(),
            tables: vec!["plan_state_projects".into()],
        }],
        reconciliation: None,
        deterministic_in: vec![],
        gap_action: None,
        tags: vec!["plan-state".into()],
        description: Some("Portfolio plan state by project".into()),
        created_by: "self-host".into(),
    }
}

fn card_queue_spec() -> ProjectionSpec {
    ProjectionSpec {
        id: ProjectionRef::new("proj:card-queue"),
        name: "Card Queue".into(),
        version: SemVer::new("1.0.0"),
        kind: ProjectionKind::CachedProjection,
        sources: vec![SourceDecl {
            source: SourceRef::Supplemental,
            filter_schemas: vec![],
            filter_derivations: vec![
                "reply-draft@1".into(),
                "reply-approval@1".into(),
                "send-record@1".into(),
            ],
        }],
        read_modes: vec![ReadModePolicy {
            mode: ReadMode::OperationalLatest,
            source_policy: "supplemental-latest".into(),
        }],
        build: BuildSpec {
            build_type: "rust".into(),
            entrypoint: None,
            projector: "card-queue".into(),
        },
        outputs: vec![OutputSpec {
            format: "json".into(),
            tables: vec!["reply_cards".into(), "card_queue_audit_log".into()],
        }],
        reconciliation: None,
        deterministic_in: vec![],
        gap_action: None,
        tags: vec!["card-queue".into(), "reply".into()],
        description: Some("Reply draft approval and send state machine".into()),
        created_by: "self-host".into(),
    }
}
