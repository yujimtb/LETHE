use super::*;
use lethe_projection_claim_queue::{ClaimGroup, ClaimState, DecisionView, ProjectionAuditEvent};
use lethe_projection_cognition::{CardState, ReplyCard};
use std::collections::BTreeMap;

#[derive(Debug, Clone, serde::Serialize)]
pub struct CorpusSourceTypeSummary {
    pub source_type: String,
    pub records: usize,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ClaimQueuePage {
    pub groups: Vec<ClaimGroup>,
    pub total: usize,
    pub limit: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub audit_log: Vec<ProjectionAuditEvent>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DecisionSearchPage {
    pub query: String,
    pub matches: Vec<DecisionView>,
    pub total: usize,
    pub limit: usize,
    pub audit_log: Vec<ProjectionAuditEvent>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CardQueuePage {
    pub cards: Vec<ReplyCard>,
    pub total: usize,
    pub limit: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
    pub audit_log: Vec<ProjectionAuditEvent>,
}

impl AppService {
    pub fn persons_response(
        &self,
        read_mode: Option<&str>,
        pin: Option<&str>,
        pagination: &PaginationParams,
    ) -> Result<ResponseEnvelope<serde_json::Value>, SelfHostError> {
        if pagination.limit == 0 || pagination.limit > self.config.resource_limits.max_page_size {
            return Err(SelfHostError::ReadMode(format!(
                "page limit must be between 1 and {}",
                self.config.resource_limits.max_page_size
            )));
        }
        let core = self.core_lock()?;
        let mode = self.resolve_read_mode(&core.catalog, "proj:person-page", read_mode, pin)?;
        self.authorize_read(
            EntityRef::new("projection:person-page"),
            ConsentStatus::RestrictedCapture,
        )?;

        let mut list: Vec<PersonListItem> = core
            .snapshot
            .person_page
            .profiles
            .iter()
            .filter_map(|profile| {
                let activity = core
                    .snapshot
                    .person_page
                    .activities
                    .iter()
                    .find(|activity| activity.person_id == profile.person_id)?;
                Some(PersonPageProjector::to_list_item(profile, activity))
            })
            .collect();
        list.sort_by(|left, right| right.last_activity.cmp(&left.last_activity));

        let (page, total) = paginate(&list, pagination);
        let payload = serde_json::to_value(PaginatedResponse::from_slice(page, total, pagination))?;

        Ok(ResponseEnvelope {
            data: self.apply_filter(payload),
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:person-page",
                mode,
                core.snapshot.built_at,
                &core.snapshot.lineage,
            )?,
        })
    }

    pub fn person_detail_response(
        &self,
        person_id: &str,
        read_mode: Option<&str>,
        pin: Option<&str>,
    ) -> Result<ResponseEnvelope<serde_json::Value>, SelfHostError> {
        let core = self.core_lock()?;
        let mode = self.resolve_read_mode(&core.catalog, "proj:person-page", read_mode, pin)?;
        let profile = core
            .snapshot
            .person_page
            .profiles
            .iter()
            .find(|profile| profile.person_id.as_str() == person_id)
            .ok_or_else(|| SelfHostError::NotFound(person_id.to_string()))?;
        self.authorize_read(
            EntityRef::new(person_id.to_string()),
            consent_status_for_person_id(&core, person_id)?,
        )?;
        let slides: Vec<_> = core
            .snapshot
            .person_page
            .slides
            .iter()
            .filter(|slide| slide.person_id == profile.person_id)
            .cloned()
            .collect();
        let messages: Vec<_> = core
            .snapshot
            .person_page
            .messages
            .iter()
            .filter(|message| message.person_id == profile.person_id)
            .cloned()
            .collect();
        let activity = core
            .snapshot
            .person_page
            .activities
            .iter()
            .find(|activity| activity.person_id == profile.person_id)
            .ok_or_else(|| SelfHostError::NotFound(format!("activity for {person_id}")))?;

        let detail: PersonDetailResponse =
            PersonPageProjector::to_detail(profile, &slides, &messages, activity);
        Ok(ResponseEnvelope {
            data: self.apply_filter(serde_json::to_value(detail)?),
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:person-page",
                mode,
                core.snapshot.built_at,
                &core.snapshot.lineage,
            )?,
        })
    }

    pub fn person_slides_response(
        &self,
        person_id: &str,
        read_mode: Option<&str>,
        pin: Option<&str>,
    ) -> Result<ResponseEnvelope<serde_json::Value>, SelfHostError> {
        let core = self.core_lock()?;
        let mode = self.resolve_read_mode(&core.catalog, "proj:person-page", read_mode, pin)?;
        self.authorize_read(
            EntityRef::new(person_id.to_string()),
            consent_status_for_person_id(&core, person_id)?,
        )?;
        let slides: Vec<_> = core
            .snapshot
            .person_page
            .slides
            .iter()
            .filter(|slide| slide.person_id.as_str() == person_id)
            .cloned()
            .collect();

        Ok(ResponseEnvelope {
            data: self.apply_filter(serde_json::to_value(slides)?),
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:person-page",
                mode,
                core.snapshot.built_at,
                &core.snapshot.lineage,
            )?,
        })
    }

    pub fn person_messages_response(
        &self,
        person_id: &str,
        read_mode: Option<&str>,
        pin: Option<&str>,
    ) -> Result<ResponseEnvelope<serde_json::Value>, SelfHostError> {
        let core = self.core_lock()?;
        let mode = self.resolve_read_mode(&core.catalog, "proj:person-page", read_mode, pin)?;
        self.authorize_read(
            EntityRef::new(person_id.to_string()),
            consent_status_for_person_id(&core, person_id)?,
        )?;
        let messages: Vec<_> = core
            .snapshot
            .person_page
            .messages
            .iter()
            .filter(|message| message.person_id.as_str() == person_id)
            .cloned()
            .collect();

        Ok(ResponseEnvelope {
            data: self.apply_filter(serde_json::to_value(messages)?),
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:person-page",
                mode,
                core.snapshot.built_at,
                &core.snapshot.lineage,
            )?,
        })
    }

    pub fn person_timeline_response(
        &self,
        person_id: &str,
        read_mode: Option<&str>,
        pin: Option<&str>,
    ) -> Result<ResponseEnvelope<serde_json::Value>, SelfHostError> {
        let core = self.core_lock()?;
        let mode = self.resolve_read_mode(&core.catalog, "proj:person-page", read_mode, pin)?;
        self.authorize_read(
            EntityRef::new(person_id.to_string()),
            consent_status_for_person_id(&core, person_id)?,
        )?;
        let mut events = Vec::new();

        for slide in core
            .snapshot
            .person_page
            .slides
            .iter()
            .filter(|slide| slide.person_id.as_str() == person_id)
        {
            if let Some(ts) = slide.last_modified {
                events.push(TimelineEvent {
                    event_type: "slide".into(),
                    document_id: Some(slide.document_id.clone()),
                    channel: None,
                    title: Some(slide.title.clone()),
                    text: None,
                    ts,
                });
            }
        }

        for message in core
            .snapshot
            .person_page
            .messages
            .iter()
            .filter(|message| message.person_id.as_str() == person_id)
        {
            events.push(TimelineEvent {
                event_type: "message".into(),
                document_id: None,
                channel: Some(message.channel.clone()),
                title: None,
                text: Some(message.text.clone()),
                ts: message.ts,
            });
        }

        events.sort_by(|left, right| right.ts.cmp(&left.ts));

        Ok(ResponseEnvelope {
            data: self.apply_filter(serde_json::to_value(events)?),
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:person-page",
                mode,
                core.snapshot.built_at,
                &core.snapshot.lineage,
            )?,
        })
    }

    pub fn corpus_records_response(
        &self,
        read_mode: Option<&str>,
        pin: Option<&str>,
        pagination: &PaginationParams,
    ) -> Result<ResponseEnvelope<serde_json::Value>, SelfHostError> {
        if pagination.limit == 0 || pagination.limit > self.config.resource_limits.max_page_size {
            return Err(SelfHostError::ReadMode(format!(
                "page limit must be between 1 and {}",
                self.config.resource_limits.max_page_size
            )));
        }
        let core = self.core_lock()?;
        let mode = self.resolve_read_mode(&core.catalog, "proj:corpus", read_mode, pin)?;
        let lineage = build_projection_lineage(
            "proj:corpus",
            core.lake.list(),
            core.snapshot.corpus.len(),
            core.snapshot.built_at,
        );
        let (page, total) = paginate(&core.snapshot.corpus, pagination);
        let payload = serde_json::to_value(PaginatedResponse::from_slice(page, total, pagination))?;
        Ok(ResponseEnvelope {
            data: payload,
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:corpus",
                mode,
                core.snapshot.built_at,
                &lineage,
            )?,
        })
    }

    pub fn corpus_grep_response(
        &self,
        request: &lethe_api::api::grep::GrepRequest,
    ) -> Result<ResponseEnvelope<lethe_api::api::grep::GrepResponse>, SelfHostError> {
        let core = self.core_lock()?;
        let mode = self.resolve_read_mode(&core.catalog, "proj:corpus", None, None)?;
        let records = core
            .snapshot
            .corpus
            .iter()
            .cloned()
            .map(grep_record_from_corpus)
            .collect::<Vec<_>>();
        let lineage = build_projection_lineage(
            "proj:corpus",
            core.lake.list(),
            core.snapshot.corpus.len(),
            core.snapshot.built_at,
        );
        let response =
            lethe_api::api::grep::GrepEngine::new(self.config.resource_limits.max_page_size)
                .search(
                    &records,
                    request,
                    lethe_projection_corpus::projection_watermark(&core.snapshot.corpus),
                )
                .map_err(|err| SelfHostError::ReadMode(err.to_string()))?;
        Ok(ResponseEnvelope {
            data: response,
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:corpus",
                mode,
                core.snapshot.built_at,
                &lineage,
            )?,
        })
    }

    pub fn corpus_source_type_summaries(
        &self,
    ) -> Result<Vec<CorpusSourceTypeSummary>, SelfHostError> {
        let core = self.core_lock()?;
        let mut counts = BTreeMap::<String, usize>::new();
        for record in &core.snapshot.corpus {
            *counts.entry(record.source_type.clone()).or_insert(0) += 1;
        }
        Ok(counts
            .into_iter()
            .map(|(source_type, records)| CorpusSourceTypeSummary {
                source_type,
                records,
            })
            .collect())
    }

    pub fn corpus_record_response(
        &self,
        record_id: &str,
    ) -> Result<ResponseEnvelope<lethe_api::api::grep::RecordDetailResponse>, SelfHostError> {
        let core = self.core_lock()?;
        let record = core
            .snapshot
            .corpus
            .iter()
            .find(|record| record.record_id == record_id)
            .cloned()
            .ok_or_else(|| SelfHostError::NotFound(record_id.to_owned()))?;
        let lineage = build_projection_lineage(
            "proj:corpus",
            core.lake.list(),
            core.snapshot.corpus.len(),
            core.snapshot.built_at,
        );
        Ok(ResponseEnvelope {
            data: lethe_api::api::grep::RecordDetailResponse {
                record: grep_record_from_corpus(record),
            },
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:corpus",
                ReadMode::OperationalLatest,
                core.snapshot.built_at,
                &lineage,
            )?,
        })
    }

    pub fn corpus_thread_response(
        &self,
        thread_ref: &str,
    ) -> Result<ResponseEnvelope<lethe_api::api::grep::ThreadResponse>, SelfHostError> {
        let core = self.core_lock()?;
        let response = build_corpus_thread_response(&core.snapshot.corpus, thread_ref)?;
        let lineage = build_projection_lineage(
            "proj:corpus",
            core.lake.list(),
            core.snapshot.corpus.len(),
            core.snapshot.built_at,
        );
        Ok(ResponseEnvelope {
            data: response,
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:corpus",
                ReadMode::OperationalLatest,
                core.snapshot.built_at,
                &lineage,
            )?,
        })
    }

    pub fn corpus_thread_response_paged(
        &self,
        thread_ref: &str,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<ResponseEnvelope<lethe_api::api::grep::ThreadResponse>, SelfHostError> {
        if limit == 0 || limit > self.config.resource_limits.max_page_size {
            return Err(SelfHostError::ReadMode(format!(
                "page limit must be between 1 and {}",
                self.config.resource_limits.max_page_size
            )));
        }
        let core = self.core_lock()?;
        let response = build_corpus_thread_response(&core.snapshot.corpus, thread_ref)?;
        let response = page_thread_response(response, limit, cursor)?;
        let lineage = build_projection_lineage(
            "proj:corpus",
            core.lake.list(),
            core.snapshot.corpus.len(),
            core.snapshot.built_at,
        );
        Ok(ResponseEnvelope {
            data: response,
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:corpus",
                ReadMode::OperationalLatest,
                core.snapshot.built_at,
                &lineage,
            )?,
        })
    }

    pub fn resolve_link_response(
        &self,
        request: &lethe_api::api::grep::ResolveLinkRequest,
    ) -> Result<ResponseEnvelope<lethe_api::api::grep::ResolveLinkResponse>, SelfHostError> {
        let core = self.core_lock()?;
        let record = core
            .snapshot
            .corpus
            .iter()
            .find(|record| {
                record.anchor_url == request.url || request.url.starts_with(&record.anchor_url)
            })
            .ok_or_else(|| SelfHostError::NotFound(request.url.clone()))?;
        let lineage = build_projection_lineage(
            "proj:corpus",
            core.lake.list(),
            core.snapshot.corpus.len(),
            core.snapshot.built_at,
        );
        Ok(ResponseEnvelope {
            data: lethe_api::api::grep::ResolveLinkResponse {
                record_id: record.record_id.clone(),
                source_type: record.source_type.clone(),
                anchor_url: record.anchor_url.clone(),
            },
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:corpus",
                ReadMode::OperationalLatest,
                core.snapshot.built_at,
                &lineage,
            )?,
        })
    }

    pub fn prior_qa_search_response(
        &self,
        request: &lethe_api::api::grep::PriorQaSearchRequest,
    ) -> Result<
        ResponseEnvelope<
            lethe_api::api::grep::PriorQaSearchResponse<lethe_projection_answer_log::PriorQaResult>,
        >,
        SelfHostError,
    > {
        let core = self.core_lock()?;
        let limit = request
            .limit
            .unwrap_or(20)
            .min(self.config.resource_limits.max_page_size);
        let projector = AnswerLogProjector;
        let matches = projector.search(&core.snapshot.answer_log, &request.query, limit);
        let lineage = build_projection_lineage(
            "proj:answer-log",
            core.lake.list(),
            core.snapshot.answer_log.len(),
            core.snapshot.built_at,
        );
        Ok(ResponseEnvelope {
            data: lethe_api::api::grep::PriorQaSearchResponse {
                matches,
                is_primary_source: false,
            },
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:answer-log",
                ReadMode::OperationalLatest,
                core.snapshot.built_at,
                &lineage,
            )?,
        })
    }

    pub fn claim_queue_response(
        &self,
        state: Option<ClaimState>,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<ResponseEnvelope<ClaimQueuePage>, SelfHostError> {
        self.claim_queue_response_filtered(state, None, None, limit, cursor)
    }

    pub fn claim_queue_response_filtered(
        &self,
        state: Option<ClaimState>,
        verification_mode: Option<&str>,
        backfill: Option<bool>,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<ResponseEnvelope<ClaimQueuePage>, SelfHostError> {
        if limit == 0 || limit > self.config.resource_limits.max_page_size {
            return Err(SelfHostError::ReadMode(format!(
                "page limit must be between 1 and {}",
                self.config.resource_limits.max_page_size
            )));
        }
        validate_verification_mode_filter(verification_mode)?;
        let offset = parse_cursor(cursor)?;
        let core = self.core_lock()?;
        self.ensure_projection_fresh(&core.catalog, "proj:claim-queue")?;
        let mode = self.resolve_read_mode(&core.catalog, "proj:claim-queue", None, None)?;
        let mut groups = core.snapshot.claim_queue.groups_matching(state, backfill);
        if let Some(verification_mode) = verification_mode {
            for group in &mut groups {
                group
                    .members
                    .retain(|claim| claim.verification_mode == verification_mode);
            }
            groups.retain(|group| !group.members.is_empty());
        }
        let total = groups.len();
        let start = offset.min(total);
        let end = (start + limit).min(total);
        let next_cursor = if end < total {
            Some(end.to_string())
        } else {
            None
        };
        let page = groups[start..end].to_vec();
        let lineage = build_supplemental_projection_lineage(
            "proj:claim-queue",
            &core.supplemental.list(),
            core.snapshot.claim_queue.claims.len() + core.snapshot.claim_queue.decisions.len(),
            core.snapshot.built_at,
        );

        Ok(ResponseEnvelope {
            data: ClaimQueuePage {
                groups: page,
                total,
                limit,
                next_cursor,
                audit_log: core.snapshot.claim_queue.audit_log.clone(),
            },
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:claim-queue",
                mode,
                core.snapshot.built_at,
                &lineage,
            )?,
        })
    }

    pub fn decision_search_response(
        &self,
        query: Option<&str>,
        limit: usize,
    ) -> Result<ResponseEnvelope<DecisionSearchPage>, SelfHostError> {
        if limit == 0 || limit > self.config.resource_limits.max_page_size {
            return Err(SelfHostError::ReadMode(format!(
                "page limit must be between 1 and {}",
                self.config.resource_limits.max_page_size
            )));
        }
        let query = query
            .map(str::trim)
            .filter(|query| !query.is_empty())
            .ok_or_else(|| SelfHostError::ReadMode("q must not be blank".to_owned()))?;
        let core = self.core_lock()?;
        self.ensure_projection_fresh(&core.catalog, "proj:claim-queue")?;
        let mode = self.resolve_read_mode(&core.catalog, "proj:claim-queue", None, None)?;
        let all_matches = core
            .snapshot
            .claim_queue
            .search_decisions(query, usize::MAX);
        let total = all_matches.len();
        let matches = all_matches.into_iter().take(limit).collect::<Vec<_>>();
        let lineage = build_supplemental_projection_lineage(
            "proj:claim-queue",
            &core.supplemental.list(),
            core.snapshot.claim_queue.claims.len() + core.snapshot.claim_queue.decisions.len(),
            core.snapshot.built_at,
        );

        Ok(ResponseEnvelope {
            data: DecisionSearchPage {
                query: query.to_owned(),
                matches,
                total,
                limit,
                audit_log: core.snapshot.claim_queue.audit_log.clone(),
            },
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:claim-queue",
                mode,
                core.snapshot.built_at,
                &lineage,
            )?,
        })
    }

    pub fn freshness_response(
        &self,
    ) -> Result<ResponseEnvelope<lethe_projection_cognition::FreshnessProjection>, SelfHostError>
    {
        let core = self.core_lock()?;
        self.ensure_projection_fresh(&core.catalog, "proj:freshness")?;
        let mode = self.resolve_read_mode(&core.catalog, "proj:freshness", None, None)?;
        let lineage = build_projection_lineage(
            "proj:freshness",
            core.lake.list(),
            core.snapshot.freshness.sources.len(),
            core.snapshot.built_at,
        );
        Ok(ResponseEnvelope {
            data: core.snapshot.freshness.clone(),
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:freshness",
                mode,
                core.snapshot.built_at,
                &lineage,
            )?,
        })
    }

    pub fn reply_slo_response(
        &self,
    ) -> Result<ResponseEnvelope<lethe_projection_cognition::ReplySloProjection>, SelfHostError>
    {
        let core = self.core_lock()?;
        self.ensure_projection_fresh(&core.catalog, "proj:reply-slo")?;
        let mode = self.resolve_read_mode(&core.catalog, "proj:reply-slo", None, None)?;
        let lineage = build_mixed_projection_lineage(
            "proj:reply-slo",
            core.lake.list(),
            &core.supplemental.list(),
            core.snapshot.reply_slo.rows.len(),
            core.snapshot.built_at,
        );
        Ok(ResponseEnvelope {
            data: core.snapshot.reply_slo.clone(),
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:reply-slo",
                mode,
                core.snapshot.built_at,
                &lineage,
            )?,
        })
    }

    pub fn break_glass_response(
        &self,
    ) -> Result<ResponseEnvelope<BreakGlassProjection>, SelfHostError> {
        let core = self.core_lock()?;
        self.ensure_projection_fresh(&core.catalog, "proj:break-glass")?;
        let mode = self.resolve_read_mode(&core.catalog, "proj:break-glass", None, None)?;
        let lineage = build_channel_registry_projection_lineage(
            "proj:break-glass",
            &core.registry.list_channels(),
            core.snapshot.break_glass.channels.len(),
            core.snapshot.built_at,
        );
        Ok(ResponseEnvelope {
            data: core.snapshot.break_glass.clone(),
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:break-glass",
                mode,
                core.snapshot.built_at,
                &lineage,
            )?,
        })
    }

    pub fn resume_snapshot_response(
        &self,
    ) -> Result<ResponseEnvelope<lethe_projection_cognition::ResumeSnapshotProjection>, SelfHostError>
    {
        let core = self.core_lock()?;
        self.ensure_projection_fresh(&core.catalog, "proj:resume-snapshot")?;
        let mode = self.resolve_read_mode(&core.catalog, "proj:resume-snapshot", None, None)?;
        let lineage = build_supplemental_projection_lineage(
            "proj:resume-snapshot",
            &core.supplemental.list(),
            core.snapshot.resume_snapshot.projects.len(),
            core.snapshot.built_at,
        );
        Ok(ResponseEnvelope {
            data: core.snapshot.resume_snapshot.clone(),
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:resume-snapshot",
                mode,
                core.snapshot.built_at,
                &lineage,
            )?,
        })
    }

    pub fn plan_state_response(
        &self,
    ) -> Result<ResponseEnvelope<lethe_projection_cognition::PlanStateProjection>, SelfHostError>
    {
        let core = self.core_lock()?;
        self.ensure_projection_fresh(&core.catalog, "proj:plan-state")?;
        let mode = self.resolve_read_mode(&core.catalog, "proj:plan-state", None, None)?;
        let lineage = build_supplemental_projection_lineage(
            "proj:plan-state",
            &core.supplemental.list(),
            core.snapshot.plan_state.projects.len(),
            core.snapshot.built_at,
        );
        Ok(ResponseEnvelope {
            data: core.snapshot.plan_state.clone(),
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:plan-state",
                mode,
                core.snapshot.built_at,
                &lineage,
            )?,
        })
    }

    pub fn card_queue_response(
        &self,
        state: Option<CardState>,
        channel: Option<&str>,
        automatic: Option<bool>,
        limit: usize,
        cursor: Option<&str>,
    ) -> Result<ResponseEnvelope<CardQueuePage>, SelfHostError> {
        if limit == 0 || limit > self.config.resource_limits.max_page_size {
            return Err(SelfHostError::ReadMode(format!(
                "page limit must be between 1 and {}",
                self.config.resource_limits.max_page_size
            )));
        }
        let offset = parse_cursor(cursor)?;
        let core = self.core_lock()?;
        self.ensure_projection_fresh(&core.catalog, "proj:card-queue")?;
        let mode = self.resolve_read_mode(&core.catalog, "proj:card-queue", None, None)?;
        let cards = core
            .snapshot
            .card_queue
            .cards
            .iter()
            .filter(|card| state.is_none_or(|state| card.state == state))
            .filter(|card| channel.is_none_or(|channel| card.channel == channel))
            .filter(|card| automatic.is_none_or(|automatic| card.automatic_send == automatic))
            .cloned()
            .collect::<Vec<_>>();
        let total = cards.len();
        let start = offset.min(total);
        let end = (start + limit).min(total);
        let next_cursor = if end < total {
            Some(end.to_string())
        } else {
            None
        };
        let lineage = build_supplemental_projection_lineage(
            "proj:card-queue",
            &core.supplemental.list(),
            core.snapshot.card_queue.cards.len(),
            core.snapshot.built_at,
        );
        Ok(ResponseEnvelope {
            data: CardQueuePage {
                cards: cards[start..end].to_vec(),
                total,
                limit,
                next_cursor,
                audit_log: core.snapshot.card_queue.audit_log.clone(),
            },
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:card-queue",
                mode,
                core.snapshot.built_at,
                &lineage,
            )?,
        })
    }
}

fn validate_verification_mode_filter(value: Option<&str>) -> Result<(), SelfHostError> {
    match value {
        None | Some("check") | Some("generate") => Ok(()),
        Some(other) => Err(SelfHostError::ReadMode(format!(
            "invalid verification_mode filter: {other}"
        ))),
    }
}

fn parse_cursor(cursor: Option<&str>) -> Result<usize, SelfHostError> {
    match cursor {
        Some(raw) if !raw.trim().is_empty() => raw
            .parse::<usize>()
            .map_err(|_| SelfHostError::ReadMode("cursor must be a numeric offset".to_owned())),
        _ => Ok(0),
    }
}

fn grep_record_from_corpus(record: CorpusRecord) -> GrepRecord {
    GrepRecord {
        record_id: record.record_id,
        source_type: record.source_type,
        anchor_url: record.anchor_url,
        source_title: record.source_title,
        source_location: record.source_location,
        timestamp: record.timestamp,
        text: record.text,
        normalized_text: record.normalized_text,
        thread_ts: record.thread_ts,
        container: record.container,
        metadata: record.metadata,
    }
}

fn build_corpus_thread_response(
    corpus: &[CorpusRecord],
    thread_ref: &str,
) -> Result<lethe_api::api::grep::ThreadResponse, SelfHostError> {
    if let Some(record) = corpus.iter().find(|record| record.record_id == thread_ref) {
        if is_coding_agent_record(record) {
            return coding_agent_thread_response(corpus, record);
        }
        if let Some(thread_ts) = record.thread_ts.as_deref() {
            if record.source_type == "slack" {
                return slack_thread_response(corpus, thread_ts);
            }
            return generic_thread_response(corpus, &record.source_type, thread_ts);
        }
        return Ok(lethe_api::api::grep::ThreadResponse {
            thread_ts: record.record_id.clone(),
            records: vec![grep_record_from_corpus(record.clone())],
            complete: true,
            limit: 1,
            next_cursor: None,
            structure: None,
        });
    }
    if let Some(record) = corpus.iter().find(|record| {
        is_coding_agent_record(record) && metadata_str(record, "thread_key") == Some(thread_ref)
    }) {
        return coding_agent_thread_response(corpus, record);
    }
    if let Some(record) = corpus.iter().find(|record| {
        is_coding_agent_record(record) && metadata_str(record, "session_id") == Some(thread_ref)
    }) {
        return coding_agent_thread_response(corpus, record);
    }
    if let Some(record) = corpus.iter().find(|record| {
        record.thread_ts.as_deref() == Some(thread_ref)
            || metadata_str(record, "thread_key") == Some(thread_ref)
    }) {
        if record.source_type != "slack" {
            return generic_thread_response(corpus, &record.source_type, thread_ref);
        }
    }

    slack_thread_response(corpus, thread_ref)
}

fn generic_thread_response(
    corpus: &[CorpusRecord],
    source_type: &str,
    thread_ref: &str,
) -> Result<lethe_api::api::grep::ThreadResponse, SelfHostError> {
    let mut records = corpus
        .iter()
        .filter(|record| record.source_type == source_type)
        .filter(|record| {
            record.thread_ts.as_deref() == Some(thread_ref)
                || metadata_str(record, "thread_key") == Some(thread_ref)
        })
        .cloned()
        .collect::<Vec<_>>();
    if records.is_empty() {
        return Err(SelfHostError::NotFound(thread_ref.to_owned()));
    }
    records.sort_by(|left, right| {
        left.timestamp
            .cmp(&right.timestamp)
            .then_with(|| left.record_id.cmp(&right.record_id))
    });
    let limit = records.len();
    Ok(lethe_api::api::grep::ThreadResponse {
        thread_ts: thread_ref.to_owned(),
        records: records.into_iter().map(grep_record_from_corpus).collect(),
        complete: true,
        limit,
        next_cursor: None,
        structure: Some(lethe_api::api::grep::ThreadStructure {
            thread_key: thread_ref.to_owned(),
            source_type: source_type.to_owned(),
            root_session: None,
            sidechains: Vec::new(),
        }),
    })
}

fn slack_thread_response(
    corpus: &[CorpusRecord],
    thread_ref: &str,
) -> Result<lethe_api::api::grep::ThreadResponse, SelfHostError> {
    let records = corpus
        .iter()
        .filter(|record| record.source_type == "slack")
        .filter(|record| record.thread_ts.as_deref() == Some(thread_ref))
        .cloned()
        .map(grep_record_from_corpus)
        .collect::<Vec<_>>();
    if records.is_empty() {
        return Err(SelfHostError::NotFound(thread_ref.to_owned()));
    }
    Ok(lethe_api::api::grep::ThreadResponse {
        thread_ts: thread_ref.to_owned(),
        limit: records.len(),
        records,
        complete: true,
        next_cursor: None,
        structure: None,
    })
}

fn coding_agent_thread_response(
    corpus: &[CorpusRecord],
    seed: &CorpusRecord,
) -> Result<lethe_api::api::grep::ThreadResponse, SelfHostError> {
    let source_type = seed.source_type.clone();
    let seed_session = metadata_owned(seed, "session_id").ok_or_else(|| {
        SelfHostError::ReadMode(format!(
            "coding-agent corpus record {} has no session_id metadata",
            seed.record_id
        ))
    })?;
    let records = corpus
        .iter()
        .filter(|record| record.source_type == source_type)
        .filter(|record| metadata_str(record, "session_id").is_some())
        .collect::<Vec<_>>();
    let parent_by_session = parent_session_map(&records)?;
    let root_session = root_session_for(&seed_session, &parent_by_session)?;
    let included_sessions = descendant_sessions(&root_session, &parent_by_session);

    let mut thread_records = records
        .into_iter()
        .filter(|record| {
            metadata_str(record, "session_id")
                .is_some_and(|session_id| included_sessions.contains(session_id))
        })
        .cloned()
        .collect::<Vec<_>>();
    thread_records.sort_by(|left, right| {
        left.timestamp
            .cmp(&right.timestamp)
            .then_with(|| left.record_id.cmp(&right.record_id))
    });

    let mut sessions: BTreeMap<String, lethe_api::api::grep::ThreadSession> = BTreeMap::new();
    for record in &thread_records {
        let session_id = metadata_owned(record, "session_id").ok_or_else(|| {
            SelfHostError::ReadMode(format!(
                "coding-agent corpus record {} has no session_id metadata",
                record.record_id
            ))
        })?;
        let parent_session_id = parent_by_session.get(&session_id).cloned().flatten();
        let is_sidechain = session_id != root_session
            || metadata_bool(record, "is_sidechain")
            || parent_session_id.is_some();
        sessions
            .entry(session_id.clone())
            .or_insert_with(|| lethe_api::api::grep::ThreadSession {
                session_id,
                parent_session_id,
                is_sidechain,
                record_ids: Vec::new(),
            })
            .record_ids
            .push(record.record_id.clone());
    }
    if !sessions.contains_key(&root_session) {
        sessions.insert(
            root_session.clone(),
            lethe_api::api::grep::ThreadSession {
                session_id: root_session.clone(),
                parent_session_id: parent_by_session.get(&root_session).cloned().flatten(),
                is_sidechain: false,
                record_ids: Vec::new(),
            },
        );
    }
    let root = sessions.remove(&root_session);
    let sidechains = sessions.into_values().collect::<Vec<_>>();
    let thread_key = format!("{source_type}:session:{root_session}");

    Ok(lethe_api::api::grep::ThreadResponse {
        thread_ts: thread_key.clone(),
        limit: thread_records.len(),
        records: thread_records
            .into_iter()
            .map(grep_record_from_corpus)
            .collect(),
        complete: true,
        next_cursor: None,
        structure: Some(lethe_api::api::grep::ThreadStructure {
            thread_key,
            source_type,
            root_session: root,
            sidechains,
        }),
    })
}

fn page_thread_response(
    mut response: lethe_api::api::grep::ThreadResponse,
    limit: usize,
    cursor: Option<&str>,
) -> Result<lethe_api::api::grep::ThreadResponse, SelfHostError> {
    let offset = parse_cursor(cursor)?;
    let total = response.records.len();
    let start = offset.min(total);
    let end = (start + limit).min(total);
    let page_records = response.records[start..end].to_vec();
    let page_ids = page_records
        .iter()
        .map(|record| record.record_id.clone())
        .collect::<BTreeSet<_>>();

    response.records = page_records;
    response.limit = limit;
    response.complete = end >= total;
    response.next_cursor = (!response.complete).then(|| end.to_string());
    if let Some(structure) = response.structure.as_mut() {
        retain_thread_structure_page(structure, &page_ids);
    }
    Ok(response)
}

fn retain_thread_structure_page(
    structure: &mut lethe_api::api::grep::ThreadStructure,
    page_ids: &BTreeSet<String>,
) {
    if let Some(root) = structure.root_session.as_mut() {
        root.record_ids
            .retain(|record_id| page_ids.contains(record_id));
    }
    for sidechain in &mut structure.sidechains {
        sidechain
            .record_ids
            .retain(|record_id| page_ids.contains(record_id));
    }
    structure
        .sidechains
        .retain(|sidechain| !sidechain.record_ids.is_empty());
}

fn parent_session_map(
    records: &[&CorpusRecord],
) -> Result<BTreeMap<String, Option<String>>, SelfHostError> {
    let mut parent_by_session = BTreeMap::new();
    for record in records {
        let Some(session_id) = metadata_owned(record, "session_id") else {
            continue;
        };
        let parent_session_id = metadata_owned(record, "parent_session_id");
        match parent_by_session.get(&session_id) {
            Some(existing) if existing != &parent_session_id => {
                return Err(SelfHostError::ReadMode(format!(
                    "conflicting parent_session_id metadata for coding-agent session {session_id}"
                )));
            }
            Some(_) => {}
            None => {
                parent_by_session.insert(session_id, parent_session_id);
            }
        }
    }
    Ok(parent_by_session)
}

fn root_session_for(
    seed_session: &str,
    parent_by_session: &BTreeMap<String, Option<String>>,
) -> Result<String, SelfHostError> {
    let mut current = seed_session.to_owned();
    let mut seen = BTreeSet::new();
    while let Some(Some(parent)) = parent_by_session.get(&current) {
        if !seen.insert(current.clone()) {
            return Err(SelfHostError::ReadMode(format!(
                "cycle in coding-agent parent_session_id metadata at {current}"
            )));
        }
        current = parent.clone();
    }
    Ok(current)
}

fn descendant_sessions(
    root_session: &str,
    parent_by_session: &BTreeMap<String, Option<String>>,
) -> BTreeSet<String> {
    let mut included = BTreeSet::from([root_session.to_owned()]);
    loop {
        let before = included.len();
        for (session_id, parent) in parent_by_session {
            if parent
                .as_ref()
                .is_some_and(|parent| included.contains(parent))
            {
                included.insert(session_id.clone());
            }
        }
        if included.len() == before {
            break;
        }
    }
    included
}

fn is_coding_agent_record(record: &CorpusRecord) -> bool {
    matches!(record.source_type.as_str(), "claude-code" | "codex")
}

fn metadata_str<'a>(record: &'a CorpusRecord, key: &str) -> Option<&'a str> {
    record.metadata.get(key).and_then(serde_json::Value::as_str)
}

fn metadata_owned(record: &CorpusRecord, key: &str) -> Option<String> {
    metadata_str(record, key).map(str::to_owned)
}

fn metadata_bool(record: &CorpusRecord, key: &str) -> bool {
    record
        .metadata
        .get(key)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}
