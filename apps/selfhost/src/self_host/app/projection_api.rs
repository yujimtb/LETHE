use super::*;
use lethe_projection_claim_queue::{ClaimGroup, ClaimState, DecisionView, ProjectionAuditEvent};
use lethe_projection_cognition::{CardState, ReplyCard};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

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
    fn persisted_person_messages(
        &self,
        person_id: &str,
        expected_count: usize,
        compact_state: &CompactProjectionState,
    ) -> Result<Vec<PersonMessage>, SelfHostError> {
        let members = compact_state
            .identity
            .component_members_for_person(person_id)
            .ok_or_else(|| SelfHostError::NotFound(person_id.to_owned()))?;
        let persistence = self.persistence_lock()?;
        let mut messages = Vec::new();
        for node_id in members {
            let items = persistence.projection_items_by_owner(
                &ProjectionRef::new("proj:person-page"),
                &identity_node_owner(*node_id),
            )?;
            messages.extend(
                items
                    .iter()
                    .filter(|item| item.item_key.starts_with("pm:"))
                    .map(|item| person_message_from_projection_item(item, compact_state))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        messages.sort_by(|left, right| left.id.cmp(&right.id));
        if messages.len() != expected_count {
            return Err(SelfHostError::Ingestion(format!(
                "person message row count for {person_id} is {}, expected {expected_count}",
                messages.len()
            )));
        }
        let mut previous_append_seq = None;
        for message in &messages {
            let append_seq = person_message_append_seq(message)?;
            if previous_append_seq.is_some_and(|previous| previous >= append_seq) {
                return Err(SelfHostError::Ingestion(format!(
                    "person message rows for {person_id} are not in strict append order"
                )));
            }
            previous_append_seq = Some(append_seq);
        }
        Ok(messages)
    }

    fn persisted_person_slides(
        &self,
        person_id: &str,
        expected_count: usize,
        compact_state: &CompactProjectionState,
    ) -> Result<Vec<PersonSlide>, SelfHostError> {
        let members = compact_state
            .identity
            .component_members_for_person(person_id)
            .ok_or_else(|| SelfHostError::NotFound(person_id.to_owned()))?;
        let persistence = self.persistence_lock()?;
        let mut slides = Vec::new();
        for node_id in members {
            let items = persistence.projection_items_by_owner(
                &ProjectionRef::new("proj:person-page"),
                &identity_node_owner(*node_id),
            )?;
            slides.extend(
                items
                    .iter()
                    .filter(|item| item.item_key.starts_with("ps:"))
                    .map(|item| person_slide_from_projection_item(item, compact_state))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        slides.sort_by(|left, right| left.id.cmp(&right.id));
        if slides.len() != expected_count {
            return Err(SelfHostError::Ingestion(format!(
                "person slide row count for {person_id} is {}, expected {expected_count}",
                slides.len()
            )));
        }
        Ok(slides)
    }

    fn communication_projection_reply_slo(
        &self,
        core: &AppCore,
    ) -> Result<ReplySloProjection, SelfHostError> {
        // IM-05/D7: normal incremental propagation is <=5s; while a background
        // migration/recovery/bootstrap rebuild runs, the last published snapshot
        // may be <=60s old and is never replaced by a partial result.
        let actual_count = u64::try_from(core.communication_projection.len()).map_err(|_| {
            SelfHostError::Ingestion("reply SLO row count does not fit u64".to_owned())
        })?;
        if actual_count != core.reply_slo_count {
            return Err(SelfHostError::Ingestion(format!(
                "communication projection row count is {actual_count}, expected {}",
                core.reply_slo_count
            )));
        }
        Ok(core.communication_projection.project(Utc::now()))
    }

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
            .person_components
            .values()
            .filter_map(|component| {
                Some(PersonPageProjector::to_list_item(
                    component.profile.as_ref()?,
                    component.activity.as_ref()?,
                ))
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
        let component = core
            .person_components
            .get(person_id)
            .ok_or_else(|| SelfHostError::NotFound(person_id.to_string()))?;
        let profile = component
            .profile
            .as_ref()
            .ok_or_else(|| SelfHostError::NotFound(person_id.to_string()))?;
        self.authorize_read(
            EntityRef::new(person_id.to_string()),
            consent_status_for_person_id(&core, person_id)?,
        )?;
        let activity = component
            .activity
            .as_ref()
            .ok_or_else(|| SelfHostError::NotFound(format!("activity for {person_id}")))?;
        let slides = self.persisted_person_slides(
            person_id,
            activity.total_slides_related,
            &core.compact_state,
        )?;
        let messages = self.persisted_person_messages(
            person_id,
            activity.total_messages,
            &core.compact_state,
        )?;

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
        let activity = core
            .person_components
            .get(person_id)
            .and_then(|component| component.activity.as_ref())
            .ok_or_else(|| SelfHostError::NotFound(format!("activity for {person_id}")))?;
        let slides = self.persisted_person_slides(
            person_id,
            activity.total_slides_related,
            &core.compact_state,
        )?;

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
        let activity = core
            .person_components
            .get(person_id)
            .and_then(|component| component.activity.as_ref())
            .ok_or_else(|| SelfHostError::NotFound(format!("activity for {person_id}")))?;
        let messages = self.persisted_person_messages(
            person_id,
            activity.total_messages,
            &core.compact_state,
        )?;

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
        let activity = core
            .person_components
            .get(person_id)
            .and_then(|component| component.activity.as_ref())
            .ok_or_else(|| SelfHostError::NotFound(format!("activity for {person_id}")))?;
        let messages = self.persisted_person_messages(
            person_id,
            activity.total_messages,
            &core.compact_state,
        )?;
        let mut events = Vec::new();

        let slides = self.persisted_person_slides(
            person_id,
            activity.total_slides_related,
            &core.compact_state,
        )?;
        for slide in &slides {
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

        for message in &messages {
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
        let ((page, total), metadata) = self.search_index.execute(|index| {
            index.read_with_metadata(|snapshot| {
                snapshot.records_page(pagination.offset, pagination.limit)
            })
        })?;
        let total = usize::try_from(total).map_err(|_| {
            SelfHostError::Ingestion("corpus record count does not fit usize".to_owned())
        })?;
        let lineage = corpus_index_lineage(&metadata)?;
        let core = self.core_lock()?;
        let mode = self.resolve_read_mode(&core.catalog, "proj:corpus", read_mode, pin)?;
        let payload = serde_json::to_value(PaginatedResponse::from_slice(page, total, pagination))?;
        Ok(ResponseEnvelope {
            data: payload,
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:corpus",
                mode,
                metadata.committed_at,
                &lineage,
            )?,
        })
    }

    pub fn corpus_grep_response(
        &self,
        request: &lethe_api::api::grep::GrepRequest,
    ) -> Result<ResponseEnvelope<lethe_api::api::grep::GrepResponse>, SelfHostError> {
        let (response, metadata) = self.search_index.execute(|index| {
            index.search_with_metadata(request, self.config.resource_limits.max_page_size)
        })?;
        let lineage = corpus_index_lineage(&metadata)?;
        let core = self.core_lock()?;
        let mode = self.resolve_read_mode(&core.catalog, "proj:corpus", None, None)?;
        Ok(ResponseEnvelope {
            data: response,
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:corpus",
                mode,
                metadata.committed_at,
                &lineage,
            )?,
        })
    }

    pub fn corpus_grep_response_with_source_summaries(
        &self,
        request: &lethe_api::api::grep::GrepRequest,
    ) -> Result<
        (
            ResponseEnvelope<lethe_api::api::grep::GrepResponse>,
            Vec<CorpusSourceTypeSummary>,
        ),
        SelfHostError,
    > {
        let ((response, source_type_counts), metadata) = self.search_index.execute(|index| {
            index.read_with_metadata(|snapshot| {
                Ok((
                    snapshot.search(request, self.config.resource_limits.max_page_size)?,
                    snapshot.source_type_counts()?,
                ))
            })
        })?;
        let source_summaries = source_type_counts
            .into_iter()
            .map(|(source_type, records)| {
                Ok(CorpusSourceTypeSummary {
                    source_type,
                    records: usize::try_from(records).map_err(|_| {
                        SelfHostError::Ingestion(
                            "source type record count does not fit usize".to_owned(),
                        )
                    })?,
                })
            })
            .collect::<Result<Vec<_>, SelfHostError>>()?;
        let lineage = corpus_index_lineage(&metadata)?;
        let core = self.core_lock()?;
        let mode = self.resolve_read_mode(&core.catalog, "proj:corpus", None, None)?;
        let envelope = ResponseEnvelope {
            data: response,
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:corpus",
                mode,
                metadata.committed_at,
                &lineage,
            )?,
        };
        Ok((envelope, source_summaries))
    }

    pub fn corpus_source_type_summaries(
        &self,
    ) -> Result<Vec<CorpusSourceTypeSummary>, SelfHostError> {
        let counts = self
            .search_index
            .execute(|index| index.source_type_counts())?;
        counts
            .into_iter()
            .map(|(source_type, records)| {
                Ok(CorpusSourceTypeSummary {
                    source_type,
                    records: usize::try_from(records).map_err(|_| {
                        SelfHostError::Ingestion(
                            "source type record count does not fit usize".to_owned(),
                        )
                    })?,
                })
            })
            .collect()
    }

    pub fn corpus_record_response(
        &self,
        record_id: &str,
    ) -> Result<ResponseEnvelope<lethe_api::api::grep::RecordDetailResponse>, SelfHostError> {
        let (record, metadata) = self
            .search_index
            .execute(|index| index.read_with_metadata(|snapshot| snapshot.record(record_id)))?;
        let record = record.ok_or_else(|| SelfHostError::NotFound(record_id.to_owned()))?;
        let lineage = corpus_index_lineage(&metadata)?;
        let core = self.core_lock()?;
        Ok(ResponseEnvelope {
            data: lethe_api::api::grep::RecordDetailResponse { record },
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:corpus",
                ReadMode::OperationalLatest,
                metadata.committed_at,
                &lineage,
            )?,
        })
    }

    pub fn corpus_thread_response(
        &self,
        thread_ref: &str,
    ) -> Result<ResponseEnvelope<lethe_api::api::grep::ThreadResponse>, SelfHostError> {
        let (response, metadata) = self.search_index.execute(|index| {
            index.read_with_metadata(|snapshot| {
                build_index_thread_response(snapshot, thread_ref, None)
            })
        })?;
        let response = response.ok_or_else(|| SelfHostError::NotFound(thread_ref.to_owned()))?;
        let lineage = corpus_index_lineage(&metadata)?;
        let core = self.core_lock()?;
        Ok(ResponseEnvelope {
            data: response,
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:corpus",
                ReadMode::OperationalLatest,
                metadata.committed_at,
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
        let offset = parse_cursor(cursor)?;
        let (response, metadata) = self.search_index.execute(|index| {
            index.read_with_metadata(|snapshot| {
                build_index_thread_response(snapshot, thread_ref, Some((offset, limit)))
            })
        })?;
        let response = response.ok_or_else(|| SelfHostError::NotFound(thread_ref.to_owned()))?;
        let lineage = corpus_index_lineage(&metadata)?;
        let core = self.core_lock()?;
        Ok(ResponseEnvelope {
            data: response,
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:corpus",
                ReadMode::OperationalLatest,
                metadata.committed_at,
                &lineage,
            )?,
        })
    }

    pub fn resolve_link_response(
        &self,
        request: &lethe_api::api::grep::ResolveLinkRequest,
    ) -> Result<ResponseEnvelope<lethe_api::api::grep::ResolveLinkResponse>, SelfHostError> {
        let (record, metadata) = self.search_index.execute(|index| {
            index.read_with_metadata(|snapshot| snapshot.resolve_link(&request.url))
        })?;
        let record = record.ok_or_else(|| SelfHostError::NotFound(request.url.clone()))?;
        let lineage = corpus_index_lineage(&metadata)?;
        let core = self.core_lock()?;
        Ok(ResponseEnvelope {
            data: lethe_api::api::grep::ResolveLinkResponse {
                record_id: record.record_id,
                source_type: record.source_type,
                anchor_url: record.anchor_url,
            },
            projection_metadata: self.projection_metadata(
                &core.catalog,
                "proj:corpus",
                ReadMode::OperationalLatest,
                metadata.committed_at,
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
            &core.snapshot.lineage.build_id,
            core.observation_stats,
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
            &core.snapshot.lineage.build_id,
            core.observation_stats,
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
        let reply_slo = self.communication_projection_reply_slo(&core)?;
        let lineage = build_mixed_projection_lineage(
            "proj:reply-slo",
            &core.snapshot.lineage.build_id,
            core.observation_stats,
            &core.supplemental.list(),
            usize::try_from(core.reply_slo_count).map_err(|_| {
                SelfHostError::Ingestion("reply SLO count does not fit usize".to_owned())
            })?,
            core.snapshot.built_at,
        );
        Ok(ResponseEnvelope {
            data: reply_slo,
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

fn corpus_index_lineage(
    metadata: &lethe_search_index::IndexCommitMetadata,
) -> Result<LineageManifest, SelfHostError> {
    let output_count = usize::try_from(metadata.record_count).map_err(|_| {
        SelfHostError::Ingestion("corpus record count does not fit usize".to_owned())
    })?;
    usize::try_from(metadata.observation_count)
        .map_err(|_| SelfHostError::Ingestion("observation count does not fit usize".to_owned()))?;
    usize::try_from(metadata.last_append_seq)
        .map_err(|_| SelfHostError::Ingestion("append sequence does not fit usize".to_owned()))?;
    Ok(build_projection_lineage(
        "proj:corpus",
        &metadata.projection_watermark,
        ObservationStats {
            count: metadata.observation_count,
            max_append_seq: metadata.last_append_seq,
        },
        output_count,
        metadata.committed_at,
    ))
}

fn build_index_thread_response(
    index: &lethe_search_index::PersistentCorpusIndex,
    thread_ref: &str,
    page: Option<(usize, usize)>,
) -> Result<Option<lethe_api::api::grep::ThreadResponse>, lethe_search_index::IndexError> {
    if let Some(record) = index.record(thread_ref)? {
        return thread_response_for_seed(index, record, page);
    }
    if let Some(record) = index.coding_record_by_thread_key(thread_ref)? {
        return coding_agent_thread_response(index, &record, page).map(Some);
    }
    if let Some(record) = index.coding_record_by_session_id(thread_ref)? {
        return coding_agent_thread_response(index, &record, page).map(Some);
    }
    if let Some(record) = index.record_by_thread_ref(thread_ref)?
        && record.source_type != "slack"
    {
        return generic_thread_response(index, &record.source_type, thread_ref, page);
    }
    slack_thread_response(index, thread_ref, page)
}

fn thread_response_for_seed(
    index: &lethe_search_index::PersistentCorpusIndex,
    record: GrepRecord,
    page: Option<(usize, usize)>,
) -> Result<Option<lethe_api::api::grep::ThreadResponse>, lethe_search_index::IndexError> {
    if is_coding_agent_record(&record) {
        return coding_agent_thread_response(index, &record, page).map(Some);
    }
    if let Some(thread_ts) = record.thread_ts.as_deref() {
        if record.source_type == "slack" {
            return slack_thread_response(index, thread_ts, page);
        }
        return generic_thread_response(index, &record.source_type, thread_ts, page);
    }

    let (records, limit) = match page {
        Some((0, limit)) => (vec![record.clone()], limit),
        Some((_offset, limit)) => (Vec::new(), limit),
        None => (vec![record.clone()], 1),
    };
    Ok(Some(lethe_api::api::grep::ThreadResponse {
        thread_ts: record.record_id,
        records,
        complete: true,
        limit,
        next_cursor: None,
        structure: None,
    }))
}

fn generic_thread_response(
    index: &lethe_search_index::PersistentCorpusIndex,
    source_type: &str,
    thread_ref: &str,
    page: Option<(usize, usize)>,
) -> Result<Option<lethe_api::api::grep::ThreadResponse>, lethe_search_index::IndexError> {
    let loaded = load_index_page(
        page,
        |offset, limit| {
            index.thread_records_page(
                source_type,
                thread_ref,
                lethe_api::api::grep::GrepOrder::DateAsc,
                offset,
                limit,
            )
        },
        || {
            index.thread_records_all(
                source_type,
                thread_ref,
                lethe_api::api::grep::GrepOrder::DateAsc,
            )
        },
    )?;
    if loaded.total == 0 {
        return Ok(None);
    }
    Ok(Some(lethe_api::api::grep::ThreadResponse {
        thread_ts: thread_ref.to_owned(),
        records: loaded.items,
        complete: loaded.complete,
        limit: loaded.limit,
        next_cursor: loaded.next_cursor,
        structure: Some(lethe_api::api::grep::ThreadStructure {
            thread_key: thread_ref.to_owned(),
            source_type: source_type.to_owned(),
            root_session: None,
            sidechains: Vec::new(),
        }),
    }))
}

fn slack_thread_response(
    index: &lethe_search_index::PersistentCorpusIndex,
    thread_ref: &str,
    page: Option<(usize, usize)>,
) -> Result<Option<lethe_api::api::grep::ThreadResponse>, lethe_search_index::IndexError> {
    let loaded = load_index_page(
        page,
        |offset, limit| {
            index.thread_records_page(
                "slack",
                thread_ref,
                lethe_api::api::grep::GrepOrder::DateDesc,
                offset,
                limit,
            )
        },
        || {
            index.thread_records_all(
                "slack",
                thread_ref,
                lethe_api::api::grep::GrepOrder::DateDesc,
            )
        },
    )?;
    if loaded.total == 0 {
        return Ok(None);
    }
    Ok(Some(lethe_api::api::grep::ThreadResponse {
        thread_ts: thread_ref.to_owned(),
        records: loaded.items,
        complete: loaded.complete,
        limit: loaded.limit,
        next_cursor: loaded.next_cursor,
        structure: None,
    }))
}

fn coding_agent_thread_response(
    index: &lethe_search_index::PersistentCorpusIndex,
    seed: &GrepRecord,
    page: Option<(usize, usize)>,
) -> Result<lethe_api::api::grep::ThreadResponse, lethe_search_index::IndexError> {
    let source_type = seed.source_type.clone();
    let seed_session = metadata_owned(seed, "session_id").ok_or_else(|| {
        lethe_search_index::IndexError::InvalidReadRequest(format!(
            "coding-agent corpus record {} has no session_id metadata",
            seed.record_id
        ))
    })?;
    let CodingSessionGraph {
        root_session,
        parent_by_session,
        included_sessions,
    } = coding_session_graph(index, &source_type, &seed_session)?;
    let session_ids = included_sessions.into_iter().collect::<Vec<_>>();
    let loaded = load_index_page(
        page,
        |offset, limit| index.coding_records_page(&source_type, &session_ids, offset, limit),
        || index.coding_records_all(&source_type, &session_ids),
    )?;
    if loaded.total == 0 {
        return Err(lethe_search_index::IndexError::InvalidDocument(format!(
            "coding-agent thread {source_type}:session:{root_session} has no records"
        )));
    }

    let mut sessions: BTreeMap<String, lethe_api::api::grep::ThreadSession> = BTreeMap::new();
    for record in &loaded.items {
        let session_id = metadata_owned(record, "session_id").ok_or_else(|| {
            lethe_search_index::IndexError::InvalidDocument(format!(
                "coding-agent corpus record {} has no session_id metadata",
                record.record_id
            ))
        })?;
        let parent_session_id = parent_by_session.get(&session_id).cloned().ok_or_else(|| {
            lethe_search_index::IndexError::InvalidDocument(format!(
                "coding-agent session graph omitted {session_id}"
            ))
        })?;
        let is_sidechain = session_id != root_session
            || metadata_bool(record, "is_sidechain")
            || parent_session_id.is_some();
        let session = sessions.entry(session_id.clone()).or_insert_with(|| {
            lethe_api::api::grep::ThreadSession {
                session_id,
                parent_session_id,
                is_sidechain,
                record_ids: Vec::new(),
            }
        });
        session.is_sidechain |= is_sidechain;
        session.record_ids.push(record.record_id.clone());
    }
    sessions
        .entry(root_session.clone())
        .or_insert_with(|| lethe_api::api::grep::ThreadSession {
            session_id: root_session.clone(),
            parent_session_id: parent_by_session.get(&root_session).cloned().flatten(),
            is_sidechain: false,
            record_ids: Vec::new(),
        });
    let root = sessions.remove(&root_session);
    let sidechains = sessions.into_values().collect::<Vec<_>>();
    let thread_key = format!("{source_type}:session:{root_session}");

    Ok(lethe_api::api::grep::ThreadResponse {
        thread_ts: thread_key.clone(),
        records: loaded.items,
        complete: loaded.complete,
        limit: loaded.limit,
        next_cursor: loaded.next_cursor,
        structure: Some(lethe_api::api::grep::ThreadStructure {
            thread_key,
            source_type,
            root_session: root,
            sidechains,
        }),
    })
}

struct CodingSessionGraph {
    root_session: String,
    parent_by_session: BTreeMap<String, Option<String>>,
    included_sessions: BTreeSet<String>,
}

fn coding_session_graph(
    index: &lethe_search_index::PersistentCorpusIndex,
    source_type: &str,
    seed_session: &str,
) -> Result<CodingSessionGraph, lethe_search_index::IndexError> {
    let mut parent_by_session = BTreeMap::new();
    let mut ascent = BTreeSet::new();
    let mut current = seed_session.to_owned();
    let root_session = loop {
        if !ascent.insert(current.clone()) {
            return Err(lethe_search_index::IndexError::InvalidReadRequest(format!(
                "cycle in coding-agent parent_session_id metadata at {current}"
            )));
        }
        let edges = index.coding_source_session_edges_all(source_type, &current)?;
        if edges.is_empty() {
            parent_by_session.entry(current.clone()).or_insert(None);
            break current;
        }
        let parent = consistent_session_parent(source_type, &current, &edges)?;
        insert_session_parent(&mut parent_by_session, &current, parent.clone())?;
        match parent {
            Some(parent) => current = parent,
            None => break current,
        }
    };

    let mut included = BTreeSet::from([root_session.clone()]);
    let mut pending = VecDeque::from([root_session.clone()]);
    while let Some(parent) = pending.pop_front() {
        let edges = index.coding_child_session_edges_all(source_type, &parent)?;
        for edge in edges {
            if edge.source_type != source_type
                || edge.parent_session_id.as_deref() != Some(parent.as_str())
            {
                return Err(lethe_search_index::IndexError::InvalidDocument(format!(
                    "coding-agent child-session index disagrees at {}",
                    edge.session_id
                )));
            }
            insert_session_parent(
                &mut parent_by_session,
                &edge.session_id,
                edge.parent_session_id.clone(),
            )?;
            if included.insert(edge.session_id.clone()) {
                pending.push_back(edge.session_id);
            }
        }
    }
    if !included.contains(seed_session) {
        return Err(lethe_search_index::IndexError::InvalidDocument(format!(
            "coding-agent session graph does not reach seed {seed_session}"
        )));
    }
    Ok(CodingSessionGraph {
        root_session,
        parent_by_session,
        included_sessions: included,
    })
}

fn consistent_session_parent(
    source_type: &str,
    session_id: &str,
    edges: &[lethe_search_index::CodingSessionEdge],
) -> Result<Option<String>, lethe_search_index::IndexError> {
    let expected = edges[0].parent_session_id.clone();
    for edge in edges {
        if edge.source_type != source_type
            || edge.session_id != session_id
            || edge.parent_session_id != expected
        {
            return Err(lethe_search_index::IndexError::InvalidReadRequest(format!(
                "conflicting parent_session_id metadata for coding-agent session {session_id}"
            )));
        }
    }
    Ok(expected)
}

fn insert_session_parent(
    parent_by_session: &mut BTreeMap<String, Option<String>>,
    session_id: &str,
    parent: Option<String>,
) -> Result<(), lethe_search_index::IndexError> {
    match parent_by_session.get(session_id) {
        Some(existing) if existing != &parent => {
            Err(lethe_search_index::IndexError::InvalidReadRequest(format!(
                "conflicting parent_session_id metadata for coding-agent session {session_id}"
            )))
        }
        Some(_) => Ok(()),
        None => {
            parent_by_session.insert(session_id.to_owned(), parent);
            Ok(())
        }
    }
}

struct LoadedIndexPage<T> {
    items: Vec<T>,
    total: usize,
    limit: usize,
    complete: bool,
    next_cursor: Option<String>,
}

fn load_index_page<T>(
    page: Option<(usize, usize)>,
    mut fetch_page: impl FnMut(usize, usize) -> Result<(Vec<T>, u64), lethe_search_index::IndexError>,
    fetch_all: impl FnOnce() -> Result<Vec<T>, lethe_search_index::IndexError>,
) -> Result<LoadedIndexPage<T>, lethe_search_index::IndexError> {
    match page {
        Some((offset, limit)) => {
            let (items, total) = fetch_page(offset, limit)?;
            let total = usize::try_from(total).map_err(|_| {
                lethe_search_index::IndexError::InvalidReadRequest(
                    "thread record count does not fit usize".to_owned(),
                )
            })?;
            let end = offset.checked_add(items.len()).ok_or_else(|| {
                lethe_search_index::IndexError::InvalidReadRequest(
                    "thread cursor overflowed usize".to_owned(),
                )
            })?;
            let complete = offset >= total || end >= total;
            Ok(LoadedIndexPage {
                items,
                total,
                limit,
                complete,
                next_cursor: (!complete).then(|| end.to_string()),
            })
        }
        None => {
            let items = fetch_all()?;
            let total = items.len();
            Ok(LoadedIndexPage {
                items,
                total,
                limit: total,
                complete: true,
                next_cursor: None,
            })
        }
    }
}

fn is_coding_agent_record(record: &GrepRecord) -> bool {
    matches!(record.source_type.as_str(), "claude-code" | "codex")
}

fn metadata_str<'a>(record: &'a GrepRecord, key: &str) -> Option<&'a str> {
    record.metadata.get(key).and_then(serde_json::Value::as_str)
}

fn metadata_owned(record: &GrepRecord, key: &str) -> Option<String> {
    metadata_str(record, key).map(str::to_owned)
}

fn metadata_bool(record: &GrepRecord, key: &str) -> bool {
    record
        .metadata
        .get(key)
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
}
