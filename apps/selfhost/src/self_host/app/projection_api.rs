use super::*;

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
        thread_ts: &str,
    ) -> Result<ResponseEnvelope<lethe_api::api::grep::ThreadResponse>, SelfHostError> {
        let core = self.core_lock()?;
        let records = core
            .snapshot
            .corpus
            .iter()
            .filter(|record| record.source_type == "slack")
            .filter(|record| record.thread_ts.as_deref() == Some(thread_ts))
            .cloned()
            .map(grep_record_from_corpus)
            .collect::<Vec<_>>();
        let lineage = build_projection_lineage(
            "proj:corpus",
            core.lake.list(),
            core.snapshot.corpus.len(),
            core.snapshot.built_at,
        );
        Ok(ResponseEnvelope {
            data: lethe_api::api::grep::ThreadResponse {
                thread_ts: thread_ts.to_owned(),
                records,
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
