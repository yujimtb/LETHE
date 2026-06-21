use super::*;

impl AppService {
    pub fn sync_all(&self) -> Result<SyncReport, SelfHostError> {
        let mut slack_ingested = 0usize;
        let mut google_ingested = 0usize;
        let mut duplicates = 0usize;

        let slack_adapter =
            SlackAdapter::new(self.slack_client.clone(), self.slack_adapter_config());
        for channel_id in &self.config.slack.channel_ids {
            let cursor_key = format!("slack:{channel_id}:oldest_ts");
            let oldest = non_empty_state(self.persistence_lock()?.get_state(&cursor_key)?);
            let mut page_cursor: Option<String> = None;
            let mut latest_ts = oldest.clone();
            let mut thread_roots = self.known_thread_roots(channel_id)?;

            loop {
                let page = self.slack_client.conversations_history(
                    channel_id,
                    oldest.as_deref(),
                    page_cursor.as_deref(),
                    200,
                )?;
                for message in page.messages {
                    if let Some(thread_root) = thread_root_ts(&message) {
                        thread_roots.insert(thread_root.to_string());
                    }
                    match self.ingest_slack_message(
                        &slack_adapter,
                        &self.slack_client,
                        channel_id,
                        message,
                        &mut latest_ts,
                    )? {
                        IngestResult::Ingested { .. } => slack_ingested += 1,
                        IngestResult::Duplicate { .. } => duplicates += 1,
                        _ => {}
                    }
                }
                if page.has_more {
                    page_cursor = page.next_cursor;
                } else {
                    break;
                }
            }

            for thread_ts in thread_roots {
                let (ingested, dupes) =
                    self.sync_thread_replies(&slack_adapter, channel_id, &thread_ts)?;
                slack_ingested += ingested;
                duplicates += dupes;
            }

            let channel_snapshot = self.slack_client.conversations_info(channel_id)?;
            match self.ingest_draft(slack_adapter.map_channel_snapshot(&channel_snapshot))? {
                IngestResult::Ingested { .. } => slack_ingested += 1,
                IngestResult::Duplicate { .. } => duplicates += 1,
                _ => {}
            }

            if let Some(latest_ts) = latest_ts.as_deref() {
                self.persistence_lock()?.set_state(&cursor_key, latest_ts)?;
            }
        }

        match self.ingest_draft(slack_adapter.heartbeat())? {
            IngestResult::Ingested { .. } => slack_ingested += 1,
            IngestResult::Duplicate { .. } => duplicates += 1,
            _ => {}
        }

        let google_adapter =
            GoogleSlidesAdapter::new(self.google_client.clone(), self.google_adapter_config());
        for presentation_id in &self.config.google.presentation_ids {
            let cursor_key = format!("gslides:{presentation_id}:revision");
            let last_revision = self.persistence_lock()?.get_state(&cursor_key)?;

            let mut page_token: Option<String> = None;
            let mut revisions = Vec::new();
            loop {
                let page = self
                    .google_client
                    .list_revisions(presentation_id, page_token.as_deref())?;
                revisions.extend(page.revisions);
                if let Some(token) = page.next_page_token {
                    page_token = Some(token);
                } else {
                    break;
                }
            }
            revisions.sort_by_key(|revision| revision.modified_time);

            let should_reset = last_revision.as_ref().is_some_and(|needle| {
                !revisions
                    .iter()
                    .any(|revision| revision.revision_id == *needle)
            });
            let new_revisions =
                revisions_after_cursor(revisions, last_revision.as_deref(), should_reset);

            let Some(captured_revision) = latest_revision_to_capture(&new_revisions).cloned()
            else {
                continue;
            };

            let meta = self.google_client.get_presentation_meta(presentation_id)?;
            let presentation = self.google_client.get_presentation(presentation_id)?;
            let native_blob = self.store_blob(&serde_json::to_vec(&presentation)?)?;
            let rendered_blobs = presentation
                .slides
                .first()
                .map(|slide| {
                    self.google_client
                        .render_slide(presentation_id, &slide.object_id, "png")
                })
                .transpose()?
                .map(|rendered| self.store_blob(&rendered.data))
                .transpose()?
                .into_iter()
                .collect::<Vec<_>>();

            match self.ingest_draft(google_adapter.map_revision(
                &captured_revision,
                &meta,
                Some(native_blob),
                rendered_blobs,
            ))? {
                IngestResult::Ingested { .. } => google_ingested += 1,
                IngestResult::Duplicate { .. } => duplicates += 1,
                _ => {}
            }

            self.persistence_lock()?
                .set_state(&cursor_key, &captured_revision.revision_id)?;
        }

        match self.ingest_draft(google_adapter.heartbeat())? {
            IngestResult::Ingested { .. } => google_ingested += 1,
            IngestResult::Duplicate { .. } => duplicates += 1,
            _ => {}
        }

        let last_sync_at = Utc::now();
        let mut core = self.core_lock()?;
        core.last_sync_at = Some(last_sync_at);
        core.last_sync_error = None;
        let should_rebuild_snapshot = slack_ingested > 0 || google_ingested > 0;

        let schema = lethe_core::domain::SchemaRef::new("schema:workspace-object-snapshot");
        let slide_observations: Vec<lethe_core::domain::Observation> =
            core.lake.by_schema(&schema).into_iter().cloned().collect();
        let slide_obs_by_presentation = slide_observations.iter().fold(
            HashMap::<String, lethe_core::domain::Observation>::new(),
            |mut acc, obs| {
                let Some(presentation_id) = obs
                    .payload
                    .pointer("/artifact/sourceObjectId")
                    .and_then(|value| value.as_str())
                else {
                    return acc;
                };

                match acc.get(presentation_id) {
                    Some(existing) if existing.published >= obs.published => {}
                    _ => {
                        acc.insert(presentation_id.to_string(), obs.clone());
                    }
                }
                acc
            },
        );
        let slide_analysis_records: Vec<lethe_core::domain::SupplementalRecord> = core
            .supplemental
            .by_kind("slide-analysis")
            .into_iter()
            .cloned()
            .collect();
        let analysis_model = self
            .slide_analyzer
            .as_ref()
            .map(|analyzer| format!("{}+continuation-v2-image-url", analyzer.model_name()))
            .unwrap_or_else(|| "heuristic-fallback+continuation-v2-image-url".to_string());
        let mut needs_analysis = false;
        for presentation_id in &self.config.google.presentation_ids {
            let Some(_observation) = slide_obs_by_presentation.get(presentation_id) else {
                continue;
            };
            let presentation = self.google_client.get_presentation(presentation_id)?;

            if presentation
                .slides
                .iter()
                .take(self.config.slide_analysis_limit)
                .any(|slide| {
                    match find_slide_analysis_record(
                        &slide_analysis_records,
                        presentation_id,
                        &slide.object_id,
                    ) {
                        Some(record) if self.slide_analyzer.is_some() => {
                            analysis_record_needs_refresh(record, &analysis_model)
                        }
                        Some(_) => false,
                        None => true,
                    }
                })
            {
                needs_analysis = true;
                break;
            }
        }

        // --- Slide Analysis + Notion write-back ---
        let mut slide_analyses = 0usize;
        let mut notion_synced = 0usize;

        if google_ingested > 0 || slack_ingested > 0 || needs_analysis {
            let mut analysis_results = Vec::new();

            for presentation_id in &self.config.google.presentation_ids {
                let Some(observation) = slide_obs_by_presentation.get(presentation_id) else {
                    continue;
                };

                let presentation = self.google_client.get_presentation(presentation_id)?;
                let canonical_uri = observation
                    .payload
                    .pointer("/artifact/canonicalUri")
                    .and_then(|value| value.as_str())
                    .unwrap_or_default()
                    .to_string();

                let candidate_slide_indices = ranked_self_intro_slide_indices(
                    &presentation,
                    self.config.slide_analysis_limit,
                );
                let mut consumed_slide_indices = HashSet::new();

                for slide_index in candidate_slide_indices {
                    if !consumed_slide_indices.insert(slide_index) {
                        continue;
                    }

                    let slide = &presentation.slides[slide_index];
                    if let Some(existing) = find_slide_analysis_record(
                        &slide_analysis_records,
                        presentation_id,
                        &slide.object_id,
                    ) {
                        if !self.slide_analyzer.is_some()
                            || !analysis_record_needs_refresh(existing, &analysis_model)
                        {
                            continue;
                        }
                    }

                    let rendered = self.google_client.render_slide(
                        presentation_id,
                        &slide.object_id,
                        "png",
                    )?;
                    let thumbnail_blob_ref = core.blobs.put(&rendered.data);
                    self.persistence_lock()?.persist_blob(&rendered.data)?;
                    let Some(mut profile) = self
                        .extract_student_profile_from_png(
                            &rendered.data,
                            observation,
                            &canonical_uri,
                        )
                        .or_else(|| heuristic_profile_for_slide(observation, slide))
                    else {
                        continue;
                    };
                    profile.normalize_in_place();

                    profile.source_slide_object_id = Some(slide.object_id.clone());
                    profile.source_document_id = Some(format!(
                        "document:gslides:{presentation_id}#slide:{}",
                        slide.object_id
                    ));
                    profile.source_canonical_uri = Some(canonical_uri.clone());
                    profile.thumbnail_blob_ref = Some(thumbnail_blob_ref.as_str().to_string());
                    profile.thumbnail_url = rendered.content_url.clone();
                    profile.companion_to_slide_object_id = None;
                    resolve_slide_image_urls(&presentation, slide, &mut profile);

                    let mut consumed_companion = false;
                    let mut companion_result = None;

                    if let Some(next_slide) = presentation.slides.get(slide_index + 1) {
                        let companion_rendered = self.google_client.render_slide(
                            presentation_id,
                            &next_slide.object_id,
                            "png",
                        )?;
                        let Some(mut companion_profile) = self
                            .extract_student_profile_from_png(
                                &companion_rendered.data,
                                observation,
                                &canonical_uri,
                            )
                            .or_else(|| heuristic_profile_for_slide(observation, next_slide))
                        else {
                            continue;
                        };
                        companion_profile.normalize_in_place();

                        companion_profile.source_slide_object_id =
                            Some(next_slide.object_id.clone());
                        companion_profile.source_document_id = Some(format!(
                            "document:gslides:{presentation_id}#slide:{}",
                            next_slide.object_id
                        ));
                        companion_profile.source_canonical_uri = Some(canonical_uri.clone());
                        companion_profile.thumbnail_url = companion_rendered.content_url.clone();
                        companion_profile.companion_to_slide_object_id =
                            Some(slide.object_id.clone());
                        resolve_slide_image_urls(&presentation, next_slide, &mut companion_profile);

                        if should_merge_companion_slide(&profile, &companion_profile, observation) {
                            let companion_blob_ref = core.blobs.put(&companion_rendered.data);
                            self.persistence_lock()?
                                .persist_blob(&companion_rendered.data)?;
                            companion_profile.thumbnail_blob_ref =
                                Some(companion_blob_ref.as_str().to_string());
                            merge_companion_profile(&mut profile, &companion_profile);
                            consumed_companion = true;
                            consumed_slide_indices.insert(slide_index + 1);
                        }
                    }

                    ensure_profile_identifier(&mut profile, &slide.object_id);
                    profile.normalize_in_place();

                    let email = profile
                        .email
                        .as_deref()
                        .or(profile.generated_email.as_deref())
                        .map(ToOwned::to_owned)
                        .or_else(|| profile.source_document_id.clone())
                        .unwrap_or_else(|| "unknown".to_string());
                    let person_entity = EntityRef::new(format!("person:{email}"));
                    analysis_results.push(SlideAnalysisResult {
                        source_observation_id: observation.id.clone(),
                        presentation_id: presentation_id.clone(),
                        profile: profile.clone(),
                        person_entity: person_entity.clone(),
                        supplemental_id: Some(lethe_core::domain::SupplementalId::new(format!(
                            "sup:slide-analysis:{presentation_id}:{}",
                            slide.object_id
                        ))),
                        analyzed_at: Utc::now(),
                        model_version: Some(analysis_model.clone()),
                        slide_object_id: Some(slide.object_id.clone()),
                        thumbnail_blob_ref: Some(thumbnail_blob_ref),
                    });

                    if consumed_companion {
                        if let Some(next_slide) = presentation.slides.get(slide_index + 1) {
                            let mut companion_profile = profile.clone();
                            companion_profile.source_slide_object_id =
                                Some(next_slide.object_id.clone());
                            companion_profile.source_document_id = Some(format!(
                                "document:gslides:{presentation_id}#slide:{}",
                                next_slide.object_id
                            ));
                            companion_profile.companion_to_slide_object_id =
                                Some(slide.object_id.clone());
                            companion_profile.thumbnail_blob_ref = None;
                            companion_profile.profile_pic = None;
                            companion_result = Some(SlideAnalysisResult {
                                source_observation_id: observation.id.clone(),
                                presentation_id: presentation_id.clone(),
                                profile: companion_profile,
                                person_entity,
                                supplemental_id: Some(lethe_core::domain::SupplementalId::new(
                                    format!(
                                        "sup:slide-analysis:{presentation_id}:{}",
                                        next_slide.object_id
                                    ),
                                )),
                                analyzed_at: Utc::now(),
                                model_version: Some(analysis_model.clone()),
                                slide_object_id: Some(next_slide.object_id.clone()),
                                thumbnail_blob_ref: None,
                            });
                        }
                    }

                    if let Some(companion_result) = companion_result {
                        analysis_results.push(companion_result);
                    }
                }
            }

            slide_analyses = analysis_results.len();

            for result in &analysis_results {
                let record = SlideAnalysisProjector::build_supplemental(result);
                let rollback = core
                    .upsert_supplemental(record)
                    .map_err(|err| SelfHostError::Ingestion(err.to_string()))?;
                let persisted_record =
                    core.supplemental
                        .get(&rollback.id)
                        .cloned()
                        .ok_or_else(|| {
                            SelfHostError::Ingestion(format!(
                                "supplemental {} missing after upsert",
                                rollback.id
                            ))
                        })?;
                if let Err(err) = self
                    .persistence_lock()?
                    .persist_supplemental(&persisted_record)
                {
                    core.rollback_supplemental(rollback);
                    return Err(SelfHostError::Persistence(err));
                }
            }

            for result in &analysis_results {
                let draft = SlideAnalysisProjector::create_analysis_observation(result);
                let observation = match core.prepare_observation(draft) {
                    Ok(observation) => observation,
                    Err(IngestResult::Rejected { message, .. }) => {
                        return Err(SelfHostError::Ingestion(message));
                    }
                    Err(IngestResult::Quarantined { ticket }) => {
                        return Err(SelfHostError::Ingestion(ticket.reason));
                    }
                    Err(result) => {
                        if let IngestResult::Duplicate { .. } = result {
                            continue;
                        }
                        return Err(SelfHostError::Ingestion(
                            "unexpected non-terminal ingestion result during slide analysis"
                                .to_owned(),
                        ));
                    }
                };
                if let IngestResult::Ingested { .. } =
                    self.append_prepared_observation(&mut core, observation)?
                {
                    // Count is derived from analysis_results; no per-row action needed here.
                }
            }
        }

        if should_rebuild_snapshot || slide_analyses > 0 {
            core.rebuild_snapshot();
        }

        let observations = core.lake.list().to_vec();
        let persistence = self.persistence_lock()?;
        let source_image_index = build_notion_source_image_index(
            &observations,
            &mut core.blobs,
            &self.google_client,
            &persistence,
        );
        let notion_write_records = core
            .snapshot
            .person_page
            .profiles
            .iter()
            .filter_map(|person| {
                let frontend = person.frontend_profile.as_ref()?;
                Some((person, frontend))
            })
            .collect::<Vec<_>>();
        let notion_write_records = notion_write_records
            .iter()
            .filter_map(|(person, frontend)| {
                let write_record = notion_write_record_for_person(
                    person,
                    frontend,
                    core.snapshot.built_at,
                    self.config.public_base_url.as_deref(),
                    source_image_index.get(frontend.source_document_id.as_str()),
                )?;
                Some((write_record.entity_id.clone(), write_record))
            })
            .collect::<HashMap<_, _>>()
            .into_values()
            .collect::<Vec<_>>();

        drop(core);

        if let Some(notion) = &self.notion_client {
            for mut write_record in notion_write_records {
                write_record.external_id = notion.find_existing(&write_record.entity_id)?;
                notion.write_record(&write_record)?;
                notion_synced += 1;
            }
        }

        Ok(SyncReport {
            slack_ingested,
            google_ingested,
            slide_analyses,
            notion_synced,
            duplicates,
            quarantined: 0,
            dead_letters: Vec::new(),
            last_sync_at,
        })
    }
}
