use super::service_support::namespace_draft;
use super::*;

impl AppService {
    pub fn sync_all(&self) -> Result<SyncReport, SelfHostError> {
        let _non_bulk_projection_operation =
            self.non_bulk_projection_operation_lock("source sync")?;
        let rebuild_was_in_flight = self
            .non_corpus_rebuild_in_flight
            .load(std::sync::atomic::Ordering::Acquire);
        if rebuild_was_in_flight {
            tracing::info!(
                sync_wait_reason = "background_non_corpus_rebuild",
                "source sync waiting for the derived projection lane without holding the bulk import operation lock"
            );
        }
        let derived_lane_wait_started_at = std::time::Instant::now();
        let _derived_lane = self
            .derived_projection_lane
            .lock()
            .map_err(|_| SelfHostError::LockPoisoned)?;
        if rebuild_was_in_flight {
            tracing::info!(
                sync_wait_reason = "background_non_corpus_rebuild",
                derived_projection_lane_wait_ms =
                    u64::try_from(derived_lane_wait_started_at.elapsed().as_millis())
                        .expect("source sync derived lane wait does not fit u64 milliseconds"),
                "source sync acquired the derived projection lane"
            );
        }
        let started_at = std::time::Instant::now();
        let mut slack_ingested = 0usize;
        let mut google_ingested = 0usize;
        let mut duplicates = 0usize;
        let mut quarantined = 0usize;
        let mut fetched = 0usize;
        let mut dead_letters = Vec::new();
        let mut post_commit_error = None;

        let slack_policy = self.slack_adapter_config();
        let slack_poll_generation = if self.slack_sources.is_empty() {
            None
        } else {
            Some(
                self.persistence_lock()?
                    .advance_slack_thread_poll_generation()?,
            )
        };
        for source in &self.slack_sources {
            let slack_adapter = SlackAdapter::new(source.client.clone(), slack_policy.clone());
            for channel_id in &source.config.channel_ids {
                if fetched >= self.config.resource_limits.max_sync_items {
                    break;
                }
                let cursor_key = format!("{}:slack:{channel_id}:oldest_ts", source.config.id);
                let oldest = non_empty_state(self.persistence_lock()?.get_state(&cursor_key)?);
                let mut page_cursor: Option<String> = None;
                let mut latest_ts = oldest.clone();

                loop {
                    let circuit = format!("slack:{}:{channel_id}", source.config.id);
                    let page = match self.resilient_executor.execute(
                        &circuit,
                        &slack_policy.retry,
                        &slack_policy.rate_limit,
                        || {
                            source.client.conversations_history(
                                channel_id,
                                oldest.as_deref(),
                                page_cursor.as_deref(),
                                200,
                            )
                        },
                    ) {
                        Ok(page) => page,
                        Err(error) => {
                            dead_letters.push(DeadLetter {
                                source: source.config.id.clone(),
                                reason: error.to_string(),
                            });
                            break;
                        }
                    };
                    for message in page.messages {
                        if fetched >= self.config.resource_limits.max_sync_items {
                            break;
                        }
                        fetched += 1;
                        match self.ingest_slack_message(
                            &slack_adapter,
                            &source.client,
                            &source.config.id,
                            channel_id,
                            message,
                            &mut latest_ts,
                        ) {
                            Ok(IngestResult::Ingested { .. }) => slack_ingested += 1,
                            Ok(IngestResult::Duplicate { .. }) => duplicates += 1,
                            Ok(IngestResult::Quarantined { .. }) => quarantined += 1,
                            Ok(_) => {}
                            Err(error) => dead_letters.push(DeadLetter {
                                source: source.config.id.clone(),
                                reason: error.to_string(),
                            }),
                        }
                    }
                    if fetched >= self.config.resource_limits.max_sync_items || !page.has_more {
                        break;
                    }
                    page_cursor = page.next_cursor;
                }

                match self.resilient_executor.execute(
                    &format!("slack:{}:{channel_id}", source.config.id),
                    &slack_policy.retry,
                    &slack_policy.rate_limit,
                    || source.client.conversations_info(channel_id),
                ) {
                    Ok(channel_snapshot) => match self.ingest_draft(namespace_draft(
                        slack_adapter.map_channel_snapshot(&channel_snapshot),
                        &source.config.id,
                    )) {
                        Ok(IngestResult::Ingested { .. }) => slack_ingested += 1,
                        Ok(IngestResult::Duplicate { .. }) => duplicates += 1,
                        Ok(IngestResult::Quarantined { .. }) => quarantined += 1,
                        Ok(_) => {}
                        Err(error) => dead_letters.push(DeadLetter {
                            source: source.config.id.clone(),
                            reason: error.to_string(),
                        }),
                    },
                    Err(error) => dead_letters.push(DeadLetter {
                        source: source.config.id.clone(),
                        reason: error.to_string(),
                    }),
                }

                if let Some(latest_ts) = latest_ts.as_deref() {
                    self.persistence_lock()?.set_state(&cursor_key, latest_ts)?;
                }
            }

            self.refresh_slack_thread_catalog()?;
            let generation = slack_poll_generation
                .expect("Slack poll generation exists when a Slack source is configured");
            for channel_id in &source.config.channel_ids {
                let remaining = self
                    .config
                    .resource_limits
                    .max_sync_items
                    .saturating_sub(fetched);
                if remaining == 0 {
                    break;
                }
                let threads = self.persistence_lock()?.slack_threads_to_poll(
                    &source.config.id,
                    channel_id,
                    generation,
                    remaining,
                )?;
                for thread in threads {
                    if fetched >= self.config.resource_limits.max_sync_items {
                        break;
                    }
                    match self.sync_thread_replies(
                        &slack_adapter,
                        &source.replies_client,
                        &source.replies_client,
                        &thread,
                        generation,
                    ) {
                        Ok((ingested, dupes, thread_fetched)) => {
                            slack_ingested += ingested;
                            duplicates += dupes;
                            fetched += thread_fetched;
                        }
                        Err(error) => dead_letters.push(DeadLetter {
                            source: source.config.id.clone(),
                            reason: error.to_string(),
                        }),
                    }
                }
            }

            match self.ingest_draft(namespace_draft(
                slack_adapter.heartbeat(),
                &source.config.id,
            )) {
                Ok(IngestResult::Ingested { .. }) => slack_ingested += 1,
                Ok(IngestResult::Duplicate { .. }) => duplicates += 1,
                Ok(IngestResult::Quarantined { .. }) => quarantined += 1,
                Ok(_) => {}
                Err(error) => dead_letters.push(DeadLetter {
                    source: source.config.id.clone(),
                    reason: error.to_string(),
                }),
            }
        }

        let google_policy = self.google_adapter_config();
        for source in &self.google_sources {
            let google_adapter =
                GoogleSlidesAdapter::new(source.client.clone(), google_policy.clone());
            for presentation_id in &source.config.presentation_ids {
                if fetched >= self.config.resource_limits.max_sync_items {
                    break;
                }
                let cursor_key = format!("{}:gslides:{presentation_id}:revision", source.config.id);
                let last_revision = self.persistence_lock()?.get_state(&cursor_key)?;
                let mut page_token: Option<String> = None;
                let mut revisions = Vec::new();
                loop {
                    let page = match self.resilient_executor.execute(
                        &format!("gslides:{}:{presentation_id}", source.config.id),
                        &google_policy.retry,
                        &google_policy.rate_limit,
                        || {
                            source
                                .client
                                .list_revisions(presentation_id, page_token.as_deref())
                        },
                    ) {
                        Ok(page) => page,
                        Err(error) => {
                            dead_letters.push(DeadLetter {
                                source: source.config.id.clone(),
                                reason: error.to_string(),
                            });
                            break;
                        }
                    };
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
                fetched += 1;

                let capture = (|| -> Result<IngestResult, SelfHostError> {
                    let circuit = format!("gslides:{}:{presentation_id}", source.config.id);
                    let meta = self.resilient_executor.execute(
                        &circuit,
                        &google_policy.retry,
                        &google_policy.rate_limit,
                        || source.client.get_presentation_meta(presentation_id),
                    )?;
                    let presentation = self.resilient_executor.execute(
                        &circuit,
                        &google_policy.retry,
                        &google_policy.rate_limit,
                        || source.client.get_presentation(presentation_id),
                    )?;
                    let native_blob = self.store_blob(&serde_json::to_vec(&presentation)?)?;
                    let rendered_blobs = presentation
                        .slides
                        .first()
                        .map(|slide| {
                            self.resilient_executor.execute(
                                &circuit,
                                &google_policy.retry,
                                &google_policy.rate_limit,
                                || {
                                    source.client.render_slide(
                                        presentation_id,
                                        &slide.object_id,
                                        "png",
                                    )
                                },
                            )
                        })
                        .transpose()?
                        .map(|rendered| self.store_blob(&rendered.data))
                        .transpose()?
                        .into_iter()
                        .collect::<Vec<_>>();
                    self.ingest_draft(namespace_draft(
                        google_adapter.map_revision(
                            &captured_revision,
                            &meta,
                            Some(native_blob),
                            rendered_blobs,
                        ),
                        &source.config.id,
                    ))
                })();
                match capture {
                    Ok(IngestResult::Ingested { .. }) => google_ingested += 1,
                    Ok(IngestResult::Duplicate { .. }) => duplicates += 1,
                    Ok(IngestResult::Quarantined { .. }) => quarantined += 1,
                    Ok(_) => {}
                    Err(error) => {
                        dead_letters.push(DeadLetter {
                            source: source.config.id.clone(),
                            reason: error.to_string(),
                        });
                        continue;
                    }
                }
                self.persistence_lock()?
                    .set_state(&cursor_key, &captured_revision.revision_id)?;
            }
            match self.ingest_draft(namespace_draft(
                google_adapter.heartbeat(),
                &source.config.id,
            )) {
                Ok(IngestResult::Ingested { .. }) => google_ingested += 1,
                Ok(IngestResult::Duplicate { .. }) => duplicates += 1,
                Ok(IngestResult::Quarantined { .. }) => quarantined += 1,
                Ok(_) => {}
                Err(error) => dead_letters.push(DeadLetter {
                    source: source.config.id.clone(),
                    reason: error.to_string(),
                }),
            }
        }

        let last_sync_at = Utc::now();
        let slide_obs_by_presentation = self.latest_workspace_slide_observations()?;
        let mut core = (*self.core_snapshot()).clone();
        core.last_sync_at = Some(last_sync_at);
        core.last_sync_error = None;
        let slide_analysis_records: Vec<lethe_core::domain::SupplementalRecord> = core
            .supplemental
            .by_kind("slide-analysis")
            .into_iter()
            .cloned()
            .collect();
        let analysis_model = self
            .slide_analyzer
            .as_ref()
            .map(|analyzer| format!("{}+continuation-v2-image-url", analyzer.model_name()));
        let mut needs_analysis = false;
        if let (Some(analysis_model), Some(slide_analysis_limit)) =
            (analysis_model.as_ref(), self.config.slide_analysis_limit)
        {
            for source in &self.google_sources {
                for presentation_id in &source.config.presentation_ids {
                    let namespaced_id = format!("{}:{presentation_id}", source.config.id);
                    let Some(_observation) = slide_obs_by_presentation.get(&namespaced_id) else {
                        continue;
                    };
                    let Ok(presentation) = self.resilient_executor.execute(
                        &format!("gslides:{}:{presentation_id}", source.config.id),
                        &google_policy.retry,
                        &google_policy.rate_limit,
                        || source.client.get_presentation(presentation_id),
                    ) else {
                        continue;
                    };

                    if presentation
                        .slides
                        .iter()
                        .take(slide_analysis_limit)
                        .any(|slide| {
                            match find_slide_analysis_record(
                                &slide_analysis_records,
                                &namespaced_id,
                                &slide.object_id,
                            ) {
                                Some(record) => {
                                    analysis_record_needs_refresh(record, analysis_model)
                                }
                                None => true,
                            }
                        })
                    {
                        needs_analysis = true;
                        break;
                    }
                }
            }
        }

        // --- Slide Analysis ---
        let mut slide_analyses = 0usize;

        if (google_ingested > 0 || slack_ingested > 0 || needs_analysis)
            && !self.google_sources.is_empty()
        {
            let Some(analysis_model) = analysis_model.clone() else {
                return Err(SelfHostError::Config(
                    crate::self_host::config::ConfigError::Invalid(
                        "slide analyzer is required for Google Slides analysis".to_owned(),
                    ),
                ));
            };
            let Some(slide_analysis_limit) = self.config.slide_analysis_limit else {
                return Err(SelfHostError::Config(
                    crate::self_host::config::ConfigError::Invalid(
                        "slide_analysis_limit is required for Google Slides analysis".to_owned(),
                    ),
                ));
            };
            let mut analysis_results = Vec::new();

            for source in &self.google_sources {
                for presentation_id in &source.config.presentation_ids {
                    let namespaced_id = format!("{}:{presentation_id}", source.config.id);
                    let Some(observation) = slide_obs_by_presentation.get(&namespaced_id) else {
                        continue;
                    };

                    let circuit = format!("gslides:{}:{presentation_id}", source.config.id);
                    let presentation = match self.resilient_executor.execute(
                        &circuit,
                        &google_policy.retry,
                        &google_policy.rate_limit,
                        || source.client.get_presentation(presentation_id),
                    ) {
                        Ok(presentation) => presentation,
                        Err(error) => {
                            dead_letters.push(DeadLetter {
                                source: source.config.id.clone(),
                                reason: error.to_string(),
                            });
                            continue;
                        }
                    };
                    let canonical_uri = observation
                        .payload
                        .pointer("/artifact/canonicalUri")
                        .and_then(|value| value.as_str())
                        .unwrap_or_default()
                        .to_string();

                    let candidate_slide_indices =
                        ranked_self_intro_slide_indices(&presentation, slide_analysis_limit);
                    let mut consumed_slide_indices = HashSet::new();

                    for slide_index in candidate_slide_indices {
                        if !consumed_slide_indices.insert(slide_index) {
                            continue;
                        }

                        let slide = &presentation.slides[slide_index];
                        if let Some(existing) = find_slide_analysis_record(
                            &slide_analysis_records,
                            &namespaced_id,
                            &slide.object_id,
                        ) && !analysis_record_needs_refresh(existing, &analysis_model)
                        {
                            continue;
                        }

                        let rendered = match self.resilient_executor.execute(
                            &circuit,
                            &google_policy.retry,
                            &google_policy.rate_limit,
                            || {
                                source
                                    .client
                                    .render_slide(presentation_id, &slide.object_id, "png")
                            },
                        ) {
                            Ok(rendered) => rendered,
                            Err(error) => {
                                dead_letters.push(DeadLetter {
                                    source: source.config.id.clone(),
                                    reason: error.to_string(),
                                });
                                continue;
                            }
                        };
                        let thumbnail_blob_ref = self
                            .persistence_lock()?
                            .put_blob(&rendered.data, self.config.resource_limits.max_blob_bytes)?;
                        core.blobs.put(&rendered.data);
                        let Some(mut profile) = (match self.extract_student_profile_from_png(
                            &rendered.data,
                            observation,
                            &canonical_uri,
                        ) {
                            Ok(profile) => profile,
                            Err(error) => {
                                dead_letters.push(DeadLetter {
                                    source: source.config.id.clone(),
                                    reason: error.to_string(),
                                });
                                continue;
                            }
                        }) else {
                            continue;
                        };
                        profile.normalize_in_place();

                        profile.source_slide_object_id = Some(slide.object_id.clone());
                        profile.source_document_id = Some(format!(
                            "document:gslides:{namespaced_id}#slide:{}",
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
                            let companion_rendered = match self.resilient_executor.execute(
                                &circuit,
                                &google_policy.retry,
                                &google_policy.rate_limit,
                                || {
                                    source.client.render_slide(
                                        presentation_id,
                                        &next_slide.object_id,
                                        "png",
                                    )
                                },
                            ) {
                                Ok(rendered) => rendered,
                                Err(error) => {
                                    dead_letters.push(DeadLetter {
                                        source: source.config.id.clone(),
                                        reason: error.to_string(),
                                    });
                                    continue;
                                }
                            };
                            let Some(mut companion_profile) = (match self
                                .extract_student_profile_from_png(
                                    &companion_rendered.data,
                                    observation,
                                    &canonical_uri,
                                ) {
                                Ok(profile) => profile,
                                Err(error) => {
                                    dead_letters.push(DeadLetter {
                                        source: source.config.id.clone(),
                                        reason: error.to_string(),
                                    });
                                    continue;
                                }
                            }) else {
                                continue;
                            };
                            companion_profile.normalize_in_place();

                            companion_profile.source_slide_object_id =
                                Some(next_slide.object_id.clone());
                            companion_profile.source_document_id = Some(format!(
                                "document:gslides:{namespaced_id}#slide:{}",
                                next_slide.object_id
                            ));
                            companion_profile.source_canonical_uri = Some(canonical_uri.clone());
                            companion_profile.thumbnail_url =
                                companion_rendered.content_url.clone();
                            companion_profile.companion_to_slide_object_id =
                                Some(slide.object_id.clone());
                            resolve_slide_image_urls(
                                &presentation,
                                next_slide,
                                &mut companion_profile,
                            );

                            if should_merge_companion_slide(
                                &profile,
                                &companion_profile,
                                observation,
                            ) {
                                let companion_blob_ref = self.persistence_lock()?.put_blob(
                                    &companion_rendered.data,
                                    self.config.resource_limits.max_blob_bytes,
                                )?;
                                core.blobs.put(&companion_rendered.data);
                                companion_profile.thumbnail_blob_ref =
                                    Some(companion_blob_ref.as_str().to_string());
                                merge_companion_profile(&mut profile, &companion_profile);
                                consumed_companion = true;
                                consumed_slide_indices.insert(slide_index + 1);
                            }
                        }

                        ensure_profile_identifier(&mut profile, &slide.object_id);
                        profile.normalize_in_place();

                        let Some(email) = profile
                            .email
                            .as_deref()
                            .or(profile.generated_email.as_deref())
                            .map(ToOwned::to_owned)
                            .or_else(|| profile.source_document_id.clone())
                        else {
                            dead_letters.push(DeadLetter {
                                source: source.config.id.clone(),
                                reason: format!(
                                    "slide analysis for {} produced no stable person identifier",
                                    slide.object_id
                                ),
                            });
                            continue;
                        };
                        let person_entity = EntityRef::new(format!("person:{email}"));
                        analysis_results.push(SlideAnalysisResult {
                            source_observation_id: observation.id.clone(),
                            presentation_id: namespaced_id.clone(),
                            profile: profile.clone(),
                            person_entity: person_entity.clone(),
                            supplemental_id: Some(lethe_core::domain::SupplementalId::new(
                                format!("sup:slide-analysis:{namespaced_id}:{}", slide.object_id),
                            )),
                            analyzed_at: observation.recorded_at,
                            model_version: Some(analysis_model.clone()),
                            slide_object_id: Some(slide.object_id.clone()),
                            thumbnail_blob_ref: Some(thumbnail_blob_ref),
                        });

                        if consumed_companion
                            && let Some(next_slide) = presentation.slides.get(slide_index + 1)
                        {
                            let mut companion_profile = profile.clone();
                            companion_profile.source_slide_object_id =
                                Some(next_slide.object_id.clone());
                            companion_profile.source_document_id = Some(format!(
                                "document:gslides:{namespaced_id}#slide:{}",
                                next_slide.object_id
                            ));
                            companion_profile.companion_to_slide_object_id =
                                Some(slide.object_id.clone());
                            companion_profile.thumbnail_blob_ref = None;
                            companion_profile.profile_pic = None;
                            companion_result = Some(SlideAnalysisResult {
                                source_observation_id: observation.id.clone(),
                                presentation_id: namespaced_id.clone(),
                                profile: companion_profile,
                                person_entity,
                                supplemental_id: Some(lethe_core::domain::SupplementalId::new(
                                    format!(
                                        "sup:slide-analysis:{namespaced_id}:{}",
                                        next_slide.object_id
                                    ),
                                )),
                                analyzed_at: observation.recorded_at,
                                model_version: Some(analysis_model.clone()),
                                slide_object_id: Some(next_slide.object_id.clone()),
                                thumbnail_blob_ref: None,
                            });
                        }

                        if let Some(companion_result) = companion_result {
                            analysis_results.push(companion_result);
                        }
                    }
                }
            }

            slide_analyses = analysis_results.len();

            let resolved_observation_ids = slide_obs_by_presentation
                .values()
                .map(|observation| observation.id.clone())
                .collect::<HashSet<_>>();
            let mut resolved_supplemental_ids = core
                .supplemental
                .list()
                .into_iter()
                .map(|record| record.id.clone())
                .collect::<HashSet<_>>();

            for result in &analysis_results {
                let record = SlideAnalysisProjector::build_supplemental(result);
                let rollback = match core.upsert_supplemental_checked(
                    record,
                    |observation_id| resolved_observation_ids.contains(observation_id),
                    |supplemental_id| resolved_supplemental_ids.contains(supplemental_id),
                ) {
                    Ok(rollback) => rollback,
                    Err(error) => {
                        dead_letters.push(DeadLetter {
                            source: result.presentation_id.clone(),
                            reason: error.to_string(),
                        });
                        continue;
                    }
                };
                let Some(persisted_record) = core.supplemental.get(&rollback.id).cloned() else {
                    dead_letters.push(DeadLetter {
                        source: result.presentation_id.clone(),
                        reason: format!("supplemental {} missing after upsert", rollback.id),
                    });
                    core.rollback_supplemental(rollback);
                    continue;
                };
                if let Err(err) = self.persistence_lock()?.put_supplemental(&persisted_record) {
                    core.rollback_supplemental(rollback);
                    dead_letters.push(DeadLetter {
                        source: result.presentation_id.clone(),
                        reason: err.to_string(),
                    });
                } else {
                    resolved_supplemental_ids.insert(persisted_record.id);
                }
            }

            for result in &analysis_results {
                let draft = SlideAnalysisProjector::create_analysis_observation(result);
                let observation = match prepare_draft(&core, draft) {
                    Ok(observation) => observation,
                    Err(IngestResult::Rejected { message, .. }) => {
                        dead_letters.push(DeadLetter {
                            source: result.presentation_id.clone(),
                            reason: message,
                        });
                        continue;
                    }
                    Err(IngestResult::Quarantined { ticket }) => {
                        quarantined += 1;
                        dead_letters.push(DeadLetter {
                            source: result.presentation_id.clone(),
                            reason: ticket.reason,
                        });
                        continue;
                    }
                    Err(other) => {
                        if let IngestResult::Duplicate { .. } = other {
                            continue;
                        }
                        dead_letters.push(DeadLetter {
                            source: result.presentation_id.clone(),
                            reason:
                                "unexpected non-terminal ingestion result during slide analysis"
                                    .to_owned(),
                        });
                        continue;
                    }
                };
                match self.append_prepared_observation(observation) {
                    Ok(IngestResult::Duplicate { .. }) => duplicates += 1,
                    Ok(IngestResult::Quarantined { .. }) => quarantined += 1,
                    Ok(_) => {}
                    Err(error) => dead_letters.push(DeadLetter {
                        source: result.presentation_id.clone(),
                        reason: error.to_string(),
                    }),
                }
            }
        }

        let retained_observations = self
            .persistence_lock()?
            .apply_retention(self.config.resource_limits.retention_days)?;
        if slide_analyses > 0 || retained_observations > 0 {
            let materialize_result = self.refresh_materialized_snapshot(&mut core);
            let index_result = self.search_index.catch_up_after_append();
            if let Err(error) = materialize_result {
                core.mark_non_corpus_materializations_stale();
                dead_letters.push(DeadLetter {
                    source: "projection:person-page".to_owned(),
                    reason: error.to_string(),
                });
                post_commit_error = Some(error);
            }
            if let Err(error) = index_result {
                dead_letters.push(DeadLetter {
                    source: "projection:corpus".to_owned(),
                    reason: error.to_string(),
                });
                post_commit_error = Some(error);
            }
        }

        self.persistence_lock()?
            .split_leaf_if_capacity(self.config.resource_limits.max_leaf_observations)?;

        core.sync_metrics.fetched += fetched as u64;
        core.sync_metrics.ingested += (slack_ingested + google_ingested + slide_analyses) as u64;
        core.sync_metrics.skipped += duplicates as u64;
        core.sync_metrics.failed += dead_letters.len() as u64;
        core.sync_metrics.quarantined += quarantined as u64;
        core.sync_metrics.latency_ms = started_at.elapsed().as_millis() as u64;
        core.last_sync_error = if dead_letters.is_empty() {
            None
        } else {
            Some(format!("{} item(s) failed", dead_letters.len()))
        };
        let latency_ms = core.sync_metrics.latency_ms;
        let persisted_sync_state = lethe_storage_api::PersistedSyncState {
            metrics: lethe_storage_api::SyncMetricRecord {
                fetched: core.sync_metrics.fetched,
                ingested: core.sync_metrics.ingested,
                skipped: core.sync_metrics.skipped,
                failed: core.sync_metrics.failed,
                quarantined: core.sync_metrics.quarantined,
                latency_ms,
            },
            completed_at: last_sync_at,
            error: core.last_sync_error.clone(),
        };
        let mut live_core = self.core_lock()?;
        *live_core = core;
        self.publish_core_snapshot(&live_core);
        drop(live_core);
        {
            let store = self.persistence_lock()?;
            for dead_letter in &dead_letters {
                store.record_dead_letter(&dead_letter.source, &dead_letter.reason)?;
            }
            store.record_sync_state("all", &persisted_sync_state)?;
            store.garbage_collect_orphan_blobs()?;
        }

        if let Some(error) = post_commit_error {
            return Err(error);
        }

        Ok(SyncReport {
            slack_ingested,
            google_ingested,
            slide_analyses,
            duplicates,
            quarantined,
            dead_letters,
            last_sync_at,
        })
    }

    pub(super) fn latest_workspace_slide_observations(
        &self,
    ) -> Result<HashMap<String, Observation>, SelfHostError> {
        let configured = self
            .google_sources
            .iter()
            .flat_map(|source| {
                source
                    .config
                    .presentation_ids
                    .iter()
                    .map(|presentation_id| format!("{}:{presentation_id}", source.config.id))
            })
            .collect::<HashSet<_>>();
        if configured.is_empty() {
            return Ok(HashMap::new());
        }
        let mut latest = HashMap::<String, Observation>::with_capacity(configured.len());
        let mut cursor = 0u64;
        loop {
            let page = self
                .persistence_lock()?
                .observation_page(cursor, self.config.corpus.rebuild_page_size)?;
            if page.is_empty() {
                break;
            }
            cursor = page
                .last()
                .expect("non-empty observation page must have a tail")
                .append_seq;
            for stored in page {
                let observation = stored.observation;
                if observation.schema.as_str() != "schema:workspace-object-snapshot" {
                    continue;
                }
                let Some(presentation_id) = observation
                    .payload
                    .pointer("/artifact/sourceObjectId")
                    .and_then(serde_json::Value::as_str)
                else {
                    continue;
                };
                let Some(source_instance) = observation
                    .meta
                    .get("source_instance")
                    .and_then(serde_json::Value::as_str)
                else {
                    continue;
                };
                let key = format!("{source_instance}:{presentation_id}");
                if !configured.contains(&key) {
                    continue;
                }
                match latest.get(&key) {
                    Some(existing) if existing.published >= observation.published => {}
                    _ => {
                        latest.insert(key, observation);
                    }
                }
            }
        }
        Ok(latest)
    }
}
