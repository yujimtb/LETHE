use super::service_support::namespace_draft;
use super::*;

impl AppService {
    pub fn sync_all(&self) -> Result<SyncReport, SelfHostError> {
        let started_at = std::time::Instant::now();
        let mut slack_ingested = 0usize;
        let mut google_ingested = 0usize;
        let mut duplicates = 0usize;
        let mut quarantined = 0usize;
        let mut fetched = 0usize;
        let mut dead_letters = Vec::new();

        let slack_policy = self.slack_adapter_config();
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
                let mut thread_roots = self.known_thread_roots(channel_id)?;

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
                        if let Some(thread_root) = thread_root_ts(&message) {
                            thread_roots.insert(thread_root.to_string());
                        }
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

                for thread_ts in thread_roots {
                    if fetched >= self.config.resource_limits.max_sync_items {
                        break;
                    }
                    match self.sync_thread_replies(&slack_adapter, source, channel_id, &thread_ts) {
                        Ok((ingested, dupes)) => {
                            slack_ingested += ingested;
                            duplicates += dupes;
                        }
                        Err(error) => dead_letters.push(DeadLetter {
                            source: source.config.id.clone(),
                            reason: error.to_string(),
                        }),
                    }
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
                let Some(source_instance) = obs
                    .meta
                    .get("source_instance")
                    .and_then(serde_json::Value::as_str)
                else {
                    return acc;
                };
                let key = format!("{source_instance}:{presentation_id}");

                match acc.get(&key) {
                    Some(existing) if existing.published >= obs.published => {}
                    _ => {
                        acc.insert(key, obs.clone());
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
        let analysis_model = format!(
            "{}+continuation-v2-image-url",
            self.slide_analyzer.model_name()
        );
        let mut needs_analysis = false;
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
                    .take(self.config.slide_analysis_limit)
                    .any(|slide| {
                        match find_slide_analysis_record(
                            &slide_analysis_records,
                            &namespaced_id,
                            &slide.object_id,
                        ) {
                            Some(record) => analysis_record_needs_refresh(record, &analysis_model),
                            None => true,
                        }
                    })
                {
                    needs_analysis = true;
                    break;
                }
            }
        }

        // --- Slide Analysis ---
        let mut slide_analyses = 0usize;

        if google_ingested > 0 || slack_ingested > 0 || needs_analysis {
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

            for result in &analysis_results {
                let record = SlideAnalysisProjector::build_supplemental(result);
                let rollback = match core.upsert_supplemental(record) {
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
                }
            }

            for result in &analysis_results {
                let draft = SlideAnalysisProjector::create_analysis_observation(result);
                let observation = match core.prepare_observation(draft) {
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
                match self.append_prepared_observation(&mut core, observation) {
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

        if should_rebuild_snapshot || slide_analyses > 0 {
            core.rebuild_snapshot();
            self.persistence_lock()?.materialize_projection(
                &ProjectionRef::new("proj:person-page"),
                &serde_json::to_value(&core.snapshot)?,
            )?;
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
        drop(core);
        {
            let store = self.persistence_lock()?;
            for dead_letter in &dead_letters {
                store.record_dead_letter(&dead_letter.source, &dead_letter.reason)?;
            }
            store.record_sync_metrics(
                "all",
                &lethe_storage_api::SyncMetricRecord {
                    fetched: fetched as u64,
                    ingested: (slack_ingested + google_ingested + slide_analyses) as u64,
                    skipped: duplicates as u64,
                    failed: dead_letters.len() as u64,
                    quarantined: quarantined as u64,
                    latency_ms,
                },
            )?;
            store.apply_retention(self.config.resource_limits.retention_days)?;
            store.garbage_collect_orphan_blobs()?;
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
}
