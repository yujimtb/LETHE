use super::*;

impl AppService {
    pub fn sync_without_notion_writeback(&self) -> Result<SyncReport, SelfHostError> {
        let mut cloned = self.clone();
        cloned.notion_client = None;
        cloned.sync_all()
    }

    pub fn notion_review_candidates(
        &self,
        limit: usize,
    ) -> Result<Vec<NotionReviewCandidate>, SelfHostError> {
        let mut core = self.core_lock()?;
        let persistence = self.persistence_lock()?;
        let observations = core.lake.list().to_vec();
        let source_image_index = build_notion_source_image_index(
            &observations,
            &mut core.blobs,
            &self.google_client,
            &persistence,
        );
        Ok(ranked_notion_write_candidates_from_snapshot(
            &core.snapshot,
            limit,
            self.config.public_base_url.as_deref(),
            &source_image_index,
        )
        .into_iter()
        .map(|candidate| candidate.preview)
        .collect())
    }

    pub fn notion_review_sync(
        &self,
        limit: usize,
        refresh_data: bool,
    ) -> Result<NotionReviewSyncReport, SelfHostError> {
        let sync_report = if refresh_data {
            self.sync_without_notion_writeback()?
        } else {
            SyncReport {
                slack_ingested: 0,
                google_ingested: 0,
                slide_analyses: 0,
                notion_synced: 0,
                duplicates: 0,
                quarantined: 0,
                dead_letters: Vec::new(),
                last_sync_at: Utc::now(),
            }
        };
        let candidates = {
            let mut core = self.core_lock()?;
            let persistence = self.persistence_lock()?;
            let observations = core.lake.list().to_vec();
            let seed_candidates = ranked_notion_write_candidates_from_snapshot(
                &core.snapshot,
                limit,
                self.config.public_base_url.as_deref(),
                &HashMap::new(),
            );
            let target_source_document_ids = seed_candidates
                .iter()
                .map(|candidate| candidate.preview.source_document_id.clone())
                .collect::<Vec<_>>();
            let source_image_index = build_targeted_notion_source_image_index(
                &observations,
                &target_source_document_ids,
                &mut core.blobs,
                &self.google_client,
                &persistence,
            );
            let seed_previews = seed_candidates
                .into_iter()
                .map(|candidate| (candidate.preview.person_id.clone(), candidate.preview))
                .collect::<HashMap<_, _>>();
            let mut ranked = core
                .snapshot
                .person_page
                .profiles
                .iter()
                .filter_map(|person| {
                    let preview = seed_previews.get(person.person_id.as_str())?.clone();
                    let frontend = person.frontend_profile.as_ref()?;
                    let write_record = notion_write_record_for_person(
                        person,
                        frontend,
                        core.snapshot.built_at,
                        self.config.public_base_url.as_deref(),
                        source_image_index.get(frontend.source_document_id.as_str()),
                    )?;
                    Some(RankedNotionWriteCandidate {
                        preview,
                        write_record,
                    })
                })
                .collect::<Vec<_>>();
            ranked.sort_by_key(|candidate| candidate.preview.rank);
            ranked
        };

        let notion = self.notion_client.as_ref().ok_or_else(|| {
            SelfHostError::Adapter(AdapterError::Other(
                "notion writeback is not configured".to_string(),
            ))
        })?;

        let mut cleaned_up = 0usize;
        let mut writes = Vec::new();

        for candidate in &candidates {
            let existing_page = notion.find_existing(&candidate.write_record.entity_id)?;
            let cleaned_existing_page = if let Some(existing_page) = existing_page {
                notion.delete_record(&existing_page)?;
                cleaned_up += 1;
                true
            } else {
                false
            };

            let result = notion.write_record(&candidate.write_record)?;
            writes.push(NotionReviewWrite {
                rank: candidate.preview.rank,
                entity_id: candidate.preview.entity_id.clone(),
                title: candidate.preview.title.clone(),
                external_id: result.external_id,
                url: result.url,
                action: result.action,
                cleaned_existing_page,
            });
        }

        Ok(NotionReviewSyncReport {
            sync_report,
            candidates: candidates
                .into_iter()
                .map(|candidate| candidate.preview)
                .collect(),
            notion_synced: writes.len(),
            writes,
            cleaned_up,
        })
    }

    pub fn attribute_inventory_documents(
        &self,
    ) -> Result<Vec<AttributeInventoryDocument>, SelfHostError> {
        let core = self.core_lock()?;
        Ok(build_inventory_documents(&core.snapshot))
    }
}
