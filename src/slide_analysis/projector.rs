//! Slide Analysis Projector — transforms workspace-object-snapshot
//! observations into supplemental records and SaaS write-back records.
//!
//! Pipeline:
//! 1. Select slide snapshot observations from the lake
//! 2. For each, extract/accept a StudentProfile (from AI or pre-populated payload)
//! 3. Store as a SupplementalRecord (kind = "slide-analysis")
//! 4. Produce a WriteRecord suitable for SaaS write-back (e.g. Notion)

use chrono::Utc;

use crate::adapter::gslides::mapper::WORKSPACE_SNAPSHOT_SCHEMA;
use crate::adapter::writeback::traits::WriteRecord;
use crate::domain::{
    ActorRef, EntityRef, Mutability, Observation, ObservationId, SchemaRef, SupplementalId,
    SupplementalRecord,
};
use crate::domain::supplemental::InputAnchorSet;
use crate::lake::LakeStore;
use crate::supplemental::SupplementalStore;

use super::types::{SlideAnalysisResult, StudentProfile};

/// The slide analysis projector.
pub struct SlideAnalysisProjector;

/// Supplemental kind string for slide analysis records.
pub const SLIDE_ANALYSIS_KIND: &str = "slide-analysis";

impl SlideAnalysisProjector {
    /// Scan the lake for workspace-object-snapshot observations that haven't
    /// been analysed yet, and process them.
    ///
    /// `analyse_fn` is the callback that performs the actual AI extraction.
    /// In production this calls Gemini; in tests it's a pure fixture.
    ///
    /// `existing_supplemental_obs_ids` is the set of observation IDs that
    /// already have a slide-analysis supplemental record, to avoid re-processing.
    ///
    /// Returns the list of analysis results produced (supplemental records
    /// are not stored here — caller is responsible for adding them).
    pub fn analyse_slides<F>(
        observations: &[&Observation],
        already_analysed: &[ObservationId],
        model_version: &str,
        analyse_fn: F,
    ) -> Vec<SlideAnalysisResult>
    where
        F: Fn(&Observation) -> Option<StudentProfile>,
    {
        let mut results = Vec::new();

        for observation in observations {
            // Skip if already analysed
            if already_analysed.contains(&observation.id) {
                continue;
            }

            // Run the analysis function
            let profile = match analyse_fn(observation) {
                Some(p) => p,
                None => continue,
            };

            // Determine the person entity
            let email = profile
                .email
                .as_deref()
                .or(profile.generated_email.as_deref())
                .unwrap_or("unknown");
            let person_entity = EntityRef::new(format!("person:{email}"));

            // Extract presentation ID from the observation payload
            let presentation_id = observation
                .payload
                .get("artifact")
                .and_then(|a| a.get("sourceObjectId"))
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();

            let sup_id = SupplementalId::new(format!(
                "sup:slide-analysis:{}:{}",
                presentation_id, observation.id
            ));

            results.push(SlideAnalysisResult {
                source_observation_id: observation.id.clone(),
                presentation_id,
                profile,
                person_entity,
                supplemental_id: Some(sup_id),
                analyzed_at: Utc::now(),
                model_version: Some(model_version.to_string()),
                slide_object_id: None,
                thumbnail_blob_ref: None,
            });
        }

        results
    }

    /// Build a SupplementalRecord for a slide analysis result.
    pub fn build_supplemental(
        result: &SlideAnalysisResult,
    ) -> SupplementalRecord {
        let profile_json = serde_json::to_value(&result.profile).unwrap_or_default();
        let sup_id = result.supplemental_id.clone().unwrap_or_else(|| {
            SupplementalId::new(format!(
                "sup:slide-analysis:{}:{}",
                result.presentation_id, result.source_observation_id
            ))
        });
        SupplementalRecord {
            id: sup_id,
            kind: SLIDE_ANALYSIS_KIND.to_string(),
            derived_from: InputAnchorSet {
                observations: vec![result.source_observation_id.clone()],
                blobs: result.thumbnail_blob_ref.clone().into_iter().collect(),
                supplementals: vec![],
            },
            payload: profile_json,
            created_by: ActorRef::new("actor:slide-analysis-projector"),
            created_at: Utc::now(),
            mutability: Mutability::ManagedCache,
            record_version: Some("1".to_string()),
            model_version: result.model_version.clone(),
            consent_metadata: None,
            lineage: None,
        }
    }

    /// Convenience: run the full pipeline (analyse + store supplemental).
    /// This is the method tests use.
    pub fn process_new_slides<F>(
        lake: &LakeStore,
        supplemental: &mut SupplementalStore,
        analyse_fn: F,
    ) -> Vec<SlideAnalysisResult>
    where
        F: Fn(&Observation) -> Option<StudentProfile>,
    {
        let schema = SchemaRef::new(WORKSPACE_SNAPSHOT_SCHEMA);
        let slide_observations: Vec<&Observation> = lake.by_schema(&schema);

        // Find already-analysed observation IDs
        let already_analysed: Vec<ObservationId> = slide_observations
            .iter()
            .filter(|obs| {
                supplemental
                    .by_observation(&obs.id)
                    .iter()
                    .any(|r| r.kind == SLIDE_ANALYSIS_KIND)
            })
            .map(|obs| obs.id.clone())
            .collect();

        let results = Self::analyse_slides(
            &slide_observations,
            &already_analysed,
            "fixture",
            analyse_fn,
        );

        // Store supplemental records
        for result in &results {
            let record = Self::build_supplemental(result);
            let _ = supplemental.add(record, lake);
        }

        results
    }

    /// Convert a `SlideAnalysisResult` into a `WriteRecord` suitable for
    /// pushing to a SaaS write-back adapter (e.g. Notion).
    pub fn to_write_record(
        result: &SlideAnalysisResult,
        external_id: Option<String>,
    ) -> WriteRecord {
        let profile = &result.profile;
        let title = profile.name.clone();
        let entity_id = profile
            .email
            .as_deref()
            .or(profile.generated_email.as_deref())
            .unwrap_or(result.person_entity.as_str())
            .to_string();

        let payload = serde_json::to_value(profile).unwrap_or_default();

        WriteRecord {
            entity_id,
            title,
            payload,
            external_id,
        }
    }

    /// Ingest a new observation into the lake for a slide analysis event.
    /// This creates a lake observation recording that an analysis was performed.
    pub fn create_analysis_observation(
        result: &SlideAnalysisResult,
    ) -> crate::adapter::traits::ObservationDraft {
        use crate::adapter::traits::ObservationDraft;
        use crate::domain::{
            AuthorityModel, CaptureModel, IdempotencyKey, ObserverRef, SemVer,
            SourceSystemRef,
        };

        let idem_key = IdempotencyKey::new(format!(
            "slide-analysis:{}:{}:{}",
            result.presentation_id,
            result.source_observation_id,
            result.slide_object_id.as_deref().unwrap_or("presentation")
        ));

        ObservationDraft {
            schema: SchemaRef::new("schema:slide-analysis-result"),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:slide-analysis-projector"),
            source_system: Some(SourceSystemRef::new("sys:lethe-internal")),
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: result.person_entity.clone(),
            target: Some(EntityRef::new(format!(
                "document:gslides:{}{}",
                result.presentation_id,
                result
                    .slide_object_id
                    .as_ref()
                    .map(|slide_id| format!("#slide:{slide_id}"))
                    .unwrap_or_default()
            ))),
            payload: serde_json::json!({
                "analysis_kind": SLIDE_ANALYSIS_KIND,
                "source_observation_id": result.source_observation_id.as_str(),
                "presentation_id": result.presentation_id,
                "person_email": result.profile.email.as_ref().or(result.profile.generated_email.as_ref()),
                "person_name": result.profile.name,
                "supplemental_id": result.supplemental_id.as_ref().map(|s| s.as_str().to_string()),
                "analyzed_at": result.analyzed_at,
                "model_version": result.model_version,
                "slide_object_id": result.slide_object_id,
                "thumbnail_blob_ref": result.thumbnail_blob_ref.as_ref().map(|blob| blob.as_str().to_string()),
                "thumbnail_url": result.profile.thumbnail_url,
            }),
            attachments: vec![],
            published: result.analyzed_at,
            idempotency_key: idem_key,
            meta: serde_json::json!({
                "projector_version": "1.0.0",
            }),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::adapter::gslides::mapper::WORKSPACE_SNAPSHOT_SCHEMA;
    use crate::domain::*;
    use crate::lake::LakeStore;
    use crate::supplemental::SupplementalStore;

    fn make_slide_observation(presentation_id: &str) -> Observation {
        Observation {
            id: Observation::new_id(),
            schema: SchemaRef::new(WORKSPACE_SNAPSHOT_SCHEMA),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:gslides-crawler"),
            source_system: Some(SourceSystemRef::new("sys:google-slides")),
            actor: None,
            authority_model: AuthorityModel::SourceAuthoritative,
            capture_model: CaptureModel::Snapshot,
            subject: EntityRef::new(format!("document:gslides:{presentation_id}")),
            target: None,
            payload: serde_json::json!({
                "title": "Student Self-Introduction",
                "artifact": {
                    "provider": "google",
                    "service": "slides",
                    "objectType": "presentation",
                    "sourceObjectId": presentation_id,
                },
                "revision": {
                    "sourceRevisionId": "rev001",
                    "captureMode": "snapshot",
                },
                "native": { "encoding": "inline-json" },
                "relations": {
                    "owner": "alice@hlab.college",
                    "editors": ["alice@hlab.college"],
                    "viewers": [],
                },
            }),
            attachments: vec![],
            published: Utc::now(),
            recorded_at: Utc::now(),
            consent: None,
            idempotency_key: IdempotencyKey::new(format!(
                "gslides:{presentation_id}:rev:rev001"
            )),
            meta: serde_json::json!({}),
        }
    }

    fn fixture_analyse(_obs: &Observation) -> Option<StudentProfile> {
        Some(StudentProfile {
            email: Some("alice@hlab.college".into()),
            generated_email: None,
            name: "Alice Johnson".into(),
            bio_text: Some("I love programming".into()),
            profile_pic: None,
            gallery_images: vec![],
            properties: super::super::types::StudentProperties {
                nickname: Some("Alice".into()),
                mbti: Some("INTJ".into()),
                ..Default::default()
            },
            attributes: vec!["CS".into()],
            source_slide_object_id: None,
            source_document_id: None,
            source_canonical_uri: None,
            thumbnail_blob_ref: None,
            thumbnail_url: None,
            companion_to_slide_object_id: None,
        })
    }

    #[test]
    fn process_produces_supplemental_and_results() {
        let mut lake = LakeStore::new();
        let obs = make_slide_observation("pres123");
        let _ = lake.append(obs);

        let mut supplemental = SupplementalStore::new();
        let results =
            SlideAnalysisProjector::process_new_slides(&lake, &mut supplemental, fixture_analyse);

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].presentation_id, "pres123");
        assert_eq!(results[0].profile.name, "Alice Johnson");
        assert!(results[0].supplemental_id.is_some());

        // Supplemental store should have one record
        assert_eq!(supplemental.len(), 1);
    }

    #[test]
    fn process_skips_already_analysed() {
        let mut lake = LakeStore::new();
        let obs = make_slide_observation("pres123");
        let _ = lake.append(obs);

        let mut supplemental = SupplementalStore::new();

        // First run
        let r1 =
            SlideAnalysisProjector::process_new_slides(&lake, &mut supplemental, fixture_analyse);
        assert_eq!(r1.len(), 1);

        // Second run — should skip
        let r2 =
            SlideAnalysisProjector::process_new_slides(&lake, &mut supplemental, fixture_analyse);
        assert_eq!(r2.len(), 0);
    }

    #[test]
    fn to_write_record_uses_email() {
        let result = SlideAnalysisResult {
            source_observation_id: ObservationId::new("obs-1"),
            presentation_id: "pres123".into(),
            profile: StudentProfile {
                email: Some("alice@hlab.college".into()),
                generated_email: None,
                name: "Alice".into(),
                bio_text: None,
                profile_pic: None,
                gallery_images: vec![],
                properties: Default::default(),
                attributes: vec![],
                source_slide_object_id: None,
                source_document_id: None,
                source_canonical_uri: None,
                thumbnail_blob_ref: None,
                thumbnail_url: None,
                companion_to_slide_object_id: None,
            },
            person_entity: EntityRef::new("person:alice@hlab.college"),
            supplemental_id: Some(SupplementalId::new("sup:1")),
            analyzed_at: Utc::now(),
            model_version: Some("fixture".into()),
            slide_object_id: Some("slide-1".into()),
            thumbnail_blob_ref: None,
        };
        let wr = SlideAnalysisProjector::to_write_record(&result, None);
        assert_eq!(wr.entity_id, "alice@hlab.college");
        assert_eq!(wr.title, "Alice");
        assert!(wr.external_id.is_none());
    }

    #[test]
    fn create_analysis_observation_has_correct_schema() {
        let result = SlideAnalysisResult {
            source_observation_id: ObservationId::new("obs-1"),
            presentation_id: "pres123".into(),
            profile: StudentProfile {
                email: Some("alice@hlab.college".into()),
                generated_email: None,
                name: "Alice".into(),
                bio_text: None,
                profile_pic: None,
                gallery_images: vec![],
                properties: Default::default(),
                attributes: vec![],
                source_slide_object_id: None,
                source_document_id: None,
                source_canonical_uri: None,
                thumbnail_blob_ref: None,
                thumbnail_url: None,
                companion_to_slide_object_id: None,
            },
            person_entity: EntityRef::new("person:alice@hlab.college"),
            supplemental_id: None,
            analyzed_at: Utc::now(),
            model_version: Some("fixture".into()),
            slide_object_id: Some("slide-1".into()),
            thumbnail_blob_ref: None,
        };
        let draft = SlideAnalysisProjector::create_analysis_observation(&result);
        assert_eq!(draft.schema.as_str(), "schema:slide-analysis-result");
        assert_eq!(draft.subject.as_str(), "person:alice@hlab.college");
    }

    #[test]
    fn replay_produces_same_results() {
        let mut lake = LakeStore::new();
        let obs = make_slide_observation("pres123");
        let _ = lake.append(obs);

        let mut sup1 = SupplementalStore::new();
        let r1 =
            SlideAnalysisProjector::process_new_slides(&lake, &mut sup1, fixture_analyse);

        let mut sup2 = SupplementalStore::new();
        let r2 =
            SlideAnalysisProjector::process_new_slides(&lake, &mut sup2, fixture_analyse);

        assert_eq!(r1.len(), r2.len());
        assert_eq!(r1[0].profile.name, r2[0].profile.name);
        assert_eq!(r1[0].presentation_id, r2[0].presentation_id);
    }
}
