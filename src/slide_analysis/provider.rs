use crate::adapter::error::AdapterError;
use crate::domain::{Observation, ObservationId};
use crate::lake::BlobStore;

use super::types::StudentProfile;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DerivationLineage {
    pub source_observation: ObservationId,
    pub provider: String,
    pub model: String,
    pub version: String,
    pub confidence: f32,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct DerivedStudentProfile {
    pub profile: StudentProfile,
    pub lineage: DerivationLineage,
}

pub trait DerivationProvider {
    fn provider_name(&self) -> &str;
    fn model_name(&self) -> &str;
    fn provider_version(&self) -> &str;

    fn derive_student_profile(
        &self,
        observation: &Observation,
        blobs: &BlobStore,
    ) -> Result<Option<DerivedStudentProfile>, AdapterError>;
}
