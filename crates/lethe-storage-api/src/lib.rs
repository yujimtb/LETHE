pub use lethe_core::domain;

pub trait ObservationStore {
    fn observations(&self) -> &[domain::Observation];
}

pub trait BlobStore {
    fn put_blob(&mut self, data: &[u8]) -> domain::BlobRef;
    fn get_blob(&self, blob_ref: &domain::BlobRef) -> Option<&[u8]>;
}

pub trait SupplementalStore {
    fn supplemental_records(&self) -> Vec<&domain::SupplementalRecord>;
}

pub trait ProjectionMaterializer {
    fn materialize_projection(
        &mut self,
        projection: &domain::ProjectionRef,
        records: serde_json::Value,
    ) -> Result<(), String>;
}
