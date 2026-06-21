use std::collections::HashMap;

use lethe_core::domain::{BlobRef, Observation, ProjectionRef, SupplementalRecord};

pub trait ObservationStore {
    fn observations(&self) -> &[Observation];
}

pub trait BlobStorePort {
    fn put_blob(&mut self, data: &[u8]) -> BlobRef;
    fn get_blob(&self, blob_ref: &BlobRef) -> Option<&[u8]>;
}

pub trait SupplementalStorePort {
    fn supplemental_records(&self) -> Vec<&SupplementalRecord>;
}

pub trait ProjectionMaterializer {
    fn materialize_projection(
        &mut self,
        projection: &ProjectionRef,
        records: serde_json::Value,
    ) -> Result<(), String>;

    fn projection_records(&self, projection: &ProjectionRef) -> Option<&serde_json::Value>;
}

#[derive(Debug, Default)]
pub struct InMemoryProjectionMaterializer {
    records: HashMap<ProjectionRef, serde_json::Value>,
}

impl InMemoryProjectionMaterializer {
    pub fn new() -> Self {
        Self::default()
    }
}

impl ProjectionMaterializer for InMemoryProjectionMaterializer {
    fn materialize_projection(
        &mut self,
        projection: &ProjectionRef,
        records: serde_json::Value,
    ) -> Result<(), String> {
        self.records.insert(projection.clone(), records);
        Ok(())
    }

    fn projection_records(&self, projection: &ProjectionRef) -> Option<&serde_json::Value> {
        self.records.get(projection)
    }
}

pub mod conformance {
    use super::*;

    pub fn blob_store_round_trip<T: BlobStorePort>(store: &mut T) {
        let blob = store.put_blob(b"conformance");
        assert_eq!(store.get_blob(&blob), Some(b"conformance".as_slice()));
    }

    pub fn materializer_round_trip<T: ProjectionMaterializer>(materializer: &mut T) {
        let projection = ProjectionRef::new("proj:conformance");
        let records = serde_json::json!({"records": [1, 2, 3]});
        materializer
            .materialize_projection(&projection, records.clone())
            .unwrap();
        assert_eq!(materializer.projection_records(&projection), Some(&records));
    }
}
