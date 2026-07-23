use lethe_storage_api::{ObservationStats, ObservationStore, StorageResult, StoredObservation};

/// Narrow paged canonical source required by Corpus index catch-up and rebuild.
///
/// Implementations may lock storage independently for each call, so a full
/// generation build never requires holding the application's storage mutex.
pub trait CorpusIndexSource: Send {
    fn observation_stats(&self) -> StorageResult<ObservationStats>;

    fn observation_page(
        &self,
        after_append_seq: u64,
        limit: usize,
    ) -> StorageResult<Vec<StoredObservation>>;

    fn observations_for_privacy_key(
        &self,
        privacy_key: &str,
    ) -> StorageResult<Vec<StoredObservation>>;
}

impl<T> CorpusIndexSource for T
where
    T: ObservationStore + ?Sized,
{
    fn observation_stats(&self) -> StorageResult<ObservationStats> {
        ObservationStore::observation_stats(self)
    }

    fn observation_page(
        &self,
        after_append_seq: u64,
        limit: usize,
    ) -> StorageResult<Vec<StoredObservation>> {
        ObservationStore::observation_page(self, after_append_seq, limit)
    }

    fn observations_for_privacy_key(
        &self,
        privacy_key: &str,
    ) -> StorageResult<Vec<StoredObservation>> {
        ObservationStore::observations_for_privacy_key(self, privacy_key)
    }
}
