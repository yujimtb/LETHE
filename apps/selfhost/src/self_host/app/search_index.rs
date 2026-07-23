use std::sync::{Arc, Mutex, Weak};
use std::thread;
use std::time::Duration;

use lethe_api::api::health::DependencyHealthInfo;
use lethe_projection_corpus::CorpusProjector;
use lethe_search_index::{CorpusIndexSource, IndexError, IndexRoot, PersistentCorpusIndex};
use lethe_storage_api::{
    ObservationStats, StorageError, StoragePorts, StorageResult, StoredObservation,
};

use super::SelfHostError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SearchIndexState {
    Opening,
    CatchingUp,
    Rebuilding,
    Ready,
    Failed,
}

impl SearchIndexState {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::Opening => "opening",
            Self::CatchingUp => "catching_up",
            Self::Rebuilding => "rebuilding",
            Self::Ready => "ready",
            Self::Failed => "failed",
        }
    }

    fn unavailable_code(self) -> &'static str {
        match self {
            Self::Failed => "search_index_failed",
            Self::Opening | Self::CatchingUp | Self::Rebuilding | Self::Ready => {
                "search_index_rebuilding"
            }
        }
    }
}

struct ManagerState {
    lifecycle: SearchIndexState,
    detail: Option<String>,
    index: Option<ManagedIndex>,
    retired_generations: Vec<RetiredGeneration>,
    cleanup_worker_active: bool,
    cleanup_error: Option<String>,
    epoch: u64,
    rebuild_worker_active: bool,
    rebuild_started: u64,
}

struct ManagedIndex {
    generation: String,
    index: Arc<PersistentCorpusIndex>,
}

struct RetiredGeneration {
    generation: String,
    index: Weak<PersistentCorpusIndex>,
}

impl ManagerState {
    fn opening() -> Self {
        Self {
            lifecycle: SearchIndexState::Opening,
            detail: None,
            index: None,
            retired_generations: Vec::new(),
            cleanup_worker_active: false,
            cleanup_error: None,
            epoch: 0,
            rebuild_worker_active: false,
            rebuild_started: 0,
        }
    }
}

#[derive(Clone)]
pub(super) struct SearchIndexManager {
    root: IndexRoot,
    projector: CorpusProjector,
    page_size: usize,
    persistence: Arc<Mutex<Box<dyn StoragePorts>>>,
    state: Arc<Mutex<ManagerState>>,
}

#[derive(Clone)]
struct LockedCorpusIndexSource {
    persistence: Arc<Mutex<Box<dyn StoragePorts>>>,
}

impl LockedCorpusIndexSource {
    fn lock(&self) -> StorageResult<std::sync::MutexGuard<'_, Box<dyn StoragePorts>>> {
        self.persistence.lock().map_err(|_| {
            StorageError::Backend("search index source storage lock is poisoned".to_owned())
        })
    }
}

impl CorpusIndexSource for LockedCorpusIndexSource {
    fn observation_stats(&self) -> StorageResult<ObservationStats> {
        self.lock()?.observation_stats()
    }

    fn observation_page(
        &self,
        after_append_seq: u64,
        limit: usize,
    ) -> StorageResult<Vec<StoredObservation>> {
        self.lock()?.observation_page(after_append_seq, limit)
    }

    fn observations_for_privacy_key(
        &self,
        privacy_key: &str,
    ) -> StorageResult<Vec<StoredObservation>> {
        self.lock()?.observations_for_privacy_key(privacy_key)
    }
}

impl SearchIndexManager {
    pub(super) fn bootstrap(
        root: IndexRoot,
        projector: CorpusProjector,
        page_size: usize,
        persistence: Arc<Mutex<Box<dyn StoragePorts>>>,
    ) -> Self {
        let manager = Self {
            root,
            projector,
            page_size,
            persistence,
            state: Arc::new(Mutex::new(ManagerState::opening())),
        };

        match manager.root.open_current() {
            Ok(opened) => {
                let generation = opened.generation.clone();
                manager.set_transition(
                    SearchIndexState::CatchingUp,
                    None,
                    Some(ManagedIndex {
                        generation: opened.generation,
                        index: Arc::new(opened.index),
                    }),
                );
                match manager.catch_up_ready_index() {
                    Ok(()) => {
                        let cleanup_detail = manager
                            .root
                            .cleanup_obsolete_generations(&generation)
                            .err()
                            .map(generation_cleanup_detail);
                        manager.set_ready_detail(cleanup_detail);
                    }
                    Err(error) => match error {
                        SelfHostError::SearchIndex(index_error)
                            if index_error.requires_rebuild() =>
                        {
                            manager.start_background_rebuild(index_error.to_string());
                        }
                        other => manager.set_transition(
                            SearchIndexState::Failed,
                            Some(other.to_string()),
                            None,
                        ),
                    },
                }
            }
            Err(IndexError::MissingCurrentGeneration) => {
                manager.start_background_rebuild("initial search index build".to_owned());
            }
            Err(error) => {
                manager.start_background_rebuild(format!(
                    "search index validation failed during bootstrap: {error}"
                ));
            }
        }
        manager
    }

    pub(super) fn execute<T>(
        &self,
        operation: impl FnOnce(&PersistentCorpusIndex) -> Result<T, IndexError>,
    ) -> Result<T, SelfHostError> {
        let (index, epoch) = self.ready_handle()?;
        match operation(&index) {
            Ok(value) => {
                let state = self.state_lock()?;
                if state.lifecycle != SearchIndexState::Ready
                    || state.epoch != epoch
                    || !state
                        .index
                        .as_ref()
                        .is_some_and(|current| Arc::ptr_eq(&current.index, &index))
                {
                    return Err(SelfHostError::SearchIndexUnavailable {
                        code: state.lifecycle.unavailable_code(),
                        detail: state.detail.clone().unwrap_or_else(|| {
                            "search index changed while the request was executing".to_owned()
                        }),
                    });
                }
                Ok(value)
            }
            Err(IndexError::Grep(error)) => Err(SelfHostError::ReadMode(error.to_string())),
            Err(IndexError::InvalidReadRequest(detail)) => Err(SelfHostError::ReadMode(detail)),
            Err(error) if error.requires_rebuild() => {
                let detail = error.to_string();
                self.claim_rebuild_if_current(&index, epoch, detail.clone())?;
                let code = match self.spawn_rebuild_worker() {
                    Ok(()) => "search_index_rebuilding",
                    Err(_) => "search_index_failed",
                };
                Err(SelfHostError::SearchIndexUnavailable { code, detail })
            }
            Err(error) => Err(SelfHostError::SearchIndex(error)),
        }
    }

    pub(super) fn catch_up_after_append(&self) -> Result<(), SelfHostError> {
        match self.catch_up_ready_index() {
            Ok(()) => Ok(()),
            Err(SelfHostError::SearchIndex(error)) if error.requires_rebuild() => {
                let detail = error.to_string();
                self.start_background_rebuild(detail.clone());
                Err(SelfHostError::SearchIndexUnavailable {
                    code: "search_index_rebuilding",
                    detail,
                })
            }
            Err(error @ SelfHostError::SearchIndexUnavailable { .. }) => Err(error),
            Err(error) => {
                let detail = format!(
                    "search index catch-up failed after canonical storage changed: {error}"
                );
                self.fail_closed(detail.clone());
                Err(SelfHostError::SearchIndexUnavailable {
                    code: "search_index_failed",
                    detail,
                })
            }
        }
    }

    pub(super) fn health_dependency(&self) -> DependencyHealthInfo {
        match self.state.lock() {
            Ok(state) => DependencyHealthInfo {
                name: "corpus_search_index".to_owned(),
                status: if state.lifecycle == SearchIndexState::Ready {
                    "ok".to_owned()
                } else {
                    state.lifecycle.as_str().to_owned()
                },
                detail: state.detail.clone(),
            },
            Err(_) => DependencyHealthInfo {
                name: "corpus_search_index".to_owned(),
                status: "failed".to_owned(),
                detail: Some("search index state lock is poisoned".to_owned()),
            },
        }
    }

    pub(super) fn deep_health_dependency(&self) -> DependencyHealthInfo {
        match self.execute(|index| index.validate()) {
            Ok(()) => self.health_dependency(),
            Err(_error) => self.health_dependency(),
        }
    }

    fn catch_up_ready_index(&self) -> Result<(), SelfHostError> {
        let (index, epoch, ready_detail) = {
            let mut state = self.state.lock().map_err(|_| SelfHostError::LockPoisoned)?;
            let ready_detail = match state.lifecycle {
                SearchIndexState::Ready => {
                    let ready_detail = state.detail.take();
                    let Some(next_epoch) = state.epoch.checked_add(1) else {
                        let detail =
                            "search index lifecycle epoch overflowed before catch-up".to_owned();
                        state.lifecycle = SearchIndexState::Failed;
                        state.detail = Some(detail.clone());
                        retire_current_index(&mut state);
                        return Err(SelfHostError::SearchIndexUnavailable {
                            code: "search_index_failed",
                            detail,
                        });
                    };
                    state.epoch = next_epoch;
                    state.lifecycle = SearchIndexState::CatchingUp;
                    state.detail =
                        Some("search index is applying the canonical observation delta".to_owned());
                    ready_detail
                }
                // Bootstrap installs the opened handle in CatchingUp before entering here.
                SearchIndexState::CatchingUp if state.epoch == 0 => state.detail.take(),
                _ => {
                    return Err(SelfHostError::SearchIndexUnavailable {
                        code: state.lifecycle.unavailable_code(),
                        detail: state.detail.clone().unwrap_or_else(|| {
                            format!("search index is {}", state.lifecycle.as_str())
                        }),
                    });
                }
            };
            let index = state
                .index
                .as_ref()
                .map(|current| Arc::clone(&current.index))
                .ok_or_else(|| SelfHostError::SearchIndexUnavailable {
                    code: state.lifecycle.unavailable_code(),
                    detail: state
                        .detail
                        .clone()
                        .unwrap_or_else(|| "search index handle is unavailable".to_owned()),
                })?;
            (index, state.epoch, ready_detail)
        };
        let source = LockedCorpusIndexSource {
            persistence: Arc::clone(&self.persistence),
        };
        index.catch_up(&source, &self.projector, self.page_size)?;
        let mut state = self.state_lock()?;
        if state.epoch != epoch
            || !state
                .index
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(&current.index, &index))
            || matches!(
                state.lifecycle,
                SearchIndexState::Rebuilding | SearchIndexState::Failed
            )
        {
            return Err(SelfHostError::SearchIndexUnavailable {
                code: state.lifecycle.unavailable_code(),
                detail: state.detail.clone().unwrap_or_else(|| {
                    "search index changed while catch-up was executing".to_owned()
                }),
            });
        }
        state.lifecycle = SearchIndexState::Ready;
        state.detail = ready_detail;
        Ok(())
    }

    fn start_background_rebuild(&self, reason: String) {
        if !self.claim_rebuild(reason) {
            return;
        }
        let _ = self.spawn_rebuild_worker();
    }

    fn spawn_rebuild_worker(&self) -> Result<(), ()> {
        let manager = self.clone();
        if let Err(error) = thread::Builder::new()
            .name("lethe-search-index-rebuild".to_owned())
            .spawn(move || manager.run_rebuild())
        {
            self.finish_rebuild(
                SearchIndexState::Failed,
                Some(format!(
                    "failed to spawn search index rebuild thread: {error}"
                )),
                None,
            );
            return Err(());
        }
        Ok(())
    }

    fn claim_rebuild_if_current(
        &self,
        expected_index: &Arc<PersistentCorpusIndex>,
        expected_epoch: u64,
        reason: String,
    ) -> Result<(), SelfHostError> {
        let mut state = self.state_lock()?;
        if state.lifecycle != SearchIndexState::Ready
            || state.epoch != expected_epoch
            || !state
                .index
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(&current.index, expected_index))
        {
            return Err(SelfHostError::SearchIndexUnavailable {
                code: state.lifecycle.unavailable_code(),
                detail: state.detail.clone().unwrap_or_else(|| {
                    "search index changed before a stale corruption error was reported".to_owned()
                }),
            });
        }
        if state.rebuild_worker_active {
            return Err(SelfHostError::SearchIndexUnavailable {
                code: state.lifecycle.unavailable_code(),
                detail: "search index rebuild is already active".to_owned(),
            });
        }
        let Some(next_epoch) = state.epoch.checked_add(1) else {
            state.lifecycle = SearchIndexState::Failed;
            state.detail = Some("search index lifecycle epoch overflowed".to_owned());
            retire_current_index(&mut state);
            return Err(SelfHostError::SearchIndexUnavailable {
                code: "search_index_failed",
                detail: "search index lifecycle epoch overflowed".to_owned(),
            });
        };
        let Some(next_rebuild_started) = state.rebuild_started.checked_add(1) else {
            state.lifecycle = SearchIndexState::Failed;
            state.detail = Some("search index rebuild counter overflowed".to_owned());
            retire_current_index(&mut state);
            return Err(SelfHostError::SearchIndexUnavailable {
                code: "search_index_failed",
                detail: "search index rebuild counter overflowed".to_owned(),
            });
        };
        state.lifecycle = SearchIndexState::Rebuilding;
        state.detail = Some(reason);
        retire_current_index(&mut state);
        state.epoch = next_epoch;
        state.rebuild_worker_active = true;
        state.rebuild_started = next_rebuild_started;
        Ok(())
    }

    fn claim_rebuild(&self, reason: String) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        if state.rebuild_worker_active {
            return false;
        }
        let Some(next_epoch) = state.epoch.checked_add(1) else {
            state.lifecycle = SearchIndexState::Failed;
            state.detail = Some("search index lifecycle epoch overflowed".to_owned());
            retire_current_index(&mut state);
            return false;
        };
        let Some(next_rebuild_started) = state.rebuild_started.checked_add(1) else {
            state.lifecycle = SearchIndexState::Failed;
            state.detail = Some("search index rebuild counter overflowed".to_owned());
            retire_current_index(&mut state);
            return false;
        };
        state.lifecycle = SearchIndexState::Rebuilding;
        state.detail = Some(reason);
        retire_current_index(&mut state);
        state.epoch = next_epoch;
        state.rebuild_worker_active = true;
        state.rebuild_started = next_rebuild_started;
        true
    }

    fn run_rebuild(&self) {
        let outcome = (|| {
            let source = LockedCorpusIndexSource {
                persistence: Arc::clone(&self.persistence),
            };
            let (generation, index) =
                self.root
                    .rebuild_from_store(&source, &self.projector, self.page_size)?;

            loop {
                index.catch_up(&source, &self.projector, self.page_size)?;
                index.validate()?;

                // Canonical writes cannot cross this final comparison/publication boundary.
                // If a write landed after catch-up but before this lock, loop and consume it.
                let persistence = self
                    .persistence
                    .lock()
                    .map_err(|_| SelfHostError::LockPoisoned)?;
                let stats = persistence.observation_stats()?;
                let metadata = index.metadata()?;
                if metadata.last_append_seq != stats.max_append_seq
                    || metadata.observation_count != stats.count
                {
                    drop(persistence);
                    continue;
                }
                let opened = self.root.publish(&generation, index)?;
                let metadata = opened.index.metadata()?;
                if metadata.last_append_seq != stats.max_append_seq
                    || metadata.observation_count != stats.count
                {
                    return Err(SelfHostError::SearchIndex(
                        IndexError::IncompatibleMetadata(format!(
                            "published index boundary ({}, {}) does not match canonical storage ({}, {})",
                            metadata.last_append_seq,
                            metadata.observation_count,
                            stats.max_append_seq,
                            stats.count,
                        )),
                    ));
                }
                self.finish_rebuild(
                    SearchIndexState::Ready,
                    None,
                    Some(ManagedIndex {
                        generation: opened.generation,
                        index: Arc::new(opened.index),
                    }),
                );
                break;
            }
            Ok::<_, SelfHostError>(())
        })();

        match outcome {
            Ok(()) => {}
            Err(error) => {
                self.finish_rebuild(SearchIndexState::Failed, Some(error.to_string()), None);
            }
        }
    }

    fn finish_rebuild(
        &self,
        lifecycle: SearchIndexState,
        detail: Option<String>,
        index: Option<ManagedIndex>,
    ) {
        let cleanup_claimed = if let Ok(mut state) = self.state.lock() {
            retire_current_index(&mut state);
            state.lifecycle = lifecycle;
            state.detail = detail.or_else(|| {
                (lifecycle == SearchIndexState::Ready)
                    .then(|| state.cleanup_error.clone())
                    .flatten()
            });
            state.index = index;
            state.rebuild_worker_active = false;
            if lifecycle == SearchIndexState::Ready && !state.cleanup_worker_active {
                state.cleanup_worker_active = true;
                true
            } else {
                false
            }
        } else {
            false
        };
        if cleanup_claimed {
            self.spawn_retired_generation_cleanup();
        }
    }

    fn fail_closed(&self, detail: String) {
        if let Ok(mut state) = self.state.lock() {
            let next_epoch = state.epoch.checked_add(1);
            state.lifecycle = SearchIndexState::Failed;
            state.detail = Some(match next_epoch {
                Some(_) => detail,
                None => "search index lifecycle epoch overflowed while failing closed".to_owned(),
            });
            retire_current_index(&mut state);
            if let Some(next_epoch) = next_epoch {
                state.epoch = next_epoch;
            }
        }
    }

    fn set_transition(
        &self,
        lifecycle: SearchIndexState,
        detail: Option<String>,
        index: Option<ManagedIndex>,
    ) {
        if let Ok(mut state) = self.state.lock() {
            retire_current_index(&mut state);
            state.lifecycle = lifecycle;
            state.detail = detail;
            state.index = index;
        }
    }

    fn set_ready_detail(&self, detail: Option<String>) {
        if let Ok(mut state) = self.state.lock()
            && state.lifecycle == SearchIndexState::Ready
        {
            if let Some(detail) = detail {
                state.cleanup_error = Some(detail.clone());
                state.detail = Some(detail);
            } else {
                state.detail = state.cleanup_error.clone();
            }
        }
    }

    fn spawn_retired_generation_cleanup(&self) {
        let root = self.root.clone();
        let state = Arc::downgrade(&self.state);
        if let Err(error) = thread::Builder::new()
            .name("lethe-search-index-cleanup".to_owned())
            .spawn(move || Self::run_retired_generation_cleanup(root, state))
            && let Ok(mut state) = self.state.lock()
        {
            state.cleanup_worker_active = false;
            let detail = format!(
                "search index is usable, but retired generation cleanup worker failed to spawn: {error}"
            );
            state.cleanup_error = Some(detail.clone());
            if state.lifecycle == SearchIndexState::Ready {
                state.detail = Some(detail);
            }
        }
    }

    fn run_retired_generation_cleanup(root: IndexRoot, state_ref: Weak<Mutex<ManagerState>>) {
        loop {
            let ready = {
                let Some(state) = state_ref.upgrade() else {
                    return;
                };
                let Ok(mut state) = state.lock() else {
                    return;
                };
                let mut ready = Vec::new();
                let mut pending = Vec::new();
                for retired in state.retired_generations.drain(..) {
                    if retired.index.strong_count() == 0 {
                        ready.push(retired);
                    } else {
                        pending.push(retired);
                    }
                }
                state.retired_generations = pending;
                if ready.is_empty() && state.retired_generations.is_empty() {
                    if state.lifecycle == SearchIndexState::Ready {
                        let cleanup_result = state
                            .index
                            .as_ref()
                            .map(|current| root.cleanup_obsolete_generations(&current.generation))
                            .unwrap_or_else(|| {
                                Err(IndexError::GenerationCleanup(
                                    "ready state has no generation during cleanup".to_owned(),
                                ))
                            });
                        if let Err(error) = cleanup_result {
                            let detail = generation_cleanup_detail(error);
                            state.cleanup_error = Some(detail.clone());
                            state.detail = Some(detail);
                        }
                    }
                    state.cleanup_worker_active = false;
                    return;
                }
                ready
            };

            if ready.is_empty() {
                thread::sleep(Duration::from_millis(10));
                continue;
            }
            let mut postponed = false;
            for retired in ready {
                match root.cleanup_retired_generation(&retired.generation) {
                    Ok(true) => {}
                    Ok(false) => {
                        postponed = true;
                        if let Some(state) = state_ref.upgrade()
                            && let Ok(mut state) = state.lock()
                        {
                            state.retired_generations.push(retired);
                        }
                    }
                    Err(error) => {
                        let detail = generation_cleanup_detail(error);
                        if let Some(state) = state_ref.upgrade()
                            && let Ok(mut state) = state.lock()
                        {
                            state.cleanup_error = Some(detail.clone());
                            if state.lifecycle == SearchIndexState::Ready {
                                state.detail = Some(detail);
                            }
                        }
                    }
                }
            }
            if postponed {
                thread::sleep(Duration::from_millis(10));
            }
        }
    }

    fn state_lock(&self) -> Result<std::sync::MutexGuard<'_, ManagerState>, SelfHostError> {
        self.state.lock().map_err(|_| SelfHostError::LockPoisoned)
    }

    fn ready_handle(&self) -> Result<(Arc<PersistentCorpusIndex>, u64), SelfHostError> {
        let state = self.state_lock()?;
        if state.lifecycle != SearchIndexState::Ready {
            return Err(SelfHostError::SearchIndexUnavailable {
                code: state.lifecycle.unavailable_code(),
                detail: state
                    .detail
                    .clone()
                    .unwrap_or_else(|| format!("search index is {}", state.lifecycle.as_str())),
            });
        }
        let index = state
            .index
            .as_ref()
            .map(|current| Arc::clone(&current.index))
            .ok_or_else(|| SelfHostError::SearchIndexUnavailable {
                code: "search_index_failed",
                detail: "search index state is ready without an index handle".to_owned(),
            })?;
        Ok((index, state.epoch))
    }

    #[cfg(test)]
    pub(super) fn state(&self) -> SearchIndexState {
        self.state.lock().unwrap().lifecycle
    }

    #[cfg(test)]
    pub(super) fn rebuild_started(&self) -> u64 {
        self.state.lock().unwrap().rebuild_started
    }

    #[cfg(test)]
    pub(super) fn cleanup_worker_active(&self) -> bool {
        self.state.lock().unwrap().cleanup_worker_active
    }
}

fn retire_current_index(state: &mut ManagerState) {
    if let Some(current) = state.index.take() {
        state.retired_generations.push(RetiredGeneration {
            generation: current.generation,
            index: Arc::downgrade(&current.index),
        });
    }
}

fn generation_cleanup_detail(error: IndexError) -> String {
    format!("search index is usable, but obsolete generation cleanup failed: {error}")
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::mpsc;
    use std::time::{Duration, Instant};

    use chrono::Utc;
    use lethe_core::domain::{
        AuthorityModel, CaptureModel, EntityRef, IdempotencyKey, Observation, ObserverRef,
        SchemaRef, SemVer, SourceSystemRef,
    };
    use lethe_projection_corpus::CorpusConfig;
    use lethe_search_index::MIN_WRITER_HEAP_BYTES;
    use lethe_storage_sqlite::persistence::SqlitePersistence;

    use super::*;

    type SearchManagerFixture = (
        PathBuf,
        IndexRoot,
        CorpusProjector,
        Arc<Mutex<Box<dyn StoragePorts>>>,
    );

    fn fixture() -> SearchManagerFixture {
        let root = std::env::temp_dir().join(format!(
            "lethe-search-manager-test-{}",
            uuid::Uuid::now_v7()
        ));
        let persistence =
            SqlitePersistence::open(&root.join("lethe.sqlite3"), &root.join("blobs"), &[7; 32])
                .unwrap();
        let corpus_config = CorpusConfig::default();
        let index_root = IndexRoot::new(
            root.join("corpus-index"),
            MIN_WRITER_HEAP_BYTES,
            corpus_config.fingerprint(),
        )
        .unwrap();
        (
            root,
            index_root,
            CorpusProjector::new(corpus_config),
            Arc::new(Mutex::new(Box::new(persistence))),
        )
    }

    fn wait_for_state(manager: &SearchIndexManager, expected: SearchIndexState) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while manager.state() != expected {
            assert!(
                Instant::now() < deadline,
                "search index did not reach {}: {:?}",
                expected.as_str(),
                manager.health_dependency().detail
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn wait_for_cleanup(manager: &SearchIndexManager) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while manager.cleanup_worker_active() {
            assert!(
                Instant::now() < deadline,
                "search index generation cleanup did not finish: {:?}",
                manager.health_dependency().detail
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn slack_observation(key: &str, recorded_days_ago: i64) -> Observation {
        let published = Utc::now() - chrono::Duration::days(recorded_days_ago);
        Observation {
            id: Observation::new_id(),
            schema: SchemaRef::new("schema:slack-message"),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:slack-crawler"),
            source_system: Some(SourceSystemRef::new("sys:slack")),
            actor: None,
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new(format!("message:slack:C01:{key}")),
            target: None,
            payload: serde_json::json!({
                "channel_id": "C01",
                "channel_name": "100_benchmark",
                "is_public_channel": true,
                "is_bot": false,
                "user_id": "U01",
                "user_name": "Benchmark User",
                "text": format!("persistent needle {key}"),
                "ts": key,
                "thread_ts": key,
                "permalink": format!("https://example.test/C01/{key}"),
            }),
            attachments: Vec::new(),
            published,
            recorded_at: published,
            consent: None,
            idempotency_key: IdempotencyKey::new(format!("slack:C01:{key}")),
            meta: serde_json::json!({
                "canonical_json": serde_json::json!({
                    "sender": "U01",
                    "body": format!("persistent needle {key}"),
                    "event_time": key,
                }).to_string(),
                "source_container": "C01",
                "object_id": format!("channel:C01:ts:{key}"),
            }),
        }
    }

    fn matching_ids(manager: &SearchIndexManager) -> Vec<String> {
        manager
            .execute(|index| {
                index.search(
                    &lethe_api::api::grep::GrepRequest {
                        pattern: "needle".to_owned(),
                        limit: Some(20),
                        ..lethe_api::api::grep::GrepRequest::default()
                    },
                    100,
                )
            })
            .unwrap()
            .matches
            .into_iter()
            .map(|record| record.record_id)
            .collect()
    }

    fn corrupt_current_store_segment(index_root: &IndexRoot) {
        let generation = fs::read_to_string(index_root.path().join("CURRENT"))
            .unwrap()
            .trim()
            .to_owned();
        let generation_path = index_root.path().join("generations").join(generation);
        let store_path = fs::read_dir(generation_path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .find(|path| {
                path.extension()
                    .is_some_and(|extension| extension == "store")
            })
            .expect("published Tantivy generation must contain a .store segment");
        let mut bytes = fs::read(&store_path).unwrap();
        assert!(!bytes.is_empty());
        let offset = bytes.len() / 2;
        bytes[offset] ^= 0xff;
        fs::write(store_path, bytes).unwrap();
    }

    #[test]
    fn initial_build_and_valid_reopen_are_ready_without_second_rebuild() {
        let (root, index_root, projector, persistence) = fixture();
        let storage_guard = persistence.lock().unwrap();
        let manager = SearchIndexManager::bootstrap(
            index_root.clone(),
            projector.clone(),
            16,
            Arc::clone(&persistence),
        );
        assert_eq!(manager.state(), SearchIndexState::Rebuilding);
        assert_eq!(manager.rebuild_started(), 1);
        assert!(matches!(
            manager.execute(|index| index.record_count()),
            Err(SelfHostError::SearchIndexUnavailable {
                code: "search_index_rebuilding",
                ..
            })
        ));
        drop(storage_guard);
        wait_for_state(&manager, SearchIndexState::Ready);
        drop(manager);

        let reopened = SearchIndexManager::bootstrap(index_root, projector, 16, persistence);
        assert_eq!(reopened.state(), SearchIndexState::Ready);
        assert_eq!(reopened.rebuild_started(), 0);
        drop(reopened);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn incremental_catch_up_fails_reads_closed_until_the_new_commit_is_visible() {
        let (root, index_root, projector, persistence) = fixture();
        let manager =
            SearchIndexManager::bootstrap(index_root, projector, 16, Arc::clone(&persistence));
        wait_for_state(&manager, SearchIndexState::Ready);
        persistence
            .lock()
            .unwrap()
            .append_observation(&slack_observation("1.500000", 0))
            .unwrap();

        let storage_guard = persistence.lock().unwrap();
        let catching_up = manager.clone();
        let worker = std::thread::spawn(move || catching_up.catch_up_after_append());
        wait_for_state(&manager, SearchIndexState::CatchingUp);
        assert!(matches!(
            manager.execute(|index| index.record_count()),
            Err(SelfHostError::SearchIndexUnavailable {
                code: "search_index_rebuilding",
                ..
            })
        ));

        drop(storage_guard);
        worker.join().unwrap().unwrap();
        assert_eq!(manager.state(), SearchIndexState::Ready);
        assert_eq!(matching_ids(&manager).len(), 1);

        drop(manager);
        drop(persistence);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn corrupt_current_starts_one_background_rebuild_and_never_serves_old_handle() {
        let (root, index_root, projector, persistence) = fixture();
        persistence
            .lock()
            .unwrap()
            .append_observation(&slack_observation("1.000000", 0))
            .unwrap();
        let initial = SearchIndexManager::bootstrap(
            index_root.clone(),
            projector.clone(),
            16,
            Arc::clone(&persistence),
        );
        wait_for_state(&initial, SearchIndexState::Ready);
        let expected_ids = matching_ids(&initial);
        assert_eq!(expected_ids.len(), 1);
        drop(initial);
        fs::write(index_root.path().join("CURRENT"), "not-a-generation\n").unwrap();

        let storage_guard = persistence.lock().unwrap();
        let manager =
            SearchIndexManager::bootstrap(index_root, projector, 16, Arc::clone(&persistence));
        assert_eq!(manager.state(), SearchIndexState::Rebuilding);
        assert!(matches!(
            manager.execute(|index| index.metadata()),
            Err(SelfHostError::SearchIndexUnavailable {
                code: "search_index_rebuilding",
                ..
            })
        ));
        for _ in 0..4 {
            manager.start_background_rebuild("concurrent corruption".to_owned());
        }
        assert_eq!(manager.rebuild_started(), 1);
        drop(storage_guard);

        wait_for_state(&manager, SearchIndexState::Ready);
        assert_eq!(manager.rebuild_started(), 1);
        assert_eq!(matching_ids(&manager), expected_ids);
        drop(manager);
        drop(persistence);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn corrupt_segment_fails_fast_then_background_rebuild_restores_matches() {
        let (root, index_root, projector, persistence) = fixture();
        persistence
            .lock()
            .unwrap()
            .append_observation(&slack_observation("1.250000", 0))
            .unwrap();
        let initial = SearchIndexManager::bootstrap(
            index_root.clone(),
            projector.clone(),
            16,
            Arc::clone(&persistence),
        );
        wait_for_state(&initial, SearchIndexState::Ready);
        let expected_ids = matching_ids(&initial);
        assert_eq!(expected_ids.len(), 1);
        drop(initial);
        corrupt_current_store_segment(&index_root);

        let storage_guard = persistence.lock().unwrap();
        let manager =
            SearchIndexManager::bootstrap(index_root, projector, 16, Arc::clone(&persistence));
        assert_eq!(manager.state(), SearchIndexState::Rebuilding);
        assert!(matches!(
            manager.execute(|index| index.record_count()),
            Err(SelfHostError::SearchIndexUnavailable {
                code: "search_index_rebuilding",
                ..
            })
        ));
        drop(storage_guard);

        wait_for_state(&manager, SearchIndexState::Ready);
        assert_eq!(manager.rebuild_started(), 1);
        assert_eq!(matching_ids(&manager), expected_ids);
        drop(manager);
        drop(persistence);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rebuild_failure_keeps_reads_failed_instead_of_serving_an_empty_result() {
        let (root, index_root, projector, persistence) = fixture();
        let manager =
            SearchIndexManager::bootstrap(index_root, projector, 16, Arc::clone(&persistence));
        wait_for_state(&manager, SearchIndexState::Ready);
        let poisoned = Arc::clone(&persistence);
        let _ = std::thread::spawn(move || {
            let _guard = poisoned.lock().unwrap();
            panic!("poison search-index source for failure-state coverage");
        })
        .join();

        manager.start_background_rebuild("forced rebuild failure".to_owned());
        wait_for_state(&manager, SearchIndexState::Failed);
        assert!(matches!(
            manager.execute(|index| index.record_count()),
            Err(SelfHostError::SearchIndexUnavailable {
                code: "search_index_failed",
                ..
            })
        ));
        assert!(manager.health_dependency().detail.is_some());

        drop(manager);
        drop(persistence);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn result_from_old_epoch_is_discarded_after_invalidation() {
        let (root, index_root, projector, persistence) = fixture();
        let manager =
            SearchIndexManager::bootstrap(index_root, projector, 16, Arc::clone(&persistence));
        wait_for_state(&manager, SearchIndexState::Ready);
        let storage_guard = persistence.lock().unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (continue_tx, continue_rx) = mpsc::channel();
        let request_manager = manager.clone();
        let request = std::thread::spawn(move || {
            request_manager.execute(|_index| {
                started_tx.send(()).unwrap();
                continue_rx.recv().unwrap();
                Ok::<_, IndexError>("old-result")
            })
        });
        started_rx.recv().unwrap();
        manager.start_background_rebuild("runtime corruption".to_owned());
        continue_tx.send(()).unwrap();
        assert!(matches!(
            request.join().unwrap(),
            Err(SelfHostError::SearchIndexUnavailable {
                code: "search_index_rebuilding",
                ..
            })
        ));
        assert_eq!(manager.rebuild_started(), 2);
        drop(storage_guard);
        wait_for_state(&manager, SearchIndexState::Ready);
        wait_for_cleanup(&manager);
        drop(manager);
        drop(persistence);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn retired_generation_is_deleted_only_after_in_flight_reader_drops() {
        let (root, index_root, projector, persistence) = fixture();
        let manager = SearchIndexManager::bootstrap(
            index_root.clone(),
            projector,
            16,
            Arc::clone(&persistence),
        );
        wait_for_state(&manager, SearchIndexState::Ready);
        let old_generation = fs::read_to_string(index_root.path().join("CURRENT"))
            .unwrap()
            .trim()
            .to_owned();
        let old_path = index_root.path().join("generations").join(&old_generation);

        let (started_tx, started_rx) = mpsc::channel();
        let (continue_tx, continue_rx) = mpsc::channel();
        let request_manager = manager.clone();
        let request = std::thread::spawn(move || {
            request_manager.execute(|_index| {
                started_tx.send(()).unwrap();
                continue_rx.recv().unwrap();
                Ok::<_, IndexError>(())
            })
        });
        started_rx.recv().unwrap();

        manager.start_background_rebuild("replace generation while reader is active".to_owned());
        wait_for_state(&manager, SearchIndexState::Ready);
        let new_generation = fs::read_to_string(index_root.path().join("CURRENT"))
            .unwrap()
            .trim()
            .to_owned();
        assert_ne!(new_generation, old_generation);
        assert!(old_path.is_dir());

        continue_tx.send(()).unwrap();
        assert!(matches!(
            request.join().unwrap(),
            Err(SelfHostError::SearchIndexUnavailable { .. })
        ));
        let deadline = Instant::now() + Duration::from_secs(10);
        while old_path.exists() {
            assert!(
                Instant::now() < deadline,
                "retired generation remained after its last reader dropped"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        wait_for_cleanup(&manager);

        drop(manager);
        drop(persistence);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn corruption_error_from_old_epoch_cannot_invalidate_rebuilt_index() {
        let (root, index_root, projector, persistence) = fixture();
        let manager =
            SearchIndexManager::bootstrap(index_root, projector, 16, Arc::clone(&persistence));
        wait_for_state(&manager, SearchIndexState::Ready);
        let (started_tx, started_rx) = mpsc::channel();
        let (continue_tx, continue_rx) = mpsc::channel();
        let request_manager = manager.clone();
        let request = std::thread::spawn(move || {
            request_manager.execute(|_index| {
                started_tx.send(()).unwrap();
                continue_rx.recv().unwrap();
                Err::<(), _>(IndexError::InvalidDocument(
                    "late corruption from obsolete generation".to_owned(),
                ))
            })
        });
        started_rx.recv().unwrap();

        manager.start_background_rebuild("replacement generation".to_owned());
        wait_for_state(&manager, SearchIndexState::Ready);
        let rebuilds_after_recovery = manager.rebuild_started();
        continue_tx.send(()).unwrap();

        assert!(matches!(
            request.join().unwrap(),
            Err(SelfHostError::SearchIndexUnavailable { .. })
        ));
        wait_for_cleanup(&manager);
        assert_eq!(manager.state(), SearchIndexState::Ready);
        assert_eq!(manager.rebuild_started(), rebuilds_after_recovery);
        drop(manager);
        drop(persistence);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn request_validation_error_does_not_trigger_rebuild() {
        let (root, index_root, projector, persistence) = fixture();
        let manager = SearchIndexManager::bootstrap(index_root, projector, 16, persistence);
        wait_for_state(&manager, SearchIndexState::Ready);
        let before = manager.rebuild_started();
        let error = manager
            .execute(|index| {
                index.search(
                    &lethe_api::api::grep::GrepRequest {
                        pattern: "[".to_owned(),
                        ..lethe_api::api::grep::GrepRequest::default()
                    },
                    100,
                )
            })
            .unwrap_err();
        assert!(matches!(error, SelfHostError::ReadMode(_)));
        assert_eq!(manager.state(), SearchIndexState::Ready);
        assert_eq!(manager.rebuild_started(), before);
        drop(manager);
        fs::remove_dir_all(root).unwrap();
    }
}
