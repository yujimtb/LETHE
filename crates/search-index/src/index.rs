use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard};

use chrono::{DateTime, Utc};
use lethe_api::api::grep::{GrepError, GrepRecord, normalize};
use lethe_projection_corpus::{CorpusProjector, CorpusRecord, linked_form_sheet_id};
use lethe_storage_api::ObservationStats;
use tantivy::collector::{Count, TopDocs};
use tantivy::query::TermQuery;
use tantivy::schema::{IndexRecordOption, TantivyDocument, Value};
use tantivy::tokenizer::NgramTokenizer;
use tantivy::{DocAddress, Index, IndexReader, IndexWriter, ReloadPolicy, Term, doc};

use crate::schema::{
    INDEX_FORMAT_VERSION, IndexCommitMetadata, IndexSchema, NGRAM_TOKENIZER, asc_sort_key,
    desc_sort_key,
};
use crate::source::CorpusIndexSource;

const GENERATIONS_DIR: &str = "generations";
const CURRENT_FILE: &str = "CURRENT";
const CURRENT_TMP_FILE: &str = "CURRENT.tmp";
pub const MIN_WRITER_HEAP_BYTES: usize = 15_000_000;

#[derive(Debug, thiserror::Error)]
pub enum IndexError {
    #[error("search index I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("search index engine failed: {0}")]
    Tantivy(#[from] tantivy::TantivyError),
    #[error("search index source storage failed: {0}")]
    Storage(#[from] lethe_storage_api::StorageError),
    #[error("search index metadata JSON failed: {0}")]
    Json(#[from] serde_json::Error),
    #[error("search request failed: {0}")]
    Grep(#[from] GrepError),
    #[error("invalid search index read request: {0}")]
    InvalidReadRequest(String),
    #[error("search index writer heap must be at least {MIN_WRITER_HEAP_BYTES} bytes")]
    WriterHeapTooSmall,
    #[error("search index has no published generation")]
    MissingCurrentGeneration,
    #[error("search index CURRENT is invalid: {0}")]
    InvalidCurrentGeneration(String),
    #[error("search index schema is incompatible")]
    IncompatibleSchema,
    #[error("search index metadata is missing")]
    MissingCommitMetadata,
    #[error("search index metadata is incompatible: {0}")]
    IncompatibleMetadata(String),
    #[error("search index checksum validation failed for: {0:?}")]
    ChecksumMismatch(HashSet<PathBuf>),
    #[error("search index document is invalid: {0}")]
    InvalidDocument(String),
    #[error("search index contains duplicate record_id {0}")]
    DuplicateRecord(String),
    #[error("search index writer lock is poisoned")]
    WriterLockPoisoned,
    #[error("search index obsolete generation cleanup failed: {0}")]
    GenerationCleanup(String),
}

impl IndexError {
    /// Whether the error means the published index can no longer be trusted.
    ///
    /// Request validation and timeout errors never trigger a rebuild. Storage
    /// source failures and invalid writer configuration are likewise external
    /// to the published Tantivy generation.
    pub fn requires_rebuild(&self) -> bool {
        !matches!(
            self,
            Self::Grep(_)
                | Self::InvalidReadRequest(_)
                | Self::Storage(_)
                | Self::WriterHeapTooSmall
                | Self::GenerationCleanup(_)
        )
    }
}

#[derive(Debug)]
pub struct OpenedIndex {
    pub generation: String,
    pub index: PersistentCorpusIndex,
}

#[derive(Debug, Clone)]
pub struct IndexRoot {
    path: PathBuf,
    writer_heap_bytes: usize,
    corpus_config_fingerprint: String,
}

impl IndexRoot {
    pub fn new(
        path: impl Into<PathBuf>,
        writer_heap_bytes: usize,
        corpus_config_fingerprint: impl Into<String>,
    ) -> Result<Self, IndexError> {
        if writer_heap_bytes < MIN_WRITER_HEAP_BYTES {
            return Err(IndexError::WriterHeapTooSmall);
        }
        let corpus_config_fingerprint = corpus_config_fingerprint.into();
        if corpus_config_fingerprint.trim().is_empty() {
            return Err(IndexError::IncompatibleMetadata(
                "corpus config fingerprint must not be blank".to_owned(),
            ));
        }
        Ok(Self {
            path: path.into(),
            writer_heap_bytes,
            corpus_config_fingerprint,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn create_generation(&self) -> Result<(String, PersistentCorpusIndex), IndexError> {
        fs::create_dir_all(self.path.join(GENERATIONS_DIR))?;
        let generation = uuid::Uuid::now_v7().to_string();
        let path = self.generation_path(&generation);
        fs::create_dir(&path)?;
        let index = PersistentCorpusIndex::create(
            path,
            self.writer_heap_bytes,
            self.corpus_config_fingerprint.clone(),
        )?;
        Ok((generation, index))
    }

    pub fn rebuild_from_store(
        &self,
        store: &dyn CorpusIndexSource,
        projector: &CorpusProjector,
        page_size: usize,
    ) -> Result<(String, PersistentCorpusIndex), IndexError> {
        if page_size == 0 {
            return Err(IndexError::IncompatibleMetadata(
                "rebuild page size must be greater than zero".to_owned(),
            ));
        }
        let high_water_stats = store.observation_stats()?;
        let high_water = high_water_stats.max_append_seq;
        let mut linked_sheet_ids = HashSet::new();
        let mut cursor = 0;
        loop {
            let mut page = store.observation_page(cursor, page_size)?;
            validate_observation_page(&page, cursor, page_size)?;
            let keep = page
                .iter()
                .take_while(|stored| stored.append_seq <= high_water)
                .count();
            page.truncate(keep);
            if page.is_empty() {
                break;
            }
            for stored in &page {
                if let Some(sheet_id) = linked_form_sheet_id(&stored.observation) {
                    linked_sheet_ids.insert(sheet_id);
                }
            }
            cursor = page.last().expect("non-empty page").append_seq;
            if cursor >= high_water {
                break;
            }
        }

        let (generation, index) = self.create_generation()?;
        cursor = 0;
        let mut observation_count = 0_u64;
        loop {
            let mut page = store.observation_page(cursor, page_size)?;
            validate_observation_page(&page, cursor, page_size)?;
            let keep = page
                .iter()
                .take_while(|stored| stored.append_seq <= high_water)
                .count();
            page.truncate(keep);
            if page.is_empty() {
                break;
            }
            let records = page
                .iter()
                .flat_map(|stored| {
                    projector.project_observation(&stored.observation, &linked_sheet_ids)
                })
                .collect::<Vec<_>>();
            observation_count = observation_count
                .checked_add(page.len() as u64)
                .ok_or_else(|| {
                    IndexError::IncompatibleMetadata("observation count overflowed u64".to_owned())
                })?;
            cursor = page.last().expect("non-empty page").append_seq;
            index.apply_delta(
                &records,
                &HashSet::new(),
                &linked_sheet_ids,
                cursor,
                observation_count,
                format!("proj:corpus:{cursor}"),
            )?;
            if cursor >= high_water {
                break;
            }
        }
        if observation_count != high_water_stats.count {
            return Err(IndexError::IncompatibleMetadata(format!(
                "rebuilt observation count is {observation_count}, high-water count is {}",
                high_water_stats.count
            )));
        }
        index.catch_up(store, projector, page_size)?;
        index.validate()?;
        Ok((generation, index))
    }

    pub fn open_current(&self) -> Result<OpenedIndex, IndexError> {
        let generation = self.read_current()?;
        let index = PersistentCorpusIndex::open(
            self.generation_path(&generation),
            self.writer_heap_bytes,
            self.corpus_config_fingerprint.clone(),
        )?;
        Ok(OpenedIndex { generation, index })
    }

    pub fn publish(
        &self,
        generation: &str,
        built_index: PersistentCorpusIndex,
    ) -> Result<OpenedIndex, IndexError> {
        validate_generation_name(generation)?;
        let generation_path = self.generation_path(generation);
        if !generation_path.is_dir() {
            return Err(IndexError::InvalidCurrentGeneration(format!(
                "generation directory does not exist: {generation}"
            )));
        }
        if fs::canonicalize(built_index.path())? != fs::canonicalize(&generation_path)? {
            return Err(IndexError::InvalidCurrentGeneration(format!(
                "built index path does not match generation {generation}"
            )));
        }
        built_index.validate()?;
        drop(built_index);
        fs::create_dir_all(&self.path)?;
        let temporary = self.path.join(CURRENT_TMP_FILE);
        let current = self.path.join(CURRENT_FILE);
        {
            let mut file = File::create(&temporary)?;
            writeln!(file, "{generation}")?;
            file.sync_all()?;
        }
        atomic_replace_current(&temporary, &current, &self.path)?;

        let opened = self.open_current()?;
        if opened.generation != generation {
            return Err(IndexError::InvalidCurrentGeneration(format!(
                "published generation changed from {generation} to {}",
                opened.generation
            )));
        }
        opened.index.validate()?;
        let smoke_count = opened.index.record_count()?;
        if smoke_count != opened.index.metadata()?.record_count {
            return Err(IndexError::IncompatibleMetadata(format!(
                "published smoke count is {smoke_count}"
            )));
        }
        Ok(opened)
    }

    /// Removes every generation other than the currently published one.
    ///
    /// This is only safe during process bootstrap, before any obsolete
    /// generation can still have an in-flight reader in this process. Runtime
    /// replacement must use [`Self::cleanup_retired_generation`] instead.
    pub fn cleanup_obsolete_generations(&self, current_generation: &str) -> Result<(), IndexError> {
        validate_generation_name(current_generation).map_err(|error| {
            IndexError::GenerationCleanup(format!(
                "current generation name cannot be validated: {error}"
            ))
        })?;
        let published = self.read_current().map_err(|error| {
            IndexError::GenerationCleanup(format!("cannot re-read CURRENT: {error}"))
        })?;
        if published != current_generation {
            return Err(IndexError::GenerationCleanup(format!(
                "CURRENT changed from {current_generation} to {published} before cleanup"
            )));
        }

        let generations_dir = self.path.join(GENERATIONS_DIR);
        let entries = fs::read_dir(&generations_dir).map_err(|error| {
            IndexError::GenerationCleanup(format!(
                "cannot read {}: {error}",
                generations_dir.display()
            ))
        })?;
        let mut obsolete = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error| {
                IndexError::GenerationCleanup(format!(
                    "cannot enumerate {}: {error}",
                    generations_dir.display()
                ))
            })?;
            let file_type = entry.file_type().map_err(|error| {
                IndexError::GenerationCleanup(format!(
                    "cannot inspect {}: {error}",
                    entry.path().display()
                ))
            })?;
            if !file_type.is_dir() {
                return Err(IndexError::GenerationCleanup(format!(
                    "unexpected non-directory entry {}",
                    entry.path().display()
                )));
            }
            let generation = entry.file_name().into_string().map_err(|_| {
                IndexError::GenerationCleanup(format!(
                    "generation name is not valid UTF-8: {}",
                    entry.path().display()
                ))
            })?;
            validate_generation_name(&generation).map_err(|error| {
                IndexError::GenerationCleanup(format!(
                    "invalid generation directory {}: {error}",
                    entry.path().display()
                ))
            })?;
            if generation != current_generation {
                obsolete.push(entry.path());
            }
        }
        obsolete.sort_unstable();

        for path in obsolete {
            // Re-read the pointer before each destructive step. IndexRoot is a
            // single-process lifecycle primitive; this additionally fails
            // closed if an accidental concurrent publisher changes CURRENT.
            let published = self.read_current().map_err(|error| {
                IndexError::GenerationCleanup(format!("cannot re-read CURRENT: {error}"))
            })?;
            if published != current_generation {
                return Err(IndexError::GenerationCleanup(format!(
                    "CURRENT changed from {current_generation} to {published} during cleanup"
                )));
            }
            fs::remove_dir_all(&path).map_err(|error| {
                IndexError::GenerationCleanup(format!("cannot remove {}: {error}", path.display()))
            })?;
        }
        sync_directory(&generations_dir).map_err(|error| {
            IndexError::GenerationCleanup(format!(
                "cannot sync {}: {error}",
                generations_dir.display()
            ))
        })?;
        Ok(())
    }

    /// Removes one unpublished generation after its last in-process reader has
    /// been dropped by the lifecycle owner.
    ///
    /// The method re-reads `CURRENT` immediately before deletion and returns
    /// `false` while that generation remains published. It deliberately does
    /// not infer reader liveness; the caller owns that proof.
    pub fn cleanup_retired_generation(&self, generation: &str) -> Result<bool, IndexError> {
        validate_generation_name(generation).map_err(|error| {
            IndexError::GenerationCleanup(format!(
                "retired generation name cannot be validated: {error}"
            ))
        })?;
        let published = self.read_current().map_err(|error| {
            IndexError::GenerationCleanup(format!("cannot re-read CURRENT: {error}"))
        })?;
        if published == generation {
            return Ok(false);
        }

        let generations_dir = self.path.join(GENERATIONS_DIR);
        let path = self.generation_path(generation);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(true),
            Err(error) => {
                return Err(IndexError::GenerationCleanup(format!(
                    "cannot inspect {}: {error}",
                    path.display()
                )));
            }
        };
        if !metadata.file_type().is_dir() {
            return Err(IndexError::GenerationCleanup(format!(
                "retired generation is not a directory: {}",
                path.display()
            )));
        }
        fs::remove_dir_all(&path).map_err(|error| {
            IndexError::GenerationCleanup(format!("cannot remove {}: {error}", path.display()))
        })?;
        sync_directory(&generations_dir).map_err(|error| {
            IndexError::GenerationCleanup(format!(
                "cannot sync {}: {error}",
                generations_dir.display()
            ))
        })?;
        Ok(true)
    }

    fn read_current(&self) -> Result<String, IndexError> {
        let path = self.path.join(CURRENT_FILE);
        let raw = match fs::read_to_string(path) {
            Ok(raw) => raw,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound && !self.path.exists() => {
                return Err(IndexError::MissingCurrentGeneration);
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(IndexError::InvalidCurrentGeneration(
                    "CURRENT is missing from an existing index root".to_owned(),
                ));
            }
            Err(error) => return Err(error.into()),
        };
        let generation = raw.trim();
        validate_generation_name(generation)?;
        Ok(generation.to_owned())
    }

    fn generation_path(&self, generation: &str) -> PathBuf {
        self.path.join(GENERATIONS_DIR).join(generation)
    }
}

#[cfg(windows)]
fn atomic_replace_current(source: &Path, target: &Path, _parent: &Path) -> Result<(), IndexError> {
    use std::os::windows::ffi::OsStrExt;

    use windows_sys::Win32::Storage::FileSystem::{
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW,
    };

    let source = source
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let target = target
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let result = unsafe {
        MoveFileExW(
            source.as_ptr(),
            target.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if result == 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

#[cfg(unix)]
fn atomic_replace_current(source: &Path, target: &Path, parent: &Path) -> Result<(), IndexError> {
    fs::rename(source, target)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> std::io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(windows)]
fn sync_directory(_path: &Path) -> std::io::Result<()> {
    // MoveFileExW(MOVEFILE_WRITE_THROUGH) durably publishes CURRENT. Windows
    // does not provide a portable directory fsync surface for deletions.
    Ok(())
}

fn validate_generation_name(generation: &str) -> Result<(), IndexError> {
    uuid::Uuid::parse_str(generation)
        .map(|_| ())
        .map_err(|_| IndexError::InvalidCurrentGeneration(generation.to_owned()))
}

pub struct PersistentCorpusIndex {
    path: PathBuf,
    index: Index,
    reader: IndexReader,
    writer: Mutex<IndexWriter<TantivyDocument>>,
    mutation: Mutex<()>,
    fields: IndexSchema,
    metadata: Mutex<IndexCommitMetadata>,
}

impl std::fmt::Debug for PersistentCorpusIndex {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PersistentCorpusIndex")
            .field("path", &self.path)
            .field("fields", &self.fields)
            .finish_non_exhaustive()
    }
}

impl PersistentCorpusIndex {
    pub fn create(
        path: impl Into<PathBuf>,
        writer_heap_bytes: usize,
        corpus_config_fingerprint: String,
    ) -> Result<Self, IndexError> {
        if writer_heap_bytes < MIN_WRITER_HEAP_BYTES {
            return Err(IndexError::WriterHeapTooSmall);
        }
        let path = path.into();
        let fields = IndexSchema::build();
        let index = Index::create_in_dir(&path, fields.schema.clone())?;
        register_tokenizers(&index)?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let writer = index.writer_with_num_threads(1, writer_heap_bytes)?;
        let metadata = IndexCommitMetadata {
            index_format_version: INDEX_FORMAT_VERSION,
            schema_fingerprint: fields.fingerprint(),
            corpus_config_fingerprint,
            last_append_seq: 0,
            observation_count: 0,
            projection_watermark: "proj:corpus:empty".to_owned(),
            committed_at: Utc::now(),
            record_count: 0,
            source_type_counts: BTreeMap::new(),
            linked_form_sheet_ids: Vec::new(),
        };
        let this = Self {
            path,
            index,
            reader,
            writer: Mutex::new(writer),
            mutation: Mutex::new(()),
            fields,
            metadata: Mutex::new(metadata),
        };
        this.commit_metadata_only()?;
        this.validate()?;
        Ok(this)
    }

    pub fn open(
        path: impl Into<PathBuf>,
        writer_heap_bytes: usize,
        corpus_config_fingerprint: String,
    ) -> Result<Self, IndexError> {
        if writer_heap_bytes < MIN_WRITER_HEAP_BYTES {
            return Err(IndexError::WriterHeapTooSmall);
        }
        let path = path.into();
        let fields = IndexSchema::build();
        let index = Index::open_in_dir(&path)?;
        if index.schema() != fields.schema {
            return Err(IndexError::IncompatibleSchema);
        }
        register_tokenizers(&index)?;
        let metadata = parse_metadata(&index)?;
        validate_metadata(&metadata, &fields, &corpus_config_fingerprint)?;
        let checksum_errors = index.validate_checksum()?;
        if !checksum_errors.is_empty() {
            return Err(IndexError::ChecksumMismatch(checksum_errors));
        }
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()?;
        let writer = index.writer_with_num_threads(1, writer_heap_bytes)?;
        let this = Self {
            path,
            index,
            reader,
            writer: Mutex::new(writer),
            mutation: Mutex::new(()),
            fields,
            metadata: Mutex::new(metadata),
        };
        this.validate_record_count()?;
        Ok(this)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn metadata(&self) -> Result<IndexCommitMetadata, IndexError> {
        self.metadata
            .lock()
            .map(|metadata| metadata.clone())
            .map_err(|_| IndexError::WriterLockPoisoned)
    }

    /// Executes a read-only operation against one immutable Tantivy commit and
    /// returns the metadata for that exact commit.
    ///
    /// The callback must only call read methods on the supplied index. Mutations
    /// are serialized behind this snapshot boundary.
    pub fn read_with_metadata<T>(
        &self,
        operation: impl FnOnce(&Self) -> Result<T, IndexError>,
    ) -> Result<(T, IndexCommitMetadata), IndexError> {
        let _mutation = self.mutation_lock()?;
        let value = operation(self)?;
        Ok((value, self.metadata()?))
    }

    pub fn record_count(&self) -> Result<u64, IndexError> {
        Ok(self
            .reader
            .searcher()
            .search(&tantivy::query::AllQuery, &Count)? as u64)
    }

    pub(crate) fn search_snapshot(
        &self,
    ) -> Result<(tantivy::Searcher, IndexCommitMetadata), IndexError> {
        let metadata = self
            .metadata
            .lock()
            .map_err(|_| IndexError::WriterLockPoisoned)?;
        Ok((self.reader.searcher(), metadata.clone()))
    }

    pub(crate) fn fields(&self) -> &IndexSchema {
        &self.fields
    }

    pub fn upsert_records(
        &self,
        records: &[CorpusRecord],
        last_append_seq: u64,
        observation_count: u64,
        projection_watermark: String,
    ) -> Result<(), IndexError> {
        let _mutation = self.mutation_lock()?;
        let linked_sheet_ids = self
            .metadata()?
            .linked_form_sheet_ids
            .into_iter()
            .collect::<HashSet<_>>();
        self.apply_delta_locked(
            records,
            &HashSet::new(),
            &linked_sheet_ids,
            last_append_seq,
            observation_count,
            projection_watermark,
        )
    }

    pub fn catch_up(
        &self,
        store: &dyn CorpusIndexSource,
        projector: &CorpusProjector,
        page_size: usize,
    ) -> Result<(), IndexError> {
        if page_size == 0 {
            return Err(IndexError::IncompatibleMetadata(
                "catch-up page size must be greater than zero".to_owned(),
            ));
        }
        // Serialize the source boundary read with every metadata/index mutation.
        // Without this guard, two callers can read the same append tail and both
        // advance observation_count even though the record upserts are idempotent.
        let _mutation = self.mutation_lock()?;
        loop {
            let metadata = self.metadata()?;
            let before_page = store.observation_stats()?;
            validate_source_boundary(&metadata, &before_page)?;
            let page = store.observation_page(metadata.last_append_seq, page_size)?;
            validate_observation_page(&page, metadata.last_append_seq, page_size)?;
            if page.is_empty() {
                if before_page.max_append_seq > metadata.last_append_seq {
                    return Err(IndexError::IncompatibleMetadata(format!(
                        "canonical source reports append sequence {}, but no tail exists after {}",
                        before_page.max_append_seq, metadata.last_append_seq
                    )));
                }
                let source_stats = store.observation_stats()?;
                validate_source_boundary(&metadata, &source_stats)?;
                if source_stats.max_append_seq > metadata.last_append_seq {
                    continue;
                }
                return Ok(());
            }
            let mut linked_sheet_ids = metadata
                .linked_form_sheet_ids
                .iter()
                .cloned()
                .collect::<HashSet<_>>();
            let mut newly_linked = HashSet::new();
            for stored in &page {
                if let Some(sheet_id) = linked_form_sheet_id(&stored.observation)
                    && linked_sheet_ids.insert(sheet_id.clone())
                {
                    newly_linked.insert(sheet_id);
                }
            }
            let records = page
                .iter()
                .flat_map(|stored| {
                    projector.project_observation(&stored.observation, &linked_sheet_ids)
                })
                .collect::<Vec<_>>();
            let last_append_seq = page.last().expect("non-empty page").append_seq;
            let observation_count = metadata
                .observation_count
                .checked_add(page.len() as u64)
                .ok_or_else(|| {
                    IndexError::IncompatibleMetadata("observation count overflowed u64".to_owned())
                })?;
            self.apply_delta_locked(
                &records,
                &newly_linked,
                &linked_sheet_ids,
                last_append_seq,
                observation_count,
                format!("proj:corpus:{last_append_seq}"),
            )?;
        }
    }

    fn apply_delta(
        &self,
        records: &[CorpusRecord],
        invalidated_source_object_ids: &HashSet<String>,
        linked_sheet_ids: &HashSet<String>,
        last_append_seq: u64,
        observation_count: u64,
        projection_watermark: String,
    ) -> Result<(), IndexError> {
        let _mutation = self.mutation_lock()?;
        self.apply_delta_locked(
            records,
            invalidated_source_object_ids,
            linked_sheet_ids,
            last_append_seq,
            observation_count,
            projection_watermark,
        )
    }

    fn apply_delta_locked(
        &self,
        records: &[CorpusRecord],
        invalidated_source_object_ids: &HashSet<String>,
        linked_sheet_ids: &HashSet<String>,
        last_append_seq: u64,
        observation_count: u64,
        projection_watermark: String,
    ) -> Result<(), IndexError> {
        let previous = self.metadata()?;
        if last_append_seq < previous.last_append_seq {
            return Err(IndexError::IncompatibleMetadata(format!(
                "append sequence regressed from {} to {last_append_seq}",
                previous.last_append_seq
            )));
        }
        if observation_count < previous.observation_count {
            return Err(IndexError::IncompatibleMetadata(format!(
                "observation count regressed from {} to {observation_count}",
                previous.observation_count
            )));
        }
        if last_append_seq == previous.last_append_seq
            && observation_count != previous.observation_count
        {
            return Err(IndexError::IncompatibleMetadata(format!(
                "observation count changed at unchanged append sequence {last_append_seq}"
            )));
        }
        let unique_ids = records
            .iter()
            .map(|record| record.record_id.as_str())
            .collect::<HashSet<_>>();
        if unique_ids.len() != records.len() {
            return Err(IndexError::InvalidDocument(
                "upsert batch contains duplicate record_id".to_owned(),
            ));
        }
        let existing_records = self.records_by_record_ids(&unique_ids)?;

        let mut source_type_counts = previous.source_type_counts.clone();
        let mut invalidated_upsert_ids = HashSet::with_capacity(unique_ids.len());
        self.visit_source_object_records(invalidated_source_object_ids, |old| {
            decrement_source_type(&mut source_type_counts, &old.source_type)?;
            if unique_ids.contains(old.record_id.as_str()) {
                invalidated_upsert_ids.insert(old.record_id);
            }
            Ok(())
        })?;
        for old in &existing_records {
            if !invalidated_upsert_ids.contains(&old.record_id) {
                decrement_source_type(&mut source_type_counts, &old.source_type)?;
            }
        }
        for record in records {
            *source_type_counts
                .entry(record.source_type.clone())
                .or_insert(0) += 1;
        }
        let record_count = source_type_counts.values().copied().sum();

        let mut writer = self.writer_lock()?;
        for source_object_id in invalidated_source_object_ids {
            writer.delete_term(Term::from_field_text(
                self.fields.source_object_id,
                source_object_id,
            ));
        }
        for record in records {
            writer.delete_term(Term::from_field_text(
                self.fields.record_id,
                &record.record_id,
            ));
            writer.add_document(self.document(record)?)?;
        }
        let mut linked_form_sheet_ids = linked_sheet_ids.iter().cloned().collect::<Vec<_>>();
        linked_form_sheet_ids.sort_unstable();
        let metadata = IndexCommitMetadata {
            last_append_seq,
            observation_count,
            projection_watermark,
            committed_at: Utc::now(),
            record_count,
            source_type_counts,
            linked_form_sheet_ids,
            ..previous
        };
        commit_with_metadata(&mut writer, &metadata)?;
        drop(writer);
        let mut published_metadata = self
            .metadata
            .lock()
            .map_err(|_| IndexError::WriterLockPoisoned)?;
        self.reader.reload()?;
        *published_metadata = metadata;
        drop(published_metadata);
        Ok(())
    }

    pub fn record(&self, record_id: &str) -> Result<Option<GrepRecord>, IndexError> {
        let searcher = self.reader.searcher();
        let query = TermQuery::new(
            Term::from_field_text(self.fields.record_id, record_id),
            IndexRecordOption::Basic,
        );
        let collector = TopDocs::with_limit(2).order_by_score();
        let docs = searcher.search(&query, &collector)?;
        match docs.as_slice() {
            [] => Ok(None),
            [(_, address)] => self.load_record(&searcher, *address).map(Some),
            _ => Err(IndexError::DuplicateRecord(record_id.to_owned())),
        }
    }

    pub fn validate(&self) -> Result<(), IndexError> {
        let checksum_errors = self.index.validate_checksum()?;
        if !checksum_errors.is_empty() {
            return Err(IndexError::ChecksumMismatch(checksum_errors));
        }
        validate_metadata(
            &self.metadata()?,
            &self.fields,
            &self.metadata()?.corpus_config_fingerprint,
        )?;
        self.validate_record_count()
    }

    fn commit_metadata_only(&self) -> Result<(), IndexError> {
        let metadata = self.metadata()?;
        let mut writer = self.writer_lock()?;
        commit_with_metadata(&mut writer, &metadata)?;
        drop(writer);
        self.reader.reload()?;
        Ok(())
    }

    fn validate_record_count(&self) -> Result<(), IndexError> {
        let actual = self.record_count()?;
        let expected = self.metadata()?.record_count;
        if actual != expected {
            return Err(IndexError::IncompatibleMetadata(format!(
                "record count is {actual}, metadata requires {expected}"
            )));
        }
        Ok(())
    }

    fn writer_lock(&self) -> Result<MutexGuard<'_, IndexWriter<TantivyDocument>>, IndexError> {
        self.writer
            .lock()
            .map_err(|_| IndexError::WriterLockPoisoned)
    }

    fn mutation_lock(&self) -> Result<MutexGuard<'_, ()>, IndexError> {
        self.mutation
            .lock()
            .map_err(|_| IndexError::WriterLockPoisoned)
    }

    fn document(&self, record: &CorpusRecord) -> Result<TantivyDocument, IndexError> {
        let metadata_json = serde_json::to_string(&record.metadata)?;
        let timestamp_nanos = record.timestamp.timestamp_nanos_opt().ok_or_else(|| {
            IndexError::InvalidDocument(format!(
                "timestamp for {} is outside signed nanosecond range",
                record.record_id
            ))
        })?;
        let mut document = doc!(
            self.fields.record_id => record.record_id.clone(),
            self.fields.source_type => record.source_type.clone(),
            self.fields.anchor_url => record.anchor_url.clone(),
            self.fields.source_title => record.source_title.clone(),
            self.fields.timestamp_nanos => timestamp_nanos,
            self.fields.text => record.text.clone(),
            self.fields.normalized_text => record.normalized_text.clone(),
            self.fields.metadata_json => metadata_json,
            self.fields.sort_asc => asc_sort_key(timestamp_nanos, &record.record_id),
            self.fields.sort_desc => desc_sort_key(timestamp_nanos, &record.record_id),
        );
        if let Some(value) = &record.source_location {
            document.add_text(self.fields.source_location, value);
        }
        if let Some(value) = &record.thread_ts {
            document.add_text(self.fields.thread_ts, value);
        }
        if let Some(value) = record
            .metadata
            .get("thread_key")
            .and_then(serde_json::Value::as_str)
        {
            document.add_text(self.fields.thread_key, value);
        }
        if let Some(value) = record
            .metadata
            .get("session_id")
            .and_then(serde_json::Value::as_str)
        {
            document.add_text(self.fields.session_id, value);
        }
        if let Some(value) = record
            .metadata
            .get("parent_session_id")
            .and_then(serde_json::Value::as_str)
        {
            document.add_text(self.fields.parent_session_id, value);
        }
        if let Some(value) = &record.container {
            document.add_text(self.fields.container, value);
        }
        if let Some(value) = record
            .metadata
            .get("source_object_id")
            .and_then(serde_json::Value::as_str)
        {
            document.add_text(self.fields.source_object_id, value);
        }
        if let Some(value) = record
            .metadata
            .get("linked_sheet_id")
            .and_then(serde_json::Value::as_str)
        {
            document.add_text(self.fields.linked_sheet_id, value);
        }
        Ok(document)
    }

    pub(crate) fn load_record(
        &self,
        searcher: &tantivy::Searcher,
        address: DocAddress,
    ) -> Result<GrepRecord, IndexError> {
        let document = searcher.doc::<TantivyDocument>(address)?;
        let required_text = |field, name: &str| {
            document
                .get_first(field)
                .and_then(|value| value.as_str())
                .map(str::to_owned)
                .ok_or_else(|| IndexError::InvalidDocument(format!("missing {name}")))
        };
        let optional_text = |field| {
            document
                .get_first(field)
                .and_then(|value| value.as_str())
                .map(str::to_owned)
        };
        let timestamp_nanos = document
            .get_first(self.fields.timestamp_nanos)
            .and_then(|value| value.as_i64())
            .ok_or_else(|| IndexError::InvalidDocument("missing timestamp_nanos".to_owned()))?;
        let timestamp = DateTime::<Utc>::from_timestamp_nanos(timestamp_nanos);
        let metadata_json = required_text(self.fields.metadata_json, "metadata_json")?;
        let text = required_text(self.fields.text, "text")?;
        Ok(GrepRecord {
            record_id: required_text(self.fields.record_id, "record_id")?,
            source_type: required_text(self.fields.source_type, "source_type")?,
            anchor_url: required_text(self.fields.anchor_url, "anchor_url")?,
            source_title: required_text(self.fields.source_title, "source_title")?,
            source_location: optional_text(self.fields.source_location),
            timestamp,
            text: text.clone(),
            normalized_text: normalize(&text),
            thread_ts: optional_text(self.fields.thread_ts),
            container: optional_text(self.fields.container),
            metadata: serde_json::from_str(&metadata_json)?,
        })
    }
}

fn decrement_source_type(
    counts: &mut BTreeMap<String, u64>,
    source_type: &str,
) -> Result<(), IndexError> {
    let count = counts.get_mut(source_type).ok_or_else(|| {
        IndexError::IncompatibleMetadata(format!("source type count is missing for {source_type}"))
    })?;
    *count = count.checked_sub(1).ok_or_else(|| {
        IndexError::IncompatibleMetadata(format!("source type count underflow for {source_type}"))
    })?;
    if *count == 0 {
        counts.remove(source_type);
    }
    Ok(())
}

fn validate_source_boundary(
    metadata: &IndexCommitMetadata,
    source_stats: &ObservationStats,
) -> Result<(), IndexError> {
    if source_stats.max_append_seq < metadata.last_append_seq {
        return Err(IndexError::IncompatibleMetadata(format!(
            "canonical append sequence regressed from {} to {}",
            metadata.last_append_seq, source_stats.max_append_seq
        )));
    }
    if source_stats.count < metadata.observation_count {
        return Err(IndexError::IncompatibleMetadata(format!(
            "canonical observation count regressed from {} to {}",
            metadata.observation_count, source_stats.count
        )));
    }
    if source_stats.max_append_seq == metadata.last_append_seq
        && source_stats.count != metadata.observation_count
    {
        return Err(IndexError::IncompatibleMetadata(format!(
            "canonical observation count is {}, index count is {} at append sequence {}",
            source_stats.count, metadata.observation_count, metadata.last_append_seq
        )));
    }
    Ok(())
}

fn validate_observation_page(
    page: &[lethe_storage_api::StoredObservation],
    after_append_seq: u64,
    limit: usize,
) -> Result<(), IndexError> {
    if page.len() > limit {
        return Err(IndexError::IncompatibleMetadata(format!(
            "canonical source returned {} observations for page limit {limit}",
            page.len()
        )));
    }
    let mut previous = after_append_seq;
    for stored in page {
        if stored.append_seq <= previous {
            return Err(IndexError::IncompatibleMetadata(format!(
                "canonical observation page is not strictly ordered after {after_append_seq}: {} followed {previous}",
                stored.append_seq
            )));
        }
        previous = stored.append_seq;
    }
    Ok(())
}

fn register_tokenizers(index: &Index) -> Result<(), IndexError> {
    index.tokenizers().register(
        NGRAM_TOKENIZER,
        NgramTokenizer::new(1, 3, false).map_err(IndexError::Tantivy)?,
    );
    Ok(())
}

fn parse_metadata(index: &Index) -> Result<IndexCommitMetadata, IndexError> {
    let payload = index
        .load_metas()?
        .payload
        .ok_or(IndexError::MissingCommitMetadata)?;
    Ok(serde_json::from_str(&payload)?)
}

fn validate_metadata(
    metadata: &IndexCommitMetadata,
    fields: &IndexSchema,
    corpus_config_fingerprint: &str,
) -> Result<(), IndexError> {
    if metadata.index_format_version != INDEX_FORMAT_VERSION {
        return Err(IndexError::IncompatibleMetadata(format!(
            "format {} != {}",
            metadata.index_format_version, INDEX_FORMAT_VERSION
        )));
    }
    if metadata.schema_fingerprint != fields.fingerprint() {
        return Err(IndexError::IncompatibleMetadata(
            "schema fingerprint differs".to_owned(),
        ));
    }
    if metadata.corpus_config_fingerprint != corpus_config_fingerprint {
        return Err(IndexError::IncompatibleMetadata(
            "corpus config fingerprint differs".to_owned(),
        ));
    }
    let mut counted_records = 0_u64;
    for (source_type, count) in &metadata.source_type_counts {
        if source_type.trim().is_empty() || *count == 0 {
            return Err(IndexError::IncompatibleMetadata(
                "source type counts contain a blank type or zero count".to_owned(),
            ));
        }
        counted_records = counted_records.checked_add(*count).ok_or_else(|| {
            IndexError::IncompatibleMetadata("source type counts overflowed u64".to_owned())
        })?;
    }
    if counted_records != metadata.record_count {
        return Err(IndexError::IncompatibleMetadata(format!(
            "source type count sum is {counted_records}, record count is {}",
            metadata.record_count
        )));
    }
    Ok(())
}

fn commit_with_metadata(
    writer: &mut IndexWriter<TantivyDocument>,
    metadata: &IndexCommitMetadata,
) -> Result<(), IndexError> {
    let payload = serde_json::to_string(metadata)?;
    let mut prepared = writer.prepare_commit()?;
    prepared.set_payload(&payload);
    prepared.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier, mpsc};
    use std::time::Duration;

    use super::*;
    use lethe_core::domain::{
        AuthorityModel, CaptureModel, EntityRef, IdempotencyKey, Observation, ObserverRef,
        SchemaRef, SemVer, SourceSystemRef,
    };
    use lethe_projection_corpus::normalized_text;
    use lethe_storage_api::{ObservationStats, StorageResult, StoredObservation};
    use lethe_storage_sqlite::persistence::SqlitePersistence;

    #[derive(Clone)]
    struct TestSource {
        rows: Arc<Vec<StoredObservation>>,
        active_page_calls: Arc<AtomicUsize>,
        max_active_page_calls: Arc<AtomicUsize>,
    }

    impl TestSource {
        fn new(observations: Vec<Observation>) -> Self {
            Self::with_append_seqs(
                observations
                    .into_iter()
                    .enumerate()
                    .map(|(index, observation)| (index as u64 + 1, observation))
                    .collect(),
            )
        }

        fn with_append_seqs(observations: Vec<(u64, Observation)>) -> Self {
            let rows = observations
                .into_iter()
                .enumerate()
                .map(|(index, (append_seq, observation))| StoredObservation {
                    leaf_id: format!("leaf-{index}"),
                    append_seq,
                    observation,
                })
                .collect();
            Self {
                rows: Arc::new(rows),
                active_page_calls: Arc::new(AtomicUsize::new(0)),
                max_active_page_calls: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl CorpusIndexSource for TestSource {
        fn observation_stats(&self) -> StorageResult<ObservationStats> {
            Ok(ObservationStats {
                count: self.rows.len() as u64,
                max_append_seq: self.rows.last().map_or(0, |row| row.append_seq),
            })
        }

        fn observation_page(
            &self,
            after_append_seq: u64,
            limit: usize,
        ) -> StorageResult<Vec<StoredObservation>> {
            let active = self.active_page_calls.fetch_add(1, Ordering::SeqCst) + 1;
            self.max_active_page_calls
                .fetch_max(active, Ordering::SeqCst);
            std::thread::sleep(Duration::from_millis(20));
            let page = self
                .rows
                .iter()
                .filter(|row| row.append_seq > after_append_seq)
                .take(limit)
                .cloned()
                .collect();
            self.active_page_calls.fetch_sub(1, Ordering::SeqCst);
            Ok(page)
        }
    }

    fn temp_root() -> PathBuf {
        std::env::temp_dir().join(format!("lethe-search-index-test-{}", uuid::Uuid::now_v7()))
    }

    fn record(record_id: &str, text: &str) -> CorpusRecord {
        let timestamp = "2026-01-02T03:04:05Z".parse().unwrap();
        CorpusRecord {
            record_id: record_id.to_owned(),
            source_type: "slack".to_owned(),
            anchor_url: format!("https://example.test/{record_id}"),
            source_title: "123_event".to_owned(),
            source_location: Some("#123_event".to_owned()),
            timestamp,
            text: text.to_owned(),
            normalized_text: normalized_text(text),
            thread_ts: Some("1.000".to_owned()),
            container: Some("123_event".to_owned()),
            metadata: serde_json::json!({"observation_id": "obs:1"}),
        }
    }

    fn observation(key: &str, schema: &str, payload: serde_json::Value) -> Observation {
        Observation {
            id: Observation::new_id(),
            schema: SchemaRef::new(schema),
            schema_version: SemVer::new("1.0.0"),
            observer: ObserverRef::new("obs:test"),
            source_system: Some(SourceSystemRef::new("sys:slack")),
            actor: None,
            authority_model: AuthorityModel::LakeAuthoritative,
            capture_model: CaptureModel::Event,
            subject: EntityRef::new(format!("message:{key}")),
            target: None,
            payload,
            attachments: Vec::new(),
            published: "2026-01-02T03:04:05Z".parse().unwrap(),
            recorded_at: "2026-01-02T03:04:06Z".parse().unwrap(),
            consent: None,
            idempotency_key: IdempotencyKey::new(key),
            meta: serde_json::json!({
                "canonical_json": serde_json::json!({"key": key}).to_string(),
                "source_container": "test",
            }),
        }
    }

    #[test]
    fn upsert_is_idempotent_and_metadata_reopens() {
        let path = temp_root();
        fs::create_dir_all(&path).unwrap();
        let index =
            PersistentCorpusIndex::create(&path, MIN_WRITER_HEAP_BYTES, "cfg".into()).unwrap();
        let created_at = index.metadata().unwrap().committed_at;
        index
            .upsert_records(&[record("r1", "first")], 1, 1, "proj:corpus:1".into())
            .unwrap();
        index
            .upsert_records(&[record("r1", "second")], 1, 1, "proj:corpus:1".into())
            .unwrap();
        assert_eq!(index.record_count().unwrap(), 1);
        assert_eq!(index.record("r1").unwrap().unwrap().text, "second");
        let committed = index.metadata().unwrap();
        assert_eq!(committed.observation_count, 1);
        assert!(committed.committed_at >= created_at);
        drop(index);

        let reopened =
            PersistentCorpusIndex::open(&path, MIN_WRITER_HEAP_BYTES, "cfg".into()).unwrap();
        assert_eq!(reopened.metadata().unwrap().last_append_seq, 1);
        assert_eq!(reopened.metadata().unwrap(), committed);
        assert_eq!(reopened.record_count().unwrap(), 1);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn generation_is_published_only_through_current() {
        let path = temp_root();
        let root = IndexRoot::new(&path, MIN_WRITER_HEAP_BYTES, "cfg").unwrap();
        assert!(matches!(
            root.open_current(),
            Err(IndexError::MissingCurrentGeneration)
        ));
        let (generation, index) = root.create_generation().unwrap();
        index
            .upsert_records(&[record("r1", "needle")], 1, 1, "proj:corpus:1".into())
            .unwrap();
        index.validate().unwrap();
        let opened = root.publish(&generation, index).unwrap();
        assert_eq!(opened.generation, generation);
        assert_eq!(opened.index.record_count().unwrap(), 1);
        drop(opened.index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn missing_current_in_existing_root_is_corruption_not_first_build() {
        let path = temp_root();
        let root = IndexRoot::new(&path, MIN_WRITER_HEAP_BYTES, "cfg").unwrap();
        let (_generation, index) = root.create_generation().unwrap();
        drop(index);

        assert!(matches!(
            root.open_current(),
            Err(IndexError::InvalidCurrentGeneration(detail))
                if detail.contains("CURRENT is missing")
        ));

        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn incompatible_config_fingerprint_fails_fast() {
        let path = temp_root();
        fs::create_dir_all(&path).unwrap();
        let index =
            PersistentCorpusIndex::create(&path, MIN_WRITER_HEAP_BYTES, "cfg-a".into()).unwrap();
        drop(index);
        assert!(matches!(
            PersistentCorpusIndex::open(&path, MIN_WRITER_HEAP_BYTES, "cfg-b".into()),
            Err(IndexError::IncompatibleMetadata(_))
        ));
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn metadata_without_format_version_requires_full_rebuild() {
        let path = temp_root();
        fs::create_dir_all(&path).unwrap();
        let index =
            PersistentCorpusIndex::create(&path, MIN_WRITER_HEAP_BYTES, "cfg".into()).unwrap();
        let mut legacy_metadata = serde_json::to_value(index.metadata().unwrap()).unwrap();
        legacy_metadata
            .as_object_mut()
            .unwrap()
            .remove("index_format_version");
        let payload = serde_json::to_string(&legacy_metadata).unwrap();
        {
            let _mutation = index.mutation_lock().unwrap();
            let mut writer = index.writer_lock().unwrap();
            let mut prepared = writer.prepare_commit().unwrap();
            prepared.set_payload(&payload);
            prepared.commit().unwrap();
        }
        drop(index);

        let error =
            PersistentCorpusIndex::open(&path, MIN_WRITER_HEAP_BYTES, "cfg".into()).unwrap_err();
        assert!(matches!(error, IndexError::Json(_)));
        assert!(error.requires_rebuild());

        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn search_snapshot_binds_reader_to_commit_metadata() {
        let path = temp_root();
        fs::create_dir_all(&path).unwrap();
        let index =
            PersistentCorpusIndex::create(&path, MIN_WRITER_HEAP_BYTES, "cfg".into()).unwrap();
        index
            .upsert_records(
                &[record("r1", "first"), record("r2", "second")],
                9,
                2,
                "proj:corpus:9".into(),
            )
            .unwrap();

        let (searcher, metadata) = index.search_snapshot().unwrap();
        assert_eq!(
            searcher.search(&tantivy::query::AllQuery, &Count).unwrap() as u64,
            metadata.record_count
        );
        assert_eq!(metadata.record_count, 2);
        assert_eq!(metadata.observation_count, 2);
        assert_eq!(metadata.projection_watermark, "proj:corpus:9");
        drop(index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn timestamp_outside_signed_nanosecond_range_fails_fast() {
        let path = temp_root();
        fs::create_dir_all(&path).unwrap();
        let index =
            PersistentCorpusIndex::create(&path, MIN_WRITER_HEAP_BYTES, "cfg".into()).unwrap();
        let mut invalid = record("r1", "outside range");
        invalid.timestamp = "2500-01-01T00:00:00Z".parse().unwrap();

        assert!(matches!(
            index.upsert_records(&[invalid], 1, 1, "proj:corpus:1".into()),
            Err(IndexError::InvalidDocument(message))
                if message.contains("outside signed nanosecond range")
        ));
        drop(index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rebuild_and_catch_up_consume_only_durable_tail() {
        let root_path = temp_root();
        let db = root_path.join("lethe.sqlite3");
        let blobs = root_path.join("blobs");
        let index_path = root_path.join("index");
        let store = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
        let first = observation(
            "first",
            "schema:slack-message",
            serde_json::json!({"text": "first needle", "channel_name": "123_event"}),
        );
        store.append_observation_idempotent(&first).unwrap();
        let projector = CorpusProjector::personal_all_text_config();
        let root = IndexRoot::new(&index_path, MIN_WRITER_HEAP_BYTES, "cfg").unwrap();
        let (generation, built_index) = root.rebuild_from_store(&store, &projector, 1).unwrap();
        let index = root.publish(&generation, built_index).unwrap().index;
        assert_eq!(index.record_count().unwrap(), 1);
        assert_eq!(index.metadata().unwrap().last_append_seq, 1);
        assert_eq!(index.metadata().unwrap().observation_count, 1);

        let second = observation(
            "second",
            "schema:slack-message",
            serde_json::json!({"text": "second needle", "channel_name": "123_event"}),
        );
        store.append_observation_idempotent(&second).unwrap();
        index.catch_up(&store, &projector, 1).unwrap();
        assert_eq!(index.record_count().unwrap(), 2);
        assert_eq!(index.metadata().unwrap().last_append_seq, 2);
        assert_eq!(index.metadata().unwrap().observation_count, 2);

        store.append_observation_idempotent(&second).unwrap();
        index.catch_up(&store, &projector, 1).unwrap();
        assert_eq!(index.record_count().unwrap(), 2);
        assert_eq!(index.metadata().unwrap().observation_count, 2);
        drop(index);
        drop(store);
        fs::remove_dir_all(root_path).unwrap();
    }

    #[test]
    fn batch_upsert_and_source_invalidation_cross_internal_keyset_pages() {
        let path = temp_root();
        fs::create_dir_all(&path).unwrap();
        let index =
            PersistentCorpusIndex::create(&path, MIN_WRITER_HEAP_BYTES, "cfg".into()).unwrap();
        let records = (0..300)
            .map(|number| {
                let mut record = record(&format!("r{number:03}"), "first");
                record.metadata = serde_json::json!({
                    "observation_id": format!("obs:{number}"),
                    "source_object_id": "sheet-many",
                });
                record
            })
            .collect::<Vec<_>>();
        index
            .upsert_records(&records, 300, 300, "proj:corpus:300".into())
            .unwrap();

        let updated = records
            .iter()
            .cloned()
            .map(|mut record| {
                record.text = "second".to_owned();
                record.normalized_text = normalized_text("second");
                record
            })
            .collect::<Vec<_>>();
        index
            .upsert_records(&updated, 300, 300, "proj:corpus:300".into())
            .unwrap();
        assert_eq!(index.record_count().unwrap(), 300);
        assert_eq!(index.record("r299").unwrap().unwrap().text, "second");
        assert_eq!(
            index.metadata().unwrap().source_type_counts,
            BTreeMap::from([("slack".to_owned(), 300)])
        );

        index
            .apply_delta(
                &[],
                &HashSet::from(["sheet-many".to_owned()]),
                &HashSet::new(),
                301,
                301,
                "proj:corpus:301".into(),
            )
            .unwrap();
        assert_eq!(index.record_count().unwrap(), 0);
        let metadata = index.metadata().unwrap();
        assert_eq!(metadata.observation_count, 301);
        assert_eq!(metadata.record_count, 0);
        assert!(metadata.source_type_counts.is_empty());

        drop(index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn concurrent_catch_up_serializes_source_boundary_and_count_update() {
        let path = temp_root();
        fs::create_dir_all(&path).unwrap();
        let index = Arc::new(
            PersistentCorpusIndex::create(&path, MIN_WRITER_HEAP_BYTES, "cfg".into()).unwrap(),
        );
        let source = TestSource::new(vec![observation(
            "only",
            "schema:slack-message",
            serde_json::json!({"text": "one needle", "channel_name": "123_event"}),
        )]);
        let start = Arc::new(Barrier::new(2));

        std::thread::scope(|scope| {
            for _ in 0..2 {
                let index = Arc::clone(&index);
                let source = source.clone();
                let start = Arc::clone(&start);
                scope.spawn(move || {
                    start.wait();
                    index
                        .catch_up(&source, &CorpusProjector::personal_all_text_config(), 1)
                        .unwrap();
                });
            }
        });

        let metadata = index.metadata().unwrap();
        assert_eq!(metadata.last_append_seq, 1);
        assert_eq!(metadata.observation_count, 1);
        assert_eq!(metadata.record_count, 1);
        assert_eq!(source.max_active_page_calls.load(Ordering::SeqCst), 1);

        drop(index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn read_with_metadata_blocks_upsert_until_callback_returns() {
        let path = temp_root();
        fs::create_dir_all(&path).unwrap();
        let index = Arc::new(
            PersistentCorpusIndex::create(&path, MIN_WRITER_HEAP_BYTES, "cfg".into()).unwrap(),
        );
        index
            .upsert_records(&[record("first", "one")], 1, 1, "proj:corpus:1".into())
            .unwrap();
        let (entered_tx, entered_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let (calling_tx, calling_rx) = mpsc::sync_channel(0);
        let (done_tx, done_rx) = mpsc::sync_channel(0);

        let reader_index = Arc::clone(&index);
        let reader = std::thread::spawn(move || {
            reader_index.read_with_metadata(|snapshot| {
                let (_, total) = snapshot.records_page(0, 10)?;
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(total)
            })
        });
        entered_rx.recv().unwrap();

        let writer_index = Arc::clone(&index);
        let writer = std::thread::spawn(move || {
            calling_tx.send(()).unwrap();
            let result = writer_index.upsert_records(
                &[record("second", "two")],
                2,
                2,
                "proj:corpus:2".into(),
            );
            done_tx.send(result).unwrap();
        });
        calling_rx.recv().unwrap();
        assert!(matches!(
            done_rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));

        release_tx.send(()).unwrap();
        let (read_count, read_metadata) = reader.join().unwrap().unwrap();
        done_rx.recv().unwrap().unwrap();
        writer.join().unwrap();
        assert_eq!(read_count, 1);
        assert_eq!(read_metadata.record_count, 1);
        assert_eq!(index.metadata().unwrap().record_count, 2);

        drop(index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn read_with_metadata_blocks_catch_up_until_callback_returns() {
        let path = temp_root();
        fs::create_dir_all(&path).unwrap();
        let index = Arc::new(
            PersistentCorpusIndex::create(&path, MIN_WRITER_HEAP_BYTES, "cfg".into()).unwrap(),
        );
        let source = TestSource::new(vec![observation(
            "only",
            "schema:slack-message",
            serde_json::json!({"text": "one needle", "channel_name": "123_event"}),
        )]);
        let (entered_tx, entered_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::sync_channel(0);
        let (calling_tx, calling_rx) = mpsc::sync_channel(0);
        let (done_tx, done_rx) = mpsc::sync_channel(0);

        let reader_index = Arc::clone(&index);
        let reader = std::thread::spawn(move || {
            reader_index.read_with_metadata(|snapshot| {
                let (_, total) = snapshot.records_page(0, 10)?;
                entered_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(total)
            })
        });
        entered_rx.recv().unwrap();

        let writer_index = Arc::clone(&index);
        let writer = std::thread::spawn(move || {
            calling_tx.send(()).unwrap();
            let result =
                writer_index.catch_up(&source, &CorpusProjector::personal_all_text_config(), 1);
            done_tx.send(result).unwrap();
        });
        calling_rx.recv().unwrap();
        assert!(matches!(
            done_rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));

        release_tx.send(()).unwrap();
        let (read_count, read_metadata) = reader.join().unwrap().unwrap();
        done_rx.recv().unwrap().unwrap();
        writer.join().unwrap();
        assert_eq!(read_count, 0);
        assert_eq!(read_metadata.record_count, 0);
        assert_eq!(index.metadata().unwrap().record_count, 1);

        drop(index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn catch_up_rejects_canonical_regression_and_tail_less_count_mismatch() {
        let make_source = |rows: &[(u64, &str)]| {
            TestSource::with_append_seqs(
                rows.iter()
                    .map(|(append_seq, key)| {
                        (
                            *append_seq,
                            observation(
                                key,
                                "schema:slack-message",
                                serde_json::json!({"text": key, "channel_name": "123_event"}),
                            ),
                        )
                    })
                    .collect(),
            )
        };
        let projector = CorpusProjector::personal_all_text_config();

        let path = temp_root();
        fs::create_dir_all(&path).unwrap();
        let index =
            PersistentCorpusIndex::create(&path, MIN_WRITER_HEAP_BYTES, "cfg".into()).unwrap();
        index
            .upsert_records(
                &[record("first", "one"), record("second", "two")],
                5,
                2,
                "proj:corpus:5".into(),
            )
            .unwrap();

        for source in [
            make_source(&[(3, "three"), (4, "four")]),
            make_source(&[(5, "five")]),
        ] {
            let error = index.catch_up(&source, &projector, 1).unwrap_err();
            assert!(matches!(error, IndexError::IncompatibleMetadata(_)));
            assert!(error.requires_rebuild());
        }

        let second_path = temp_root();
        fs::create_dir_all(&second_path).unwrap();
        let second =
            PersistentCorpusIndex::create(&second_path, MIN_WRITER_HEAP_BYTES, "cfg".into())
                .unwrap();
        second
            .upsert_records(&[record("only", "one")], 5, 1, "proj:corpus:5".into())
            .unwrap();
        let error = second
            .catch_up(&make_source(&[(4, "four"), (5, "five")]), &projector, 1)
            .unwrap_err();
        assert!(matches!(error, IndexError::IncompatibleMetadata(_)));
        assert!(error.requires_rebuild());

        drop(second);
        drop(index);
        fs::remove_dir_all(second_path).unwrap();
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rebuild_counts_rows_when_append_sequences_have_holes() {
        let path = temp_root();
        let source = TestSource::with_append_seqs(vec![
            (
                2,
                observation(
                    "first",
                    "schema:slack-message",
                    serde_json::json!({"text": "first", "channel_name": "123_event"}),
                ),
            ),
            (
                9,
                observation(
                    "second",
                    "schema:slack-message",
                    serde_json::json!({"text": "second", "channel_name": "123_event"}),
                ),
            ),
        ]);
        let root = IndexRoot::new(&path, MIN_WRITER_HEAP_BYTES, "cfg").unwrap();
        let (_generation, index) = root
            .rebuild_from_store(&source, &CorpusProjector::personal_all_text_config(), 1)
            .unwrap();

        let metadata = index.metadata().unwrap();
        assert_eq!(metadata.last_append_seq, 9);
        assert_eq!(metadata.observation_count, 2);
        assert_eq!(metadata.record_count, 2);

        drop(index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rebuild_accepts_source_without_observation_store_surface() {
        let path = temp_root();
        let source = TestSource::new(vec![
            observation(
                "first",
                "schema:slack-message",
                serde_json::json!({"text": "first", "channel_name": "123_event"}),
            ),
            observation(
                "second",
                "schema:slack-message",
                serde_json::json!({"text": "second", "channel_name": "123_event"}),
            ),
        ]);
        let root = IndexRoot::new(&path, MIN_WRITER_HEAP_BYTES, "cfg").unwrap();
        let (_generation, index) = root
            .rebuild_from_store(&source, &CorpusProjector::personal_all_text_config(), 1)
            .unwrap();
        assert_eq!(index.metadata().unwrap().observation_count, 2);
        assert_eq!(index.record_count().unwrap(), 2);

        drop(index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn rebuild_rejects_oversized_or_non_monotonic_source_pages() {
        #[derive(Clone, Copy)]
        enum InvalidPage {
            Oversized,
            NonMonotonic,
        }

        struct InvalidSource {
            kind: InvalidPage,
            rows: Vec<StoredObservation>,
        }

        impl CorpusIndexSource for InvalidSource {
            fn observation_stats(&self) -> StorageResult<ObservationStats> {
                Ok(ObservationStats {
                    count: self.rows.len() as u64,
                    max_append_seq: self
                        .rows
                        .iter()
                        .map(|row| row.append_seq)
                        .max()
                        .unwrap_or(0),
                })
            }

            fn observation_page(
                &self,
                _after_append_seq: u64,
                limit: usize,
            ) -> StorageResult<Vec<StoredObservation>> {
                Ok(match self.kind {
                    InvalidPage::Oversized => self.rows.iter().take(limit + 1).cloned().collect(),
                    InvalidPage::NonMonotonic => self.rows.clone(),
                })
            }
        }

        let row = |append_seq, key| StoredObservation {
            leaf_id: format!("leaf-{key}"),
            append_seq,
            observation: observation(
                key,
                "schema:slack-message",
                serde_json::json!({"text": key, "channel_name": "123_event"}),
            ),
        };
        let cases = [
            (
                InvalidSource {
                    kind: InvalidPage::Oversized,
                    rows: vec![row(1, "one"), row(2, "two")],
                },
                1,
            ),
            (
                InvalidSource {
                    kind: InvalidPage::NonMonotonic,
                    rows: vec![row(2, "two"), row(1, "one")],
                },
                2,
            ),
        ];

        for (source, page_size) in cases {
            let path = temp_root();
            let root = IndexRoot::new(&path, MIN_WRITER_HEAP_BYTES, "cfg").unwrap();
            let error = root
                .rebuild_from_store(
                    &source,
                    &CorpusProjector::personal_all_text_config(),
                    page_size,
                )
                .unwrap_err();
            assert!(matches!(error, IndexError::IncompatibleMetadata(_)));
            assert!(error.requires_rebuild());
            if path.exists() {
                fs::remove_dir_all(path).unwrap();
            }
        }
    }

    #[test]
    fn runtime_publish_leaves_retired_generation_until_owner_cleans_it() {
        let path = temp_root();
        let root = IndexRoot::new(&path, MIN_WRITER_HEAP_BYTES, "cfg").unwrap();

        let (first_generation, first) = root.create_generation().unwrap();
        first
            .upsert_records(&[record("first", "one")], 1, 1, "proj:corpus:1".into())
            .unwrap();
        let first = root.publish(&first_generation, first).unwrap();
        assert_eq!(first.index.record_count().unwrap(), 1);

        let (second_generation, second) = root.create_generation().unwrap();
        second
            .upsert_records(
                &[record("second", "two"), record("third", "three")],
                2,
                2,
                "proj:corpus:2".into(),
            )
            .unwrap();
        let opened = root.publish(&second_generation, second).unwrap();
        assert_eq!(opened.generation, second_generation);
        assert_ne!(opened.generation, first_generation);
        assert_eq!(opened.index.record_count().unwrap(), 2);
        assert!(opened.index.record("first").unwrap().is_none());
        assert!(root.generation_path(&first_generation).is_dir());
        assert!(root.generation_path(&second_generation).is_dir());

        drop(first.index);
        assert!(root.cleanup_retired_generation(&first_generation).unwrap());
        assert!(!root.generation_path(&first_generation).exists());
        assert!(!root.cleanup_retired_generation(&second_generation).unwrap());

        drop(opened.index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn bootstrap_cleanup_keeps_only_the_current_generation() {
        let path = temp_root();
        let root = IndexRoot::new(&path, MIN_WRITER_HEAP_BYTES, "cfg").unwrap();
        let mut current_generation = String::new();

        for number in 1..=3 {
            let (generation, index) = root.create_generation().unwrap();
            index
                .upsert_records(
                    &[record(&format!("r{number}"), "needle")],
                    number,
                    number,
                    format!("proj:corpus:{number}"),
                )
                .unwrap();
            let opened = root.publish(&generation, index).unwrap();
            current_generation = opened.generation;
            drop(opened.index);
        }

        root.cleanup_obsolete_generations(&current_generation)
            .unwrap();

        let generations = fs::read_dir(path.join(GENERATIONS_DIR))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(generations, vec![current_generation]);

        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn cleanup_failure_is_explicit_without_invalidating_published_index() {
        let path = temp_root();
        let root = IndexRoot::new(&path, MIN_WRITER_HEAP_BYTES, "cfg").unwrap();
        let (generation, index) = root.create_generation().unwrap();
        index
            .upsert_records(&[record("r1", "needle")], 1, 1, "proj:corpus:1".into())
            .unwrap();
        fs::write(
            path.join(GENERATIONS_DIR).join("unexpected-entry"),
            b"fixture",
        )
        .unwrap();

        let opened = root.publish(&generation, index).unwrap();
        assert_eq!(opened.generation, generation);
        let cleanup_error = root.cleanup_obsolete_generations(&generation).unwrap_err();
        assert!(matches!(
            cleanup_error,
            IndexError::GenerationCleanup(ref detail)
                if detail.contains("unexpected non-directory")
        ));
        assert_eq!(opened.index.record_count().unwrap(), 1);
        assert_eq!(opened.index.record("r1").unwrap().unwrap().text, "needle");
        assert_eq!(root.read_current().unwrap(), generation);

        drop(opened.index);
        fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn later_form_link_removes_already_indexed_response_sheet() {
        let root_path = temp_root();
        let db = root_path.join("lethe.sqlite3");
        let blobs = root_path.join("blobs");
        let index_path = root_path.join("index");
        let store = SqlitePersistence::open(&db, &blobs, &[7; 32]).unwrap();
        let mut sheet = observation(
            "sheet",
            "schema:workspace-object-snapshot",
            serde_json::json!({
                "title": "Responses",
                "artifact": {
                    "service": "sheets",
                    "objectType": "spreadsheet",
                    "sourceObjectId": "sheet-1",
                    "canonicalUri": "https://sheets.example/sheet-1"
                },
                "native": {"tabs": [{"name": "Responses", "rows": [{
                    "rowNumber": 2,
                    "cells": [{"header": "Answer", "value": "private answer"}]
                }]}]}
            }),
        );
        sheet.source_system = Some(SourceSystemRef::new("sys:google"));
        store.append_observation_idempotent(&sheet).unwrap();
        let projector = CorpusProjector::default_config();
        let root = IndexRoot::new(&index_path, MIN_WRITER_HEAP_BYTES, "cfg").unwrap();
        let (_generation, index) = root.rebuild_from_store(&store, &projector, 1).unwrap();
        assert_eq!(index.record_count().unwrap(), 1);
        let sheet_record_id = format!("corpus:sheets:{}:Responses:2", sheet.id);
        assert!(index.record(&sheet_record_id).unwrap().is_some());

        let mut form = observation(
            "form",
            "schema:workspace-object-snapshot",
            serde_json::json!({
                "title": "Survey",
                "artifact": {
                    "service": "forms",
                    "objectType": "form",
                    "sourceObjectId": "form-1",
                    "canonicalUri": "https://forms.example/form-1"
                },
                "metadata": {"linkedSheetId": "sheet-1"},
                "native": {"questions": [{"title": "Question"}]}
            }),
        );
        form.source_system = Some(SourceSystemRef::new("sys:google"));
        store.append_observation_idempotent(&form).unwrap();
        index.catch_up(&store, &projector, 1).unwrap();
        assert!(index.record(&sheet_record_id).unwrap().is_none());
        assert_eq!(index.record_count().unwrap(), 1);
        assert_eq!(
            index.metadata().unwrap().linked_form_sheet_ids,
            vec!["sheet-1"]
        );
        drop(index);
        drop(store);
        fs::remove_dir_all(root_path).unwrap();
    }
}
