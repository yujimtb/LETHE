use std::collections::BTreeMap;
use std::env;
use std::fs;
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use lethe_adapter_api::error::AdapterError;
use lethe_adapter_coding_agent::{BackboneHistoryRecord, ClaudeCodeImporter, CodexImporter};
use lethe_core::domain::{DataSpaceId, SemVer};
use lethe_history::{
    HISTORY_ACTIVATION_HANDOFF_SCHEMA, HISTORY_ACTIVATION_HANDOFF_VERSION,
    HistoryActivationHandoff, HistoryActivationSessionEntry, HistoryActivationSource, HistoryError,
    HistoryImportReceipt, HistoryImportResult, HistoryManifestDigestBuilder, HistoryRawRecord,
    HistoryRecordKind, HistorySourceKind, HistorySourceManifest, LetheConversationPartition,
    OwnershipAssignment, RequiredHistorySource, coding_agent_source_inventory,
    history_session_reference, history_upstream_identity, lethe_observation_cutover_cursor,
    manifest_entry_for_raw_record, prepare_history_message_request,
    prepare_history_receipt_request, visit_lethe_conversation_observations,
};
use lethe_runtime::runtime::partition::RoutingKeyOrder;
use lethe_storage_api::{OperationalAppendOutcome, OperationalStoragePorts};
use lethe_storage_postgres::PostgresOperationalEventStore;
use lethe_storage_sqlite::{SqliteOperationalEventStore, SqlitePersistence};
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};

const HELP: &str = "\
Inventory or import native Claude Code and Codex histories into a Personal LETHE DataSpace.

Usage:
  lethe-import-history --mode=dry-run --inventory-id=<id> --data-space-id=<id> --owner-id=<id> --captured-at=<rfc3339> --spool-database=<new-path> --max-source-record-bytes=<n> --max-resident-batch-records=<n> --max-handoff-session-entries=<n> [--require-activation-source-set=true] [source options]
  lethe-import-history --mode=execute --inventory-id=<id> --data-space-id=<id> --owner-id=<id> --captured-at=<rfc3339> --spool-database=<new-path> --max-source-record-bytes=<n> --max-resident-batch-records=<n> --max-handoff-session-entries=<n> --expected-manifest-digest=<sha256> --max-blob-bytes=<n> [--require-activation-source-set=true] [source options] [backend options]

Source options (at least one pair is required):
  --claude-root=<path>                 Native .claude/projects directory
  --claude-source-instance=<id>        Stable Claude source instance id
  --codex-root=<path>                  Native .codex directory; sessions and archived_sessions are included
  --codex-source-instance=<id>         Stable Codex source instance id
  --history-jsonl=<kind>:<instance>:<path>
                                        Repeatable generic HistoryRawRecord JSONL source
  --lethe-source-backend=sqlite          Existing Personal Lake source backend
  --lethe-claude-ai-source-instance=<id> Explicit Claude AI partition source instance
  --lethe-residual-source-instance=<id>  Explicit residual LETHE partition source instance
  --lethe-direct-coding-source-policy=exclude
                                        Required: exclude sys:claude-code and sys:codex;
                                        their native archives are imported directly
  --lethe-source-database=<path>         Existing Personal Lake SQLite database
  --lethe-source-blob-dir=<path>         Existing Personal Lake blob directory
  --lethe-source-key-env=<name>          Environment variable containing its 32-byte hex key
  --lethe-source-routing-key-order=<order>
                                        Explicit source Lake routing keyspec order
  --lethe-source-page-size=<n>           Maximum observations held per source page
  --lethe-upstream-instance=<sys:id>=<instance>
                                        Repeatable upstream native identity mapping

SQLite execute backend:
  --backend=sqlite --sqlite-database=<path> --sqlite-blob-dir=<path> --sqlite-key-env=<name>

PostgreSQL execute backend:
  --backend=postgres --postgres-dsn-env=<name> --postgres-schema=<name> --postgres-role=<name>

The spool path must not exist. JSONL is processed one bounded record at a time and import
holds at most --max-resident-batch-records event requests in memory.
Dry-run prints counts, digests, cursors, and ownership only. It never prints message bodies or raw records.
Execute requires the exact manifest digest from a preceding dry-run and fails if the source tree changed.
--require-activation-source-set=true requires exactly one of every activation source kind.
";

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        print!("{HELP}");
        return Ok(());
    }
    let options = Options::parse(args)?;
    let spool = HistorySpool::create(&options.spool_database)?;
    spool.begin_scan()?;
    let mut scan_stats = ScanStats::default();
    let mut required_sources = Vec::new();

    if let Some(source) = &options.claude {
        let cutover_cursor = tree_cursor(&[("projects", source.root.as_path())])?;
        let source_manifest = source_manifest(
            HistorySourceKind::ClaudeCode,
            &source.source_instance,
            &cutover_cursor,
            &options.owner_id,
        );
        spool.insert_source(&source_manifest)?;
        required_sources.push(RequiredHistorySource {
            source_kind: HistorySourceKind::ClaudeCode,
            source_instance_id: source.source_instance.clone(),
        });
        let importer = ClaudeCodeImporter::new(SemVer::new("1.0.0"));
        importer.visit_native_root(&source.root, options.max_source_record_bytes, |record| {
            scan_stats.source_records += 1;
            let history = coding_history_record(
                HistorySourceKind::ClaudeCode,
                &source.source_instance,
                &options.owner_id,
                record,
            )
            .map_err(adapter_other)?;
            if spool
                .insert_record(&source_manifest, &history)
                .map_err(adapter_other)?
            {
                scan_stats.unique_records += 1;
            } else {
                scan_stats.duplicate_source_records += 1;
            }
            Ok(())
        })?;
    }
    if let Some(source) = &options.codex {
        let cursor_roots = codex_cursor_roots(&source.root);
        let borrowed = cursor_roots
            .iter()
            .map(|(label, root)| (*label, root.as_path()))
            .collect::<Vec<_>>();
        let cutover_cursor = tree_cursor(&borrowed)?;
        let source_manifest = source_manifest(
            HistorySourceKind::Codex,
            &source.source_instance,
            &cutover_cursor,
            &options.owner_id,
        );
        spool.insert_source(&source_manifest)?;
        required_sources.push(RequiredHistorySource {
            source_kind: HistorySourceKind::Codex,
            source_instance_id: source.source_instance.clone(),
        });
        let importer = CodexImporter::new(SemVer::new("1.0.0"));
        importer.visit_native_path(&source.root, options.max_source_record_bytes, |record| {
            scan_stats.source_records += 1;
            let history = coding_history_record(
                HistorySourceKind::Codex,
                &source.source_instance,
                &options.owner_id,
                record,
            )
            .map_err(adapter_other)?;
            if spool
                .insert_record(&source_manifest, &history)
                .map_err(adapter_other)?
            {
                scan_stats.unique_records += 1;
            } else {
                scan_stats.duplicate_source_records += 1;
            }
            Ok(())
        })?;
    }
    if let Some(source) = &options.lethe {
        if !source.database.is_file() {
            return Err(format!(
                "existing LETHE source database is not a file: {}",
                source.database.display()
            )
            .into());
        }
        if !source.blob_dir.is_dir() {
            return Err(format!(
                "existing LETHE source blob directory is not a directory: {}",
                source.blob_dir.display()
            )
            .into());
        }
        let key = decode_key_env(&source.key_env)?;
        let source_store = SqlitePersistence::open_with_routing_key_order(
            &source.database,
            &source.blob_dir,
            &key,
            source.routing_key_order,
        )?;
        let max_append_seq = source_store.observation_stats()?.max_append_seq;
        let cutover_cursor = lethe_observation_cutover_cursor(max_append_seq);
        let claude_ai_manifest = source_manifest(
            HistorySourceKind::ClaudeAi,
            &source.claude_ai_source_instance,
            &cutover_cursor,
            &options.owner_id,
        );
        let residual_manifest = source_manifest(
            HistorySourceKind::Lethe,
            &source.residual_source_instance,
            &cutover_cursor,
            &options.owner_id,
        );
        spool.insert_source(&claude_ai_manifest)?;
        spool.insert_source(&residual_manifest)?;
        required_sources.push(RequiredHistorySource {
            source_kind: HistorySourceKind::ClaudeAi,
            source_instance_id: source.claude_ai_source_instance.clone(),
        });
        required_sources.push(RequiredHistorySource {
            source_kind: HistorySourceKind::Lethe,
            source_instance_id: source.residual_source_instance.clone(),
        });
        visit_lethe_conversation_observations(
            &source_store,
            max_append_seq,
            source.page_size,
            &source.claude_ai_source_instance,
            &source.upstream_instances,
            |partition, history| {
                scan_stats.source_records += 1;
                let manifest = match partition {
                    LetheConversationPartition::ClaudeAi => &claude_ai_manifest,
                    LetheConversationPartition::ResidualLethe => &residual_manifest,
                };
                if spool
                    .insert_record(manifest, &history)
                    .map_err(|error| HistoryError::Invariant(error.to_string()))?
                {
                    scan_stats.unique_records += 1;
                } else {
                    scan_stats.duplicate_source_records += 1;
                }
                Ok(())
            },
        )?;
    }
    for source in &options.generic_sources {
        let cutover_cursor = file_cursor(&source.path)?;
        let source_manifest = source_manifest(
            source.source_kind,
            &source.source_instance,
            &cutover_cursor,
            &options.owner_id,
        );
        spool.insert_source(&source_manifest)?;
        required_sources.push(RequiredHistorySource {
            source_kind: source.source_kind,
            source_instance_id: source.source_instance.clone(),
        });
        visit_generic_jsonl(&source.path, options.max_source_record_bytes, |history| {
            scan_stats.source_records += 1;
            if spool.insert_record(&source_manifest, &history)? {
                scan_stats.unique_records += 1;
            } else {
                scan_stats.duplicate_source_records += 1;
            }
            Ok(())
        })?;
    }
    required_sources.sort_by(|left, right| {
        source_key(left.source_kind, &left.source_instance_id)
            .cmp(&source_key(right.source_kind, &right.source_instance_id))
    });
    spool.finish_scan()?;
    let plan = spool.build_plan(
        &options.inventory_id,
        &DataSpaceId::new(options.data_space_id.clone()),
        options.captured_at,
        &required_sources,
        scan_stats,
        options.max_handoff_session_entries,
    )?;
    if options.require_activation_source_set {
        validate_activation_source_set(&required_sources)?;
    }
    println!("{}", serde_json::to_string_pretty(&plan.safe_json())?);

    let Mode::Execute {
        expected_manifest_digest,
        max_blob_bytes,
        backend,
    } = &options.mode
    else {
        return Ok(());
    };
    if &plan.manifest_digest != expected_manifest_digest {
        return Err(format!(
            "history manifest mismatch: expected {expected_manifest_digest}, actual {}",
            plan.manifest_digest
        )
        .into());
    }
    let store = backend.connect(plan.data_space_id.clone())?;
    let (result, metrics) = execute_spool(
        &spool,
        &plan,
        store.as_ref(),
        *max_blob_bytes,
        options.max_resident_batch_records,
    )?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "receipt": result.receipt,
            "appended_messages": result.appended_messages,
            "duplicate_messages": result.duplicate_messages,
            "receipt_cursor": result.receipt_cursor,
            "receipt_was_duplicate": result.receipt_was_duplicate,
            "max_resident_batch_records": metrics.max_resident_batch_records,
            "activation_handoff": plan.activation_handoff,
        }))?
    );
    Ok(())
}

fn coding_history_record(
    source_kind: HistorySourceKind,
    source_instance: &str,
    owner_id: &str,
    record: BackboneHistoryRecord,
) -> Result<HistoryRawRecord, Box<dyn std::error::Error>> {
    let mut inventory = coding_agent_source_inventory(
        source_kind,
        source_instance,
        "streaming-spool",
        OwnershipAssignment::Personal {
            owner_id: owner_id.to_owned(),
        },
        std::slice::from_ref(&record),
    )?;
    inventory
        .records
        .pop()
        .ok_or_else(|| "coding-agent record conversion returned no record".into())
}

fn adapter_other(error: impl std::fmt::Display) -> AdapterError {
    AdapterError::Other(error.to_string())
}

#[derive(Default, Clone, Copy)]
struct ScanStats {
    source_records: u64,
    unique_records: u64,
    duplicate_source_records: u64,
}

struct HistorySpool {
    connection: Connection,
}

impl HistorySpool {
    fn create(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        if path.exists() {
            return Err(format!(
                "spool database already exists; provide a new path: {}",
                path.display()
            )
            .into());
        }
        let parent = path.parent().ok_or("spool database path has no parent")?;
        if !parent.is_dir() {
            return Err(format!(
                "spool parent directory does not exist: {}",
                parent.display()
            )
            .into());
        }
        let connection = Connection::open(path)?;
        connection.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = FULL;
            CREATE TABLE sources (
                source_key TEXT PRIMARY KEY,
                source_json TEXT NOT NULL
            );
            CREATE TABLE records (
                identity TEXT PRIMARY KEY,
                source_key TEXT NOT NULL REFERENCES sources(source_key),
                published_at TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                source_session_id TEXT NOT NULL,
                source_message_id TEXT NOT NULL,
                raw_sha256 TEXT NOT NULL,
                record_json TEXT NOT NULL,
                raw BLOB NOT NULL
            );
            CREATE INDEX records_canonical_order
                ON records(source_key, published_at, ordinal, source_session_id, source_message_id);
            CREATE TABLE upstream_provenance (
                identity TEXT NOT NULL,
                source_key TEXT NOT NULL REFERENCES sources(source_key),
                PRIMARY KEY(identity, source_key)
            );
            ",
        )?;
        Ok(Self { connection })
    }

    fn begin_scan(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.connection.execute_batch("BEGIN IMMEDIATE")?;
        Ok(())
    }

    fn finish_scan(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.connection.execute_batch("COMMIT")?;
        Ok(())
    }

    fn insert_source(
        &self,
        source: &HistorySourceManifest,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.connection.execute(
            "INSERT INTO sources(source_key, source_json) VALUES (?1, ?2)",
            params![
                source_key(source.source_kind, &source.source_instance_id),
                serde_json::to_string(source)?
            ],
        )?;
        Ok(())
    }

    fn insert_record(
        &self,
        source: &HistorySourceManifest,
        record: &HistoryRawRecord,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let identity = format!(
            "{}\0{}\0{}",
            source.source_instance_id, record.source_session_id, record.source_message_id
        );
        let raw_sha256 = hex::encode(Sha256::digest(&record.raw));
        let existing = self
            .connection
            .query_row(
                "SELECT raw_sha256 FROM records WHERE identity = ?1",
                [&identity],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if let Some(existing) = existing {
            if existing == raw_sha256 {
                return Ok(false);
            }
            return Err(format!("history source identity collision for {identity}").into());
        }
        let upstream_identity = history_upstream_identity(record)?;
        let source_key = source_key(source.source_kind, &source.source_instance_id);
        let ordinal =
            i64::try_from(record.ordinal).map_err(|_| "history ordinal exceeds SQLite INTEGER")?;
        let mut metadata = record.clone();
        let raw = std::mem::take(&mut metadata.raw);
        self.connection.execute(
            "INSERT INTO records(
                identity, source_key, published_at, ordinal, source_session_id,
                source_message_id, raw_sha256, record_json, raw
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            params![
                identity,
                &source_key,
                record.published_at.to_rfc3339(),
                ordinal,
                record.source_session_id,
                record.source_message_id,
                raw_sha256,
                serde_json::to_string(&metadata)?,
                raw,
            ],
        )?;
        if let Some(upstream_identity) = upstream_identity {
            self.connection.execute(
                "INSERT OR IGNORE INTO upstream_provenance(identity, source_key)
                 VALUES (?1, ?2)",
                params![upstream_identity, source_key],
            )?;
        }
        Ok(true)
    }

    fn build_plan(
        &self,
        inventory_id: &str,
        data_space_id: &DataSpaceId,
        captured_at: DateTime<Utc>,
        required_sources: &[RequiredHistorySource],
        scan_stats: ScanStats,
        max_handoff_session_entries: usize,
    ) -> Result<StreamingPlan, Box<dyn std::error::Error>> {
        let cross_source_overlap_identities = self.connection.query_row(
            "SELECT COUNT(*) FROM (
                SELECT identity FROM upstream_provenance
                GROUP BY identity HAVING COUNT(*) > 1
             )",
            [],
            |row| row.get::<_, u64>(0),
        )?;
        let mut digest = HistoryManifestDigestBuilder::new(
            inventory_id,
            data_space_id,
            captured_at,
            required_sources,
            cross_source_overlap_identities,
        )?;
        let sources = self.sources()?;
        let mut raw_bytes = 0_u64;
        let mut source_summaries = Vec::new();
        let mut open_commitments_digest = Sha256::new();
        open_commitments_digest.update(b"lethe-open-commitments-projection-v1\0");
        let mut current_state_digest = Sha256::new();
        current_state_digest.update(b"lethe-current-state-projection-v1\0");
        for source in &sources {
            digest.push_source(source)?;
            let mut source_digest = Sha256::new();
            source_digest.update(b"lethe-history-activation-source-v1\0");
            update_digest_json(
                &mut source_digest,
                &serde_json::json!({
                    "source_kind": source.source_kind,
                    "source_instance_id": source.source_instance_id,
                    "cutover_cursor": source.cutover_cursor,
                }),
            )?;
            let mut statement = self.connection.prepare(
                "SELECT record_json, raw FROM records WHERE source_key = ?1
                 ORDER BY published_at, ordinal, source_session_id, source_message_id",
            )?;
            let mut rows =
                statement.query([source_key(source.source_kind, &source.source_instance_id)])?;
            let mut records = 0_u64;
            let mut source_bytes = 0_u64;
            while let Some(row) = rows.next()? {
                let record = row_record(row)?;
                let entry = manifest_entry_for_raw_record(&record)?;
                digest.push_entry(&entry)?;
                update_digest_json(&mut source_digest, &entry)?;
                match &entry.record_kind {
                    HistoryRecordKind::Commitment { .. } => {
                        update_digest_json(&mut open_commitments_digest, &entry)?;
                    }
                    HistoryRecordKind::CurrentState { .. } => {
                        update_digest_json(&mut current_state_digest, &entry)?;
                    }
                    _ => {}
                }
                records += 1;
                raw_bytes = raw_bytes
                    .checked_add(entry.raw_bytes)
                    .ok_or("raw byte count overflow")?;
                source_bytes = source_bytes
                    .checked_add(entry.raw_bytes)
                    .ok_or("source raw byte count overflow")?;
            }
            source_summaries.push(SourceSummary {
                source_kind: source.source_kind,
                source_instance_id: source.source_instance_id.clone(),
                cutover_cursor: source.cutover_cursor.clone(),
                ownership: source.ownership.clone(),
                records,
                raw_bytes: source_bytes,
                source_digest: hex::encode(source_digest.finalize()),
            });
        }
        if scan_stats.unique_records
            != source_summaries
                .iter()
                .map(|source| source.records)
                .sum::<u64>()
        {
            return Err("spool unique record count mismatch".into());
        }
        let manifest_digest = digest.finish();
        let sessions = self.activation_sessions(max_handoff_session_entries)?;
        let session_index_ref = projection_ref("session-index", &sessions)?;
        let open_commitments_ref = format!(
            "history-projection:open-commitments:sha256:{}",
            hex::encode(open_commitments_digest.finalize())
        );
        let current_state_ref = format!(
            "history-projection:current-state:sha256:{}",
            hex::encode(current_state_digest.finalize())
        );
        let activation_handoff = HistoryActivationHandoff {
            schema: HISTORY_ACTIVATION_HANDOFF_SCHEMA.to_owned(),
            schema_version: HISTORY_ACTIVATION_HANDOFF_VERSION.to_owned(),
            inventory_id: inventory_id.to_owned(),
            data_space_id: data_space_id.clone(),
            manifest_digest: manifest_digest.clone(),
            record_count: scan_stats.unique_records,
            raw_bytes,
            cross_source_overlap_identities,
            sources: source_summaries
                .iter()
                .map(|source| HistoryActivationSource {
                    source_id: source_key(source.source_kind, &source.source_instance_id),
                    source_kind: source.source_kind,
                    ownership: match &source.ownership {
                        OwnershipAssignment::Personal { .. } => "personal".to_owned(),
                        OwnershipAssignment::Unresolved { .. } => "unresolved".to_owned(),
                    },
                    owner_id: match &source.ownership {
                        OwnershipAssignment::Personal { owner_id } => Some(owner_id.clone()),
                        OwnershipAssignment::Unresolved { .. } => None,
                    },
                    record_count: source.records,
                    raw_bytes: source.raw_bytes,
                    digest_sha256: source.source_digest.clone(),
                    cutover_cursor: source.cutover_cursor.clone(),
                })
                .collect(),
            session_count: u64::try_from(sessions.len())?,
            sessions,
            session_index_ref,
            open_commitments_ref,
            current_state_ref,
        };
        activation_handoff.validate()?;
        Ok(StreamingPlan {
            inventory_id: inventory_id.to_owned(),
            data_space_id: data_space_id.clone(),
            captured_at,
            sources,
            source_summaries,
            source_records: scan_stats.source_records,
            unique_records: scan_stats.unique_records,
            duplicate_source_records: scan_stats.duplicate_source_records,
            cross_source_overlap_identities,
            raw_bytes,
            manifest_digest,
            activation_handoff,
        })
    }

    fn activation_sessions(
        &self,
        max_entries: usize,
    ) -> Result<Vec<HistoryActivationSessionEntry>, Box<dyn std::error::Error>> {
        if max_entries == 0 {
            return Err("max_handoff_session_entries must be positive".into());
        }
        let query_limit = i64::try_from(
            max_entries
                .checked_add(1)
                .ok_or("max_handoff_session_entries overflow")?,
        )?;
        let mut statement = self.connection.prepare(
            "SELECT s.source_json, r.source_session_id,
                    MIN(r.published_at), MAX(r.published_at), COUNT(*)
             FROM records r JOIN sources s ON s.source_key = r.source_key
             GROUP BY r.source_key, r.source_session_id
             ORDER BY r.source_key, r.source_session_id
             LIMIT ?1",
        )?;
        let mut rows = statement.query([query_limit])?;
        let mut sessions = Vec::new();
        while let Some(row) = rows.next()? {
            if sessions.len() == max_entries {
                return Err(format!(
                    "activation session index exceeds max_handoff_session_entries {max_entries}"
                )
                .into());
            }
            let source = serde_json::from_str::<HistorySourceManifest>(&row.get::<_, String>(0)?)?;
            let source_session_id: String = row.get(1)?;
            let first_message_at =
                DateTime::parse_from_rfc3339(&row.get::<_, String>(2)?)?.to_utc();
            let last_message_at = DateTime::parse_from_rfc3339(&row.get::<_, String>(3)?)?.to_utc();
            sessions.push(HistoryActivationSessionEntry {
                session_ref: history_session_reference(
                    &source.source_instance_id,
                    &source_session_id,
                )?,
                source_kind: source.source_kind,
                source_id: source_key(source.source_kind, &source.source_instance_id),
                source_session_id,
                first_message_at,
                last_message_at,
                message_count: row.get(4)?,
            });
        }
        Ok(sessions)
    }

    fn sources(&self) -> Result<Vec<HistorySourceManifest>, Box<dyn std::error::Error>> {
        let mut statement = self
            .connection
            .prepare("SELECT source_json FROM sources ORDER BY source_key")?;
        let sources = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .map(|row| Ok(serde_json::from_str::<HistorySourceManifest>(&row?)?))
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
        Ok(sources)
    }
}

fn row_record(row: &rusqlite::Row<'_>) -> Result<HistoryRawRecord, Box<dyn std::error::Error>> {
    let json: String = row.get(0)?;
    let mut record: HistoryRawRecord = serde_json::from_str(&json)?;
    record.raw = row.get(1)?;
    Ok(record)
}

struct StreamingPlan {
    inventory_id: String,
    data_space_id: DataSpaceId,
    captured_at: DateTime<Utc>,
    sources: Vec<HistorySourceManifest>,
    source_summaries: Vec<SourceSummary>,
    source_records: u64,
    unique_records: u64,
    duplicate_source_records: u64,
    cross_source_overlap_identities: u64,
    raw_bytes: u64,
    manifest_digest: String,
    activation_handoff: HistoryActivationHandoff,
}

struct SourceSummary {
    source_kind: HistorySourceKind,
    source_instance_id: String,
    cutover_cursor: String,
    ownership: OwnershipAssignment,
    records: u64,
    raw_bytes: u64,
    source_digest: String,
}

impl StreamingPlan {
    fn safe_json(&self) -> serde_json::Value {
        serde_json::json!({
            "inventory_id": self.inventory_id,
            "data_space_id": self.data_space_id,
            "captured_at": self.captured_at,
            "manifest_digest": self.manifest_digest,
            "source_records": self.source_records,
            "unique_records": self.unique_records,
            "duplicate_source_records": self.duplicate_source_records,
            "cross_source_overlap_identities": self.cross_source_overlap_identities,
            "raw_bytes": self.raw_bytes,
            "unresolved_sources": [],
            "ready_for_import": self.cross_source_overlap_identities == 0,
            "activation_handoff": self.activation_handoff,
            "sources": self.source_summaries.iter().map(|source| serde_json::json!({
                "source_kind": source.source_kind,
                "source_instance_id": source.source_instance_id,
                "cutover_cursor": source.cutover_cursor,
                "ownership": source.ownership,
                "records": source.records,
                "raw_bytes": source.raw_bytes,
            })).collect::<Vec<_>>(),
        })
    }
}

#[derive(Default)]
struct ImportMetrics {
    max_resident_batch_records: usize,
}

fn execute_spool<T: OperationalStoragePorts + ?Sized>(
    spool: &HistorySpool,
    plan: &StreamingPlan,
    store: &T,
    max_blob_bytes: usize,
    max_resident_batch_records: usize,
) -> Result<(HistoryImportResult, ImportMetrics), Box<dyn std::error::Error>> {
    if store.data_space_id() != &plan.data_space_id {
        return Err(format!(
            "history plan data space {} does not match Lake {}",
            plan.data_space_id,
            store.data_space_id()
        )
        .into());
    }
    if max_blob_bytes == 0 || max_resident_batch_records == 0 {
        return Err("blob and resident batch limits must be positive".into());
    }
    if plan.cross_source_overlap_identities > 0 {
        return Err(format!(
            "history import blocked by {} cross-source native identity overlaps",
            plan.cross_source_overlap_identities
        )
        .into());
    }
    let mut appended_messages = 0_u64;
    let mut duplicate_messages = 0_u64;
    let mut metrics = ImportMetrics::default();
    for source in &plan.sources {
        let owner_id = match &source.ownership {
            OwnershipAssignment::Personal { owner_id } => owner_id,
            OwnershipAssignment::Unresolved { reason } => {
                return Err(format!("history ownership unresolved: {reason}").into());
            }
        };
        let mut statement = spool.connection.prepare(
            "SELECT record_json, raw FROM records WHERE source_key = ?1
             ORDER BY published_at, ordinal, source_session_id, source_message_id",
        )?;
        let mut rows =
            statement.query([source_key(source.source_kind, &source.source_instance_id)])?;
        let mut records = Vec::with_capacity(max_resident_batch_records);
        while let Some(row) = rows.next()? {
            records.push(row_record(row)?);
            metrics.max_resident_batch_records =
                metrics.max_resident_batch_records.max(records.len());
            if records.len() == max_resident_batch_records {
                apply_record_batch(
                    store,
                    source,
                    owner_id,
                    &mut records,
                    max_blob_bytes,
                    &mut appended_messages,
                    &mut duplicate_messages,
                )?;
            }
        }
        apply_record_batch(
            store,
            source,
            owner_id,
            &mut records,
            max_blob_bytes,
            &mut appended_messages,
            &mut duplicate_messages,
        )?;
    }
    let cutover_cursors = plan
        .sources
        .iter()
        .map(|source| {
            (
                source_key(source.source_kind, &source.source_instance_id),
                source.cutover_cursor.clone(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let receipt = HistoryImportReceipt {
        receipt_id: format!("history-receipt:{}", plan.manifest_digest),
        inventory_id: plan.inventory_id.clone(),
        data_space_id: plan.data_space_id.clone(),
        manifest_digest: plan.manifest_digest.clone(),
        captured_at: plan.captured_at,
        source_count: u64::try_from(plan.sources.len())?,
        message_count: plan.unique_records,
        raw_bytes: plan.raw_bytes,
        cross_source_overlap_identities: plan.cross_source_overlap_identities,
        cutover_cursors,
    };
    let receipt_outcome = store.append_operational_event(&prepare_history_receipt_request(
        store.data_space_id(),
        &receipt,
    )?)?;
    let (receipt_cursor, receipt_was_duplicate) = match receipt_outcome {
        OperationalAppendOutcome::Appended { cursor, .. } => (cursor, false),
        OperationalAppendOutcome::Duplicate { cursor, .. } => (cursor, true),
        OperationalAppendOutcome::VersionConflict { expected, actual } => {
            return Err(format!(
                "unexpected history receipt conflict: expected {expected}, actual {actual}"
            )
            .into());
        }
    };
    Ok((
        HistoryImportResult {
            receipt,
            appended_messages,
            duplicate_messages,
            receipt_cursor,
            receipt_was_duplicate,
        },
        metrics,
    ))
}

fn apply_record_batch<T: OperationalStoragePorts + ?Sized>(
    store: &T,
    source: &HistorySourceManifest,
    owner_id: &str,
    records: &mut Vec<HistoryRawRecord>,
    max_blob_bytes: usize,
    appended: &mut u64,
    duplicates: &mut u64,
) -> Result<(), Box<dyn std::error::Error>> {
    if records.is_empty() {
        return Ok(());
    }
    let raw = records
        .iter()
        .map(|record| record.raw.as_slice())
        .collect::<Vec<_>>();
    let blob_refs = store.put_blobs(&raw, max_blob_bytes)?;
    if blob_refs.len() != records.len() {
        return Err("blob store returned the wrong history batch reference count".into());
    }
    let mut requests = Vec::with_capacity(records.len());
    for (record, blob_ref) in records.iter().zip(blob_refs) {
        let entry = manifest_entry_for_raw_record(record)?;
        requests.push(prepare_history_message_request(
            store.data_space_id(),
            source,
            owner_id,
            &entry,
            blob_ref,
        )?);
    }
    apply_message_batch(store, &mut requests, appended, duplicates)?;
    records.clear();
    Ok(())
}

fn apply_message_batch<T: OperationalStoragePorts + ?Sized>(
    store: &T,
    requests: &mut Vec<lethe_storage_api::OperationalAppendRequest>,
    appended: &mut u64,
    duplicates: &mut u64,
) -> Result<(), Box<dyn std::error::Error>> {
    if requests.is_empty() {
        return Ok(());
    }
    let outcomes = store.append_operational_events(requests)?;
    if outcomes.len() != requests.len() {
        return Err("operational store returned the wrong history batch outcome count".into());
    }
    for outcome in outcomes {
        match outcome {
            OperationalAppendOutcome::Appended { .. } => *appended += 1,
            OperationalAppendOutcome::Duplicate { .. } => *duplicates += 1,
            OperationalAppendOutcome::VersionConflict { expected, actual } => {
                return Err(format!(
                    "unexpected history message conflict: expected {expected}, actual {actual}"
                )
                .into());
            }
        }
    }
    requests.clear();
    Ok(())
}

fn source_manifest(
    source_kind: HistorySourceKind,
    source_instance_id: &str,
    cutover_cursor: &str,
    owner_id: &str,
) -> HistorySourceManifest {
    HistorySourceManifest {
        source_kind,
        source_instance_id: source_instance_id.to_owned(),
        cutover_cursor: cutover_cursor.to_owned(),
        ownership: OwnershipAssignment::Personal {
            owner_id: owner_id.to_owned(),
        },
        records: vec![],
    }
}

fn update_digest_json(
    digest: &mut Sha256,
    value: &impl serde::Serialize,
) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = serde_json::to_vec(value)?;
    digest.update(u64::try_from(bytes.len())?.to_le_bytes());
    digest.update(bytes);
    Ok(())
}

fn projection_ref(
    projection: &str,
    value: &impl serde::Serialize,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut digest = Sha256::new();
    digest.update(format!("lethe-{projection}-projection-v1\0").as_bytes());
    update_digest_json(&mut digest, value)?;
    Ok(format!(
        "history-projection:{projection}:sha256:{}",
        hex::encode(digest.finalize())
    ))
}

struct Options {
    mode: Mode,
    inventory_id: String,
    data_space_id: String,
    owner_id: String,
    captured_at: DateTime<Utc>,
    spool_database: PathBuf,
    max_source_record_bytes: usize,
    max_resident_batch_records: usize,
    max_handoff_session_entries: usize,
    require_activation_source_set: bool,
    claude: Option<NativeSource>,
    codex: Option<NativeSource>,
    generic_sources: Vec<GenericSource>,
    lethe: Option<LetheSource>,
}

struct NativeSource {
    root: PathBuf,
    source_instance: String,
}

struct GenericSource {
    source_kind: HistorySourceKind,
    source_instance: String,
    path: PathBuf,
}

struct LetheSource {
    claude_ai_source_instance: String,
    residual_source_instance: String,
    database: PathBuf,
    blob_dir: PathBuf,
    key_env: String,
    routing_key_order: RoutingKeyOrder,
    page_size: usize,
    upstream_instances: BTreeMap<String, String>,
}

enum Mode {
    DryRun,
    Execute {
        expected_manifest_digest: String,
        max_blob_bytes: usize,
        backend: Backend,
    },
}

enum Backend {
    Sqlite {
        database: PathBuf,
        blob_dir: PathBuf,
        key_env: String,
    },
    Postgres {
        dsn_env: String,
        schema: String,
        role: String,
    },
}

impl Backend {
    fn connect(
        &self,
        data_space_id: DataSpaceId,
    ) -> Result<Box<dyn OperationalStoragePorts>, Box<dyn std::error::Error>> {
        match self {
            Self::Sqlite {
                database,
                blob_dir,
                key_env,
            } => {
                let decoded = hex::decode(required_env(key_env)?).map_err(|error| {
                    format!("{key_env} must contain 64 hex characters: {error}")
                })?;
                let key: [u8; 32] = decoded.try_into().map_err(|bytes: Vec<u8>| {
                    format!("{key_env} must decode to 32 bytes, got {}", bytes.len())
                })?;
                Ok(Box::new(SqliteOperationalEventStore::open(
                    data_space_id,
                    database,
                    blob_dir,
                    &key,
                )?))
            }
            Self::Postgres {
                dsn_env,
                schema,
                role,
            } => Ok(Box::new(PostgresOperationalEventStore::connect_no_tls(
                data_space_id,
                &required_env(dsn_env)?,
                schema,
                role,
            )?)),
        }
    }
}

impl Options {
    fn parse(args: Vec<String>) -> Result<Self, Box<dyn std::error::Error>> {
        let mut values = BTreeMap::new();
        let mut generic_sources = Vec::new();
        let mut lethe_upstream_instances = BTreeMap::new();
        for arg in args {
            let Some((name, value)) = arg.split_once('=') else {
                return Err(format!("argument must use --name=value syntax: {arg}").into());
            };
            if !name.starts_with("--") || value.trim().is_empty() {
                return Err(format!("invalid argument: {arg}").into());
            }
            if name == "--history-jsonl" {
                generic_sources.push(parse_generic_source(value)?);
                continue;
            }
            if name == "--lethe-upstream-instance" {
                let (source_system, source_instance) = value.split_once('=').ok_or(
                    "--lethe-upstream-instance must use <source-system>=<source-instance>",
                )?;
                if !matches!(
                    source_system,
                    "sys:claude"
                        | "sys:claude-ai"
                        | "sys:chatgpt"
                        | "sys:claude-code"
                        | "sys:codex"
                        | "sys:slack"
                        | "sys:discord"
                ) || source_instance.trim().is_empty()
                {
                    return Err(
                        format!("invalid --lethe-upstream-instance mapping: {value}").into(),
                    );
                }
                if lethe_upstream_instances
                    .insert(source_system.to_owned(), source_instance.to_owned())
                    .is_some()
                {
                    return Err(
                        format!("duplicate --lethe-upstream-instance for {source_system}").into(),
                    );
                }
                continue;
            }
            if values.insert(name.to_owned(), value.to_owned()).is_some() {
                return Err(format!("duplicate argument: {name}").into());
            }
        }
        let mode_name = take_required(&mut values, "--mode")?;
        let inventory_id = take_required(&mut values, "--inventory-id")?;
        let data_space_id = take_required(&mut values, "--data-space-id")?;
        let owner_id = take_required(&mut values, "--owner-id")?;
        let captured_at =
            DateTime::parse_from_rfc3339(&take_required(&mut values, "--captured-at")?)
                .map_err(|error| format!("--captured-at must be RFC3339: {error}"))?
                .to_utc();
        let spool_database = PathBuf::from(take_required(&mut values, "--spool-database")?);
        let max_source_record_bytes = positive_usize(&mut values, "--max-source-record-bytes")?;
        let max_resident_batch_records =
            positive_usize(&mut values, "--max-resident-batch-records")?;
        let max_handoff_session_entries =
            positive_usize(&mut values, "--max-handoff-session-entries")?;
        let require_activation_source_set =
            optional_exact_bool(&mut values, "--require-activation-source-set")?.unwrap_or(false);
        let claude = take_source(&mut values, "--claude-root", "--claude-source-instance")?;
        let codex = take_source(&mut values, "--codex-root", "--codex-source-instance")?;
        let lethe = take_lethe_source(&mut values, lethe_upstream_instances)?;
        if claude.is_none() && codex.is_none() && lethe.is_none() && generic_sources.is_empty() {
            return Err("at least one native history source is required".into());
        }
        let mode = match mode_name.as_str() {
            "dry-run" => Mode::DryRun,
            "execute" => {
                let expected_manifest_digest =
                    take_required(&mut values, "--expected-manifest-digest")?;
                if expected_manifest_digest.len() != 64
                    || !expected_manifest_digest
                        .bytes()
                        .all(|byte| byte.is_ascii_hexdigit())
                {
                    return Err("--expected-manifest-digest must be a SHA-256 hex digest".into());
                }
                let max_blob_bytes = positive_usize(&mut values, "--max-blob-bytes")?;
                let backend = match take_required(&mut values, "--backend")?.as_str() {
                    "sqlite" => Backend::Sqlite {
                        database: PathBuf::from(take_required(&mut values, "--sqlite-database")?),
                        blob_dir: PathBuf::from(take_required(&mut values, "--sqlite-blob-dir")?),
                        key_env: take_required(&mut values, "--sqlite-key-env")?,
                    },
                    "postgres" => Backend::Postgres {
                        dsn_env: take_required(&mut values, "--postgres-dsn-env")?,
                        schema: take_required(&mut values, "--postgres-schema")?,
                        role: take_required(&mut values, "--postgres-role")?,
                    },
                    other => {
                        return Err(
                            format!("--backend must be sqlite or postgres, got {other}").into()
                        );
                    }
                };
                Mode::Execute {
                    expected_manifest_digest,
                    max_blob_bytes,
                    backend,
                }
            }
            other => {
                return Err(format!("--mode must be dry-run or execute, got {other}").into());
            }
        };
        if let Some((name, _)) = values.first_key_value() {
            return Err(format!("unknown or mode-incompatible argument: {name}").into());
        }
        Ok(Self {
            mode,
            inventory_id,
            data_space_id,
            owner_id,
            captured_at,
            spool_database,
            max_source_record_bytes,
            max_resident_batch_records,
            max_handoff_session_entries,
            require_activation_source_set,
            claude,
            codex,
            generic_sources,
            lethe,
        })
    }
}

fn take_lethe_source(
    values: &mut BTreeMap<String, String>,
    upstream_instances: BTreeMap<String, String>,
) -> Result<Option<LetheSource>, Box<dyn std::error::Error>> {
    let names = [
        "--lethe-source-backend",
        "--lethe-claude-ai-source-instance",
        "--lethe-residual-source-instance",
        "--lethe-direct-coding-source-policy",
        "--lethe-source-database",
        "--lethe-source-blob-dir",
        "--lethe-source-key-env",
        "--lethe-source-routing-key-order",
        "--lethe-source-page-size",
    ];
    if !names.iter().any(|name| values.contains_key(*name)) {
        if !upstream_instances.is_empty() {
            return Err("--lethe-upstream-instance requires a LETHE source".into());
        }
        return Ok(None);
    }
    let backend = take_required(values, "--lethe-source-backend")?;
    if backend != "sqlite" {
        return Err(format!(
            "--lethe-source-backend must be sqlite for the existing Personal Lake, got {backend}"
        )
        .into());
    }
    let claude_ai_source_instance = take_required(values, "--lethe-claude-ai-source-instance")?;
    let residual_source_instance = take_required(values, "--lethe-residual-source-instance")?;
    if claude_ai_source_instance == residual_source_instance {
        return Err(
            "--lethe-claude-ai-source-instance and --lethe-residual-source-instance must differ"
                .into(),
        );
    }
    if take_required(values, "--lethe-direct-coding-source-policy")? != "exclude" {
        return Err("--lethe-direct-coding-source-policy must be exclude".into());
    }
    for source_system in [
        "sys:claude",
        "sys:claude-ai",
        "sys:claude-code",
        "sys:codex",
    ] {
        if upstream_instances.contains_key(source_system) {
            return Err(format!(
                "--lethe-upstream-instance must not map {source_system}; its partition is explicit"
            )
            .into());
        }
    }
    Ok(Some(LetheSource {
        claude_ai_source_instance,
        residual_source_instance,
        database: PathBuf::from(take_required(values, "--lethe-source-database")?),
        blob_dir: PathBuf::from(take_required(values, "--lethe-source-blob-dir")?),
        key_env: take_required(values, "--lethe-source-key-env")?,
        routing_key_order: match take_required(values, "--lethe-source-routing-key-order")?.as_str()
        {
            "month_year_source_container_published" => {
                RoutingKeyOrder::MonthYearSourceContainerPublished
            }
            "year_month_source_container_published" => {
                RoutingKeyOrder::YearMonthSourceContainerPublished
            }
            other => {
                return Err(format!(
                    "--lethe-source-routing-key-order has unsupported value {other}"
                )
                .into());
            }
        },
        page_size: positive_usize(values, "--lethe-source-page-size")?,
        upstream_instances,
    }))
}

fn parse_generic_source(value: &str) -> Result<GenericSource, Box<dyn std::error::Error>> {
    let parts = value.splitn(3, ':').collect::<Vec<_>>();
    if parts.len() != 3 || parts.iter().any(|part| part.trim().is_empty()) {
        return Err("--history-jsonl must use <source_kind>:<source_instance_id>:<path>".into());
    }
    let source_kind = match parts[0] {
        "claude_code" => HistorySourceKind::ClaudeCode,
        "claude_ai" => HistorySourceKind::ClaudeAi,
        "codex" => HistorySourceKind::Codex,
        "intercom" => HistorySourceKind::Intercom,
        "lethe" => HistorySourceKind::Lethe,
        "nanihold_legacy" => HistorySourceKind::NaniholdLegacy,
        "system_snapshot" => HistorySourceKind::SystemSnapshot,
        other => return Err(format!("unknown history source kind: {other}").into()),
    };
    Ok(GenericSource {
        source_kind,
        source_instance: parts[1].to_owned(),
        path: PathBuf::from(parts[2]),
    })
}

fn positive_usize(
    values: &mut BTreeMap<String, String>,
    name: &str,
) -> Result<usize, Box<dyn std::error::Error>> {
    let value = take_required(values, name)?
        .parse::<usize>()
        .map_err(|error| format!("{name} must be positive: {error}"))?;
    if value == 0 {
        Err(format!("{name} must be positive").into())
    } else {
        Ok(value)
    }
}

fn optional_exact_bool(
    values: &mut BTreeMap<String, String>,
    name: &str,
) -> Result<Option<bool>, Box<dyn std::error::Error>> {
    let Some(value) = values.remove(name) else {
        return Ok(None);
    };
    match value.as_str() {
        "true" => Ok(Some(true)),
        "false" => Ok(Some(false)),
        _ => Err(format!("{name} must be true or false").into()),
    }
}

fn validate_activation_source_set(
    sources: &[RequiredHistorySource],
) -> Result<(), Box<dyn std::error::Error>> {
    let expected = [
        HistorySourceKind::ClaudeCode,
        HistorySourceKind::ClaudeAi,
        HistorySourceKind::Codex,
        HistorySourceKind::Intercom,
        HistorySourceKind::Lethe,
        HistorySourceKind::NaniholdLegacy,
        HistorySourceKind::SystemSnapshot,
    ];
    if sources.len() != expected.len() {
        return Err(format!(
            "activation source set requires exactly {} sources, got {}",
            expected.len(),
            sources.len()
        )
        .into());
    }
    for kind in expected {
        if sources
            .iter()
            .filter(|source| source.source_kind == kind)
            .count()
            != 1
        {
            return Err(format!(
                "activation source set requires exactly one {} source",
                source_key(kind, "<instance>")
            )
            .into());
        }
    }
    Ok(())
}

fn take_source(
    values: &mut BTreeMap<String, String>,
    root_name: &str,
    instance_name: &str,
) -> Result<Option<NativeSource>, Box<dyn std::error::Error>> {
    match (values.remove(root_name), values.remove(instance_name)) {
        (Some(root), Some(source_instance)) => Ok(Some(NativeSource {
            root: PathBuf::from(root),
            source_instance,
        })),
        (None, None) => Ok(None),
        _ => Err(format!("{root_name} and {instance_name} must be provided together").into()),
    }
}

fn take_required(
    values: &mut BTreeMap<String, String>,
    name: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    values
        .remove(name)
        .ok_or_else(|| format!("missing required argument {name}").into())
}

fn required_env(name: &str) -> Result<String, Box<dyn std::error::Error>> {
    let value = env::var(name).map_err(|_| format!("missing environment variable {name}"))?;
    if value.trim().is_empty() {
        Err(format!("environment variable {name} must not be blank").into())
    } else {
        Ok(value)
    }
}

fn decode_key_env(name: &str) -> Result<[u8; 32], Box<dyn std::error::Error>> {
    let decoded =
        hex::decode(required_env(name)?).map_err(|error| format!("{name} must be hex: {error}"))?;
    decoded.try_into().map_err(|bytes: Vec<u8>| {
        format!("{name} must decode to 32 bytes, got {}", bytes.len()).into()
    })
}

fn codex_cursor_roots(root: &Path) -> Vec<(&'static str, PathBuf)> {
    let sessions = root.join("sessions");
    if sessions.is_dir() {
        let mut roots = vec![("sessions", sessions)];
        let archived = root.join("archived_sessions");
        if archived.is_dir() {
            roots.push(("archived_sessions", archived));
        }
        roots
    } else {
        vec![("codex", root.to_path_buf())]
    }
}

fn tree_cursor(roots: &[(&str, &Path)]) -> Result<String, Box<dyn std::error::Error>> {
    let mut files = Vec::new();
    for (label, root) in roots {
        if !root.is_dir() {
            return Err(
                format!("native source root is not a directory: {}", root.display()).into(),
            );
        }
        collect_jsonl_files(label, root, root, &mut files)?;
    }
    files.sort_by(|left, right| left.0.cmp(&right.0));
    if files.is_empty() {
        return Err("native source contains no jsonl files".into());
    }
    let mut tree = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    for (relative, path) in files {
        let metadata = fs::metadata(&path)?;
        let mut file_digest = Sha256::new();
        let mut reader = BufReader::new(fs::File::open(&path)?);
        loop {
            let read = reader.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            file_digest.update(&buffer[..read]);
        }
        tree.update(relative.as_bytes());
        tree.update([0]);
        tree.update(metadata.len().to_le_bytes());
        tree.update(file_digest.finalize());
    }
    Ok(format!(
        "native-tree:sha256:{}",
        hex::encode(tree.finalize())
    ))
}

fn file_cursor(path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    if !path.is_file() {
        return Err(format!("generic history JSONL is not a file: {}", path.display()).into());
    }
    let mut digest = Sha256::new();
    let mut reader = BufReader::new(fs::File::open(path)?);
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("file:sha256:{}", hex::encode(digest.finalize())))
}

fn visit_generic_jsonl<F>(
    path: &Path,
    max_record_bytes: usize,
    mut visitor: F,
) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnMut(HistoryRawRecord) -> Result<(), Box<dyn std::error::Error>>,
{
    use std::io::BufRead;

    let reader = BufReader::new(fs::File::open(path)?);
    for (index, line) in reader.split(b'\n').enumerate() {
        let mut raw = line?;
        if raw.last() == Some(&b'\r') {
            raw.pop();
        }
        if raw.is_empty() {
            continue;
        }
        if raw.len() > max_record_bytes {
            return Err(format!(
                "generic history record exceeds max_source_record_bytes at {}:{}: {} > {max_record_bytes}",
                path.display(),
                index + 1,
                raw.len()
            )
            .into());
        }
        let record = serde_json::from_slice::<HistoryRawRecord>(&raw).map_err(|error| {
            format!(
                "invalid HistoryRawRecord JSON at {}:{}: {error}",
                path.display(),
                index + 1
            )
        })?;
        visitor(record)?;
    }
    Ok(())
}

fn collect_jsonl_files(
    label: &str,
    root: &Path,
    path: &Path,
    files: &mut Vec<(String, PathBuf)>,
) -> Result<(), Box<dyn std::error::Error>> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            collect_jsonl_files(label, root, &path, files)?;
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("jsonl") {
            let relative = path
                .strip_prefix(root)?
                .to_string_lossy()
                .replace('\\', "/");
            files.push((format!("{label}/{relative}"), path));
        }
    }
    Ok(())
}

fn source_key(kind: HistorySourceKind, instance: &str) -> String {
    let kind = match kind {
        HistorySourceKind::ClaudeCode => "claude_code",
        HistorySourceKind::ClaudeAi => "claude_ai",
        HistorySourceKind::Codex => "codex",
        HistorySourceKind::Intercom => "intercom",
        HistorySourceKind::Lethe => "lethe",
        HistorySourceKind::NaniholdLegacy => "nanihold_legacy",
        HistorySourceKind::SystemSnapshot => "system_snapshot",
    };
    format!("{kind}:{instance}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use lethe_history::{HistoryRecordKind, HistorySourceManifest};

    #[test]
    fn execute_requires_explicit_memory_bounds() {
        let error = match Options::parse(vec![
            "--mode=execute".to_owned(),
            "--inventory-id=inventory:test".to_owned(),
            "--data-space-id=space:personal".to_owned(),
            "--owner-id=owner".to_owned(),
            "--captured-at=2026-07-20T00:00:00Z".to_owned(),
            "--spool-database=C:\\spool.sqlite3".to_owned(),
            "--codex-root=C:\\codex".to_owned(),
            "--codex-source-instance=codex-personal".to_owned(),
        ]) {
            Ok(_) => panic!("execute without memory bounds must fail"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("--max-source-record-bytes"));
    }

    #[test]
    fn repeatable_generic_sources_are_parsed_and_unknown_kind_fails() {
        let options = Options::parse(vec![
            "--mode=dry-run".to_owned(),
            "--inventory-id=inventory:test".to_owned(),
            "--data-space-id=space:personal".to_owned(),
            "--owner-id=owner".to_owned(),
            "--captured-at=2026-07-20T00:00:00Z".to_owned(),
            "--spool-database=C:\\spool.sqlite3".to_owned(),
            "--max-source-record-bytes=1024".to_owned(),
            "--max-resident-batch-records=2".to_owned(),
            "--max-handoff-session-entries=100".to_owned(),
            "--history-jsonl=intercom:intercom-personal:C:\\intercom.jsonl".to_owned(),
            "--history-jsonl=nanihold_legacy:nanihold-personal:C:\\legacy.jsonl".to_owned(),
        ])
        .unwrap();
        assert_eq!(options.generic_sources.len(), 2);

        assert!(
            Options::parse(vec![
                "--mode=dry-run".to_owned(),
                "--inventory-id=inventory:test".to_owned(),
                "--data-space-id=space:personal".to_owned(),
                "--owner-id=owner".to_owned(),
                "--captured-at=2026-07-20T00:00:00Z".to_owned(),
                "--spool-database=C:\\spool.sqlite3".to_owned(),
                "--max-source-record-bytes=1024".to_owned(),
                "--max-resident-batch-records=2".to_owned(),
                "--max-handoff-session-entries=100".to_owned(),
                "--history-jsonl=unknown:source:C:\\bad.jsonl".to_owned(),
            ])
            .is_err()
        );
    }

    #[test]
    fn activation_source_set_gate_requires_explicit_boolean() {
        let mut args = vec![
            "--mode=dry-run".to_owned(),
            "--inventory-id=inventory:test".to_owned(),
            "--data-space-id=space:personal".to_owned(),
            "--owner-id=owner".to_owned(),
            "--captured-at=2026-07-20T00:00:00Z".to_owned(),
            "--spool-database=C:\\spool.sqlite3".to_owned(),
            "--max-source-record-bytes=1024".to_owned(),
            "--max-resident-batch-records=2".to_owned(),
            "--max-handoff-session-entries=100".to_owned(),
            "--history-jsonl=intercom:intercom-personal:C:\\intercom.jsonl".to_owned(),
            "--require-activation-source-set=true".to_owned(),
        ];
        assert!(
            Options::parse(args.clone())
                .unwrap()
                .require_activation_source_set
        );
        args.pop();
        args.push("--require-activation-source-set=yes".to_owned());
        assert!(Options::parse(args).is_err());
    }

    #[test]
    fn existing_lethe_source_requires_explicit_backend_location_key_and_page_size() {
        let base = vec![
            "--mode=dry-run".to_owned(),
            "--inventory-id=inventory:test".to_owned(),
            "--data-space-id=space:personal".to_owned(),
            "--owner-id=owner".to_owned(),
            "--captured-at=2026-07-20T00:00:00Z".to_owned(),
            "--spool-database=C:\\spool.sqlite3".to_owned(),
            "--max-source-record-bytes=1024".to_owned(),
            "--max-resident-batch-records=2".to_owned(),
            "--max-handoff-session-entries=100".to_owned(),
            "--lethe-source-backend=sqlite".to_owned(),
        ];
        let error = Options::parse(base).err().unwrap().to_string();
        assert!(error.contains("--lethe-claude-ai-source-instance"));
    }

    #[test]
    fn existing_lethe_partitions_are_explicit_and_do_not_accept_coding_mappings() {
        let mut args = vec![
            "--mode=dry-run".to_owned(),
            "--inventory-id=inventory:test".to_owned(),
            "--data-space-id=space:personal".to_owned(),
            "--owner-id=owner".to_owned(),
            "--captured-at=2026-07-20T00:00:00Z".to_owned(),
            "--spool-database=C:\\spool.sqlite3".to_owned(),
            "--max-source-record-bytes=1024".to_owned(),
            "--max-resident-batch-records=2".to_owned(),
            "--max-handoff-session-entries=100".to_owned(),
            "--lethe-source-backend=sqlite".to_owned(),
            "--lethe-claude-ai-source-instance=claude-ai-personal".to_owned(),
            "--lethe-residual-source-instance=lethe-personal".to_owned(),
            "--lethe-direct-coding-source-policy=exclude".to_owned(),
            "--lethe-source-database=C:\\lethe.sqlite3".to_owned(),
            "--lethe-source-blob-dir=C:\\blobs".to_owned(),
            "--lethe-source-key-env=LETHE_SOURCE_KEY".to_owned(),
            "--lethe-source-routing-key-order=year_month_source_container_published".to_owned(),
            "--lethe-source-page-size=100".to_owned(),
        ];
        assert!(Options::parse(args.clone()).is_ok());
        args.push("--lethe-upstream-instance=sys:codex=codex-personal".to_owned());
        assert!(
            Options::parse(args)
                .err()
                .unwrap()
                .to_string()
                .contains("must not map sys:codex")
        );
    }

    #[test]
    fn generic_jsonl_rejects_invalid_record_and_identity_collision() {
        let root = test_root("generic");
        fs::create_dir_all(&root).unwrap();
        let invalid = root.join("invalid.jsonl");
        fs::write(&invalid, b"{}\n").unwrap();
        assert!(visit_generic_jsonl(&invalid, 1024, |_| Ok(())).is_err());

        let spool = HistorySpool::create(&root.join("spool.sqlite3")).unwrap();
        let source = source_manifest(
            HistorySourceKind::Intercom,
            "intercom-personal",
            "file:sha256:test",
            "owner",
        );
        spool.insert_source(&source).unwrap();
        let mut first = sample_record(1);
        let mut changed = first.clone();
        changed.raw = b"changed".to_vec();
        assert!(spool.insert_record(&source, &first).unwrap());
        assert!(spool.insert_record(&source, &first).unwrap() == false);
        assert!(spool.insert_record(&source, &changed).is_err());
        first.raw.clear();
        drop(spool);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn generic_jsonl_contract_preserves_first_class_node_memory() {
        let root = test_root("node-memory");
        fs::create_dir_all(&root).unwrap();
        let path = root.join("nanihold-history.jsonl");
        let mut record = sample_record(1);
        record.record_kind = HistoryRecordKind::NodeMemory {
            memory_id: "memory:1".to_owned(),
            node_id: "node:interface".to_owned(),
        };
        let mut bytes = serde_json::to_vec(&record).unwrap();
        bytes.push(b'\n');
        fs::write(&path, bytes).unwrap();
        let mut imported = Vec::new();
        visit_generic_jsonl(&path, 4096, |record| {
            imported.push(record);
            Ok(())
        })
        .unwrap();
        assert_eq!(imported, vec![record]);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn streaming_plan_reports_cross_source_overlap_and_blocks_readiness() {
        let root = test_root("overlap");
        fs::create_dir_all(&root).unwrap();
        let spool = HistorySpool::create(&root.join("spool.sqlite3")).unwrap();
        let native = source_manifest(
            HistorySourceKind::ClaudeCode,
            "claude-code-personal",
            "native:1",
            "owner",
        );
        let lake = source_manifest(
            HistorySourceKind::Lethe,
            "lethe-personal",
            "lethe:1",
            "owner",
        );
        spool.insert_source(&native).unwrap();
        spool.insert_source(&lake).unwrap();
        for (source, message_id, raw) in [
            (&native, "native-occurrence", b"native".as_slice()),
            (&lake, "lethe-observation", b"lake".as_slice()),
        ] {
            let mut record = sample_record(1);
            record.source_message_id = message_id.to_owned();
            record.raw = raw.to_vec();
            for (key, value) in [
                ("upstream_source_kind", "claude_code"),
                ("upstream_source_instance_id", "claude-code-personal"),
                ("upstream_session_id", "session"),
                ("upstream_message_id", "native-message"),
            ] {
                record.metadata.insert(key.to_owned(), value.to_owned());
            }
            spool.insert_record(source, &record).unwrap();
        }
        let plan = spool
            .build_plan(
                "inventory:overlap",
                &DataSpaceId::new("space:personal"),
                Utc.with_ymd_and_hms(2026, 7, 20, 1, 0, 0).unwrap(),
                &[
                    RequiredHistorySource {
                        source_kind: HistorySourceKind::ClaudeCode,
                        source_instance_id: "claude-code-personal".to_owned(),
                    },
                    RequiredHistorySource {
                        source_kind: HistorySourceKind::Lethe,
                        source_instance_id: "lethe-personal".to_owned(),
                    },
                ],
                ScanStats {
                    source_records: 2,
                    unique_records: 2,
                    duplicate_source_records: 0,
                },
                10,
            )
            .unwrap();
        assert_eq!(plan.cross_source_overlap_identities, 1);
        assert_eq!(plan.safe_json()["ready_for_import"], false);
        drop(spool);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn seven_source_activation_plan_is_partitioned_and_overlap_free() {
        let root = test_root("seven-source");
        fs::create_dir_all(&root).unwrap();
        let spool = HistorySpool::create(&root.join("spool.sqlite3")).unwrap();
        let sources = [
            (HistorySourceKind::ClaudeCode, "claude-code-personal"),
            (HistorySourceKind::ClaudeAi, "claude-ai-personal"),
            (HistorySourceKind::Codex, "codex-personal"),
            (HistorySourceKind::Intercom, "intercom-personal"),
            (HistorySourceKind::Lethe, "lethe-personal"),
            (
                HistorySourceKind::NaniholdLegacy,
                "nanihold-legacy-personal",
            ),
            (HistorySourceKind::SystemSnapshot, "system-current"),
        ]
        .into_iter()
        .map(|(kind, instance)| source_manifest(kind, instance, "cursor:1", "owner"))
        .collect::<Vec<_>>();
        for (ordinal, source) in sources.iter().enumerate() {
            spool.insert_source(source).unwrap();
            let mut record = sample_record(u32::try_from(ordinal + 1).unwrap());
            record.source_session_id = format!("session-{ordinal}");
            record.source_message_id = format!("message-{ordinal}");
            record.raw = format!("raw-{ordinal}").into_bytes();
            spool.insert_record(source, &record).unwrap();
        }
        let required = sources
            .iter()
            .map(|source| RequiredHistorySource {
                source_kind: source.source_kind,
                source_instance_id: source.source_instance_id.clone(),
            })
            .collect::<Vec<_>>();
        let plan = spool
            .build_plan(
                "inventory:seven-source",
                &DataSpaceId::new("space:personal"),
                Utc.with_ymd_and_hms(2026, 7, 20, 1, 0, 0).unwrap(),
                &required,
                ScanStats {
                    source_records: 7,
                    unique_records: 7,
                    duplicate_source_records: 0,
                },
                100,
            )
            .unwrap();
        assert_eq!(plan.sources.len(), 7);
        assert_eq!(plan.cross_source_overlap_identities, 0);
        assert_eq!(plan.safe_json()["ready_for_import"], true);
        validate_activation_source_set(&required).unwrap();
        assert!(validate_activation_source_set(&required[..6]).is_err());
        drop(spool);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn spool_import_never_exceeds_configured_resident_batch() {
        let root = test_root("batch");
        fs::create_dir_all(&root).unwrap();
        let spool = HistorySpool::create(&root.join("spool.sqlite3")).unwrap();
        let source = HistorySourceManifest {
            source_kind: HistorySourceKind::Codex,
            source_instance_id: "codex-personal".to_owned(),
            cutover_cursor: "cursor:5".to_owned(),
            ownership: OwnershipAssignment::Personal {
                owner_id: "owner".to_owned(),
            },
            records: vec![],
        };
        spool.insert_source(&source).unwrap();
        for ordinal in 1..=5 {
            spool
                .insert_record(
                    &source,
                    &HistoryRawRecord {
                        source_session_id: "session".to_owned(),
                        source_message_id: format!("message-{ordinal}"),
                        parent_message_id: None,
                        published_at: Utc.with_ymd_and_hms(2026, 7, 20, 0, 0, ordinal).unwrap(),
                        ordinal: ordinal.into(),
                        author: "owner".to_owned(),
                        surface: "codex".to_owned(),
                        channel: "local".to_owned(),
                        text: format!("message {ordinal}"),
                        record_kind: HistoryRecordKind::Message,
                        raw: format!("raw {ordinal}").into_bytes(),
                        metadata: BTreeMap::new(),
                    },
                )
                .unwrap();
        }
        let required = vec![RequiredHistorySource {
            source_kind: HistorySourceKind::Codex,
            source_instance_id: "codex-personal".to_owned(),
        }];
        let plan = spool
            .build_plan(
                "inventory:test",
                &DataSpaceId::new("space:personal"),
                Utc.with_ymd_and_hms(2026, 7, 20, 1, 0, 0).unwrap(),
                &required,
                ScanStats {
                    source_records: 5,
                    unique_records: 5,
                    duplicate_source_records: 0,
                },
                100,
            )
            .unwrap();
        let store = SqliteOperationalEventStore::open(
            DataSpaceId::new("space:personal"),
            &root.join("lake.sqlite3"),
            &root.join("blobs"),
            &[7; 32],
        )
        .unwrap();
        let (result, metrics) = execute_spool(&spool, &plan, &store, 1024, 2).unwrap();
        assert_eq!(result.appended_messages, 5);
        assert_eq!(metrics.max_resident_batch_records, 2);
        drop(store);
        drop(spool);
        let _ = fs::remove_dir_all(root);
    }

    fn test_root(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "lethe-history-spool-test-{label}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn sample_record(ordinal: u32) -> HistoryRawRecord {
        HistoryRawRecord {
            source_session_id: "session".to_owned(),
            source_message_id: "message".to_owned(),
            parent_message_id: None,
            published_at: Utc.with_ymd_and_hms(2026, 7, 20, 0, 0, ordinal).unwrap(),
            ordinal: ordinal.into(),
            author: "owner".to_owned(),
            surface: "intercom".to_owned(),
            channel: "discord".to_owned(),
            text: "message".to_owned(),
            record_kind: HistoryRecordKind::Message,
            raw: b"raw".to_vec(),
            metadata: BTreeMap::new(),
        }
    }
}
