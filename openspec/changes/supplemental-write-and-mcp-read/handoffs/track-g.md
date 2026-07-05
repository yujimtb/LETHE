# Track G Handoff: personal corpus all-text and coding-agent threads

Date: 2026-07-06
Status: Complete

## Implemented

- Added `CorpusMode` with `workspace_filtered` and `personal_all_text`.
- Wired selfhost config so personal lake deployments can select `corpus.mode = "personal_all_text"`.
- Made personal corpus projection include every text-bearing observation instead of carrying over dorm-lake consent/selection filters.
- Added personal source type mapping for:
  - `claude-ai`
  - `github-issue`
  - `github-pr`
  - `github-comment`
  - `github-commit`
  - `claude-code`
  - `codex`
- Preserved coding-agent metadata needed for thread reconstruction: `thread_key`, `session_id`, `parent_session_id`, `is_sidechain`, `message_id`, and `parent_message_id`.
- Extended corpus `get_thread` responses for coding-agent records with `structure.root_session` and `structure.sidechains`.
- Updated personal lake config/docs/scripts so personal deployments use all-text corpus mode and read tokens include `read:corpus`.

## Changed Files

- `crates/projections/corpus/src/lib.rs`
- `crates/api/src/api/grep.rs`
- `apps/selfhost/src/self_host/config.rs`
- `apps/selfhost/src/self_host/app/mod.rs`
- `apps/selfhost/src/self_host/app/projection_api.rs`
- `apps/selfhost/src/self_host/registry.rs`
- `tests/e2e/Cargo.toml`
- `tests/e2e/tests/self_host_api.rs`
- `deploy/personal-lake/config.toml`
- `deploy/personal-lake/config.host.toml`
- `config.example.toml`
- `scripts/personal_lake_pipeline_smoke.py`
- `scripts/personal_lake_w0_check.py`
- `docs/development/personal-lake-ingestion.md`
- `README.md`
- `openspec/specs/corpus-projection/spec.md`
- `openspec/changes/supplemental-write-and-mcp-read/tasks.md`

## Tests

- `cargo fmt --all -- --check`: passed.
- `cargo test -p lethe-projection-corpus`: passed, 15 tests.
- `cargo test -p lethe-e2e personal_corpus_grep_hits_all_text_source_types --test self_host_api`: passed.
- `cargo test -p lethe-e2e coding_agent_get_thread_preserves_parent_child_sessions --test self_host_api`: passed.
- `cargo test -p lethe-e2e --test self_host_api`: failed with 15 passed and 1 failed. The failure is `claim_queue_api_filters_pages_and_searches_decisions` at `tests/e2e/tests/self_host_api.rs:1687`, where the Track C claim-queue decision response did not include the expected superseded decision. Track G E2E tests passed in this run.

No external device or live MCP connector was required for Track G.

## Track H Corpus API Contract

Track H `search_lake`, `get_record`, and `get_thread` should call the corpus projection API or the equivalent in-process `AppService` methods. They must not read raw observations or raw supplemental records.

### `search_lake`

Use `POST /api/projections/proj:corpus/grep` or `AppService::corpus_grep_response`.

Request:

```json
{
  "pattern": "needle",
  "filters": {
    "types": ["claude-code", "github-commit"]
  },
  "limit": 10
}
```

Response shape:

```json
{
  "data": {
    "matches": [
      {
        "record_id": "corpus:claude-code:obs:example",
        "source_type": "claude-code",
        "anchor_url": "claude-code://session/main-session/message/msg-1",
        "source_title": "claude-code session main-session",
        "source_location": null,
        "timestamp": "2026-07-05T00:00:01Z",
        "snippet": "needle appears in the coding-agent backbone",
        "matched_ranges": [{"start": 0, "end": 6}],
        "metadata": {
          "source_system": "sys:claude-code",
          "thread_key": "claude-code:session:main-session",
          "session_id": "main-session",
          "is_sidechain": false
        }
      }
    ],
    "next_cursor": null,
    "complete": true,
    "projection_watermark": "proj:corpus:example"
  },
  "projection_metadata": {
    "projection_id": "proj:corpus",
    "version": "1.0.0",
    "built_at": "2026-07-05T00:00:10Z",
    "read_mode": "operational_latest",
    "stale": false
  }
}
```

`filters.types` is the source-type filter. For Track G the valid personal-lake source types include `claude-ai`, `github-issue`, `github-pr`, `github-comment`, `github-commit`, `claude-code`, and `codex`. Track H should not restrict the default search to a subset of these.

### `get_record`

Use `GET /api/projections/proj:corpus/records/{record_id}` or `AppService::corpus_record_response`.

Response shape:

```json
{
  "data": {
    "record": {
      "record_id": "corpus:github-commit:obs:example",
      "source_type": "github-commit",
      "anchor_url": "https://github.example/org/repo/commit/abc123",
      "source_title": "Commit abc123",
      "source_location": "org/repo",
      "timestamp": "2026-07-05T00:00:02Z",
      "text": "Commit message text",
      "normalized_text": "Commit message text",
      "thread_ts": null,
      "container": "org/repo",
      "metadata": {
        "source_system": "sys:github",
        "object_type": "commit",
        "repo": "org/repo"
      }
    }
  },
  "projection_metadata": {
    "projection_id": "proj:corpus",
    "version": "1.0.0",
    "built_at": "2026-07-05T00:00:10Z",
    "read_mode": "operational_latest",
    "stale": false
  }
}
```

### `get_thread`

Use `GET /api/projections/proj:corpus/threads/{thread_ref}` or `AppService::corpus_thread_response`.

`thread_ref` may be a corpus `record_id`, a coding-agent `thread_key`, or a coding-agent `session_id`. For coding-agent records, Track H must preserve `structure` and must not flatten sidechains into anonymous records.

Response shape:

```json
{
  "data": {
    "thread_ts": "claude-code:session:main-session",
    "records": [
      {
        "record_id": "corpus:claude-code:obs:main",
        "source_type": "claude-code",
        "anchor_url": "claude-code://session/main-session/message/msg-main",
        "source_title": "claude-code session main-session",
        "source_location": null,
        "timestamp": "2026-07-05T00:00:01Z",
        "text": "Main session backbone text",
        "normalized_text": "Main session backbone text",
        "thread_ts": "claude-code:session:main-session",
        "container": "main-session",
        "metadata": {
          "thread_key": "claude-code:session:main-session",
          "session_id": "main-session",
          "is_sidechain": false
        }
      },
      {
        "record_id": "corpus:claude-code:obs:child",
        "source_type": "claude-code",
        "anchor_url": "claude-code://session/child-session/message/msg-child",
        "source_title": "claude-code session child-session",
        "source_location": null,
        "timestamp": "2026-07-05T00:00:02Z",
        "text": "Sidechain backbone text",
        "normalized_text": "Sidechain backbone text",
        "thread_ts": "claude-code:session:main-session",
        "container": "child-session",
        "metadata": {
          "thread_key": "claude-code:session:main-session",
          "session_id": "child-session",
          "parent_session_id": "main-session",
          "is_sidechain": true
        }
      }
    ],
    "structure": {
      "thread_key": "claude-code:session:main-session",
      "source_type": "claude-code",
      "root_session": {
        "session_id": "main-session",
        "is_sidechain": false,
        "record_ids": ["corpus:claude-code:obs:main"]
      },
      "sidechains": [
        {
          "session_id": "child-session",
          "parent_session_id": "main-session",
          "is_sidechain": true,
          "record_ids": ["corpus:claude-code:obs:child"]
        }
      ]
    }
  },
  "projection_metadata": {
    "projection_id": "proj:corpus",
    "version": "1.0.0",
    "built_at": "2026-07-05T00:00:10Z",
    "read_mode": "operational_latest",
    "stale": false
  }
}
```

## SHALL Evidence

| Requirement | SHALL | Evidence |
| --- | --- | --- |
| MCPR-05 | Personal lake corpus targets all text-bearing observations, including claude.ai, GitHub issue/PR/comment/commit, and coding-agent conversations. | `CorpusMode::PersonalAllText`; `personal_all_text_indexes_personal_lake_source_types` unit test; `personal_corpus_grep_hits_all_text_source_types` E2E. |
| MCPR-05 | Dorm-lake selection filters are not applied to personal lake corpus. | `personal_all_text` path projects by text-bearing content and does not apply workspace allow-channel/form-response filters; personal deployment config uses `mode = "personal_all_text"`. |
| CAGT-03 | Sidechain transcripts are represented with the same backbone content rules and keep parent relationship metadata for projection reconstruction. | Corpus metadata preserves `session_id`, `parent_session_id`, `is_sidechain`, and `thread_key`; `personal_all_text_preserves_coding_agent_sidechain_metadata` unit test. |
| CAGT-03 | Projection can restore the parent/sidechain thread structure. | `ThreadResponse.structure`; `coding_agent_get_thread_preserves_parent_child_sessions` E2E. |
| MCPR-04 | `get_thread` returns context through projection, not raw stores. | `AppService::corpus_thread_response` reads `core.snapshot.corpus`; Track H should call this API/method. |

## Open Items for Integration

- Full `cargo test -p lethe-e2e --test self_host_api` is blocked by the Track C claim-queue failure noted above. Track I should rerun the suite after Track C is completed.
- Track H should enforce `read:corpus` for `search_lake`, `get_record`, and `get_thread` and propagate corpus API errors instead of converting corpus misses into empty results.
- Track H should URL-encode `record_id`, `thread_key`, or `session_id` when using HTTP paths. In-process calls avoid this concern.
