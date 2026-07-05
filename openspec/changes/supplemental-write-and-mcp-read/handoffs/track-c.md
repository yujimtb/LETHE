# Track C Handoff: Claim Queue Projection

## 実装した内容

- `crates/projections/claim-queue` を追加し、supplemental 集合から claim queue / 同源 group / decision view / audit log を再構築する Projection を実装した。
- claim dedup は `(kind, derivedFrom の集合, 正規化 payload hash)` で判定し、`model_version` は等価判定から除外した。代表 ID は `created_at, id` 順で最初に観測した claim の ID とし、吸収 ID は `absorbed_ids` に保持する。
- claim 状態 fold は初期 `open` から `claim-transition@1` と `verification-result@1` を `created_at, id` 順で畳み込む。`verification-result.verdict` は `consistent -> verified`, `inconsistent -> refuted`, `inconclusive -> inconclusive` に写像する。不正遷移は skip し、`audit_log` に記録する。
- 同源 group は `derivedFrom` の会話/観測 root で束ねる。状態管理は claim 単位のまま、API では group 単位で返す。
- `decision@1` の `statement` / `rationale` を全文検索対象にし、`supersedes` チェーンを解決して置換済み decision に `superseded_by` を付ける。
- selfhost に `GET /projections/claim-queue` と `GET /projections/decisions?q=` を追加した。どちらも `read:corpus` scope で認可し、`proj:claim-queue` の Projection envelope と lineage を返す。
- Projection が stale の場合は空結果にせず、HTTP 503 `projection_stale` を返す。

## 変更ファイル一覧

- `Cargo.toml`
- `Cargo.lock`
- `crates/projections/claim-queue/Cargo.toml`
- `crates/projections/claim-queue/src/lib.rs`
- `crates/engine/src/supplemental/store.rs`
- `apps/selfhost/Cargo.toml`
- `apps/selfhost/src/self_host/app/mod.rs`
- `apps/selfhost/src/self_host/app/projection_api.rs`
- `apps/selfhost/src/self_host/app/service_support.rs`
- `apps/selfhost/src/self_host/registry.rs`
- `apps/selfhost/src/self_host/server.rs`
- `apps/selfhost/src/self_host/mcp.rs`
- `crates/api/src/api/envelope.rs`
- `tests/e2e/Cargo.toml`
- `tests/e2e/tests/self_host_api.rs`
- `crates/registry/src/registry/mod.rs`
- `crates/registry/src/registry/store.rs`
- `apps/selfhost/src/self_host/app/supplemental_write.rs`
- `openspec/changes/supplemental-write-and-mcp-read/tasks.md`
- `openspec/changes/supplemental-write-and-mcp-read/specs/claim-queue-projection/spec.md`
- `openspec/changes/supplemental-write-and-mcp-read/handoffs/track-c.md`

Registry / supplemental write の 3 ファイルは、既存 Track A/B 途中実装の型 re-export 競合と error variant 不一致で selfhost/registry がコンパイル不能だったため、Track C の build/test gate を通すために補正した。

## 実行したテストと結果

- `cargo check -p lethe-projection-claim-queue` : pass
- `cargo check -p lethe-selfhost` : pass
- `cargo test -p lethe-projection-claim-queue` : pass, 5 tests
- `cargo test -p lethe-e2e --test self_host_api claim_queue_api_filters_pages_and_searches_decisions` : pass
- `cargo test -p lethe-selfhost` : pass, 27 tests
- `cargo test -p lethe-e2e --test self_host_api` : pass, 16 tests
- `cargo test -p lethe-registry` : pass, 19 tests
- `cargo fmt --all -- --check` : pass
- `openspec validate supplemental-write-and-mcp-read --strict` : pass

`cargo test -p lethe-e2e --test self_host_api claim_queue_api_filters_pages_and_searches_decisions` は 1 回目に診断なしで exit 1 になったため、`-- --nocapture` 付きで即時再実行し pass を確認した。

外部実機を必要とする Track C 項目はない。MCP connector 実機疎通は Track H/I の範囲。

## Track H 向け API 契約

### `GET /projections/claim-queue`

Auth: `read:corpus`

Query:

- `state`: optional。`open`, `dispatched`, `verified`, `refuted`, `inconclusive`, `terminated`, `parked`
- `limit`: optional。省略時 `20`。`1..=resource_limits.max_page_size`
- `cursor`: optional。数値 offset 文字列

Response example:

```json
{
  "data": {
    "groups": [
      {
        "group_id": "source:obs:conversation-1",
        "source_refs": ["obs:conversation-1"],
        "members": [
          {
            "representative_id": "sup:claim-a",
            "absorbed_ids": ["sup:claim-a-retry"],
            "kind": "claim@1",
            "derived_from": { "observations": ["obs:conversation-1"], "blobs": [], "supplementals": [] },
            "source_refs": ["obs:conversation-1"],
            "payload_hash": "sha256...",
            "statement": "Adapter A must preserve sidechain parent ids.",
            "verification_mode": "manual",
            "state": "open",
            "created_at": "2026-07-05T00:00:00Z",
            "updated_at": "2026-07-05T00:00:00Z",
            "state_history": []
          }
        ]
      }
    ],
    "total": 3,
    "limit": 2,
    "next_cursor": "2",
    "audit_log": []
  },
  "projection_metadata": {
    "projection_id": "proj:claim-queue",
    "version": "1.0.0",
    "built_at": "2026-07-05T00:00:00Z",
    "read_mode": "operational_latest",
    "stale": false,
    "lineage_ref": "lineage:proj:claim-queue:..."
  }
}
```

### `GET /projections/decisions?q=`

Auth: `read:corpus`

Query:

- `q`: required non-blank search text
- `limit`: optional。省略時 `20`。`1..=resource_limits.max_page_size`

Response example:

```json
{
  "data": {
    "query": "adapter A",
    "matches": [
      {
        "id": "sup:decision-a",
        "statement": "Use adapter A for archived Codex sessions.",
        "rationale": "It preserves session lineage.",
        "alternatives": [],
        "supersedes": [],
        "superseded_by": "sup:decision-b",
        "derived_from": { "observations": ["obs:conversation-1"], "blobs": [], "supplementals": [] },
        "created_by": "agent:codex",
        "created_at": "2026-07-05T00:00:00Z"
      }
    ],
    "total": 1,
    "limit": 20,
    "audit_log": []
  },
  "projection_metadata": {
    "projection_id": "proj:claim-queue",
    "version": "1.0.0",
    "built_at": "2026-07-05T00:00:00Z",
    "read_mode": "operational_latest",
    "stale": false,
    "lineage_ref": "lineage:proj:claim-queue:..."
  }
}
```

### ProjectionStale

Projection stale は成功 envelope では返さない。HTTP status は `503`、body は以下。

```json
{
  "error": "projection_stale",
  "detail": "proj:claim-queue is stale",
  "retry_after": 30
}
```

MCP tool 側は stale を「空キュー」や「検索ヒットなし」に変換しないこと。

## 未完了または統合担当に引き継ぐ事項

- Track C 内のスタブは残していない。
- Track H の `claim_queue` / `search_decisions` はこの API に結線できる。生 supplemental を tool 側で列挙する必要はない。
- Track I では `POST /supplementals` から claim / decision を書き、再構築後に本 API で読める横断 E2E を追加する。
- 作業ツリーには他 Track の未コミット変更が混在しているため、統合時は Track C 以外の差分を巻き戻さないこと。

## 仕様 SHALL と evidence の対応

- CLQ-01 読みは Projection 経由: `apps/selfhost/src/self_host/app/projection_api.rs` の API は `core.snapshot.claim_queue` のみを読む。Projection 構築は `ClaimQueueProjector` に集約し、API は生 `SupplementalRecord` を行動根拠として列挙しない。
- CLQ-02 重複解消: `crates/projections/claim-queue/src/lib.rs` の dedup key は kind / sorted derivedFrom / canonical payload hash。`model_version` は payload hash 前に除外。test `batch_rerun_claims_deduplicate_and_keep_absorbed_ids`。
- CLQ-03 状態機械: `fold_claim_states` が transition / verification-result を時刻順 fold。不正遷移は audit。tests `replay_is_deterministic_for_different_input_orders`, `invalid_transition_is_skipped_and_audited`。
- CLQ-04 同源グループ: `claim_groups` が source refs 単位で grouping。test `same_conversation_claims_are_returned_as_one_group`。
- CLQ-05 決定台帳ビュー: `decision_views` / `decision_replacement_map` が検索 view と supersedes 解決を構築。test `decision_supersedes_chain_sets_superseded_by_on_old_decision`。
- CLQ-06 読み取り API: `apps/selfhost/src/self_host/server.rs` に 2 route を追加し、`read:corpus` 認可を適用。E2E `claim_queue_api_filters_pages_and_searches_decisions` が状態 filter / paging / decision search / `superseded_by` を検証。
