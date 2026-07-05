# Track B Handoff: Supplemental Write API

## 実装した内容

- selfhost に `POST /supplementals` を追加した。
- 既存 `authorize_headers` で `write:supplemental` scope を要求する。
- request body は `id`, `kind`, `derived_from`, `payload`, `created_by`, `mutability`, `model_version`, `consent_metadata`, `lineage` を受け付ける。`created_at` は selfhost が設定し、`record_version` は新規作成時 `None`。
- 成功時は HTTP 201 と `{ "data": <SupplementalRecord> }` を返す。
- `sup:{uuid}` 形式のクライアント採番 ID を必須化した。
- Store 投入前に以下を検証する。
  - payload size limit
  - 空 `derived_from` 拒否
  - observation anchor の存在
  - supplemental anchor の存在
  - Track A の kind/payload schema 検証
- API error contract を追加した。
  - 空アンカー、未解決 observation、未解決 supplemental、schema violation は 422 と `details`
  - 同一 ID 再 POST は 409
  - scope 不足は 403
- 書き込み側の内容ベース dedup は実装していない。同一内容でも別 UUID なら別レコードとして永続化する。
- 設定例に `LETHE_API_WRITE_TOKEN` / `write:supplemental` を追加した。
- README / personal-lake docs / supplemental-write-api spec を実装済み契約に合わせて更新した。

## 変更ファイル一覧

Track B の主要変更:

- `apps/selfhost/src/self_host/app/supplemental_write.rs`
- `apps/selfhost/src/self_host/app/mod.rs`
- `apps/selfhost/src/self_host/server.rs`
- `crates/api/src/api/envelope.rs`
- `crates/engine/src/supplemental/store.rs`
- `crates/storage/api/src/lib.rs`
- `crates/storage/sqlite/src/persistence/mod.rs`
- `tests/e2e/tests/self_host_api.rs`
- `tests/e2e/Cargo.toml`
- `README.md`
- `.env.example`
- `config.example.toml`
- `deploy/personal-lake/.env.example`
- `deploy/personal-lake/compose.yaml`
- `deploy/personal-lake/config.toml`
- `deploy/personal-lake/config.host.toml`
- `docs/development/personal-lake-ingestion.md`
- `openspec/changes/supplemental-write-and-mcp-read/specs/supplemental-write-api/spec.md`
- `openspec/changes/supplemental-write-and-mcp-read/tasks.md`

結線/コンパイル維持のために触った関連箇所:

- `apps/selfhost/src/self_host/app/projection_api.rs`
  - Track H の MCP `claim_queue` / `search_decisions` contract stub wrapper を復旧した。Track B の HTTP 書き込み API では使わない。

ワークツリーには Track A/C/E/G/H 由来の未コミット変更も存在する。Track B では unrelated な変更を戻していない。

## 実行したテストと結果

- `cargo fmt --all`: pass
- `cargo test -p lethe-registry`: pass, 19 passed
- `cargo test -p lethe-engine supplemental`: pass, 9 passed
- `cargo test -p lethe-selfhost supplemental`: pass, 1 passed
- `cargo test -p lethe-e2e supplemental_post`: pass, 5 passed
- `cargo test -p lethe-selfhost`: pass, 27 passed
- `cargo test -p lethe-e2e --test self_host_api`: pass, 16 passed
- `cargo test -p lethe-e2e --test mcp_read_port`: pass, 4 passed
- `openspec validate supplemental-write-and-mcp-read --strict`: pass

外部実機確認は不要。Track B の受け入れ条件は local e2e/contract test で完結する。

## POST サンプル

前提:

- `config.toml` の `[[api_tokens]]` に `scopes = ["write:supplemental"]` を持つ token を設定する。
- 例: `token_env = "LETHE_API_WRITE_TOKEN"`
- 対象 observation は既に SQLite/lake に存在している必要がある。
- `kind` は `claim@1` など登録済みの versioned kind を使う。

```powershell
$id = "sup:$([guid]::NewGuid().ToString())"
$body = @{
  id = $id
  kind = "claim@1"
  derived_from = @{
    observations = @("obs:existing-observation-id")
    blobs = @()
    supplementals = @()
  }
  payload = @{
    statement = "検証対象の主張"
    verification_mode = "check"
  }
  created_by = "actor:extraction-pass"
  mutability = "append_only"
  model_version = "model:example-2026-07"
  consent_metadata = $null
  lineage = $null
} | ConvertTo-Json -Depth 8

Invoke-RestMethod `
  -Method Post `
  -Uri "http://127.0.0.1:8080/supplementals" `
  -Headers @{ Authorization = "Bearer $env:LETHE_API_WRITE_TOKEN" } `
  -ContentType "application/json" `
  -Body $body
```

`created_by` は pipeline/client の安定 actor にする。モデル名やモデル version は `model_version` に入れる。

## Fixture 作成方法

E2E では `tests/e2e/tests/self_host_api.rs` の helper を使う。

- observation: `slack_observation(...)` を作り、`SqlitePersistence::persist_observation(&observation)` で永続化する。
- write config: `supplemental_write_config(db, blobs)` を使う。token は `write-token`、scope は `write:supplemental`。
- request body: `claim_supplemental_body(&id, &observation)` を使う。
- request: `post_supplemental(app, "write-token", body)` を使う。

未解決 anchor の fixture は `derived_from.observations` または `derived_from.supplementals` を存在しない ID に差し替える。schema violation の fixture は `payload.verification_mode` を削る。

## 未完了または統合担当への引き継ぎ

- Track B の未完了項目はなし。
- Track I は HTTP `POST /supplementals` を使って claim / transition / decision の統合 E2E を作れる。
- MCP `claim_queue` / `search_decisions` は Track H の contract stub wrapper が残っている。Track I が検証すべき読み取り面は、現時点では HTTP `GET /projections/claim-queue` と `GET /projections/decisions` の実 projection 経由を使うこと。
- write API は raw supplemental を読み取り根拠にしない。読み取り消費者は projection 経由に限定する方針を維持すること。

## SHALL と evidence

- SUPW-01 書き込みエンドポイント
  - Evidence: `POST /supplementals` route と `create_supplemental` handler。
  - Test: `supplemental_post_returns_201_and_persists_across_restart`
- SUPW-02 認可
  - Evidence: handler が `authorize_headers(&headers, "write:supplemental")` を呼ぶ。
  - Test: `supplemental_post_requires_write_scope_and_does_not_write_on_forbidden`
- SUPW-03 Store 不変条件の API 契約化
  - Evidence: `validate_non_empty_anchor`, `resolve_observation_anchors`, `resolve_supplemental_anchors`, 409 conflict mapping。
  - Test: `supplemental_post_maps_store_invariants_to_422_details`, `supplemental_post_same_id_conflicts_but_same_content_different_uuid_is_allowed`
- SUPW-04 kind schema 検証
  - Evidence: `RegistryStore::validate_supplemental_record_kind` を Store 投入前に呼ぶ。
  - Test: `supplemental_post_rejects_claim_missing_verification_mode_before_write`
- SUPW-05 ID 採番と重複
  - Evidence: `validate_supplemental_id`, persistent/core duplicate checks, no content hash dedup path。
  - Test: `supplemental_post_same_id_conflicts_but_same_content_different_uuid_is_allowed`
- SUPW-06 created_by 帰属規約
  - Evidence: README と spec に、安定 actor を `created_by`、モデル名を `model_version` に置く規約を明記。
  - Test: 正常系 E2E が `created_by = "actor:extraction-pass"` と `model_version` を POST し、201 を確認。
