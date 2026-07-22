## Why

schema v8 のグローバル `observation_identity_registry` は、既存行と新規 v1 行を v1 の identity key のまま登録する。一方 v2 は別形式の server-derived identity を使用するため、同じ論理 Observation の v1 行を発見できず、client の切替・切戻し・遅延 retry で二重 append が起こり得る。`ingestion-api-contract` の HIGH 受入指摘を解消しない限り、既存 client を安全に v2 へ切り替えられない。

## What Changes

- canonical ledger から v2 identity alias を append 順に構築する、再開可能な増分 bridge projection と単調 watermark を定義する。
- `source_instance_id` 単位の v1 drain・admission fence・projection catch-up・v2 activation を状態機械として定義し、client ごとの独立移行を可能にする。
- 同一 cutover unit で v1/v2 を同時に受理せず、v2 が v1 由来 Observation を既存 ID の `duplicate` として解決する契約を定義する。
- activation 前後の検証 gate、障害時の再開、最初の v2 `outcome=ingested` 前後で異なる rollback 条件を定義する。
- **BREAKING**: 最初の v2 `outcome=ingested` 後は、同一 source history を v1 へ自動切戻しできない。安全条件を満たさない切戻しは fail closed とする。

## Capabilities

### New Capabilities

- `ingestion-cutover-bridge`: v1/v2 identity alias の増分 projection、source 単位 cutover fence、混在期間の排他、rollback、検証契約を規定する。

### Modified Capabilities

なし。M03 `observation-lake` の Append-Only / IngestResult、および `ingestion-api-contract` の凍結 v1・strict v2 契約は変更しない。

## Impact

- 対象: M03 `openspec/specs/observation-lake.md`、`ingestion-api-contract`、SQLite identity registry 周辺、selfhost ingress admission、client cutover tooling、`docs/development/personal-lake-ingestion.md`。
- System Laws: Append-Only Law、Idempotency Law、Replay Law、Effect Isolation Law、Explicit Authority Lawを維持する。canonical Observation の更新・削除、全量 rebuild、v1 identity 判定の変更は行わない。
- 任意数の client/source を `source_instance_id` による cutover unit として扱い、既知 client の列挙に依存しない。

## Non-goals

- v1 の応答形式、request-level error、identity 判定の変更。
- nanihold_intercom / Nanihold OS その他 client の実装、本番接続・デプロイ・実データ移行。
- canonical Observation の重複削除・統合、projection の全量 rebuild。
- v2 の per-item 契約・identity formula・schema v8 registry 自体の再設計。
