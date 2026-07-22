## Why

監査(`docs/development/principles-audit-20260722.md`)の性能フェーズ優先 #1。canonical 事実・派生状態・運用補助状態が一つの同期 critical section へ押し込まれ、取り込み応答が全量再計算まで返らない。

- **B-01:** canonical append 後、非 corpus materialize・audit・検索 index catch-up を同期実行し、その失敗を HTTP 失敗として返す(`mod.rs:5478/5512/5534`、`server.rs:399`)。append 済みなのに timeout でクライアントは未保存と判断し、再送が Duplicate になると `request_appended_observations` が空で materialization を再実行しない。取り込み結果と canonical 事実が分離する。
- **B-02:** AppCore / primary persistence / OEL / history projection が単一 `Mutex`(`mod.rs:1320/5447/5172`)、PostgreSQL も単一 `Client` を mutex 化(`postgres/lib.rs:19`)。1 件の長い import / history query / blob I/O が既知 ID 読み・cursor page・別 projection 読みまで停止させる。実測で監査系 2 並行が両方ハング。
- **B-03/04/05:** 差分処理なのに境界確認が毎回 `COUNT(*)`(`service_support.rs:245`)、append ごとに partition_log 全体を replay(`persistence/mod.rs:190`)、全 write で manifest 全体を JSON 上書き・ClaimQueue 全量 project(`mod.rs:685/969`)。逐次 S 件で O(S²)。
- **B-12:** 全保護操作で同期 audit insert しながら、lock/serialization/DB 失敗をログだけで握り潰す fail-open(`mod.rs:5388`)。無制限 in-memory mirror(`audit.rs:18`)は再起動で消える。

## What Changes

- canonical append の成功(ledger 永続化 + per-item ID 確定 + 最小 durable audit/outbox)を一つの commit 境界として応答を確定し、projection materialize・検索 index・遅延許容 audit を応答後の append-seq consumer へ分離する。
- AppCore / OEL / primary persistence の単一巨大 mutex を、canonical 書き込み系 / 派生消費者系 / 読み取り系の 3 lane へ分割し、読み取りを書き込み critical section へ直列化しない。
- 差分処理の境界確認・partition tree・manifest・lineage digest を、O(全集合)でなく O(差分)または保存済み scalar で行う。
- 監査の durability 契約を fail-closed へ改め(fail-open 廃止)、同期必須部分と遅延許容部分を区分し、無制限 in-memory mirror を廃止する。

## Capabilities

### New Capabilities

- `commit-ack-boundary`: canonical commit 境界が応答を確定し、派生処理を append-seq consumer として分離する契約。ingestion-api-contract IRC-04 の実装的裏付け。
- `storage-lock-partition`: 単一 mutex の 3 lane 分割、読み取りの非直列化、並行読み取りの非ブロック、I/O 中の排他ロック不保持。
- `bounded-delta-write`: 境界確認の scalar 化(B-03)、partition tree の snapshot + 差分適用(B-04)、manifest / ClaimQueue の per-row 分割と lineage digest の保存済み供給(B-05)。
- `audit-durability-contract`: mandatory audit の commit 境界内 fail-closed 化、同期必須 / 遅延許容の区分、in-memory mirror の廃止(B-12)。

### Modified Capabilities

なし。`observation-lake` の Append-Only / IngestResult 契約、`operational-event-ledger` の append 契約は変更せず、内部の commit 境界・並行性・計算量を規定する新規 capability を定義する。

## Impact

- 主対象: `apps/selfhost/src/self_host/app/mod.rs`(AppCore lock / materialize orchestration / audit)、`server.rs`(spawn_blocking)、`service_support.rs`、`crates/storage/sqlite/`、`crates/storage/postgres/`。
- API: wire contract は変更しない。応答は commit 境界成功で返し、派生処理を応答から外す。
- System Laws: Effect Isolation Law(派生を commit 境界外へ)、Append-Only Law / No Direct Mutation Law(audit を上書きしない)、Replay Law(snapshot は canonical から再構築可能)、Explicit Authority Law(audit durability)を維持・強化する。
- 対象外: client 実装、本番 selfhost デプロイ、既存 `data/`。

## Non-goals

- 取り込み wire 契約(per-item 応答・identity)の再設計 — ingestion-api-contract の責務。
- materialize の増分化・classify 分岐・reply-SLO projection の内容 — communication-projection の責務。
- 読み取り経路の keyset/index 化(offset・全 scan・cursor 統一)— indexed-keyset-reads の責務。本 change は書き込み側の commit 境界と lock lane、および write 時の scalar 化に限定する。
- consent / retraction などプライバシー改修。
