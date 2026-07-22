## Why

観測取り込み API(`POST /api/import/observation-drafts`)は正式契約(`observation-lake.md` 4.5 の `IngestResult = Ingested{id} | Duplicate{existingId} | Rejected | Quarantined`)を満たしていない。実装は件数だけの `ImportReport { ingested, duplicates, quarantined }` を返し、Observation ID・既存 ID・item 別理由を捨てる(B-07)。batch 内 1 件の quarantine/rejected は最初の 1 件を `SelfHostError::Ingestion` へ変換して request 全体を 400 で abort し、有効 item も append されない/結果が返らない(B-16、partial success 契約違反)。冪等 identity は client 提供の `idempotency_key` を `source_instance_id` で prefix するだけで、サーバは canonical identity(`source:object_id:H(canonical_json)`)を導出・検証しない(B-08)。結果、client は timeout 後に corpus grep で ID を回収せざるを得ず(実測 6 往復の障害連鎖)、event_time を作り直した再送が別実体化して 3 重複を生んだ。加えて C 章の暗黙契約(HTTP 成功前 append 済み・件数のみ応答・source_instance_id が identity の一部・retry 完全固定要件・未来時刻 10 分・OEL cursor/limit 必須・grep 500ms・body 上限)は client に開示されていない。

本 change は監査の改修方針「契約 → 性能 → プライバシー」の**契約フェーズ本体**であり、取り込み API を正式契約へ収斂させる仕様・設計のみを扱う(実装はオーナー承認後)。

## What Changes

- import は各 draft につき `{ client_ref, observation_id, outcome ∈ ingested|duplicate|quarantined|rejected, existing_id?, reason? }` を入力順で返す。件数のみ応答を廃止し、client の corpus grep 回収を不要化する。
- batch 内の一部が quarantine/rejected でも有効 item は append され、item 別結果で返る。1 件の失敗で全体 400 を返さない。
- サーバが canonical identity を導出・検証し、同一実体の再送は同一 observation_id の duplicate として返る。client 時計揺れで別実体化しない設計を提案する。
- error を transient(再送可)/ validation(再送不可)/ quarantine(ticket 付き)へ構造化し、HTTP status と error code で機械判別可能にする。未来時刻(clock skew 10 分)は request 全体 400 でなく item 別 quarantine にする。
- ACK セマンティクス(応答コード↔台帳状態)を明文化する。派生処理の失敗は append 成功応答に影響しない(宣言と応答形状のみ。派生処理の分離実装は communication-projection / 性能フェーズへ委ねる)。
- 暗黙契約(limit 上限・body 上限・grep 制約・cursor 形式・identity 構成要素)を API 応答と文書に明示する。
- 後方互換は API バージョニングで行う(応答フィールド追加は非破壊。partial success 等の挙動変更は新バージョンにのみ導入し、旧バージョンは意味論凍結 → 非推奨化 → 廃止)。opt-in header は client 列挙に依存しスケールしないため却下。

## Capabilities

### New Capabilities

- `ingestion-response-contract`: per-item 応答形状、partial success、構造化 error 分類、ACK セマンティクス、暗黙契約の開示、後方互換移行を規定する。
- `ingestion-idempotency`: サーバ側 canonical identity の導出・検証、duplicate/collision セマンティクス、client 時計非依存の identity 設計を規定する。

### Modified Capabilities

なし。`observation-lake` の `IngestResult` 契約と Append-Only Law は変更せず、既存の prose 契約を wire 上で強制可能にする新規 capability として定義する。

## Impact

- 主対象: `apps/selfhost/src/self_host/app/mod.rs`(`ingest_observation_drafts_with_session` / `prepare_observation_draft_batch` / `ImportReport`)、`crates/engine/src/lake/ingestion.rs`(`ObservationPreparer::prepare`)、`crates/storage/sqlite/src/persistence/mod.rs`(`append_observations_in_transaction` の canonical identity)、`apps/selfhost/src/self_host/server.rs`(import route と `ApiError` mapping)。
- API: 応答形状の拡張(item 別結果 + error code)。フィールド追加は非破壊、挙動変更は API バージョニングで導入。
- System Laws: Append-Only Law と Effect Isolation Law を維持(canonical append 成功を派生失敗で取り消さない)。Explicit Authority Law を維持。
- 対象外: nanihold_intercom / Nanihold_OS(client)の実装、本番 selfhost デプロイ、既存 `data/`。

## Non-goals

- ロック分割・派生処理の背景化などの性能改修(次フェーズ / communication-projection)。
- consent / retraction などプライバシー改修(プライバシーフェーズ)。
- communication-projection の内容(reply-SLO projection、増分 fold、materialization 背景化)。
- Schema Registry の strict 化(B-09)そのものの再設計。本 change は error 分類として validation を参照するのみ。
- 検索契約・grep 実装・OEL query 契約の再設計(暗黙契約の**開示**は行うが実装は変えない)。
