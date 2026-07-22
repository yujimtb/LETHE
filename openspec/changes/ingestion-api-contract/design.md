## Context

取り込み API の実装は正式契約(`observation-lake.md` 4.5)から乖離している。事故起点は 2026-07-22 のメッセージ消失で、監査(`docs/development/principles-audit-20260722.md`)は取り込み契約の欠陥として B-07 / B-08 / B-16 と C 章暗黙契約を特定した。実コードの現状:

- **応答形状:** `ImportReport { ingested, duplicates, quarantined }` の件数のみ(`apps/selfhost/src/self_host/app/mod.rs:146`)。`append_observations` の per-item outcome(`DurableAppendOutcome::{Appended(id), Duplicate(id), CanonicalCollision(id)}`)は集計へ捨てられる(`mod.rs:5494-5510`)。
- **batch abort:** `prepare_observation_draft_batch` は 1 件でも `IngestResult::Rejected` / `Quarantined` を受けると `SelfHostError::Ingestion` を早期 return し(`mod.rs:5599-5610`)、route は `SelfHostError::Ingestion` を一律 400 へ落とす(`server.rs:879`)。有効 item も append されない。
- **冪等 identity:** `namespace_draft` は client 提供 `idempotency_key` を `source_instance_id:` で prefix するだけ(`service_support.rs:714`)。storage は `(leaf_id, identity_key)` の UNIQUE と、別途 `meta.canonical_json` の sha256 一致で duplicate / collision を分岐する(`crates/storage/sqlite/src/persistence/mod.rs:2131-2208`)。`leaf_id` は routing_key(published を含み得る)から決まる。サーバは `crates/adapters/api/src/idempotency.rs::identity_key`(`source:object_id:H(canonical_json)`)を**再導出・検証しない**。
- **未来時刻:** `ObservationPreparer::prepare` は `published > recordedAt + MAX_CLOCK_SKEW`(10 分)を `IngestResult::Quarantined` で返す(`crates/engine/src/lake/ingestion.rs:201-212`)が、batch abort により request 全体 400 になる。

canonical Observation ledger は append-only の正本であり、per-item outcome は storage が既に返している。本 change は「storage が持つ per-item 事実を wire 契約として client へ返す」ことと「identity をサーバ権威で決める」ことを仕様化する。実装はオーナー承認後。

## 運用判断原則(オーナー確定 2026-07-22)

本 change の設計判断は次の原則に従う。

- **運用上の自由選択はスケールするかで判断する。** 選択肢が既知の少数事例(現在の client 2 つなど)を列挙する運用に依存するなら不採用。データ量・partition 数・source 数・client 数に対して劣化しない側を選ぶ。
- **製品の境界は厳格に分ける。** wire 契約(応答形状・error 語彙・バージョン)を製品境界として明確にし、内部実装の都合を境界へ漏らさない。
- **クライアントは Intercom / Nanihold に限らず任意に接続できる前提とする。** 特定 client を名指しした opt-in や既知 client 列挙で契約を分岐しない。

この原則により、後方互換は API バージョニング(D7)、partial success は HTTP 200 + item 別 outcome(D4)、冪等一意性はグローバル一意(D3)へ確定した。

## Goals / Non-Goals

**Goals:**

- 件数のみ応答を per-item 結果(`observation_id` / `existing_id` / outcome / 理由)へ置換し、client の grep 回収を不要化する。
- batch を partial success 化し、有効 item の append と item 別結果を保証する。
- 冪等 identity をサーバ権威で導出・検証し、client 時計揺れで別実体化しない設計を定める。
- error を transient / validation / quarantine へ構造化し、HTTP status + error code で機械判別可能にする。
- ACK セマンティクス(応答↔台帳)を明文化する(宣言まで。派生分離実装は次フェーズ)。
- 暗黙契約(閾値・identity 構成・cursor 形式)を応答と文書に開示する。
- 後方互換の移行パスを定める。

**Non-Goals:**

- ロック分割・派生処理の背景化・非同期 outbox 実装(性能フェーズ / communication-projection)。
- consent / retraction / privacy projection(プライバシーフェーズ)。
- Schema Registry の strict 化(B-09)そのもの。
- grep / OEL / cursor 実装の変更(暗黙契約の**開示**のみ行い実装は変えない)。

## Decisions

### D1: per-item 結果を storage outcome から直接構成する(IRC-01)

`append_observations` は既に `Vec<DurableAppendOutcome>` を prepared observations と同順で返す。応答はこれを 1:1 で per-item 結果へ写像する:

| DurableAppendOutcome | outcome | 付随フィールド |
|---|---|---|
| `Appended(id)` | `ingested` | `observation_id = id` |
| `Duplicate(existing)` | `duplicate` | `existing_id = existing` |
| `CanonicalCollision(existing)` | `quarantined` + ticket | `existing_id`, `error_code = canonical_collision` |

各 item は client 相関キー `client_ref` を持つ。既存 request は per-draft の client 相関キーを持たないため、design 提案として draft に任意の `client_ref`(未指定時は request 内 index)を追加する。集計 `ImportReport` は per-item 配列の summary として残してよい(非破壊)。

### D2: partial success — prepare 失敗を item 結果へ変換する(IRC-02 / IRC-03)

`prepare_observation_draft_batch` の早期 return を廃止し、`ObservationPreparer::prepare` の `Err(IngestResult::{Rejected, Quarantined})` を per-item 結果へ落とす。有効 item だけを `append_observations` へ渡し、prepare 失敗 item と append 段の collision を最終結果配列へマージ(入力順を保持)する。

- `IngestResult::Rejected { class, message }` → outcome `rejected`、分類はキャリア `FailureClass` から `validation`(`ValidationFailure` / `PolicyFailure`)へ写像。
- `IngestResult::Quarantined { ticket }` → outcome `quarantined`、`ticket` 添付。未来時刻・policy deny がここに入る。
- lock 競合・下流一時不能 → `transient`。

route の `SelfHostError::Ingestion → 400` 一律変換は、item 別失敗には使わない。request 全体が成立しない事由(認可失敗・body 上限超過・source_instance_id 空)のみ request レベルのエラーとする。

### D3: サーバ側 canonical identity(IDEM-01 / IDEM-02 / IDEM-03)

サーバは `crates/adapters/api/src/idempotency.rs` の `identity_key(source, object_id, canonical_json)` を取り込み経路で権威的に用いる。client は `object_id` と canonical tuple(または導出済み identity + それらの原料)を提供し、サーバが identity を導出/再検証する。`meta.canonical_json` は現状 storage の collision 判定に使われており(`persistence/mod.rs:2143`)、これを identity の一次入力へ格上げする。

**collision(IDEM-02)— 確定:** 現行 storage は同一 `identity_key` で canonical hash 不一致を `CanonicalCollision` として既に検出する。これを **`quarantined` + ticket**(既存 ID 付き)として返し、`duplicate` と別 outcome / error_code で機械判別可能にする。即時 `rejected` では捨てず、上流の非決定・訂正・client バグの兆候として ticket で追跡・後処理へ回す(Append-Only 維持、既存 Observation は上書きしない)。

**時計非依存(IDEM-03)— 確定(グローバル一意):** canonical identity 自体は `object_id` と canonical tuple で決まり、tuple に生の `event_time` を含めなければ時計揺れで hash は変わらない。ただし冪等の**取りこぼし**は identity ではなく storage の UNIQUE スコープで起きる。現行 UNIQUE は `(leaf_id, identity_key)` で、`leaf_id` は routing_key(published を含み得る)由来のため、**同一実体が published の違いで別 leaf に落ちると per-leaf UNIQUE をすり抜けて重複 append される**(B-08「partition を跨げば per-leaf UNIQUE も効かない」)。運用判断原則(一意性はスケールしなければならない)より、**冪等一意性はグローバル一意へ格上げし、可変時刻由来のルーティングに依存させない**ことを確定する。実装の具体形(identity_key を leaf 非依存の global UNIQUE にする / routing_key を `source:object_id` 由来の不変キーへ再定義する 等)は storage スキーマ移行を伴うため実装時に選ぶが、いずれも「per-leaf スコープで取りこぼす」構造を排除しなければならない。partition-crossing の解消に storage 変更が要る点は Risks に記す。

### D4: 構造化 error 分類と HTTP status(IRC-03)— 確定(200 + item 別)

partial success 応答は **request 全体として HTTP `200 OK`** を返し、item 別の成否は per-item `outcome` + `error_code` で判別する。**確定**: 一部失敗で `207 Multi-Status` や 4xx へ HTTP を分岐しない(判断基準: HTTP ツールチェーン互換性はクライアント種別を問わずスケールする。207 は一般的な 2xx 一括処理を壊し得る)。HTTP status を非 200 にするのは request 全体が成立しない事由(認可失敗・body 上限超過・source_instance_id 空・不正バージョン)のみで、その場合の分類 → status は下表:

| 分類 | 意味 | HTTP(request 全体不成立時のみ) | item outcome(partial success 内) |
|---|---|---|---|
| `validation` | 再送不可・恒久 | 400 | `rejected` |
| `quarantine` | 隔離・ticket 追跡 | (item 別、request は 200) | `quarantined` |
| `transient` | 再送可・一時 | 503 / 409 | (item 別 `error_code`) |

`error_code` は機械可読な**凍結 taxonomy**(IRC-03 / Q6 確定)であり、API バージョンに紐づけて管理する。初期語彙(例): `clock_skew_future`, `schema_validation`, `canonical_collision`, `payload_too_large`, `body_too_large`, `page_limit_exceeded`, `identity_mismatch`, `lock_contention`。バージョン内で意味変更・削除はしない。

### D5: ACK セマンティクスの宣言と派生分離の境界(IRC-04)

`ingested` / `duplicate` は canonical ledger commit 済みを意味し、派生処理(materialize / index / audit)の成否に依存しない。**現状はこれが破れている**(B-01: append 後に materialize / index / audit を同期実行し失敗を HTTP 失敗として返す、`mod.rs:5512-5556`)。本 change は**契約の宣言と応答形状**を定めるが、append commit と派生処理を別 commit 境界へ分離する実装は communication-projection / 性能フェーズへ委ねる。したがって IRC-04 の完全な実現は次フェーズに依存する(下記 Dependencies)。本 change では最低限、append 成功済み item の outcome を派生失敗で反転させない応答構築を要件化する。

### D6: 暗黙契約の開示(IRC-05)

C 章の暗黙値を error response と文書へ出す。閾値はコード直書き(`server.rs:37` の body 上限、`values.rs:103` の clock skew、`config.toml` の page/payload 上限)を単一の設定源から参照し、超過時にその実値と上限をエラーへ含める。cursor 形式差(OEL 数値 / Claim-Card offset / grep opaque)は**開示**の対象だが統一は Non-goal。

### D7: 後方互換 — API バージョニング(IRC-06)確定

運用判断原則(opt-in header は既知 client 列挙に依存しスケールしない・製品境界は厳格に分ける・任意 client 接続前提)より、後方互換は **API バージョニング**で行う(**opt-in header 案は却下**)。

- **非破壊のフィールド追加:** per-item 配列・`error_code` の加算は、既存フィールドを変えない限り同一バージョン内で導入してよい(件数だけ読む client は無視して動く)。
- **挙動変更:** partial success の HTTP 200 化・サーバ権威 identity の厳格検証・厳格 validation は、既存 client の「200=全件成功 / 400=全件失敗」前提を壊すため**新バージョンでのみ**導入する。
- **旧バージョン:** 意味論(応答形状・HTTP status・outcome / error_code 語彙)を凍結し、非推奨化 → 廃止の明示プロセスに載せる。無通知で挙動を変えない。
- **新バージョン:** 初日から厳格契約を適用し、warn-only 猶予期間を設けない(Q4 統合)。

バージョンの表現形式(URL パス `/api/v2/...` / メディアタイプ / バージョン header)は実装時に選ぶが、上記の意味論凍結・新版厳格化を満たすこと。

## Risks / Trade-offs

- **[per-item 化で応答が肥大]** → summary 件数は残しつつ、item 配列は draft 数に線形(既に O(B))。body 上限内。
- **[partial success が既存 client を壊す]** → API バージョニング(D7)で新バージョンにのみ導入し、旧バージョンの既定挙動を凍結。
- **[identity のサーバ導出が既存取り込みを壊す]** → warn-only 猶予は設けない(Q4 確定)。厳格検証は新バージョンにのみ適用し、旧バージョンは従来の client 提供 key 挙動を凍結したまま非推奨化 → 廃止する。バージョン境界で厳格化するため既存 client は旧バージョンで無停止。
- **[IDEM-03 の partition-crossing 修正が storage スキーマに及ぶ]** → グローバル一意への格上げは storage の UNIQUE スコープ / routing 変更を伴い、移行が必要。本 change は契約(グローバル一意の要求)を確定し、スキーマ移行は実装時に含める。
- **[IRC-04 が次フェーズ依存で部分的にしか満たせない]** → 本 change は宣言と応答形状に限定と明記。派生分離の実測完了は communication-projection / 性能フェーズ。

## Dependencies / スコープ重複の回避

- **communication-projection(並行実装中):** materialize の増分化・背景化・派生処理の応答からの切り離しを扱う。IRC-04 の「派生失敗が append 成功を覆さない」の**完全実現**はそちらの派生分離に依存する。本 change は wire 契約(応答形状と宣言)のみを担当し、materialization の内部設計・classify 分岐・projection には触れない。両 change は `mod.rs` の取り込み経路を共有するため、実装時は per-item 応答構築(本 change)と materialize 背景化(comm-projection)のマージ順序を調整する。
- **persistent-search-index:** grep 500ms / 全 scan は暗黙契約として**開示**するのみ。検索実装は変えない。
- **observation-lake(既存 capability):** `IngestResult` 契約を変更せず、wire 上で強制可能にする。

## 確定事項(オーナー決定 2026-07-22)

運用判断原則(スケール優先・製品境界の厳格分離・任意 client 接続前提)に基づき、旧 Open Questions は以下に確定した。

1. **Q1 後方互換の方式 = API バージョニング。** opt-in header 案は「既知 client 2 つ」前提でスケールしないため却下。旧バージョンは意味論凍結、新バージョンで厳格化。表現形式(パス / header)は design 提案どおり実装時に選ぶ(D7 / IRC-06)。
2. **Q2 identity のルーティング時刻依存の切り離し = グローバル一意へ格上げ。** 可変時刻由来ルーティングに依存しない一意性を要件化。per-leaf スコープの取りこぼしを排除。storage スキーマ移行は実装時に含める(D3 / IDEM-03)。
3. **Q3 partial success の HTTP status = 200 + item 別 outcome。** 207 / 4xx へ分岐しない(D4 / IRC-03)。
4. **Q4 サーバ identity 検証の enforcement = バージョニングに統合。** 新バージョンは初日から厳格検証(warn-only 期間なし)、旧バージョンは意味論凍結のまま非推奨化 → 廃止(D7 / IRC-06)。
5. **Q5 CanonicalCollision の既定 outcome = `quarantined` + ticket。** 既存 ID 付きで返し、上流の非決定・訂正・client バグを追跡可能にする。即時 `rejected` にはしない(D3 / IDEM-02)。
6. **Q6 error_code 語彙 = 凍結 taxonomy。** API バージョンに紐づけ、バージョン内で意味変更・削除しない(D4 / IRC-03)。

## 実装確定事項

### Wire version

挙動変更を URL パスで分離する。既存の
POST /api/import/observation-drafts は v1 として意味論を凍結し、
POST /api/v2/import/observation-drafts を新契約の endpoint とする。
request header による opt-in は使用しない。

| Endpoint | 契約 |
|---|---|
| /api/import/observation-drafts (v1) | 既存の件数 summary、request-level failure、既存の client identity namespace を凍結。client_ref などの純粋なフィールド追加は無視できる client を壊さない範囲で許可する。 |
| /api/v2/import/observation-drafts (v2) | HTTP 200 の per-item results、partial success、server-derived identity、strict validation を初日から適用する。 |

v2 の移行は、client が client_ref と meta.object_id /
meta.canonical_json を入力順に固定して送信し、source_instance_id を
identity の source 成分として identity_key を再計算する形で行う。v1 は
非推奨化を告知した後も意味論を変更せず、後続リリースで Sunset を告知してから
廃止する。Intercom、Nanihold、その他の任意 client は同じ v2 契約へ移行する。

### v2 response mapping

response は results と summary を持つ。results は input と同じ順序で
1 item につき 1 件返し、client_ref が未指定の場合は 0-based input index の
文字列表現を使う。

| 状態 | outcome | fields |
|---|---|---|
| durable append | ingested | observation_id |
| same canonical identity | duplicate | existing_id |
| canonical collision | quarantined | existing_id, ticket, error_code=canonical_collision |
| prepare/validation failure | rejected | failure_class, error_code, reason |

構造化 error の初期 taxonomy は clock_skew_future, policy_quarantine,
quarantine_required, schema_validation, policy_validation, identity_conflict,
determinism_failure, non_retryable_failure, transient_failure,
identity_components_missing, identity_mismatch, canonical_json_invalid,
canonical_collision, client_ref_required, payload_too_large, body_too_large,
draft_count_exceeded, page_limit_exceeded, invalid_json とする。既存 version 内で意味変更・削除はせず、追加時は
この文書と versioned API documentation を更新する。

### ACK と制約値

| HTTP / item | canonical ledger | projection / index |
|---|---|---|
| 200 + ingested | item は commit 済み | 後続処理の成否に依存しない |
| 200 + duplicate | existing_id が commit 済み | 後続処理の成否に依存しない |
| 200 + quarantined / rejected | item は append されない | ticket / error code を追跡する |
| request-level 400 | request 自体を受理しない | item append なし |

v2 は body 128 MiB、単一 payload resource_limits.max_payload_bytes（personal
設定の既定 1 MiB）、draft 件数 resource_limits.max_sync_items、page limit
resource_limits.max_page_size（personal 設定の既定 500）を適用する。超過時は
actual と maximum を details に含める。clock skew は
MAX_CLOCK_SKEW=600 秒を単一定義源とし、超過 draft を request-level 400 ではなく
clock_skew_future の ticket 付き quarantine にする。

### v2 response examples

`results` は常に入力順で 1 item につき 1 件を返す。以下は 4 種類の outcome の
最小例である。

```json
{
  "results": [
    {"client_ref": "0", "outcome": "ingested", "observation_id": "019..."},
    {"client_ref": "1", "outcome": "duplicate", "existing_id": "019..."},
    {"client_ref": "2", "outcome": "quarantined", "existing_id": "019...", "ticket": {"id": "qt_...", "reason": "canonical identity collision"}, "error_code": "canonical_collision", "failure_class": "quarantine"},
    {"client_ref": "3", "outcome": "rejected", "error_code": "identity_mismatch", "failure_class": "validation", "reason": "idempotency_key does not match the server-derived canonical identity"}
  ],
  "summary": {"ingested": 1, "duplicates": 1, "quarantined": 1, "rejected": 1}
}
```

identity は source_instance_id:object_id:sha256(canonical_json) とし、
retry では source_instance_id、object_id、canonical tuple、
idempotency_key を固定する。published / event time は identity tuple と
routing uniqueness の根拠にしない。SQLite には leaf routing とは別の
observation_identity_registry(identity_key PRIMARY KEY, ...) を追加し、
既存 rows を backfill して全 leaf 共通の判定境界にする。

## 受入レビュー回帰カバレッジ

受入レビューで指摘された未検証境界を、次のテストで固定する。

- schema v8 移行前の複数 leaf に同一 `identity_key` が存在する場合、registry
  backfill はクラッシュせず、最小 `append_seq` の Observation を勝者にする。
- v2 HTTP 取り込みは、実際に split 済みの別 leaf を跨いで event time を変えた
  retry を単一の `duplicate` へ収束させる。
- v2 の canonical collision は HTTP 200 の item 結果として
  `quarantined`、ticket、`existing_id`、`error_code=canonical_collision` を返し、
  既存 Observation を変更しない。
- `clock_skew_future` と `policy_quarantine` は、ticket の表示文言ではなく
  `QuarantineKind` から導出する。同一 HTTP e2e で両方の wire code と ticket を
  検証する。
