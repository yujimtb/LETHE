## Context

監査 D 章は読み取り経路の共通欠陥を「API が ID / 検索キーを返さず、クライアントに cursor 0 scan や grep を強いる」「差分処理なのに pagination で全集合を再計算する」と特定した。実コードの現状:

- **OEL(B-06):** trait は `operational_event_page(after_cursor, limit)` / `operational_events_for_stream` / `operational_event_by_id` を提供するが、correlation/causation/event_type の索引付き検索がない(`crates/storage/api/src/lib.rs:128-166`)。correlation は JSON 内、SQLite index は stream 用のみ(`schema.rs:28`)。既知 correlation の監査 trace は cursor 0 から全 page を読みクライアント側で filter する。
- **ページング(B-14):** person 一覧は全 person collect・sort(`projection_api.rs:150`)、Claim/Card は全集合 filter・clone → offset slice(`projection_api.rs:660/901`)、Corpus offset page は先頭 skip(`read.rs:658`)、person messages/slides/timeline と ReplySLO は pagination なし全件(`projection_api.rs:122/196`)。
- **blob 認可(B-15):** 既知 hash でも全 `person_components` と slide refs を `any()` 走査(`service_support.rs:389`)。
- **検索(B-13):** literal 抽出不能 regex は `AllQuery` で全 document scan + regex 判定、timeout 固定 500ms(`search.rs:178`、`grep.rs:12`)。
- **sync 状態(B-17):** `sync_metrics` を永続化するが AppCore 生成時に default へ戻す(`mod.rs:1084`)。
- **cursor 形式(C-8):** OEL 数値 keyset / Claim-Card offset / grep opaque keyset / persons offset の 4 系統が混在。

canonical Observation・OEL・projection は append-only の派生であり、索引・可視表・sync 状態はいずれも canonical から再構築可能な派生 materialization である(Replay Law)。

## Goals / Non-Goals

**Goals:**

- OEL に correlation/causation/event_type/stream の索引付き keyset 検索を追加し cursor 0 全走査を廃止する。
- persisted sort key + keyset cursor でページングを O(返却件数)にし offset・全集合コピーを廃止する。
- 無制限全件応答 API へ cursor を必須化する。
- cursor 形式を API 横断で単一の不透明 keyset cursor に統一する。
- 既知 BlobRef の認可を可視 blob 参照表で O(1) 化する。
- exact/metadata 検索を専用索引経路にし任意 regex を cost class として分離する。
- 再起動時に永続 sync 状態を厳密復元する。

**Non-Goals:**

- 取り込み契約(ingestion-api-contract)、commit 境界・lock(append-commit-and-lock-split)、プライバシー、reply-SLO 判定規則、persistent-search-index の索引実装。

## Decisions

### D1: OEL に索引付き keyset filter を追加する(OIQ-01/02)

`OperationalEventStore` trait へ correlation_id / causation_id / event_type による filter query を追加し、いずれも `after_cursor` + `limit` の keyset cursor を伴う。SQLite は correlation_id / causation_id / event_type を列・複合 index 化する(現状 JSON 内・stream index のみ)。既知 correlation/causation/type の検索は O(log N + k)。cursor 0 からの全 page scan + クライアント側 filter を廃止する。archive/replay の canonical 契約(`operational-event-ledger` OEL-01/03/06)は変更せず、索引は派生として付与する。

### D2: persisted sort key + keyset cursor で O(k) ページング(KSP-01)

person 一覧・ClaimQueue・CardQueue・Corpus records の各読みに persisted sort key(例: person は表示順キー、claim/card は due/priority キー、corpus は既存 encoded keyset)を持たせ、keyset cursor で返却件数 k に対して O(k) にする。全集合の collect/clone/sort と offset slice、深い offset page の先頭 skip を廃止する。filter は複合 index で解決する。

### D3: 無制限全件応答 API への cursor 必須化(KSP-02)

person detail / messages / slides / timeline / ReplySLO 読みなど従来 pagination なしで全件返した API に keyset cursor を必須化する。**reply-SLO 読みの cursor は communication-projection が定義する reply-SLO projection の上に載る**(そのデータモデルは comm-projection、本 change はその読みの cursor 契約のみ)。person detail は当該人物の全履歴を返さず cursor page で返す。

### D4: cursor 形式の API 間統一(KSP-03 / C-8)

OEL 数値 keyset / Claim-Card offset / grep opaque keyset / persons offset の混在を、単一の不透明 keyset cursor 抽象へ統一する。cursor は各 API の persisted sort key を封じた opaque token とし、クライアントが cursor を共通抽象として扱えることを契約する。統一は非破壊追加(既存 cursor を残しつつ opaque 版を追加)から段階導入する。

### D5: 可視 blob 参照表による O(1) blob 認可(BAI-01/02)

projection materialization に「可視 blob reference → owner/projection」表を持たせ、既知 BlobRef の参照可否を O(1)〜O(log N) で判定する。全 `person_components` + slide refs の `any()` 走査を廃止する。可視表は projection materialization と同一 commit で(consent delta と同時に)upsert/delete し、Filtering-before-Exposure Law を保つ。可視表の増分維持は append-commit-and-lock-split の派生 consumer 経路に載る想定だが、本 change は可視表の**認可契約と再構築可能性**を定義する。

### D6: exact/metadata 索引経路と regex cost class の分離(SCC-01/02)

exact metadata / object-id 検索を、任意 regex 全文書走査とは別の専用 API・index 経路で O(postings + candidate) にする。ID 回収を grep 500ms に依存する現在のクライアント契約(B-13 の破綻)を、exact 検索経路で置き換える。任意 regex は通常 SLO から分離した cost class(非同期 search job / 明示的 cost class / 必須 filter)とする。`persistent-search-index` の索引実装・catch-up は変更せず、その上に cost-class 契約を積層する。exact 検索は append-commit-and-lock-split の commit 境界に依存せず既存索引の keyset 読みで成立する。

### D7: 再起動時の sync 状態復元(SSR-01/02)

AppCore 生成時に `last_sync_at` / error / metrics を default へ戻さず、永続 `sync_metrics`(`schema.rs:150`)から起動時に厳密ロードする。health は台帳と整合した実 sync 状態を返す。persisted metrics が欠損・不整合なら明示し偽の初期値で埋めない。

## Risks / Trade-offs

- **[cursor 必須化が既存 client を壊す]** → 応答への cursor 追加は非破壊。無制限全件応答の cursor 必須化は挙動変更のため opt-in / version で段階移行(下記 Q1)。
- **[OEL 索引追加が storage schema に及ぶ]** → correlation/causation/event_type の列・index 追加は移行を伴う。canonical archive 形式は変えず派生索引の追加に限る。
- **[cursor 統一の移行コスト]** → 既存 cursor を残し opaque 版を追加してから収斂させる段階案。
- **[可視 blob 参照表の増分維持]** → 維持経路は append-commit-and-lock-split の consumer に依存(下記 Dependencies)。本 change は認可契約と再構築可能性を定義する。
- **[regex cost class の運用形態]** → 非同期 job / 必須 filter / 明示 cost class のいずれを既定にするかはオーナー確定(Q2)。

## Dependencies / スコープ重複の回避

- **append-commit-and-lock-split:** 読み取り lane(SLP)の並行非ブロックは同 change が提供し、本 change はその lane 上の keyset/index query を定義する。lineage は write digest(同 change)と read pagination(本 change)で責務分割。可視 blob 参照表の増分維持は同 change の派生 consumer に載る。
- **communication-projection:** reply-SLO projection のデータモデルは comm-projection、本 change はその読みへ keyset cursor を課すのみ。
- **persistent-search-index:** 索引実装・catch-up は変更せず、exact/regex の cost class 契約を積層する。
- **ingestion-api-contract:** 取り込み契約は Non-goal。C 章 cursor 形式の**開示**は ingestion 側、cursor 形式の**統一(実装契約)**は本 change。
- **operational-event-ledger / person-page / grep-api:** append・意味論を変えず読みの計算量・cursor を規定する。

## Open Questions(オーナー確定が必要)

1. **Q1 cursor 必須化の移行方式:** 無制限全件応答 API の cursor 必須化を (a) opt-in header、(b) API version、(c) 既定上限 + 明示 cursor のいずれで段階移行するか。
2. **Q2 任意 regex の cost class 形態:** 既定を (a) 非同期 search job、(b) 必須 filter 強制(filter なし regex を拒否)、(c) 明示的 slow-query opt-in のどれにするか。
3. **Q3 cursor 統一の到達点:** 4 系統を単一 opaque 版へ収斂させ旧形式を将来撤去するか、非破壊追加のみに留めるか。
4. **Q4 OEL 索引の対象列:** correlation / causation / event_type に加え actor_id / occurred_at レンジ検索も索引対象にするか。
5. **Q5 可視 blob 参照表の粒度:** owner 単位 / projection 単位 / consent scope 単位のどこをキーにするか(プライバシーフェーズとの整合)。
