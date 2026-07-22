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

`OperationalEventStore` trait へ correlation_id / causation_id / event_type / actor_id による filter query を追加し、いずれも `after_cursor` + `limit` の keyset cursor を伴う。SQLite の初期索引対象列は **correlation_id / causation_id / event_type / stream / actor_id + occurred_at(時刻レンジ)**とする(現状 JSON 内・stream index のみ)。既知 correlation/causation/type/actor の検索は O(log N + k)。cursor 0 からの全 page scan + クライアント側 filter を廃止する。canonical append は append-only ゆえ索引は後付け可能であり、この初期集合を超える索引列は投機的に足さず実需要に駆動されて拡張する(旧 Q4 確定 — actor_id / occurred_at は最初から含める)。archive/replay の canonical 契約(`operational-event-ledger` OEL-01/03/06)は変更せず、索引は派生として付与する。

### D2: persisted sort key + keyset cursor で O(k) ページング(KSP-01)

person 一覧・ClaimQueue・CardQueue・Corpus records の各読みに persisted sort key(例: person は表示順キー、claim/card は due/priority キー、corpus は既存 encoded keyset)を持たせ、keyset cursor で返却件数 k に対して O(k) にする。全集合の collect/clone/sort と offset slice、深い offset page の先頭 skip を廃止する。filter は複合 index で解決する。

### D3: 無制限全件応答 API への cursor 必須化(KSP-02)

person detail / messages / slides / timeline / ReplySLO 読みなど従来 pagination なしで全件返した API に keyset cursor を必須化する。**reply-SLO 読みの cursor は communication-projection が定義する reply-SLO projection の上に載る**(そのデータモデルは comm-projection、本 change はその読みの cursor 契約のみ)。person detail は当該人物の全履歴を返さず cursor page で返す。

### D4: cursor 形式の API 間統一と API バージョニング移行(KSP-03 / C-8)

OEL 数値 keyset / Claim-Card offset / grep opaque keyset / persons offset の混在を、単一の不透明 keyset cursor 抽象へ統一する。cursor は各 API の persisted sort key を封じた opaque token とし、クライアントが cursor を共通抽象として扱えることを契約する。統一と無制限全件応答の cursor 必須化(D3 / KSP-02)は、契約フェーズ(ingestion-api-contract)と**同じ API バージョニング方式**で移行する — 新 API version で統一 keyset cursor を提供し、旧 version は凍結して将来廃止する(旧 Q1 / Q3 確定)。旧 version へ既存 client を無通知で壊す破壊的変更は加えない。

### D5: 可視 blob 参照表による O(1) blob 認可(BAI-01/02)

projection materialization に「可視 blob reference → owner/projection」表を持たせ、既知 BlobRef の参照可否を O(1)〜O(log N) で判定する。全 `person_components` + slide refs の `any()` 走査を廃止する。可視表は projection materialization と同一 commit で(consent delta と同時に)upsert/delete し、Filtering-before-Exposure Law を保つ。可視表の増分維持は append-commit-and-lock-split の派生 consumer 経路に載る想定だが、本 change は可視表の**認可契約と再構築可能性**を定義する。

### D6: exact/metadata 索引経路と regex cost class の分離(SCC-01/02)

exact metadata / object-id 検索を、任意 regex 全文書走査とは別の専用 API・index 経路で O(postings + candidate) にする。ID 回収を grep 500ms に依存する現在のクライアント契約(B-13 の破綻)を、exact 検索経路で置き換える。任意 regex は通常の同期検索 SLO から分離した **非同期 search job** として実行する(旧 Q2 確定)。同一プロセス内の低優先同期実行はスケール原則上、資源の食い合いで規模破綻するため却下し、job キューで隔離する。**この非同期 job 方式は後続フェーズ・オプション扱いにせず本 change の初期実装スコープに含める必須経路**とする(オーナー明言: 判断の先延ばしは技術的負債)。`persistent-search-index` の索引実装・catch-up は変更せず、その上に非同期 job 契約を積層する。exact 検索は append-commit-and-lock-split の commit 境界に依存せず既存索引の keyset 読みで成立する。

### D7: 再起動時の sync 状態復元(SSR-01/02)

AppCore 生成時に `last_sync_at` / error / metrics を default へ戻さず、永続 `sync_metrics`(`schema.rs:150`)から起動時に厳密ロードする。health は台帳と整合した実 sync 状態を返す。persisted metrics が欠損・不整合なら明示し偽の初期値で埋めない。

## Risks / Trade-offs

- **[cursor 必須化・統一が既存 client を壊す]** → API バージョニングで移行(確定 1)。新 version で統一 keyset cursor を提供し旧 version を凍結、既存 client の既定挙動を無通知で変えない。
- **[OEL 索引追加が storage schema に及ぶ]** → correlation/causation/event_type/occurred_at の列・index 追加は移行を伴う。canonical archive 形式は変えず派生索引の追加に限り、append-only ゆえ後付け可能(確定 3)。
- **[可視 blob 参照表の増分維持]** → 維持経路は append-commit-and-lock-split の consumer に依存(下記 Dependencies)。粒度キーはプライバシーフェーズに依存(確定 4)。本 change は認可契約と再構築可能性を定義する。
- **[非同期 regex job のインフラ増]** → job キュー・進捗・結果取得の実装が要る。ただし同一プロセス同期実行の規模破綻を避けるため初期スコープ必須(確定 2)。

## Dependencies / スコープ重複の回避

- **append-commit-and-lock-split:** 読み取り lane(SLP)の並行非ブロックは同 change が提供し、本 change はその lane 上の keyset/index query を定義する。lineage は write digest(同 change)と read pagination(本 change)で責務分割。可視 blob 参照表の増分維持は同 change の派生 consumer に載る。
- **communication-projection:** reply-SLO projection のデータモデルは comm-projection、本 change はその読みへ keyset cursor を課すのみ。
- **persistent-search-index:** 索引実装・catch-up は変更せず、exact/regex の cost class 契約を積層する。
- **ingestion-api-contract:** 取り込み契約は Non-goal。C 章 cursor 形式の**開示**は ingestion 側、cursor 形式の**統一(実装契約)**は本 change。移行方式(API バージョニング)は契約フェーズと同方式。
- **プライバシーフェーズ change(起草中):** 可視 blob 参照表の粒度(owner / projection / consent scope)は consent モデルが正であり、その確定はプライバシーフェーズの spec に依存する(旧 Q5 委譲)。本 change は BAI で認可契約と再構築可能性を定義し、粒度キーの確定はプライバシー spec に委ねる。
- **operational-event-ledger / person-page / grep-api:** append・意味論を変えず読みの計算量・cursor を規定する。

## 確定事項(オーナー決定 2026-07-23)

1. **cursor 必須化・cursor 統一 = API バージョニング(旧 Q1 / Q3 確定):** 無制限全件応答 API の cursor 必須化と 4 系統 cursor の統一は、契約フェーズ(ingestion-api-contract)と同じ API バージョニング方式で移行する。新 version で統一 keyset cursor を提供し、旧 version は凍結して将来廃止する。
2. **任意 regex = 非同期 search job(旧 Q2 確定・初期スコープ必須):** 任意 regex は非同期 search job で隔離する。同一プロセス低優先同期は資源食い合いで規模破綻するため却下。この方式は後続フェーズ・オプションにせず本 change の初期実装スコープに含める必須経路とする(判断の先延ばしは技術的負債)。
3. **OEL 索引列 = 初期集合(actor_id / occurred_at を最初から含む)+ さらなる列は需要駆動(旧 Q4 改訂確定):** 初期索引対象は correlation / causation / event_type / stream / actor_id + occurred_at とする。これを超える列は append-only ゆえ後付け可能なので投機的に足さず実需要で拡張する。
4. **可視 blob 表の粒度 = プライバシーフェーズへ委譲(旧 Q5):** consent モデルが粒度の正であり、プライバシーフェーズ change の spec に依存として記載(Dependencies 参照)。本 change の Open Question からは除外する。

残る Open Question はない(全事項が確定または委譲済み)。
