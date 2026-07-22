## Why

監査(`docs/development/principles-audit-20260722.md`)の性能フェーズ優先 #3。読み取り経路が「既知キー O(1)〜O(log N) / cursor O(返却件数)」の期待計算量を満たさず、API がキーや cursor を返さないためクライアントに cursor 0 全走査や grep 回収を強いている。

- **B-06:** OEL モデルは `correlation_id` / `causation_id` を持つが storage trait は cursor / stream / event-id しか提供せず(`storage/api/lib.rs:24/128`)、correlation/causation は JSON 内、index は stream 用だけ(`schema.rs:28`)。既知 correlation の監査 trace が cursor 0 から O(N) page scan + クライアント側 filter になり、Nanihold 実測で 1 回 3〜6 分。
- **B-14:** person 一覧は全 person を collect・sort(`projection_api.rs:150`)、ClaimQueue / CardQueue は全集合を filter・clone してから offset slice(`projection_api.rs:660/901`)、Corpus offset page は先頭から skip(`read.rs:658`)、person messages/slides/timeline と ReplySLO は pagination なしで全件(`projection_api.rs:122/196`)。limit=20 でも母集合増で悪化する。
- **B-15:** 既知 BlobRef でも全 `person_components` と slide refs を走査して認可判定(`service_support.rs:389`)。
- **B-13:** safe literal n-gram を抽出できない regex は `AllQuery` で 128 件ずつ全 document を読み regex 判定、timeout 固定 500ms(`search.rs:178`、`grep.rs:12`)。exact 検索まで巻き込まれる。
- **B-17:** `sync_metrics` を永続化しながら AppCore 生成時に `last_sync_at=None` へ戻し(`mod.rs:1084`)、health が偽の初期値を返す。
- **C-8(暗黙契約):** cursor 形式が API ごとに異なる(OEL 数値 / Claim-Card offset / grep opaque / offset pagination)。

## What Changes

- OEL に correlation/causation/event_type/stream の索引付き keyset 検索契約を追加し、cursor 0 全走査の強制を廃止する。
- ページングを persisted sort key + keyset cursor で O(返却件数)にし、offset 比例・全集合 collect/clone/sort を廃止する。cursor 形式を API 横断で統一する。
- 無制限全件応答 API(person detail / messages / slides / timeline / ReplySLO)へ cursor を必須化する。
- 既知 BlobRef の認可を可視 blob 参照表で O(1)〜O(log N) にする。
- exact / metadata 検索を専用索引経路として分離し、任意 regex 全文書走査を明示的 cost class(非同期 job / 必須 filter)へ切り出す。
- 再起動時に永続 sync 状態を厳密復元する。

## Capabilities

### New Capabilities

- `operational-event-indexed-query`: OEL の correlation/causation/event_type 索引列化と keyset cursor 付き filter 検索契約(B-06)。
- `keyset-pagination`: persisted sort key + keyset cursor による O(k) ページング、無制限全件応答への cursor 必須化、cursor 形式の API 間統一(B-14 / C-8)。
- `blob-authorization-index`: 可視 blob 参照表による O(1) blob 認可(B-15)。
- `search-cost-class`: exact/metadata 索引経路と任意 regex の cost class 分離(B-13)。
- `sync-state-restore`: 再起動時の永続 sync 状態の厳密復元(B-17)。

### Modified Capabilities

なし。`operational-event-ledger` の append 契約、`persistent-search-index` の索引契約、person-page / grep-api の意味論は変更せず、読み取りの計算量・cursor 契約・認可経路・状態復元を規定する新規 capability を定義する。

## Impact

- 主対象: `crates/storage/api/src/lib.rs`(OEL trait)、`crates/storage/sqlite/src/persistence/`(index / schema)、`apps/selfhost/src/self_host/app/projection_api.rs`、`service_support.rs`、`crates/search-index/`、`crates/api/src/api/grep.rs`、`apps/selfhost/src/self_host/server.rs`。
- API: 読み取り応答に keyset cursor を追加(非破壊)。無制限全件応答は cursor 必須化(移行を伴う)。cursor 形式の統一を段階導入する。
- System Laws: Filtering-before-Exposure Law(可視 blob 参照表で認可を保つ)、Replay Law(索引・可視表は canonical から再構築可能)を維持する。
- 対象外: client 実装、本番 selfhost デプロイ、既存 `data/`。

## Non-goals

- 取り込み契約(per-item 応答・identity・partial success)— ingestion-api-contract の責務。
- プライバシー(consent / retraction)— 後続フェーズ。
- commit 境界・lock 分割・書き込み側 O(差分) 化 — append-commit-and-lock-split の責務。本 change は読み取り経路の計算量・cursor・認可・状態復元に限定する。
- `persistent-search-index` の索引実装・catch-up state machine の再設計。本 change はその上に exact/regex の cost class 契約を積層するのみ。
- reply-SLO の判定規則・communication projection のデータモデル — communication-projection の責務。本 change はその読みへ keyset cursor を課すのみ。
