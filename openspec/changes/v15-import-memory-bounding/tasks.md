## 1. 設定と import admission

- [x] 1.1 [Implementer] `openspec/specs/platform-robustness.md` と `import-memory-bounding` の admission 要件に従い、`ResourceLimits`/設定サンプル/各 deploy config/test fixture に `max_concurrent_imports=2`、`max_import_drafts`、`max_search_job_records` を必須追加し、positive validation を実装する。受入: 全設定 parser と既存 fixture が通り、未設定を silent fallback しない。
- [x] 1.2 [Implementer] `apps/selfhost` の v1/v2 service import に shared non-blocking permit を追加し、満杯を `import_concurrency_limit`/429、v1のdraft超過を凍結request error、v2の超過itemを`draft_count_exceeded`/rejectedとして返す。受入: 並行上限rejectとv1/v2 draft上限fail-fastテストが通り、v2 per-item分類が変わらない。

## 2. Publish/materialization 境界

- [x] 2.1 [Implementer] `openspec/specs/observation-lake.md` の append/result semantics を維持し、consent 単独 publish を append consumer/materialization 境界へ統合する。通常 import は consumer を起動し、bulk session は consent/stale marker を同一 publish で処理し、search catch-up はrequest watermark後のsingle-flightで高々一度起動する。受入: watermark 後の single-flight と既存 bulk session 契約が通る。
- [x] 2.2 [Implementer] `publish_core_snapshot` に test instrumentation counter を追加し、1000 件一 request と 25 件×40 直列 request の publish 回数 assertion を実装する。受入: それぞれ `<=2`、`<= request 数+固定定数`。

## 3. Search job retention

- [x] 3.1 [Implementer] `openspec/specs/api-serving.md` の not-found/error semantics を維持し、search job record に insertion sequence と terminal oldest-first eviction を実装する。受入: completed/failed の上限、queued/running 保持、evicted status の 404 テストが通る。

## 4. Communication projection の body-free 化

- [x] 4.1 [Implementer] `crates/projections/cognition` の communication state から Observation body cache を撤去し、scalar/reverse index、`forget_observation`、explicit `rematerialize_observations` API に整理する。受入: serde に body map が出ず、retraction が subject/source/privacy residual を残さない。
- [x] 4.2 [Implementer] selfhost の consent incremental path と paged rebuild を reverse-index SQLite read + bounded page repull に接続する。受入: opt-out→re-consent の遮蔽解除が content/fact を復元し、対象外 observation を読み込まない。
- [x] 4.3 [Implementer] non-corpus manifest version を 10 から 11 に bump し、pre-deserialize version guard と実 v10 shape restart fixture を追加する。受入: v10 は再構築、future version は fail-fast、旧 field の silent compatibility はない。

## 5. SQLite v13 streaming migration

- [x] 5.1 [Implementer] `crates/storage/sqlite/src/persistence/schema.rs` の v13 backfill を append_seq cursor/page 処理へ置換する。受入: page を超える Observation Vec を作らず、DDL/schema version/rows semantics が不変である。
- [x] 5.2 [Reviewer] 大きめ synthetic corpus で旧全件計算と streaming 結果の等価性、transaction rollback/re-run safety を検証する。受入: migration tests が全緑で checkpoint table が存在しない。

## 6. RSS 受入と文書

- [x] 6.1 [Implementer] `scripts/` または `tests/` に Linux container 用 synthetic corpus/RSS/VmHWM harness を追加し、N と bound を引数化する。受入: bound 超過時 non-zero、CI は小 N + publish counter 代替であることが記載される。
- [x] 6.2 [Reviewer] `openspec/changes/v15-import-memory-bounding` の design/tasks と関連 README/config 運用文書を実装結果に合わせて更新し、cargo fmt と `cargo test --workspace` を実行する。受入: 変更ファイル、全テスト数、publish before/after 実測、manifest/schema 判断、残課題が記録される。

## 7. v15.2 boot restore

- [x] 7.1 [Implementer] SQLite open が今回実際に schema migration を適用したかを返し、boot の `full_rebuild_reason="migration"` をその結果に限定する。manifest restore の legacy/watermark/fingerprint rejection は理由を log し、不正な version/current shape は fail-fast にする。受入: 現行 DB+manifest の二回目 boot は rebuild 0、未適用 migration を作った再起動は rebuild 1/reason migration。

## 8. v15.2 rebuild/import concurrency

- [x] 8.1 [Implementer] background non-corpus rebuild の writer persistence mutex を page/read/commit 単位へ分割し、fixed high-water より後の append を base snapshot から除外する。base 完了後は全件 retry せず append-consumer cursor へ tail を handoff する。受入: 継続 append で starvation せず、既存 freshness/communication/row-store 整合テストが通る。
- [x] 8.2 [Implementer] import timing に `bulk_operation_lock_wait_ms`、`persistence_lock_wait_ms`、`spawn_blocking_wait_ms`、rebuild に page/elapsed/lock hold log を追加する。background rebuild 中の単発 v2 import が synthetic test で5秒以内に応答し、consumer収束後の count/watermark が一致することを検証する。

## 9. v15.2 検証と文書

- [x] 9.1 [Reviewer] `docs/development/persistent-index-design.md` と本 change の Verification を実装結果へ更新し、`cargo fmt --all -- --check` と `cargo test --workspace` を実行する。受入: 根本原因A/B、変更ファイル、全テスト数、lock/watermark設計、残課題を記録する。

## 10. v15.2 sync/rebuild/import convoy 解消

- [x] 10.1 [Implementer] source sync と supplemental write の bulk-session 排他を短い admission handshake へ分離し、`bulk_import_operation` を derived lane 待ち・source fetch・検索 catch-up・background rebuild 完了待ちの間は保持しない。bulk session end も CatchingUp 遷移後に mutex を解放し、最終 Ready 遷移時だけ再取得する。受入: 通常 v1/v2 import は sync/rebuild/search の完了を待たず durable append と結果応答まで進める。
- [x] 10.2 [Implementer] 空 Google Slides source の sync が canonical 全件 scan を行わないようにし、background rebuild と空-source sync が同時進行中の単発 v1 import が synthetic test で5秒以内に応答することを検証する。`cargo fmt --all -- --check` と `cargo test --workspace` を実行し、設計・運用文書へ根本原因と全テスト数を記録する。

## 11. v15.2.2 final atomic publish 有界化

- [x] 11.1 [Implementer] SQLite schema v15 migrationでlogical projectionからphysical generationへのheadとdurable retirement queueを追加し、staging→live publishをitem copy/deleteからhead 1行のatomic切替へ置換する。base DDLへv15 objectを追加せず、真のv14形状からのbackfill/fail-fast invariantを検証する。
- [x] 11.2 [Implementer] retired generationを短いpage transactionで回収するsingle-flight workerとwait/hold/row計器を追加する。publish/cleanupのcrash再開、5,000件publishの定数変更row上限、2,000 Observation rebuild final phase中のv1 import 2秒上限を回帰テストする。
- [x] 11.3 [Reviewer] `cargo fmt --all -- --check` と `cargo test --workspace` を実行し、本designと永続index運用文書へ実測件数、テスト総数、crash safety、残課題を記録する。

## 12. v15.2.2 generation read snapshot review

- [x] 12.1 [Implementer] projection item/key/owner/page/blob visibility/countの6読取をhead解決と同じSQLite statementへJOINし、head切替とretired generation cleanupの間にも旧世代または新世代の完全な一方だけを返す。Replace commitが世代をretireした場合もcleanup single-flightを要求する。
- [x] 12.2 [Reviewer] publishと1-row cleanupを反復する並行testで6読取の空・部分結果を禁止し、`cargo fmt --all -- --check`と`cargo test --workspace --quiet`を実行して設計・検証件数を更新する。

## 13. v15.2.3 sync/rebuild non-bulk convoy

- [x] 13.1 [Implementer] `sync_all` はoperation lock取得前にbackground non-corpus rebuildが進行中なら理由付きlogを出してcycleを成功扱いでskipする。sync/supplementalはderived lane取得後にnon-bulk admissionを行い、lane待機中のbulk session beginをrebuild全期間conflictさせない。
- [x] 13.2 [Reviewer] page-delay rebuild中のsync即時skip、同時bulk session begin成功、rebuild完了後の次回sync通常実行を一つの回帰testで検証する。OpenSpec/設計文書を更新し、`cargo fmt --all -- --check`と`cargo test --workspace --quiet`を実行する。
