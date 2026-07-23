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
