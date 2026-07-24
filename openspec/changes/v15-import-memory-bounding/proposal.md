## Why

568k 規模の corpus に対する 1000 件 bulk import で、単発 import では発生しない AppCore の deep clone と通信 projection の Observation 常駐複製が request 数に比例して RSS を押し上げ、サーバーが OOM になる。migration の全件 `Vec` 収集と import/search の無制限な同時・完了状態保持もピークを増幅するため、v15 で bounded processing を API・materialization・migration の契約として固定する。

## What Changes

- v1/v2 import に設定可能な同時実行数と drafts 件数上限を追加し、満杯・上限超過を既存 error envelope で fail-fast に返す。v2 の per-item 分類と v1 の凍結契約は維持する。
- consent snapshot と append consumer の materialization 境界を統合し、1 request/consumer batch あたりの core snapshot publish を一回に制限する。publish 回数をテスト可能なカウンタで計測し、検索 catch-up は watermark 確定後に一回だけ起動する。
- search job の完了・失敗レコードに設定可能な件数上限または TTL eviction を加え、evicted job の status 参照を明確な not-found 契約にする。
- **BREAKING**: communication projection の非 corpus manifest version を 11 に上げ、`Observation` 本体 cache を manifest/in-memory state から撤去する。re-consent は privacy reverse index で ID を引き、SQLite の page 読みで再 materialize する。旧形状は silent fallback せず再構築へ送る。
- v13 の `observation_privacy_keys` backfill を page/cursor 単位で streaming 化する。DDL と schema version は変更せず、既存 semantics を保つ。
- Linux コンテナ向け RSS/VmHWM 受入ハーネスと、publish 回数・上限・re-consent・実 v10 manifest・migration 等価性の回帰テストを追加する。
- v15.2 では boot ごとの schema migration 適用結果と materialized snapshot の復元判定を分離し、現行 manifest の再起動では保存 snapshot を復元する。復元を拒否する場合は理由を捨てずに記録する。
- v15.2 では background non-corpus rebuild の SQLite writer mutex 保持を page/commit 単位へ分割する。rebuild 中の通常 import は canonical append と per-item 結果までを有界時間で返し、投影 tail は既存 append consumer が非同期に追跡する。
- v15.2 follow-up では source sync/supplemental/bulk end が `bulk_import_operation` を保持したまま derived lane、検索 catch-up、background rebuild 完了を待つ convoy を除去する。空 Google Slides source の sync は canonical 全件 scan を行わない。
- v15.2.3 では background rebuild 中の sync cycle を理由付きでskipし、sync/supplementalのderived lane待機中は `non_bulk_projection_operation` を保持しない。migration rebuild中のstale catalogでもbulk session beginだけは許可し、次回syncがrebuild完了後に通常実行する。

## Capabilities

### New Capabilities

- `import-memory-bounding`: import admission、bounded drafts、publish/materialization 境界、search job eviction、communication state の bounded rebuild、migration streaming を定義する。

### Modified Capabilities

なし。既存の `observation-lake`（v1/v2 result semantics）、`platform-robustness`（resource limits）、`api-serving`（error envelope）の既存意味論を参照し、変更点は本 capability の delta として明示する。

## Impact

- 主対象: `apps/selfhost/src/self_host/server.rs`、`apps/selfhost/src/self_host/config.rs`、`apps/selfhost/src/self_host/app/{mod.rs,service_support.rs,projection_api.rs}`、`crates/projections/cognition/src/lib.rs`、`crates/storage/sqlite/src/persistence/schema.rs`。
- v15.2 追加対象: `apps/selfhost/src/self_host/app/{bulk_import.rs,sync.rs,supplemental_write.rs}`、`crates/storage/sqlite/src/persistence/{mod.rs,schema.rs,tests.rs}`。
- テスト/運用: selfhost app/API tests、cognition projection tests、SQLite migration tests、`scripts/` または `tests/` の memory harness。
- System Laws: Append-Only Law、Replay Law、Filtering-before-Exposure Law を維持する。AppCore の書込/読取分離と deep clone 全廃は次期候補として残し、v15 では publish 回数と常駐複製を bounded にする。
- SQLite DDL は変更しないため schema version は据え置く。manifest version の不一致は fail-fast 再構築に限定する。

## Non-goals

- AppCore の書込/読取分離や deep clone 全廃そのもの。
- v1 の permissive 契約、v2 の per-item 結果分類、canonical Observation の append-only semantics の変更。
- 本番デプロイ、外部接続、push、既存秘密情報の読取。
