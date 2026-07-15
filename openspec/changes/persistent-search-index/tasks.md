## 1. Index 基盤

- [x] 1.1 [Implementer] `persistent-search-index` PSI-01/03 に従い Tantivy 依存、versioned schema、stored fields、1〜3-gram tokenizer、複合 sort key、commit metadata を実装する（受入: schema / sort key / metadata の単体テストが通る）
- [x] 1.2 [Implementer] PSI-02/06 に従い record_id delete+add upsert、page batch commit、manual reader reload、watermark catch-up を実装する（受入: 同一 record 再処理後も一件で crash-window replay が通る）
- [x] 1.3 [Implementer] PSI-01/05 に従い generation build、checksum / count 検証、atomic `CURRENT` publish と schema mismatch 判定を実装する（受入: 初回、再 open、schema mismatch のテストが通る）

## 2. 検索契約

- [x] 2.1 [Implementer] PSI-03 に従い安全な literal 1〜3-gram / AllQuery 候補、現行 regex 最終判定、filter、timeout を persistent index search へ接続する（受入: in-memory reference と match 集合が一致する）
- [x] 2.2 [Implementer] PSI-03 に従い date asc / desc 複合 keyset cursor、limit+1、snippet / matched_ranges を維持する（受入: 全角空白 AND、from/to/source_types、両順序の複数 page 契約テストが通る）
- [x] 2.3 [Implementer] PSI-03/04 に従い get_record、get_thread、resolve_link、corpus page、source summaries を index query 化する（受入: 既存 HTTP / MCP / e2e 検索テストが無変更で通る）

## 3. selfhost 統合と増分投影

- [x] 3.1 [Implementer] PSI-01/04 に従い Corpus を `ProjectionSnapshot` と request-local 全件 Vec から除去し、SQLite page source と `SearchIndexManager` を AppService に統合する（受入: 有効 index 再起動で全 Observation / Corpus を検索用にロードしない）
- [x] 3.2 [Implementer] PSI-02/06 に従い single / bulk / sync の durable append 後に watermark 差分だけを commit し、duplicate-only を no-op にする（受入: 増分・bulk・duplicate テストで全再構築がなく件数が一致する）
- [x] 3.3 [Implementer] PSI-01/02 に従い workspace Form-linked Sheet の二 pass rebuild と incremental invalidation、personal mode の Observation 単位投影を実装する（受入: filtering-before-exposure の既存・追加テストが通る）
- [ ] 3.4 [Implementer] PSI-04 に従い検索対象 Observation を resident LakeStore へ複製しない構造へ変更する（受入: 500k harness が全 Observation / Corpus Vec を保持せず peak RSS 条件を満たす）
  - 2026-07-15: `IMPORT_PROCESS_BATCH_SIZE=512` と request batch 10,000 により全 Observation / Corpus Vec を保持しない構造は確認。選択肢2のext4 DB条件で500kは完走したが、VmHWM 3,870,176 KiBが2.5GiB peak RSS受入を超過したため未チェックを維持する。

## 4. 破損復旧と運用状態

- [x] 4.1 [Implementer] PSI-05 に従い Opening/CatchingUp/Rebuilding/Ready/Failed 状態機械と single-flight background rebuild を実装する（受入: 同時破損要求でも rebuild task が一つだけである）
- [x] 4.2 [Implementer] PSI-05 に従い HTTP 503 / MCP 明示エラー、health/readiness detail、空結果・旧 index・全件 scan fallback 禁止を実装する（受入: rebuilding / failed の契約テストが通る）
- [x] 4.3 [Reviewer] index metadata / segment を破損させ、検知→fail-fast→background rebuild→検索復帰を検証する（受入: 破損中に空結果を一度も返さず、再構築後に元の match 集合へ戻る）

## 5. 設定・品質・性能実証

- [x] 5.1 [Implementer] PSI-04/08設計に従い必須 index_dir / writer_heap_bytes / rebuild_page_size 設定と全 example / deploy / test fixture を更新する（受入: 欠落・不正値が fail-fast し、暗黙 default がない）
- [x] 5.2 [Reviewer] workspace 全テスト、cargo fmt、clippy を実行し、既存検索 v2 / HTTP / MCP 契約の回帰がないことを確認する（受入: 全コマンド成功）
- [ ] 5.3 [Implementer] PSI-07 の repository 外一時物・4 GiB・2並列・各群20回以上の 10k/50k/100k/500k harness を追加して実測する（受入: 日付・channel/source・複合語 AND の実効 p95≦2秒、全体検索の参考 p95、peak RSS≦2.5GiB、warm-up failure/OOM/swap 0 の表が得られる）
  - 2026-07-15: 選択肢2（DBをWSL native ext4へbind、4GiB / swap0 / CPU4）で4 stageのimport・検索は完了。500k実効p95 1.152609秒、全体p95 0.866089秒、warm-up/計測failure・swap・OOMは0。ただしVmHWM 3,870,176 KiBが2.5GiB受入を超過したため未チェックを維持する。全実測表は `result.md`。
- [x] 5.4 [Spec Designer] `docs/development/persistent-index-design.md` と OpenSpec 結果報告を、設計、運用、再現コマンド、全実測表、制約、検証結果で更新する（受入: 文書の全値が保存結果と一致し markdown link check が通る）
  - 2026-07-15: 選択肢2のext4 DB配置、4 stageの全実測表、500k完走/p95/OOM、2.5GiB RSS headroom未達、SSD/VHDX依存、再現・cleanup手順を `result.md` と設計書へ反映した。品質検証は実装変更後に再実行する。
