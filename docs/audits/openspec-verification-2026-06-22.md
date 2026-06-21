# OpenSpec Implementation Verification

- Date: 2026-06-22
- Scope: `generalize-platform`, `sharding-refactor`
- Status: Active changes retained

## Result

両 change は構文上 valid であり、workspace の全 test も成功した。ただし、
仕様と実装の直接照合で未実装要件が確認されたため、archive は実施しない。
該当する `tasks.md` の完了表示を未完了へ戻した。

## Critical gaps

### generalize-platform

- raw CAS 配信は撤去済みで、Projection-scoped blob route は Bearer token、
  scope、filter 済み Projection からの参照を検証する。
- `SqlitePersistence` は storage port trait を実装しておらず、selfhost は具象型を
  直接使用している。
- sync は外部API失敗を `?` で即時返却し、dead-letter と部分成功を構成しない。
- config は平坦な環境変数から legacy source 構成を生成しており、構造化
  multi-source config ではない。
- bootstrap は Observation、blob、supplemental を全件RAMへ展開する。
- tracing、metrics、実体のある deep health、secret保存時暗号化、
  resource limit強制が未実装である。

### sharding-refactor

- selfhost は全 Observation を単一 `LakeStore` へロードしており、
  per-leaf SQLite authoritative の運用になっていない。
- watermark は `HashMap` ベースの in-memory storeで、
  `projection_leaf_watermarks` 永続テーブルがない。
- split cutover は状態機械と計画生成までで、物理leafのbuild/rehome/cutoverへ
  接続されていない。
- blue/green keyspec migration は状態遷移モデルまでで、新構造作成、
  bulk rehome、read cutover、旧構造retireへ接続されていない。

## Verified repository remediation

- virtual Cargo workspaceとcrate source ownership
- root `src/`、`#[path]`、root package逆依存の撤去
- dependency DAG check
- `AppService` と SQLite persistence の責務分割
- 未実装の Write-Back / Notion adapter の撤去
- `docs/` taxonomyと`docs/post/`
- 単一のクロスプラットフォーム公開監査
- `cargo fmt --check`、`cargo test --workspace`、Markdown link check、
  public release audit、OpenSpec strict validation
