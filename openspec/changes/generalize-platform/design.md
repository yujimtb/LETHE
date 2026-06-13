# Design: generalize-platform

**Change:** generalize-platform
**Version:** 0.1 (draft)
**Date:** 2026-06-13

本書は `proposal.md`(WHY)と `specs/*/spec.md`(WHAT)を受けて、
実装上の判断・前提・未確定事項(HOW / リスク)を記録する。
normative な要件は spec 側にあり、本書はそれを覆さない。

---

## Context

- 本 change は **System Laws と既存モジュール契約(M01–M15)を不変** とした
  semantics-preserving refactoring である(`proposal.md` の Non-goals 参照)。
- 既存仕様の中核(Lake / Registry / Projection / Governance)はすでにドメイン
  非依存であり、本 change は「実装に残る固定化」のみを解消する。
- 本書および spec の crate 名・trait 名は **提案値** であり、実装着手時に
  現行コードへ合わせて調整する(下記「未確定事項」)。

## Key Decisions

### D1. Functional Core / Imperative Shell をレイヤード workspace で強制

- pure(`lethe-core` / `lethe-policy`)と effectful(storage / adapter /
  runtime / selfhost)を crate 境界で分離し、依存方向を CI で DAG として
  強制する(GEN-02)。
- 強制手段は `cargo deny` を第一候補とし、表現力が不足する場合は custom
  check(workspace metadata 走査)へフォールバックする。

### D2. すべての副作用を Effect Ports(trait)経由にする

- `ObservationStore` / `BlobStore` / `SupplementalStore` /
  `ProjectionMaterializer`(GEN-04)、および `SourceReader` / `Observer` /
  `WriteBackAdapter`(GEN-03)、`DerivationProvider`(GEN-06)を trait 化。
- 各 port には **共通 conformance test suite** を用意し、具象実装は同一の
  受け入れテストに合格することを差し替え可能性の保証とする。
- SQLite + ローカル blob は「参照実装の一つ」へ降格(既定のまま残す)。

### D3. blob は sha256 content-addressing を保存先非依存契約として固定

- 参照にファイルパス・URL を含めない。backend 交換後も Replay Law を満たす
  (GEN-04)。orphan blob は ROB-05 の GC 対象として扱う。

### D4. 公開前提の安全条件を Phase 1 で先行投入

- 「単一バイナリ + 認証なし API」は internal-only 前提が破れた瞬間に
  安全性を失うため、adapter 一般化(Phase 2)より先に ROB-01 認証認可と
  ROB-02 IngestionGate 接続を入れる(`proposal.md` の Sequencing)。

### D5. ドメイン語彙はシードデータとして外部化

- 基盤 Entity Type(`et:person` 等)は **コード定数ではなくシードデータ**。
  `person_page` 相当は `lethe-projection-person` としてコア外に置き、
  API は型非依存の `/api/projections/{id}/*` を正路とする(GEN-01)。
  互換のため `/api/persons/*` を 1 リリースだけ deprecation alias で維持。

### D6. 失敗は隔離し、sync は部分成功を返す

- 外部 API 呼び出しは共通ミドルウェア(retry / backoff / rate limit /
  circuit breaker)を通す。単一失敗で全体停止させず、dead-letter +
  部分成功レポート + 永続 cursor で再開可能にする(ROB-03 / ROB-04)。

## 未確定事項 / 実装着手時に確定する前提

> これらは **コードを読まずに README と仕様文書から起こした** ため、
> 実装着手の最初のステップで現物と突き合わせて確定する。

### U1. Crate 境界の確定手順(Phase 0 の前提)

`specs/platform-generalization/spec.md` GEN-02 の crate 表は提案値である。
Phase 0 着手時に以下を実施してから境界を確定すること:

1. 現行 `src/` のモジュール間依存を実測する
   (例: `cargo modules` / `cargo depgraph`、または `mod` と `use` の走査)。
2. pure に保てるモジュール(型・law・判定)と effectful なモジュール
   (`adapter/`, `self_host/`, storage)を仕分けする。
3. 既存の循環依存・レイヤ違反を列挙し、crate 分割前に解消すべきものと、
   分割で自然に解ける（境界をまたぐだけの）ものを区別する。
4. 確定した DAG を GEN-02 の表へ反映し、CI 強制ルールを書く。

`Gate P0`(全 crate ビルド + 既存 `cargo test` 通過 + conformance 通過)を
満たすまで後続 Phase へ進まない。

### U2. trait 名・粒度

`SourceReader` / `Observer` / `WriteBackAdapter` / `DerivationProvider` 等の
名称と分割粒度は M09 Adapter Policy / 既存 `src/adapter/` の実コードに合わせて
調整してよい。spec の Scenario(受け入れ基準)を満たす限り内部設計は自由。

### U3. config スキーマ

GEN-08 の構造化 config は形式(TOML / YAML)とキー設計を実装時に決める。
secret は本体に平文で持たず env / secret store 参照とする制約のみ固定。

## Open Questions(→ `adr_backlog.md` 候補)

1. **plugin の配布形態**: 同一 workspace 内 crate に留めるか、dynamic load
   (dlopen / WASM component)まで踏み込むか。本 change は workspace 内 crate
   を前提とし、dynamic load は別 ADR とする。
2. **マルチテナント(namespace 分離)**: 本 change の scope に含めるか、別
   change とするか。現状は scope 外(`proposal.md` Non-goals)。
3. **Postgres 参照実装の昇格時期**: SQLite を既定のまま残すか。GEN-04 で
   差し替え可能性は担保するが、Postgres 実装の昇格は別 ADR とする。

## Frozen During This Change

- M07 Write-Back の実装着手は本 change 中 **凍結**。Phase 2 完了
  (GEN-03 adapter contract 凍結)後に解除する(`tasks.md` 参照)。
