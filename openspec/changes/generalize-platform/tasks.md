# Tasks: generalize-platform

**Change:** generalize-platform
**Date:** 2026-06-13

実施順は `proposal.md` の Rollout に従う。各 Phase の完了が次 Phase の
merge gate となる。要件 ID は `specs/platform-generalization/spec.md`
(GEN-*)および `specs/platform-robustness/spec.md`(ROB-*)を参照。
各 Gate の受け入れ基準は対応する Scenario をテスト化したものとする。

## Phase 0 — Foundation (GEN-02, GEN-04)

> 前提: `design.md` U1「Crate 境界の確定手順」を先に実施し、GEN-02 の
> crate 表を現行 `src/` の実測依存に合わせて確定してから 0.1 に着手する。

- [x] 0.1 Cargo workspace へ分割(`lethe-core`, `lethe-policy`,
      `lethe-storage-api`, `lethe-storage-sqlite`, `lethe-adapter-*`,
      `lethe-runtime`, `lethe-selfhost`, `lethe-projection-person`)
- [x] 0.2 CI に依存方向検査(`cargo deny` / custom check)を追加
- [ ] 0.3 `ObservationStore` / `BlobStore` / `SupplementalStore` /
      `ProjectionMaterializer` trait を定義し、既存 SQLite 実装を移植
- [ ] 0.4 storage conformance test suite を作成し SQLite 実装で通過
- [x] 0.5 blob 参照を sha256 content-addressing 契約として固定

**Gate P0:** 全 crate ビルド + 既存 `cargo test` 通過 + conformance 通過
(GEN-02「依存方向違反の検出」/ GEN-04「storage 差し替え」「blob 非依存」)

## Phase 1 — Safety Preconditions (ROB-01, ROB-02)

- [x] 1.1 scope 付き token 認証を API 層に導入(`/health` 除く)
- [x] 1.2 capability model と scope のマッピングを M08 に追記
- [x] 1.3 Projection-scoped blob route に認証と filtered-reference check を導入
- [x] 1.4 `IngestionGate` → `PolicyEngine` 接続 + quarantine surface
- [ ] 1.5 AuditLog を write / export / filtering の必須経路に固定 (ROB-08 一部)

**Gate P1:** 未認証アクセス拒否・quarantine の Scenario テスト通過
(ROB-01「未認証アクセスの拒否」「blob 期限付きアクセス」/
ROB-02「consent のない Observation」)

## Phase 2 — Adapter Generality & Failure Isolation (GEN-03, ROB-03, ROB-04)

- [x] 2.1 read-side `SourceAdapter` trait を定義
- [x] 2.2 Slack / Google Slides を trait 実装へ移植
- [x] 2.3 adapter 宣言メタデータ(credential / rate limit / cursor)を
      Source Contract に拡張
- [ ] 2.4 共通ミドルウェア(retry / backoff / circuit breaker / rate limit)
- [ ] 2.5 dead-letter queue + 部分成功レポート
- [x] 2.6 cursor 永続化と中断再開の統合テスト
- [x] 2.7 idempotencyKey の冪等スキップ / 矛盾 quarantine 二分 +
      property test
- [x] 2.8 adapter conformance test suite

**Gate P2:** 新規ダミー adapter がコア変更なしで追加できることを実証
(GEN-03「新規 source の追加」「contract conformance test」/
ROB-03「リトライ後の再取り込み」「同一 key で payload 相違」/
ROB-04「1 件失敗の隔離」「中断からの再開」)

## Phase 3 — Domain Decoupling (GEN-01, GEN-05, GEN-06, GEN-07, GEN-08)

- [x] 3.1 `person_page` を `lethe-projection-person` へ切り出し
      (person-page delta「Person Page Placement」)
- [x] 3.2 `/api/projections/{id}/*` ルート導入 + `/api/persons/*` 削除
- [x] 3.3 基盤 Entity Type をシードデータ化、コアからドメイン語彙を除去
- [x] 3.4 ingest 時 JSON Schema 検証 + SemVer 互換性検査
- [x] 3.5 `DerivationProvider` trait 化(Gemini を一実装へ)+
      出力スキーマ検証
- [x] 3.6 identity claim の複数種別対応と戦略の Registry 構成化
- [ ] 3.7 構造化 config(複数 source インスタンス、secret 参照分離)

**Gate P3:** person crate 除外ビルドが成功(GEN-01「寮ドメインを含まない
ビルド」「型非依存 API ルート」/ GEN-05 / GEN-06 / GEN-07 / GEN-08 の
各 Scenario 通過)

## Phase 4 — Operational Quality (ROB-05〜ROB-09)

- [x] 4.1 原子性単位の明文化 + orphan blob GC
- [ ] 4.2 ストリーミング bootstrap + API ページネーション必須化
- [x] 4.3 versioned migration 基盤 + golden replay test を CI へ
- [ ] 4.4 tracing / metrics / deep health check
- [ ] 4.5 secret 型ラップ + redaction テスト + 保存時暗号化
- [x] 4.6 公開監査スクリプトのクロスプラットフォーム化(CI 組込み)
- [ ] 4.7 リソース上限(blob / payload / sync / page size)+ retention 接続

**Gate P4:** 全 Scenario テスト通過、`_index.md` 更新、change を archive
(ROB-05〜ROB-09 の各 Scenario 通過)

## Index Update (merge 時)

`openspec/specs/_index.md` への追記案:

| #   | Module                  | Spec File                  | Scope                          | MVP? |
| --- | ----------------------- | -------------------------- | ------------------------------ | ---- |
| M16 | Platform Generalization | platform-generalization.md | ドメイン分離 / plugin / storage port | —    |
| M17 | Platform Robustness     | platform-robustness.md     | authn / 冪等 / 失敗隔離 / 運用品質       | —    |

Dependency DAG 追記: M16 は M01/M02/M09 に依存(cross-cutting)、
M17 は M03/M08/M14/M15 に依存(cross-cutting)。
M13 Person Page は M16 完了後「コア外参照実装」へ位置付け変更。

## Frozen During This Change

- M07 Write-Back の実装着手(Phase 2 完了 = adapter contract 凍結後に解除)

## Phase 5 — Completion Remediation (GEN-02, GEN-01, ROB-07〜09)

2026-06-22 の実装監査で、Phase 0/3/4 の完了表示と実体に差異が確認された。
以下を完了するまで本 change を archive しない。

- [x] 5.1 workspace root を virtual manifest 化し、旧ルート `src/` を削除する
- [x] 5.2 全実装を所有 crate へ物理移動し、`#[path]` とルート crate 逆依存を削除する
- [x] 5.3 adapter / projection / storage / selfhost の依存 DAG を CI で強制する
- [x] 5.4 `AppService` と SQLite persistence を責務別モジュールへ分割する
- [x] 5.5 文書を `docs/` taxonomy へ再配置し、参照リンクと実装スナップショットを更新する
- [x] 5.6 外部ロードマップ・監査受け入れ用 `docs/post/` と運用説明を追加する
- [x] 5.7 公開監査を単一実装へ統合し、ローカルと GitHub Actions の双方で通過させる
- [x] 5.8 fmt / build / test / dependency check / document link check / OpenSpec verify を通過させる

**Gate P5:** ルート `src/` が存在しない、全 workspace member が自身の source
を所有する、禁止依存がない、全文書リンクと全検証が成功する。
