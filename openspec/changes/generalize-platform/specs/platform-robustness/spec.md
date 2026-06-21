# Spec Delta: platform-robustness

**Change:** generalize-platform
**Version:** 0.1 (draft)
**Date:** 2026-06-13

## Dependencies

- M01 Domain Kernel — failure model の正規参照
- M03 Observation Lake — append / quarantine surface
- M08 Governance — capability / consent / audit の正規参照
- M14 API Serving — 認証・認可の適用面
- M15 Runtime — queue / health / checkpoint の実装面

---

## ADDED Requirements

### Requirement: ROB-01 API Authentication and Authorization

すべての API endpoint(`/health` を除く)はリクエストを認証 **しなければならない (SHALL)**。認可は M08 の capability model に接続 **しなければならない (SHALL)**(最小実装: scope 付き token)。blob は Projection-scoped route でのみ配信し、filter 済み Projection 内に参照が存在することを確認 **しなければならない (SHALL)**。raw CAS を hash だけで取得可能にしてはならない。blob 配信は Filtering-before-Exposure Law の適用対象で **なければならない (SHALL)**。

#### Scenario: 未認証アクセスの拒否

- **WHEN** token なしで `GET /api/projections/{id}/records` を呼ぶ
- **THEN** 401 が返り、AuditLog に拒否イベントが記録される

#### Scenario: Projection-scoped blob access

- **WHEN** 認証済みクライアントが filter 済み Projection から参照される thumbnail blob を取得する
- **THEN** 200 が返る
- **AND WHEN** 同じ token で Projection 未参照の raw CAS blob を取得する
- **THEN** 404 が返る

### Requirement: ROB-02 Ingestion Gate Policy Enforcement

`lake::IngestionGate` は append 前に M08 PolicyEngine を呼び出し **しなければならない (SHALL)**。consent / policy 違反の Observation は append されず quarantine され **なければならない (SHALL)**。quarantine は黙殺ではなく、件数と理由が sync レポートに含まれ **なければならない (SHALL)**。

#### Scenario: consent のない Observation

- **WHEN** ConsentRef が無効な Observation を ingest する
- **THEN** Lake に append されず quarantine に入り、AuditLog と
  sync レポートに理由が記録される

### Requirement: ROB-03 Idempotent Ingestion

全 adapter は `idempotencyKey` を実装 **しなければならない (SHALL)**。同一 key の再到着は「冪等スキップ」として扱い、payload が異なる同一 key は「矛盾エラー」として quarantine **しなければならない (SHALL)**(現行の「重複は一律エラー」を二分する)。`published` / `recordedAt` の二重時刻は全経路で維持され、out-of-order 到着を許容 **しなければならない (SHALL)**。

#### Scenario: リトライ後の再取り込み

- **WHEN** sync が途中失敗し、同じページを再取得して ingest する
- **THEN** 既存と同一 key・同一 payload の Observation はスキップされ、
  Lake の件数は増えない(property test で検証)

#### Scenario: 同一 key で payload が異なる

- **WHEN** 同一 idempotencyKey で内容の異なる Observation が到着する
- **THEN** append されず矛盾として quarantine され、運用者に可視化される

### Requirement: ROB-04 Failure Isolation and Resumable Sync

外部 API 呼び出し(source / write-back / derivation provider)は共通ミドルウェアとして retry + exponential backoff + rate limit 遵守 + circuit breaker を備え **なければならない (SHALL)**。単一 observation の失敗は sync 全体を停止 **させてはならない (SHALL NOT)**。失敗 item は dead-letter queue に隔離し、sync は部分成功レポート(成功 / スキップ / 失敗 / quarantine の件数と理由)を返却 **しなければならない (SHALL)**。source ごとの cursor は永続化され、中断後に重複・欠落なく再開 **できなければならない (SHALL)**。

#### Scenario: 1 件の slide-analysis 失敗

- **WHEN** 100 件中 1 件の derivation が provider error で失敗する
- **THEN** 99 件は正常に取り込まれ、1 件は dead-letter に入り、
  レポートに失敗理由が含まれる

#### Scenario: 中断からの再開

- **WHEN** sync をプロセス kill で中断し再起動する
- **THEN** 永続化された cursor から再開し、Lake に重複も欠落も生じない
  (統合テストで検証)

### Requirement: ROB-05 Transactional Boundaries and Recovery

observation append / blob save / supplemental write の原子性単位は M01 failure model に従って明示 **しなければならない (SHALL)**。部分失敗(blob は書けたが metadata が書けない等)からの復旧手順(orphan blob GC、再 ingest)を定義し、実装 **しなければならない (SHALL)**。現行の個別 rollback 対処は本一般則に統合する。

#### Scenario: metadata 書き込み失敗

- **WHEN** blob 保存後に ObservationStore への書き込みが失敗する
- **THEN** Observation は存在しない状態に収束し、orphan blob は GC の
  対象としてマークされる(RAM/DB 不整合を残さない)

### Requirement: ROB-06 Scalable Bootstrap and Concurrency Model

bootstrap は Lake 全体の RAM 展開を前提と **してはならない (SHALL NOT)**。ストリーミング / ページング読み出しで RAM 超過サイズの Lake を扱え **なければならない (SHALL)**。SQLite 実装では writer 直列化を明示し、他 storage 実装ではマルチ writer を許容できる設計とする。長時間 sync は runtime の job queue 上で再開可能チェックポイントを持つ。

#### Scenario: RAM を超える Lake の起動

- **WHEN** 利用可能メモリより大きい Lake で selfhost を起動する
- **THEN** 起動は完了し、API はページング付きで応答する

### Requirement: ROB-07 Migration and Replay Compatibility

DB schema はバージョン管理された migration で管理 **しなければならない (SHALL)**(`dokp.sqlite3` → `lethe.sqlite3` 型の移行を体系化)。Projection rebuild に対しては golden replay test を CI に置き、pinned 入力からの同一出力(Replay Law)を継続検証 **しなければならない (SHALL)**。

#### Scenario: migration 適用後の replay

- **WHEN** 新 migration を適用し、pin 済み入力で Projection を rebuild する
- **THEN** 出力は golden snapshot と一致する

### Requirement: ROB-08 Observability and Mandatory Audit Path

構造化ログ(`tracing`)、sync metrics(取得 / スキップ / 失敗 / レイテンシ)、依存 store と直近 sync 状態を含む deep health check を提供 **しなければならない (SHALL)**。すべての write / export / filtering 判定は AuditLog を必ず通過する経路に固定 **しなければならない (SHALL)**(バイパス経路を残さない)。

#### Scenario: filtering 判定の監査

- **WHEN** person detail で `identities` が Filtering-before-Exposure に
  より除外される
- **THEN** 当該判定が AuditLog に記録され、後から追跡できる

### Requirement: ROB-09 Secret Handling and Resource Limits

token / refresh token は secret 型でラップし、Debug 出力・ログ・エラーメッセージへ漏出 **してはならない (SHALL NOT)**。保存時は暗号化する。公開監査(`public-release-audit`)は CI で動作するクロスプラットフォーム実装に置き換え **なければならない (SHALL)**。blob サイズ・payload サイズ・1 sync の取得上限・API ページネーションの各上限を構成可能なデフォルト付きで設け **なければならない (SHALL)**。retention / GC は M08 の retention policy に接続する。

#### Scenario: エラーメッセージへの token 非漏出

- **WHEN** 無効 token で Slack API 呼び出しが失敗する
- **THEN** ログとエラーに token 値が含まれない(redaction テストで検証)

#### Scenario: 上限超過 blob の拒否

- **WHEN** 構成上限を超えるサイズの attachment を ingest する
- **THEN** ingest は明示的エラーで拒否され、部分書き込みを残さない
