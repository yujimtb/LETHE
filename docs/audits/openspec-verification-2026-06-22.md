# OpenSpec Implementation Verification

- Date: 2026-06-22
- Scope: `generalize-platform`, `sharding-refactor`
- Status: Critical gaps resolved

## Result

2026-06-22 の初回監査で確認された Critical gaps はすべて実装・テスト済み。
両 change の task は完了状態へ戻し、strict validation と workspace 全テストを
通過した。

## Resolved: generalize-platform

- storage Effect Ports を実用的な append / page / tail / blob / supplemental /
  materialization / runtime-state 契約へ変更し、`SqlitePersistence` が全 port を実装。
- SQLite storage conformance suite を追加。
- selfhost bootstrap は Observation / blob / supplemental を全件ロードせず、
  永続 materialized Projection から起動。
- TOML の構造化 config と source instance 配列を導入。secret は `*_env`
  参照のみ許可し、旧平坦環境変数構成を削除。
- retry / exponential backoff / rate-limit / circuit breaker を実呼び出しへ接続。
- item 単位 failure isolation、永続 dead-letter、部分成功レポートを実装。
- structured tracing、sync metrics、認証必須 deep health、直近 sync 状態を実装。
- AuditLog を認証判定、filtering、Observation write の必須経路へ接続し永続化。
- AES-256-GCM による secret 保存時暗号化を実装。
- blob / payload / sync item / API page / leaf capacity の上限と retention / orphan GC
  を構造化 config へ接続。

## Resolved: sharding-refactor

- `observations` を `leaf_id` / `routing_key` / leaf-local `append_seq` を持つ
  SQLite authoritative store とし、in-memory `LakeStore` は実行中キャッシュに限定。
- 容量到達時に新 child leaf を作り、全親行を再配置してから
  `split_commit` する単一 transaction cutover を実装。
- `projection_leaf_watermarks` を永続化し、per-leaf
  `WHERE append_seq > ? ORDER BY append_seq` tail propagation を実装。
- propagation apply は `CommutativeIdempotentObservationFold` 実装に限定し、
  成功後だけ watermark を進める at-least-once 契約へ接続。
- blue/green migration は別 observation / partition-log 構造を構築し、
  mode (b) で identity column、canonical JSON、serialized Observation の
  idempotency key を同時更新後、transactional read cutover、旧物理表削除、
  旧 keyspec / partition log 履歴保存まで実装。

## Verification

- `cargo fmt --all -- --check`
- `cargo check --workspace --all-targets`
- `cargo test --workspace`
- `./scripts/check_dependency_layers.ps1`
- `python scripts/check_markdown_links.py`
- `python scripts/public_release_audit.py`
- `openspec validate sharding-refactor --type change --strict --no-interactive`
- `openspec validate generalize-platform --type change --strict --no-interactive`
- `git diff --check`
