# M17 Platform Robustness

## Scope

公開前提で破綻しない API・ingestion・storage・runtime の安全条件を定義する。

## Requirements

- `/health` を除く API endpoint は Bearer token 認証を必須とする。
- token は scope を持ち、`projection:read`, `blob:read`, `admin:sync`, `*` により最小権限で認可する。
- `GET /public/blobs/{sha256}` は無認可公開せず `blob:read` scope を要求する。
- `lake::IngestionGate` は append 前に JSON Schema 検証と `PolicyEngine` 評価を実行する。
- 同一 `idempotencyKey` かつ同一 payload は duplicate skip とし、同一 key で payload が異なる場合は quarantine とする。
- API は projection 汎用 route `/api/projections/{projection_id}/records` のみを提供し、ドメイン固有aliasを設けない。
- secret は `SecretString` で wrap し、Debug 出力で値を表示してはならない。
- blob / payload / sync / page size の上限は `ResourceLimits` と環境変数で構成可能にする。

## Verification

- `tests/e2e/tests/self_host_api.rs` の未認証拒否・認証付き projection/blob tests
- `lake::store` / `lake::ingestion` の idempotency conflict tests
- `cargo test --workspace`
