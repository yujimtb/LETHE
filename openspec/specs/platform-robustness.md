# M17 Platform Robustness

## Scope

公開前提で破綻しない API・ingestion・storage・runtime の安全条件を定義する。

## Requirements

- `/health` を除く API endpoint は Bearer token 認証を必須とする。
- token は scope を持ち、`read:persons`, `read:timeline`, `admin:sync`, `*` により最小権限で認可する。
- blob は `GET /api/projections/{projection_id}/blobs/{sha256}` でのみ配信し、`read:persons` scope と filter 済み Projection 内の参照を要求する。
- raw CAS は hash を知っているだけでは取得できない。
- `lake::IngestionGate` は append 前に JSON Schema 検証と `PolicyEngine` 評価を実行する。
- 同一 `idempotencyKey` かつ同一 payload は duplicate skip とし、同一 key で payload が異なる場合は quarantine とする。
- API は projection 汎用 route `/api/projections/{projection_id}/records` のみを提供し、ドメイン固有aliasを設けない。
- secret は `SecretString` で wrap し、Debug 出力で値を表示してはならない。
- blob / payload / sync / page size の上限は `ResourceLimits` と環境変数で構成可能にする。
- 必須環境変数の欠落・空欄・不正形式は起動時エラーとし、暗黙の既定値を使わない。

## Verification

- `tests/e2e/tests/self_host_api.rs` の未認証拒否・認証付き projection/blob tests
- `lake::store` / `lake::ingestion` の idempotency conflict tests
- `cargo test --workspace`
