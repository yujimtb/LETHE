# LETHE

LETHE は、外部ソースから Observation を取り込み、append-only Lake、
Projection、Governance を通じて再現可能な知識ビューを構築する Rust workspace
です。

## Repository layout

```text
apps/
  selfhost/                    self-host API と実行時配線
  tools/                       運用CLI
crates/
  core/                        Domain Kernel
  policy/                      Governance policy
  registry/                    Schema / observer registry
  engine/                      Lake / projection / propagation / identity
  api/                         API contract
  runtime/                     partition / resolver / runtime control
  storage/                     storage port と SQLite 実装
  adapters/                    source / write-back adapter
  derivations/                 AI derivation provider
  projections/                 domain projection
docs/                          説明文書、監査、外部投稿
openspec/                      正典仕様とchange
resources/                     seedなどの静的資源
tests/e2e/                     workspace横断テスト
```

workspace root はvirtual manifestです。Rust実装を持つルート`src/`は存在せず、
各crateが自身のsourceを所有します。

## Documentation

- [Documentation index](docs/README.md)
- [System overview](docs/architecture/system-overview.md)
- [Domain algebra](docs/architecture/domain-algebra.md)
- [Runtime reference](docs/architecture/runtime-reference.md)
- [Repository layout](docs/development/repository-layout.md)
- [OpenSpec module index](openspec/specs/_index.md)
- [Security](SECURITY.md)

外部で作成したロードマップや監査は[docs/post/](docs/post/README.md)へ配置します。

## Build and test

```powershell
cargo fmt --all -- --check
cargo check --workspace
cargo test --workspace
./scripts/check_dependency_layers.ps1
python ./scripts/public_release_audit.py
python ./scripts/check_markdown_links.py
```

## Self-host

`.env.example`を基に必要なcredentialとsource設定を定義して起動します。

```powershell
cargo run -p lethe-selfhost
```

主なendpoint:

- `GET /health`
- `GET /health/deep`
- `POST /admin/sync`
- `GET /public/blobs/{sha256}`
- `GET /api/projections/{projection_id}/records`
- `GET /api/projections/{projection_id}/records/{record_id}`
- `GET /api/projections/{projection_id}/records/{record_id}/slides`
- `GET /api/projections/{projection_id}/records/{record_id}/messages`
- `GET /api/projections/{projection_id}/records/{record_id}/timeline`

`/health`とblob配信以外はBearer tokenを必要とします。Person projection IDは
`proj:person-page`です。旧`/api/persons/*`ルートは提供しません。

## Local state

SQLite、blob、取得済み資料、credentialはGit管理対象外です。既定のローカル配置は
`data/`ですが、公開用fixture以外のruntime payloadをcommitしてはいけません。

公開前に次を実行してください。

```powershell
python ./scripts/public_release_audit.py --check-history
```
