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
  adapters/                    read-side source adapter
  derivations/                 AI derivation provider
  projections/                 domain projection
docs/                          説明文書、監査、外部投稿
openspec/                      正典仕様とchange
resources/                     seedなどの静的資源
tests/e2e/                     workspace横断テスト
```

workspace root は virtual manifest です。Rust 実装を持つルート `src/` は存在せず、
各 crate が自身の source を所有します。

## Current implementation

- Slack / Google Slides の read-side adapter
- append-only Observation Lake と SQLite 永続化
- Gemini による slide-analysis derivation
- identity resolution と Person Page Projection
- scope 付き Bearer token、consent、Filtering-before-Exposure
- Projection lineage と、Projection が参照する blob の限定配信
- storage Effect Ports と per-leaf SQLite authoritative store
- 容量駆動 split、永続 per-leaf watermark、blue/green keyspec migration
- retry / rate limit / circuit breaker、dead-letter、部分成功 sync
- structured multi-source config、structured tracing、metrics、deep health

M07 Write-Back は Post-MVP の仕様のみ存在し、write router や
source-native write adapter は実装していません。

## Documentation

- [Documentation index](docs/README.md)
- [System overview](docs/architecture/system-overview.md)
- [Domain algebra](docs/architecture/domain-algebra.md)
- [Runtime reference](docs/architecture/runtime-reference.md)
- [Repository layout](docs/development/repository-layout.md)
- [OpenSpec module index](openspec/specs/_index.md)
- [Security](SECURITY.md)

外部で作成したロードマップや監査は [docs/post/](docs/post/README.md) へ配置します。

## Build and test

```powershell
cargo fmt --all -- --check
cargo check --workspace
cargo test --workspace
./scripts/check_dependency_layers.ps1
python ./scripts/public_release_audit.py
python ./scripts/check_markdown_links.py
```

## Self-host quickstart

前提:

- Rust stable toolchain
- Slack Bot Token
- Slack thread reply 読み取り用 token
- Google Slides / Drive を読める OAuth access token、または
  `client_id` / `client_secret` / `refresh_token`
- Gemini API key

`config.example.toml` をコピーして構造化設定を作成し、`.env.example` に列挙した
環境変数をプロセス環境へ設定します。アプリケーションが読む設定入口は
`LETHE_CONFIG_PATH` のみです。TOML には secret 値を書かず、`*_env` で環境変数名を
参照します。Slack / Google Slides source は配列で複数 instance を定義でき、
instance ごとに独立 cursor と failure isolation を持ちます。

```powershell
cargo run -p lethe-selfhost
```

必須値が欠落・空欄・不正形式の場合は起動時にエラー終了します。暗黙の既定値、
代替 credential、heuristic fallback は使用しません。

主な endpoint:

- `GET /health`
- `GET /health/deep` (`admin:health`)
- `POST /admin/sync` (`admin:sync`)
- `GET /api/projections/{projection_id}/records` (`read:persons`)
- `GET /api/projections/{projection_id}/records/{record_id}`
  (`read:persons` + `read:timeline`)
- `GET /api/projections/{projection_id}/records/{record_id}/slides`
  (`read:timeline`)
- `GET /api/projections/{projection_id}/records/{record_id}/messages`
  (`read:timeline`)
- `GET /api/projections/{projection_id}/records/{record_id}/timeline`
  (`read:timeline`)
- `GET /api/projections/{projection_id}/blobs/{sha256}` (`read:persons`)
- `GET /api/projections/{projection_id}/lineage` (`read:persons`)

`/health` 以外は `Authorization: Bearer <token>` が必要です。Person Projection ID
は `proj:person-page` です。旧 `/api/persons/*` ルートや raw CAS 配信ルートは
提供しません。

## Runtime guarantees

- SQLite の UNIQUE 制約を ingest の正規重複判定に使用します。
- SQLite は per-leaf authoritative store で、起動時に Lake 全体を RAM 展開しません。
- Projection materialization、per-leaf watermark、dead-letter、audit、metrics は
  SQLite に永続化します。
- secret を永続化する場合は AES-256-GCM で暗号化します。
- blob / payload / sync item / page / leaf capacity 上限を超えた処理は明示的に拒否し、
  retention と orphan blob GC を sync 後に適用します。
- consent decision がない人物は `restricted_capture` として扱い、最新 decision が
  `opted_out` の人物は Projection から除外します。
- blob は filter 済み Person Page が参照するものだけを認証付きで返します。
- Projection response の `lineage_ref` は実在する lineage manifest を指します。
- identity / person-page の時刻は入力から導出し、replay の決定性を保ちます。
- Slack timestamp の形式不正、Gemini 解析失敗、必須設定欠落は即時エラーにします。

SQLite、blob、取得済み資料、credential は Git 管理対象外です。公開用 fixture
以外の runtime payload を commit してはいけません。

公開前に次を実行してください。

```powershell
python ./scripts/public_release_audit.py --check-history
```
