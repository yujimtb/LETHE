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
- Supplemental Kind Registry と初期 kind schema (`claim@1` など)
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
- [Personal lake ingestion](docs/development/personal-lake-ingestion.md)
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
GitHub/Claude の one-shot import 専用 instance では `sources` を空配列にできます。
その場合も API token、routing key order、`supplemental.reject_unregistered_kinds`
は TOML で明示します。
MCP read port を使う場合も設定は必須です。`server.mcp_bind_addr` は内部 API と
別ポートにし、`[mcp]` には公開 resource URL、protected resource metadata URL、
managed ID provider の issuer、audience、JWKS path を明示します。MCP は固定
Bearer token / API key を受け付けず、JWKS で署名検証できる OAuth JWT だけを
受け付けます。JWT の権限 grant は標準の `scope` claim と Auth0 RBAC/API permission
構成で使われる `permissions` claim の両方から読みます。期限切れ・不正 token には
`WWW-Authenticate` で protected resource metadata と必要 scope を返すため、
refresh token を保持している MCP client は認可サーバ側で access token を更新できます。
LETHE 自身は token endpoint、refresh token exchange、DCR、同意画面を実装しません。
個人 lake の現在の公開 MCP resource は `https://yujiws.tail474356.ts.net/mcp`、
issuer は Auth0 tenant `https://lethe-mcp.jp.auth0.com/` です。
`oauth_audience` は Auth0 API identifier と同じ
`https://yujiws.tail474356.ts.net/mcp` にします。
Auth0 signing key が rotate された場合は
`https://lethe-mcp.jp.auth0.com/.well-known/jwks.json` から
`deploy/personal-lake/mcp-jwks.json` を更新してから selfhost を再起動します。

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
- `GET /api/projections/proj:corpus/records` (`read:corpus`)
- `POST /api/projections/proj:corpus/grep` (`read:corpus`)
- `GET /api/projections/proj:corpus/records/{record_id}` (`read:corpus`)
- `GET /api/projections/proj:corpus/threads/{record_id}` (`read:corpus`)
- `POST /supplementals` (`write:supplemental`)

`/health` 以外は `Authorization: Bearer <token>` が必要です。Person Projection ID
は `proj:person-page` です。旧 `/api/persons/*` ルートや raw CAS 配信ルートは
提供しません。

`POST /supplementals` は supplemental record を作成します。ID はクライアント採番の
`sup:{uuid}`、`kind` は登録済みの `claim@1` などの形式です。body は
`id`, `kind`, `derived_from`, `payload`, `created_by`, `mutability`,
`model_version`, `consent_metadata`, `lineage` を受け付け、`created_at` は
selfhost が設定します。`derived_from` は空にできず、未解決の observation /
supplemental 参照は 422 と詳細で拒否されます。同一 ID の再 POST は 409 です。
内容ベースの重複排除は書き込み側では行いません。
ただし Registry で `anchor_required=false` として登録済みの system-event kind
は、`payload.origin` が schema を満たす場合に限り空の `derived_from` を許可します。
`briefing-feedback@1` は Eos ブリーフィング満足度フィードバック用の登録済み kind
で、`rating=good|bad`、`surface=cli|serve-web` の payload schema で検証されます。

`created_by` には `actor:codex-import` のような安定した pipeline/client actor を
入れ、使用モデル名は `model_version` に記録します。モデル名を `created_by` に
混ぜないでください。

MCP read port は別 listener で提供します。

- `GET /.well-known/oauth-protected-resource`
- `GET /.well-known/oauth-protected-resource/mcp`
- `POST /mcp` (Streamable HTTP / JSON-RPC)

MCP read tools that accept `limit` cap it at 20 and report clamp metadata in
`_meta["lethe/response_limit"]`; `search_lake` snippets are capped at 240
characters and `matched_ranges` at 20 per record. `search_lake` accepts
`from` / `to` ISO 8601 timestamps and `order = "newest_first" | "oldest_first"`;
unknown `source_types` are rejected with the valid list, while
`_meta["lethe/available_source_types"]` reports live source_type counts.
`search_lake` matches expose `thread_key` at top level and omit internal
plumbing metadata from the MCP search response; call `get_record` for full
record metadata. `matched_ranges.start/end` are UTF-8 byte offsets.
MCP `get_thread` defaults to 20 records and returns `next_cursor` when a thread
continues.
公開する場合は Tailscale Funnel の対象を MCP host port のみに限定してください。
内部 API port、`/admin/*`、管理面を Funnel に晒してはいけません。この self-host
構成の MCP endpoint は本 PC が起動し、selfhost プロセスと Funnel が稼働している
間だけ到達可能です。常時稼働 SLA は提供しません。
現在の個人 lake Funnel は `https://yujiws.tail474356.ts.net/` を
`127.0.0.1:8090` に転送します。Claude custom connector には
`https://yujiws.tail474356.ts.net/mcp` を登録します。ChatGPT custom app、
Codex MCP、Claude Code の claude.ai-scoped connector も同じ URL を使います。
2026-07-06 時点で、claude.ai、ChatGPT、Claude Code、Codex はいずれも
`search_lake(query="aquisition", source_types=["github-commit"], limit=3)` で
`corpus:github-commit:019f2dea-4cf8-7e53-9f1c-863986634345` を取得済みです。
2026-07-08 に再認可し、claude.ai、ChatGPT.com、Claude Code、Codex CLI は
`search_lake(query="aquisition", source_types=["github-commit"], limit=1)` で
`corpus:github-commit:019f35ff-3750-7721-8748-326adacde778` を取得済みです。
同日に ChatGPT.com custom app は App settings の `Refresh` と `Reconnect` 後、
Auth0 consent で `mcp:read`、`write:supplemental`、`offline_access` を許可し、
`write_supplemental` で `sup:beaf7489-61dd-48bb-8015-068390fb5cc5` を作成後、
`search_decisions` で同 decision を取得済みです。
ChatGPT/Codex の refresh token は 2026-07-23 頃の idle expiry と
2026-08-07 頃の absolute expiry が次の手動再認証リスクです。このホストでは
Windows Task Scheduler に `LETHE MCP Reauth Idle Precheck`
(2026-07-22 09:00 JST) と `LETHE MCP Reauth Absolute Renewal`
(2026-08-06 09:00 JST) を登録済みです。タスクは
`scripts/reauthorize_lethe_mcp.ps1` を起動し、ChatGPT/Claude の設定ページと
Codex/Claude Code の MCP login 用ターミナルを開きます。

個人 lake を常駐させる場合は `scripts/start_personal_lake_services.cmd` を使います。
このホストでは Windows Startup の `LETHEPersonalLakeServices.vbs` が
`scripts\start_personal_lake_services.cmd` を実行し、Docker Compose selfhost と
Tailscale Funnel を起動します。`.vbs` の `shell.Run` 引数は 1 行の引用済みパスにし、
改行で分割しないでください。Docker Desktop はログイン時起動、Tailscale は
Windows service 自動起動かつ unattended mode、Docker container は
`restart: unless-stopped` です。このホストでは AC/DC とも sleep/hibernate を無効化し、
本番運用を開始済みです。

Auth0 `google-oauth2` connection は 2026-07-06 に Google Cloud project
`skcollege-dictionary` の tenant-owned OAuth client `LETHE MCP Auth0 Google` へ
切り替え済みです。Google client secret は Auth0 の connection 設定にのみ保存し、
repository には置きません。
再認証の頻度を下げるには、Auth0 側で API の Allow Offline Access と対象 client の
Refresh Token grant/rotation を有効にし、MCP client が `offline_access` を要求する
必要があります。`offline_access` は refresh token 取得用の認可サーバ scope であり、
LETHE の protected resource metadata の `scopes_supported` には含めません。
Auth0 の `Default Permissions for third-party applications` は `mcp:read` と
`write:supplemental` の両方にしておきます。新規 DCR client がここで 1/2 に
なると、Claude.ai、ChatGPT.com、Codex の再認可時に `write:supplemental` が
consent へ出ません。

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
