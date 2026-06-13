# LETHE — Dormitory Observation & Knowledge Platform

公開用リポジトリとして維持する前提で、機密情報とローカル実行データは Git 管理対象から外しています。運用上の扱いは [SECURITY.md](SECURITY.md) を参照してください。

## Document Map

### Specifications

| Document | Role | Status |
|---|---|---|
| [plan.md](plan.md) | 親仕様。システム全体の概念・構造・要件を定義する | Active — 正典 |
| [openspec/specs/_index.md](openspec/specs/_index.md) | モジュール仕様索引。M01-M15、依存 DAG、開発レーンを定義する | Active — 実装/検証の起点 |
| [domain_algebra.md](domain_algebra.md) | 型定義・law・失敗モデル・write 正規化・storage 意味境界 | Active — plan.md の意味論補強 |
| [governance_capability_model.md](governance_capability_model.md) | consent・access・agent capability・write review・retention | Active — plan.md §7 の拡張 |
| [runtime_reference_architecture.md](runtime_reference_architecture.md) | runtime topology・技術マッピング・運用制御 | Active — 交換可能な参照実装 |
| [adr_backlog.md](adr_backlog.md) | 未確定の設計判断を追跡する backlog | Active — 継続更新 |
| [sharding_refactor.md](sharding_refactor.md) | Lake 物理分割（sharding）の確定設計。identity/routing keyspec、exact index、Patricia split、failover、watermark propagation、logical→physical resolver、migration | Active — sharding 実装の正典（D0–D12 LOCKED） |

### Issues

| Document | Role | Status |
|---|---|---|
| [open_issues.md](open_issues.md) | Issue インデックス（各ラウンドの概要） | Active |
| [issues/README.md](issues/README.md) | Round 2 Issue 一覧・優先順位・担当エージェント | Active |
| [issues/R2-01〜R2-08](issues/) | 個別 Issue ファイル | Active |

### Development

| Document | Role | Status |
|---|---|---|
| [dev_advice.md](dev_advice.md) | 開発方針・AI と人間の役割分担 | Reference |
| [agents/README.md](agents/README.md) | マルチエージェント開発体制の概要 | Active |
| [agents/spec-designer.md](agents/spec-designer.md) | Spec Designer エージェント定義 | Active |
| [agents/implementer.md](agents/implementer.md) | Implementer エージェント定義 | Active |
| [agents/reviewer.md](agents/reviewer.md) | Reviewer エージェント定義 | Active |

### Archive

| Document | 元の役割 | アーカイブ理由 |
|---|---|---|
| [archive/design_questions.md](archive/design_questions.md) | 追加設計論点の Q&A シート | 回答済み。成果は domain_algebra / governance / adr_backlog に反映済み |
| [archive/plan_refinement_functional_architecture.md](archive/plan_refinement_functional_architecture.md) | 関数型アーキテクチャ観点の洗練メモ | 提案は domain_algebra / runtime に反映済み |
| [archive/open_issues_round1.md](archive/open_issues_round1.md) | Round 1 の横断的論点整理と提案 | 回答済み。成果は各仕様文書と adr_backlog に反映済み |
| [archive/open_issues_round2.md](archive/open_issues_round2.md) | Round 2 の統合版 Issue | 個別ファイルに分割済み |

## Current Implementation Snapshot

このリポジトリの現行コードは、仕様群の MVP 垂直スライスとコア意味論を **Rust crate** として検証する参照実装です。  
`plan.md` と `runtime_reference_architecture.md` の技術マッピングは参考構成であり、このリポジトリの実装言語やライブラリ選定を拘束するものではありません。

| Scope | Status | Evidence |
|---|---|---|
| M01-M06 Domain / Registry / Lake / Supplemental / Projection / Propagation | Implemented | `src/domain`, `src/registry`, `src/lake`, `src/supplemental`, `src/projection`, `src/propagation` |
| M08 Governance | MVP 最小実装 | `src/governance` (`PolicyEngine`, `AuditLog`, `FilteringGate`) |
| M09-M14 Adapters / Identity / Person Page / API | Implemented | `src/adapter`, `src/identity`, `src/person_page`, `src/api` |
| M15 Runtime | MVP 最小実装 | `src/runtime` (`LocalBuildRunner`, `config`, `health`, `heartbeat`) |
| M07 Write-Back | Post-MVP / 未実装 | `openspec/specs/write-back.md`, `src/domain/command.rs` |
| Platform Generalization / Robustness | Refactoring in progress | Cargo workspace crates under `crates/`, storage ports, authenticated projection API |

### Verification

- `cargo build`
- `cargo test`
- 2026-03-10 時点で self-host binary と API integration test を含めて `cargo test` が通過

### Current Follow-Ups

- `M07 Write-Back` は仕様化済みだが、現行コードでは `Command` / `EffectPlan` の定義までで、write router や source-native write adapter は未実装
- `M08 Governance` は最小実装で、`lake::IngestionGate` は append 前に `PolicyEngine` と JSON Schema 検証を通過する
- `M15 Runtime` は local build runner ベースで、container sandbox は Growth 以降の扱い

## Self-Host Quickstart

このリポジトリには、Slack と Google Slides をローカルで取り込み、person page API を返す self-host 用 binary が追加されています。

### Prerequisites

- Rust stable toolchain
- Slack Bot Token
- Google Slides / Drive を読める OAuth access token、または `client_id` / `client_secret` / `refresh_token`

### Configuration

1. `.env.example` を参考に `.env` を作る
2. 最低限、以下を設定する

`.env`、OAuth client JSON、SQLite、blob directory はローカル専用です。公開リポジトリには含めません。

- `LETHE_SLACK_BOT_TOKEN`
- `LETHE_SLACK_THREAD_TOKEN` (任意。thread reply 読み取り用の token を分けたい場合)
- `LETHE_SLACK_CHANNEL_IDS`
- `LETHE_GOOGLE_PRESENTATION_IDS`
- `LETHE_GOOGLE_ACCESS_TOKEN`
- `LETHE_API_TOKENS` (`token:scope+scope` の comma 区切り。例: `qa-read-token:read:persons+read:timeline,sync-token:admin:sync`)

access token を毎回手で入れたくない場合は、代わりに以下を設定します。

- `LETHE_GOOGLE_CLIENT_ID`
- `LETHE_GOOGLE_CLIENT_SECRET`
- `LETHE_GOOGLE_REFRESH_TOKEN`

Notion への write-back も確認したい場合は、以下も設定します。

- `LETHE_NOTION_TOKEN`
- `LETHE_NOTION_DATABASE_ID`
- `LETHE_PUBLIC_BASE_URL` (thumbnail blob を Notion 外部 URL として fallback したい場合)

Google Slides の AI 抽出を有効にする場合は、以下も設定します。

- `LETHE_GEMINI_API_KEY`
- `LETHE_GEMINI_MODEL` (`gemini-3-flash-preview` 既定)

Notion database 側には title property が最低限 1 つ必要です。`Email` property は必須ではありませんが、ページ照合の安定性のため強く推奨します。

現在の adapter は、存在する場合に以下の database property も同期します。property 名は大文字小文字・空白・`_` の揺れを吸収して解決します。

- `Birthplace` (rich text)
- `DoB` (rich text)
- `Hashtag` (rich text)
- `Major_Interests` (rich text)
- `LETHE Person ID` (rich text)
- `Source Slide URL` (url / rich text)
- `Last Synced At` (date / rich text)
- `Projection Version` (rich text)
- `Status` (status / rich text)
- `Visibility` (checkbox)

プロフィール本文、画像、ギャラリー、narrative sections は database property ではなく page body block として描画されます。

Notion の gallery cover は page cover ではなく、page body の先頭 image block を使っています。現在の write-back は `thumbnail_blob_ref` がある場合、まず Notion File Upload API に PNG を送って **Notion-hosted image block** として描画します。blob が無い場合だけ、Google Slides export URL や `LETHE_PUBLIC_BASE_URL` 配下の `GET /public/blobs/{sha256}` を external image fallback として使います。

### Run

```bash
cargo run --bin lethe-selfhost
```

起動後の主な endpoint:

- `GET /health`
- `POST /admin/sync` (`admin:sync` scope)
- `GET /public/blobs/{sha256}` (Phase 0 では無認証公開。公開blobに個人情報や非公開資料を置かない)
- `GET /api/projections/{projection_id}/records` (`read:persons` scope)
- `GET /api/projections/{projection_id}/records/{record_id}` (`read:persons` + `read:timeline` scope)
- `GET /api/projections/{projection_id}/records/{record_id}/slides` (`read:timeline` scope)
- `GET /api/projections/{projection_id}/records/{record_id}/messages` (`read:timeline` scope)
- `GET /api/projections/{projection_id}/records/{record_id}/timeline` (`read:timeline` scope)
- `GET /api/persons/*` は `Deprecation: true` 付きの person projection alias

### Notes

- 永続化は SQLite + ローカル blob directory を使います
- API は `/health` と `/public/blobs/{sha256}` を除いて `Authorization: Bearer <token>` を必須とします
- `LETHE_API_TOKENS` の scope は `read:persons`, `read:timeline`, `admin:sync`, `*` を使います
- Slack channel / Google presentation は内部で構造化 source instance (`sources`) に正規化され、secret は `credential_ref` で参照されます
- 既定の runtime state は `./data/lethe.sqlite3` と `./data/blobs/` です。このディレクトリは Git では無視されます
- 旧 `dokp.sqlite3` は deprecated です。ローカル検証や運用フローでは `LETHE_DATABASE_PATH=./data/lethe.sqlite3` を使ってください
- SQLite の `partition_log` は初回起動時に `initialize` を記録し、`routing_keyspec` と `identity_keyspec` を pin します。`partition_log` は UPDATE / DELETE を拒否する append-only table です
- SQLite の `observations` は `append_seq INTEGER PRIMARY KEY AUTOINCREMENT`, `identity_key TEXT NOT NULL UNIQUE`, `canonical_json TEXT NOT NULL` を持つ durable schema です。旧 schema の暗黙 migration は行いません
- self-host の通常 ingest は SQLite の UNIQUE 制約を正規判定とし、成功した Observation だけを in-memory Lake cache に反映します。重複は `Duplicate(existing_id)` として返り、cache へは追加されません
- slide-analysis の supplemental cache も SQLite に保存され、bootstrap 後も person detail に復元されます
- Google Slides export URL を Notion が取得できるかは、元 presentation の共有設定に依存します。private なままだと external image fallback は失敗する可能性があります
- `LETHE_PUBLIC_BASE_URL` を fallback に使う場合は **Notion から到達可能な公開 URL** を指定してください。`http://127.0.0.1:8080` のような loopback URL はローカル検証には使えますが、Notion 自体は取得できません
- person detail では `Filtering-before-Exposure` により `Visibility=false` のレコードと、`identities` / `DoB` / `Birthplace` / 連絡先系フィールドをレスポンス前に構造的に除外します
- identity / person-page の時刻は壁時計ではなく入力観測・補助レコードから導出し、replay の決定性を保ちます
- slide-analysis と Notion write-back の失敗は同期中に握り潰さず、その場で返します
- Google Slides で未取得 revision が複数ある場合、self-host は取得可能な **最新 revision の正しい snapshot** だけを materialize し、古い revision に最新内容を誤って付与しません
- Slack sync は thread parent を見つけたとき `conversations.replies` も辿り、thread reply を個別 observation として取り込みます
- 秘密鍵・アクセストークンを一度でもローカルで使った場合は、公開前に新しい値へローテーションしてください

### Phase 0 Q&A service

`src/qa` は 6/24 チャットボット納品向けの薄い Q&A サービス境界です。

- 保持 token は `read:persons` / `read:timeline` のみを許可し、`admin:sync` を含む token は拒否します
- 質問文は `RetrievalQuery` として型付きで retriever へ渡し、URL・コマンド・SQLへ直接展開しません
- 対象外カテゴリ、本人特定、低確信、日次コストキャップ超過は運営者誘導の定型文を返します
- 回答末尾に人間可読の由来と observation ID を表示し、SQLiteログには provenance を observation ID で保存します
- `tests/fixtures/phase0_golden_questions.json` は検収・回帰用のゴールデン質問セットです

### Token rotation

1. 新しい token を発行する。
2. `.env` の `LETHE_API_TOKENS` を新 token に更新する。sync token を変える場合は cron や運用側の `Authorization` 設定も同時に更新する。
3. `cargo run --bin lethe-selfhost` のプロセスを再起動する。
4. 新 token で対象 endpoint が通ること、旧 token が 401 になることを確認する。
5. 公開前、または秘密値を一度でもローカルで使った後は必ずローテーションする。

### Publication Safety Check

公開前、または公開リポジトリ向けの PR を作る前に、以下を実行してください。

```powershell
./scripts/public-release-audit.ps1
```

公開切り替えの最終確認では、履歴も含めて以下を実行してください。

```powershell
./scripts/public-release-audit.ps1 -CheckHistory
```

この監査は、tracked files と git history に対して以下を検査します。

- `.env` と `client_secret.json` の混入
- `data/` 配下の runtime payload や `target/` の混入
- 既知の token / secret pattern の混入
- `.env.example` が placeholder のみを保持していること

GitHub Actions では、履歴を除く既定の監査が自動実行されます。`-CheckHistory` は public 化の最終確認用です。

## Reading Order

1. **plan.md** — まず全体像を掴む
2. **domain_algebra.md** — 型と law の厳密な定義を確認する
3. **governance_capability_model.md** — consent・権限・agent の扱いを確認する
4. **runtime_reference_architecture.md** — 実装に落とすときの参照構成を見る
5. **adr_backlog.md** — 何が未確定かを把握する
6. **sharding_refactor.md** — Lake 物理分割の確定設計（sharding を実装するときの正典）を確認する
7. **open_issues.md** → **issues/** — 次の設計ラウンドで詰めるべき論点を確認する
8. **agents/** — マルチエージェント開発体制と各ロールの定義を確認する
