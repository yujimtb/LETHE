## Why

外部の Workspace Search Bot が LETHE をデータ基盤として利用するにあたり、LETHE 側に不足している機能がある。Bot は MCP Server 経由で LETHE の HTTP API を呼び出す設計であり、以下が必要になる:

1. **検索コーパスの生成**: Bot に露出してよいレコードだけを生成する Access Controlled Corpus Projection が存在しない
2. **Grep API**: 正規表現による全文検索 API が存在しない
3. **Google Workspace adapter の横展開**: Slides adapter のみ実装済みで、Docs / Sheets / Forms / Drive file の adapter がない
4. **回答ログの保存**: Bot の過去回答を LETHE に蓄積する Projection がない

本 change は LETHE 側の基盤を整備し、Search Bot が grep + ReAct で情報検索できる状態を作る。

## What Changes

- Access Controlled Corpus Projection を新規追加する (M05 Projection Engine の新しい Projection spec)
- Grep API エンドポイントを M14 API Serving に追加する
- Google Docs adapter を M11 Google Slides Adapter の `schema:workspace-object-snapshot` を横展開して新規実装する
- Google Sheets adapter を同様に新規実装する
- Google Forms adapter を新規実装する (回答事実と回答内容の分離を含む)
- Drive file adapter を新規実装する
- Answer Log Projection を新規追加する

## Capabilities

### New Capabilities
- `corpus-projection`: Bot 向け Access Controlled Corpus Projection (Slack channel ルール、Drive 共有閾値、Form 回答非露出、opt-out)
- `grep-api`: NFKC 正規化済み正規表現 grep API (cursor pagination、trigram 高速化、フィルタ)
- `google-docs-adapter`: Google Docs source adapter (workspace-object-snapshot 横展開)
- `google-sheets-adapter`: Google Sheets source adapter (workspace-object-snapshot 横展開)
- `google-forms-adapter`: Google Forms source adapter (構造 + 回答事実 + 回答内容の分離)
- `drive-file-adapter`: Google Drive file source adapter (allowlist フォルダ配下の汎用ファイル)
- `answer-log-projection`: Bot 回答の構造化ログを蓄積する Projection

### Modified Capabilities
- `api-serving`: Grep API エンドポイントの追加

## Non-Goals

- Search Bot 本体の実装 (別リポジトリ)
- MCP Server の実装 (別リポジトリ)
- per-user ACL (MVP は共通コーパス)
- embedding 検索、RAG retriever
- private channel / DM / group DM の取り込み
- Slack Events API によるリアルタイム取り込み

## System Laws への影響

- **Append-Only Law**: 影響なし。新 adapter は既存の Lake append パスを使う
- **Replay Law**: 影響なし。Corpus Projection は Projection Engine の replay 保証下で動作する
- **Filtering-before-Exposure Law**: **直接関連**。Corpus Projection がこの law の新しい適用先になる
- **Effect Isolation Law**: 影響なし
- **Explicit Authority Law**: 影響なし。新 adapter は source-authoritative

## Impact

- Affected specs: M05 (Projection Engine), M09 (Adapter Policy), M14 (API Serving), M08 (Governance)
- New crates: `crates/adapters/gdocs`, `crates/adapters/gsheets`, `crates/adapters/gforms`, `crates/adapters/gdrive`, `crates/projections/corpus`, `crates/projections/answer-log`
- Modified crates: `crates/api` (Grep API endpoint)
- New dependencies: Google Docs API, Google Sheets API, Google Forms API, Google Drive API
- Existing dependency reuse: `unicode-normalization` (NFKC), `regex` (grep engine)
