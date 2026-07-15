## Why

現行の検索 v2 は検索要求ごとに全 Corpus record から trigram index を再構築し、起動時と取込後にも全 Observation / Corpus をメモリへ再 materialize する。10,000 件で検索 p95 4.104 秒となり 2 秒 SLO を hard-stop したため、66.5 万件を RAM 3.7 GiB の NAS で扱える永続・増分 materialization が納期上の必須条件である。

## What Changes

- Corpus Projection の検索用 materialization を、スキーマ版付きの永続ディスク index として実装する。
- 起動時は有効な index を開き、初回またはスキーマ変更時だけ canonical Observation から再構築する。
- 新規 Observation の Corpus record を冪等に差分反映し、検索時および通常取込時の全件再構築を廃止する。
- 検索は index で候補と必要フィールドを取得し、全 Observation / Corpus record を常駐させない。
- index 破損を検知した場合は検索を明示エラーにし、単一のバックグラウンド再構築を開始する。空結果や旧 index への silent fallback は行わない。
- 検索 v2 の複合語 AND、NFKC / regex 最終判定、フィルタ、順序、snippet、limit / cursor、HTTP / MCP 応答契約を維持する。
- 10k / 50k / 100k / 500k 合成データを 4 GiB 制限下で測定し、日付・channel/source・複合語 AND の実効クエリと、絞り込み不能な全体検索を分けて p95 と peak RSS を記録する。

## Capabilities

### New Capabilities

- `persistent-search-index`: Corpus Projection の永続検索 materialization、増分・冪等更新、スキーマ移行、破損検知とバックグラウンド再構築、readiness、性能・メモリ条件を規定する。

### Modified Capabilities

なし。`corpus-projection` の watermark 増分更新、`grep-api` の検索 v2 契約、`observation-lake` の append-only 契約は変更せず、その実装を永続 index へ移す。

## Impact

- 主対象: `crates/projections/corpus`、`crates/api`、`apps/selfhost`、SQLite persistence / 起動・取込 orchestration。
- 新規依存: 実績あるオンディスク全文検索 engine（Tantivy を第一候補）。
- API: wire contract の変更なし。index unavailable / rebuilding の明示的な service error を追加する。
- 運用: index directory、schema version、再構築状態と検証・ベンチ手順を追加する。本番 selfhost、既存 `data/`、デプロイには触れない。
- System Laws: Append-Only Law を維持し index は派生 materialization のみにする。Replay Law に従い canonical Observation から決定的に再構築する。Filtering-before-Exposure Law に従い Corpus で許可された record だけを index 化する。Effect Isolation Law に従い永続 index I/O と再構築 orchestration を imperative shell に閉じ込める。

## Non-goals

- canonical Observation を検索 index へ置き換えること。
- 検索 v2 の意味論、ranking、レスポンス形状、MCP / HTTP client 契約を変更すること。
- PostgreSQL 移行、selfhost 本番デプロイ、顧客データの投入、NAS 設定変更。
- 破損時に空結果、旧 snapshot、インメモリ全走査へ fallback すること。
