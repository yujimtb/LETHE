## Context

外部の Workspace Search Bot が LETHE をデータ基盤として利用する。Bot は MCP Server 経由で LETHE の HTTP API を呼び出し、grep + ReAct で情報検索を行う。LETHE 側に必要な追加機能は大きく 3 カテゴリに分かれる:

1. **Google Workspace adapter の横展開** (Docs, Sheets, Forms, Drive)
2. **検索基盤** (Corpus Projection + Grep API)
3. **回答ログ** (Answer Log Projection + prior_qa_search API)

既存の基盤:
- Lake, Projection Engine, DAG Propagation, API Serving は動作済み
- Slack adapter, Google Slides adapter は実装済み
- `schema:workspace-object-snapshot` が gslides adapter で定義済み
- `unicode-normalization` crate (NFKC)、`regex` crate が依存済み
- Adapter API の idempotency, heartbeat 基盤が存在

## Goals / Non-Goals

**Goals:**
- Google Docs, Sheets, Forms, Drive のデータを Lake に取り込める
- Access Controlled Corpus Projection で Bot に露出してよいレコードのみを生成する
- 正規表現 grep API で Corpus Projection を全文検索できる
- Answer Log Projection で過去回答を scaffolding 検索できる
- 全機能が LETHE の System Laws (Append-Only, Replay, Filtering-before-Exposure) に準拠する

**Non-Goals:**
- Search Bot 本体の実装 (別リポジトリ)
- MCP Server の実装 (別リポジトリ)
- per-user ACL (MVP は共通コーパス)
- embedding 検索、ranking 検索
- PDF/画像の OCR
- Slack Events API によるリアルタイム取り込み

## Decisions

### D1: workspace-object-snapshot schema の横展開
**選択:** 既存の gslides adapter が定義した `schema:workspace-object-snapshot` を Docs/Sheets/Forms/Drive で再利用する
**理由:** Observation の形式を統一し、Corpus Projection のフィルタルールを schema ではなく `artifact.service` フィールドで分岐させる。新しい schema を定義するより、既存の schema を拡張する方が Projection の実装が単純になる。
**代替案:** サービスごとに別 schema → Projection のフィルタが schema × ルールの直積になり複雑化。

### D2: Forms の回答事実と回答内容の Observation 分離
**選択:** `objectType: "form-response-fact"` と `objectType: "form-response-content"` を別 Observation にする
**理由:** Corpus Projection のフィルタで objectType ベースの単純なルールで回答内容を除外できる。Lake にはすべて入れつつ、Projection で露出制御する LETHE の設計原則 (Filtering-before-Exposure) に合致する。
**代替案:** 回答内容を Lake に入れない → 将来の管理者向け分析など、回答内容が必要なユースケースに対応できない。

### D3: Corpus Projection の実装方式
**選択:** M05 Projection Engine の BuildSpec + M06 DAG Propagation の watermark 増分更新で実装する
**理由:** 既存の Projection Engine の仕組みをそのまま使える。フィルタルールは Projection spec の source declaration に条件として記述する。
**代替案:** API レイヤーでのフィルタリング → Filtering-before-Exposure Law 違反。Bot がフィルタ前のデータを一瞬でも受け取る。

### D4: Grep engine の実装
**選択:** Rust の regex crate を使い、NFKC 正規化済みテキストに対して grep を実行する。高速化として trigram index を使用可能にするが、最終判定は regex semantics。
**理由:** regex crate は線形時間保証があり ReDoS を防止する。NFKC は既に依存している `unicode-normalization` crate で実装できる。trigram index は候補絞り込みのみで、match の完全性を損なわない。
**代替案:** tantivy 等の全文検索エンジン → ranking semantics が入り、grep の意味論と合わない。

### D5: Grep API の配置
**選択:** M14 API Serving に新しいエンドポイント `POST /api/projections/{projection_id}/grep` を追加する
**理由:** 既存の API Serving 層 (Axum) に乗せることで、認証、pagination、response envelope を再利用できる。
**代替案:** 別サービスとして分離 → データアクセス層の重複、Projection watermark の整合性管理が複雑化。

### D6: Answer Log の schema
**選択:** `schema:bot-answer-log` を新規定義し、Bot 回答専用の Observation 型とする
**理由:** workspace-object-snapshot は SaaS ドキュメントのスナップショット向けであり、Bot 回答ログには payload 構造が合わない。専用 schema にすることで、Corpus Projection からの除外も objectType ではなく schema で判定できる。

### D7: 新 crate の配置
**選択:**
- `crates/adapters/gdocs` — Google Docs adapter
- `crates/adapters/gsheets` — Google Sheets adapter
- `crates/adapters/gforms` — Google Forms adapter
- `crates/adapters/gdrive` — Drive file adapter
- `crates/projections/corpus` — Corpus Projection
- `crates/projections/answer-log` — Answer Log Projection
- `crates/api` (既存) に grep endpoint を追加
**理由:** 既存の crate 構成 (adapters/, projections/, api/) に合わせる。

## Risks / Trade-offs

**[Forms 回答 Sheet の除外漏れ]** → Form adapter がメタデータに連携 Sheet ID を記録し、Corpus Projection がそれを参照して除外する。Sheet adapter 側にも exclude_form_response_sheets の設定フラグを持つ。二重防御。

**[Trigram index と regex の整合性]** → index は候補絞り込みのみ。MUST として regex 意味論を最終判定とし、index による match 欠落がないことをテストで保証する。

**[Google API の rate limit]** → M09 Adapter Policy の rate_limit 設定に従う。4 つの adapter が同時にクロールする場合、Google API の quota を共有する点に注意。

**[Corpus Projection のフィルタルール変更]** → フィルタルールは Projection spec として定義するため、ルール変更時は Projection の再 build が必要。watermark reset で全量再構築可能だが、コーパスサイズが大きい場合の所要時間に注意。

**[NFKC 正規化の不可逆性]** → 検索は正規化済みテキストに対して行い、表示には原文を使う。正規化で失われる情報 (全角英数の意図的使用等) は検索精度に影響しない前提。

## Open Questions

- Drive の broad_visibility_threshold の具体的な閾値 (domain-wide? anyone-with-link?)
- opt-out の登録形式と管理方法
- Form 回答連携 Sheet の識別方法 (Forms API のメタデータから取得可能か、別途管理が必要か)
- 4 adapter 並行クロール時の Google API quota の配分方法
