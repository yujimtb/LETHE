## 1. Google Docs Adapter

- [x] 1.1 `crates/adapters/gdocs` crate を作成し、Cargo.toml workspace に追加する
- [x] 1.2 Google Docs API client (document.get, revisions.list) を実装する
- [x] 1.3 `schema:workspace-object-snapshot` (service: "docs") の mapper を実装する
- [x] 1.4 見出しセクション単位の chunk 分割を実装する
- [x] 1.5 revision ベース差分取り込み (cursor 管理) を実装する
- [x] 1.6 M09 Adapter Policy に準拠した idempotency key 生成を実装する
- [x] 1.7 Google Docs adapter の contract テストを作成する

## 2. Google Sheets Adapter

- [x] 2.1 `crates/adapters/gsheets` crate を作成する
- [x] 2.2 Google Sheets API client (spreadsheets.get, spreadsheets.values.get) を実装する
- [x] 2.3 `schema:workspace-object-snapshot` (service: "sheets") の mapper を実装する (行単位、ヘッダ文脈)
- [x] 2.4 revision ベース差分取り込みを実装する
- [x] 2.5 idempotency key 生成を実装する
- [x] 2.6 Google Sheets adapter の contract テストを作成する

## 3. Google Forms Adapter

- [x] 3.1 `crates/adapters/gforms` crate を作成する
- [x] 3.2 Google Forms API client (forms.get, forms.responses.list) を実装する
- [x] 3.3 Form 構造 (objectType: "form") の Observation mapper を実装する
- [x] 3.4 回答事実 (objectType: "form-response-fact") の Observation mapper を実装する
- [x] 3.5 回答内容 (objectType: "form-response-content") の Observation mapper を実装する
- [x] 3.6 連携 Sheet ID のメタデータ記録を実装する
- [x] 3.7 idempotency key 生成を実装する (構造・回答事実・回答内容それぞれ)
- [x] 3.8 回答事実と回答内容の分離を検証する contract テストを作成する

## 4. Drive File Adapter

- [x] 4.1 `crates/adapters/gdrive` crate を作成する
- [x] 4.2 Google Drive API client (files.list, files.get, files.export) を実装する
- [x] 4.3 allowlist フォルダ配下の再帰巡回を実装する
- [x] 4.4 `schema:workspace-object-snapshot` (service: "drive") の mapper を実装する
- [x] 4.5 テキスト抽出可能ファイルの本文取得を実装する
- [x] 4.6 共有レベルのメタデータ記録を実装する
- [x] 4.7 revision ベース差分取り込みを実装する
- [x] 4.8 日次クロール間隔と設定変更を実装する
- [x] 4.9 idempotency key 生成を実装する
- [x] 4.10 Drive file adapter の contract テストを作成する

## 5. Access Controlled Corpus Projection

- [x] 5.1 `crates/projections/corpus` crate を作成する
- [x] 5.2 Slack チャンネルフィルタ (public + `^\d{3}_` + bot 除外 + opt-out) を実装する
- [x] 5.3 Drive ファイルフィルタ (allowed_folder + sharing 閾値 + opt-out + 除外) を実装する
- [x] 5.4 Form 回答内容 (objectType: "form-response-content") の除外ルールを実装する
- [x] 5.5 Form 回答連携 Sheet の除外ルールを実装する
- [x] 5.6 コーパスレコード粒度の生成 (Slack: 1msg, Docs: heading chunk, Sheets: 1row, Forms: 構造+事実, Slides: slide) を実装する
- [x] 5.7 anchor URL の生成 (Slack permalink, Docs URL, Sheet URL+row, Form URL, Slides URL+slide_id) を実装する
- [x] 5.8 M06 DAG Propagation による watermark 増分更新を接続する
- [x] 5.9 Filtering-before-Exposure の end-to-end テストを作成する (除外レコードが API で返らないことの検証)

## 6. Grep API

- [x] 6.1 NFKC 正規化済みテキストカラムを Corpus Projection のレコードに追加する
- [x] 6.2 `regex` crate を使った grep engine を実装する (線形時間保証)
- [x] 6.3 `POST /api/projections/{projection_id}/grep` エンドポイントを `crates/api` に追加する
- [x] 6.4 request パラメータ (pattern, filters, normalization, order, limit, cursor) の解析を実装する
- [x] 6.5 response 形状 (matches[], next_cursor, complete, projection_watermark) を実装する
- [x] 6.6 ソース種別、日時範囲、チャンネル、コンテナのフィルタを実装する
- [x] 6.7 cursor pagination を実装する
- [x] 6.8 regex 実行時間タイムアウト (既定 500ms) を実装する
- [x] 6.9 trigram index による候補絞り込みを実装する (match 欠落なしの保証つき)
- [x] 6.10 `GET /api/projections/{projection_id}/records/{record_id}` (get_record) エンドポイントを実装する
- [x] 6.11 `GET /api/projections/{projection_id}/threads/{thread_ts}` (get_thread) エンドポイントを実装する
- [x] 6.12 `POST /api/projections/{projection_id}/resolve-link` (resolve_link) エンドポイントを実装する
- [x] 6.13 Grep API のインテグレーションテストを作成する (NFKC 正規化、OR パターン、pagination、フィルタ)
- [x] 6.14 trigram index の match completeness テストを作成する (index あり/なしで同一結果)

## 7. Answer Log Projection

- [x] 7.1 `schema:bot-answer-log` を M02 Registry に定義する
- [x] 7.2 `crates/projections/answer-log` crate を作成する
- [x] 7.3 Answer Log Projection の materialization を実装する
- [x] 7.4 `POST /api/projections/{projection_id}/prior-qa-search` エンドポイントを実装する
- [x] 7.5 レスポンスに `is_primary_source: false` を含めることを実装する
- [x] 7.6 Answer Log が Corpus Projection に含まれないことを検証するテストを作成する

## 8. 設定と受け入れテスト

- [x] 8.1 channel_allow_regex, channel_opt_in, exclude_bot_authors の Projection 設定を実装する
- [x] 8.2 allowed_folder_ids, sharing 閾値, exclude_form_response_sheets の Projection 設定を実装する
- [x] 8.3 opt-out 人物リストの設定と適用を実装する
- [x] 8.4 Form 個別回答が grep 結果に出ないことの end-to-end テストを作成する
- [x] 8.5 Bot 投稿が grep 結果に出ないことの end-to-end テストを作成する
- [x] 8.6 resolve_link で Slack permalink と Drive URL を解決できることの end-to-end テストを作成する
