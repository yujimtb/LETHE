# Tasks: cognition-substrate

**並列実装の原則:** change ① と同じ。トラック間依存は spec 契約への依存のみで、全トラック同時着手可能。各タスク 2 時間以内、implementer=Codex、reviewer は要件被覆の検証(change ⑤ のハーネスが通り次第そちらに載せる)。

## 依存グラフ

```
Track A(registry 拡張)          Track B(ChatGPT 取り込み)      Track E(ブラウザ自動化)
  A1 → A2                           B1 → B2 → B3                     E1 → E2 → E3
   │                                                                  │
   ▼                                                                  │
Track C(MCP write)               Track D(projection 4本)           │
  C1 → C2 → C3                      D1 / D2 / D3 / D4(相互独立)     │
   │                                  │                               │
   └──────────────┬───────────────────┴───────────────────────────────┘
                  ▼
          Track F(統合・E2E・検索検証)
            F1 → F2 → F3
```

- A は C の前提(anchor_required と新 kind を MCP write の検証が参照)
- D の 4 projection は相互に独立。D4(カードキュー)のみ A の新 kind スキーマ(spec fixture)に依存するが、fixture 開発で並行可
- E はコード依存なし(運用ジョブ)。F の E2E が実データを要するため先行推奨

## Track A. Registry 拡張

- [x] A1 `anchor_required` を kind schema に追加し、false 時の origin 必須検証を実装する
  - Spec: SKIND-05 / 受け入れ: 空アンカー受理+origin 欠落拒否 test、既存 6 kind の後方互換 test
- [x] A2 新 7 kind(reply-draft@1 / reply-approval@1 / send-record@1 / nudge-event@1 / eos-state-transition@1 / mode-transition@1 / briefing-issue@1)の JSON Schema を定義・登録する
  - Spec: SKIND-06 / 受け入れ: 各 kind の必須欠落・enum 違反拒否 test

## Track B. ChatGPT 取り込み

- [x] B1 ChatGPT エクスポート形式の parser(canonical 写像、不正レコード skip+quarantine)を実装する
  - Spec: CEXP-02 / 受け入れ: 実 fixture パース test(壊れレコード混入で完走)
- [x] B2 identity key `chatgpt:{conversation_id}:{message_id}:H(canonical)`、published=メッセージ timestamp を実装する
  - Spec: CEXP-03 / 受け入れ: 再実行全件 duplicate の idempotency test
- [ ] B3 取り込み CLI(archive ワーキングコピー入力、範囲指定、backfill フラグ搬送、構造化終了報告)を完成させ、実データ初回取り込みを行う
  - Spec: CEXP-01, CEXP-04, CEXP-05 / 受け入れ: E2E+件数 evidence 記録
  - 2026-07-06: CLI 実装・範囲指定・backfill・構造化終了報告・synthetic E2E は完了。実データ初回取り込み evidence は ChatGPT export 実ファイルが必要なため未完了。

## Track C. MCP write

- [x] C1 `write_supplemental` ツール(HTTP と共有の検証関数経由)を実装する
  - Spec: MCPW-01, MCPW-02 / 受け入れ: 全 kind contract test、HTTP/MCP エラー対照 test
- [x] C2 scope 検証(`write:supplemental` 保持トークンのみ許可)とツール説明文(事後処理ワークフロー明記)を実装する
  - Spec: MCPW-03, MCPW-04 / 受け入れ: read-only トークン拒否 test、未解決アンカー拒否 test、説明文レビュー
- [ ] C3 claude.ai / ChatGPT の実クライアントから公開 endpoint 経由の write → projection 反映を手動疎通し evidence を記録する
  - Spec: MCPW-05 / 受け入れ: 両クライアントの E2E evidence
  - 2026-07-06: ローカル MCP e2e は完了。公開 claude.ai / ChatGPT custom app の手動 write evidence は未実施。
  - 2026-07-06 本番: `https://yujiws.tail474356.ts.net/.well-known/oauth-protected-resource` は browser-use で到達し、`scopes_supported=["mcp:read","write:supplemental"]` を確認。Claude 実クライアントでは `write_supplemental` が表示されたが、単発許可後の実行は `{"error":"Error occurred during tool execution","request_id":"req_011CckfUfezTrCZsvuUWXyN5"}` で失敗し、公開クライアント由来の supplemental は作成されなかった。ChatGPT custom app は `write_supplemental unavailable in LETHE_Personal_Lake read-only tool` と返した。よって C3 は未完了。
  - 2026-07-06 本番切り分け: 同一 payload は内部 HTTP `POST /supplementals` で成功し、`sup:71591976-99db-4c29-bf71-c2c756d41c5f` を作成後、`sup:cd488fa0-248e-4d0a-a4e3-b29c44853332` で `terminated` へ遷移。`/projections/claim-queue?state=terminated` は `terminated_matches=1`、`state=open` は `open_matches=0`。実装/API/Projection 側は通っており、残件は公開クライアントの write scope/tool 公開設定。
  - 2026-07-07 本番再認可: browser-use で Auth0 管理画面へ `mitobex7@gmail.com` と `y.mitobe@hlab.college` の両方でログインしたが、どちらも `https://auth0.com/profile` の新規 profile flow に落ち、`lethe-mcp` tenant dashboard へ入れなかった。Claude の LETHE connector は切断→再接続し、Auth0 consent 画面で要求 scope が `mcp:read` と `offline_access` のみで `write:supplemental` を含まないことを確認。再接続後の単発許可 write も `{"error":"Error occurred during tool execution","request_id":"req_011Ccm3mgpE61o4wkzmpVUSy"}` で失敗し、`sup:a93e1874-5a8f-4e70-90b1-da235627185d` は本番 SQLite に作成されなかった。Anthropic 公式 docs は `401 WWW-Authenticate` に scope が無い場合 `scopes_supported` を要求するとしているため、現時点の実ブロッカーは Auth0 resource server/API permission 側で `write:supplemental` が発行されていないこと、または Claude/Auth0 間で当該 scope が落ちていること。Auth0 tenant 管理権限または Management API 資格情報が必要。
  - 2026-07-07 本番解消(Claude 側): Auth0 tenant `dev-muwlx2h3vvs2z7xt` を作成し、API `LETHE MCP`(identifier `https://yujiws.tail474356.ts.net/mcp`)に `mcp:read` / `write:supplemental` を定義。Dynamic Client Registration を有効化し、`google-oauth2` connection を domain-level に昇格。Claude DCR client `tpc_11NbEAfZ19vHyL5bGG1eL6` に API Access grant `cgr_qOVeYy4ndc50ZjnQ` で 2/2 permissions を付与した。
  - 2026-07-07 本番解消(Claude 側): Claude connector から実 tool call で `sup:86eea51a-03d4-4fa8-b241-3de111ed0ffb` を `claim@1` として作成し、`claim_queue(state="open")` で反映を確認後、`sup:ad779751-43ec-4172-99b6-7b63040b4941` の `claim-transition@1` で `terminated` へ遷移。`claim_queue(state="terminated")`、SQLite `supplementals`、HTTP `/projections/claim-queue?state=terminated&limit=20` で state=`terminated`, transition=`sup:ad779751-43ec-4172-99b6-7b63040b4941`, stale=`false`, built_at=`2026-07-06T16:33:19.160389651Z` を確認。ChatGPT 側 write はユーザー指示どおり ChatGPT export/app 後回しのため、C3 チェック自体は未完了のまま残す。

## Track D. Projection 4本

- [x] D1 鮮度 projection+`GET /projections/freshness`(閾値 config 化)を実装する
  - Spec: FRSH-01, FRSH-02, OPS-01 / 受け入れ: 閾値判定 test、replay 決定性 test
- [x] D2 再開スナップショット projection を実装する
  - Spec: RSNP-01 / 受け入れ: 複数セッション → 1 カード test、決定性 test
- [x] D3 plan-state projection を実装する
  - Spec: PLST-01 / 受け入れ: superseded 除外 test、滞留時間 test
- [x] D4 カードキュー projection(状態機械、first-approval-wins、自動送信区別)+`GET /projections/card-queue`+claim queue の backfill フィルタを実装する
  - Spec: CARD-01〜03, CLQ-07 / 受け入れ: 3面順不同承認 fixture test、不正遷移 skip test、フィルタ contract test

## Track E. ブラウザ自動化エクスポート(運用ジョブ)

- [x] E1 claude.ai の既存 issue 系エクスポート自動化を日次ジョブ化し、archive へ追記配置する
  - Spec: CEXP-01, CEXP-05 / 受け入れ: 手動実行で当日会話が archive に現れる
  - 2026-07-07 本番: browser-use で Claude Privacy -> Export data を実行し、Anthropic email `Your data is ready for download` から `data-853e3da4-8afa-4e83-b4ac-69ceacef6264-1783352287-aced0e5a-batch-0000.zip` を取得。`run_claude_personal_lake_import.ps1` で archive commit `6eaae97 Archive claude.ai export 2026-07-07` を作成し、当日会話 `bc804247-0bf4-41c3-984b-0594e83016a2.json` を含む 8 ファイルを追記/更新。初回 import `ingested=106`, `duplicates=365`, `quarantined=0`、二回目 no-op `ingested=0`, `duplicates=471`, `quarantined=0`、sanity `expected=471`, `actual=471`。
  - 2026-07-07 本番: `scripts/claude_export_browser.mjs`, `scripts/run_claude_personal_lake_daily_export.ps1`, `scripts/register_claude_personal_lake_daily_export.ps1` を追加し、Windows Task Scheduler に `LETHE Claude Personal Lake Daily Export` を毎日 `03:30` で登録。次回実行 `2026-07-07 03:30:00`, state `Ready`。
- [ ] E2 ChatGPT のブラウザ自動化エクスポートを同型で実装する
  - Spec: CEXP-01, CEXP-05 / 受け入れ: 同上
- [ ] E3 exit code 失敗の Slack DM 即時通知(従系)を設置する
  - Spec: CEXP-05(design D3) / 受け入れ: 故意の失敗で通知が届く
  - 2026-07-07: `scripts/notify_personal_lake_job_failure.ps1` と daily wrapper の `-NotifyOnFailure` 経路は実装済み。`LETHE_EXPORT_FAILURE_SLACK_WEBHOOK_URL` がプロセス環境・`deploy/personal-lake/.env` のどちらにも存在しないため、本番 Task Scheduler には通知オプションをまだ付与していない。missing webhook の fail-fast negative test は通過。実 Slack DM 到達 acceptance は webhook secret 設置後に残る。
  - 2026-07-07: ユーザー指定により通知先を Slack private channel `999_非公開緑地` (`C03L75JL6RM`) とし、SHIMOKITA COLLEGE workspace に Slack app `LETHE Personal Lake Alerts` (`A0BFKEVERS8`) を作成して incoming-webhook install request を提出。workspace admin approval が必須で webhook URL はまだ発行されていないため、実通知 acceptance は approval 後に残る。

## Track F. 統合

- [x] F1 E2E: ChatGPT fixture 取り込み → 交差規則相当の別系統クライアントで MCP write(decision)→ search_decisions で引ける
  - Spec: CEXP-03, MCPW-02, 既存 CLQ-05 / 受け入れ: 横断シナリオ test
- [x] F2 E2E: reply-draft → 2面から順不同承認 → card-queue が単一確定状態 → send-record → sent
  - Spec: CARD-01, CARD-02 / 受け入れ: シナリオ test が CI で通る
- [x] F3 regex grep 修正後の broad クエリ疎通検証(実公開面)+SHALL 被覆表更新
  - Spec: SRCH-01 / 受け入れ: evidence 記録。予算超過継続なら FTS change を起票して本タスクを閉じる
  - 2026-07-06: local/e2e 検証は完了。browser-use で Claude 実クライアントから公開 MCP `search_lake(query="。", source_types=["codex","claude-code","claude-ai"], limit=10)` を再実行し、`tool_ok="yes"`, `result_count=10`, `source_types_seen=["codex"]`, `error=null` を確認。公開面で予算超過は再現しなかったため FTS change は起票しない。

## 2026-07-06 実装・検証 evidence

- `cargo check --workspace`
- `cargo test -p lethe-registry --lib`
- `cargo test -p lethe-adapter-chatgpt --lib`
- `cargo test -p lethe-projection-claim-queue --lib`
- `cargo test -p lethe-projection-cognition --lib`
- `cargo test -p lethe-selfhost --lib`
- `cargo test -p lethe-e2e --test mcp_read_port`
- `cargo test -p lethe-e2e --test self_host_api`
- `openspec validate cognition-substrate --strict`
- `python .\scripts\personal_lake_pipeline_smoke.py --work-dir C:\Users\mitob\AppData\Local\Temp\lethe-pipeline-smoke-cognition-20260706-2`
  - result: `status=ok`, observations=11, first imports `claude=2`, `chatgpt=2`, `github=7`, second imports all duplicate.

## 2026-07-06 本番実行 evidence

- `docker compose --env-file deploy/personal-lake/.env -f deploy/personal-lake/compose.yaml up -d --build`
  - result: release image rebuilt with `lethe-import-chatgpt`; `personal-lake-lethe-selfhost-1` recreated and healthy.
- 本番 import no-op rerun:
  - GitHub: `ingested=0`, `duplicates=160`, `quarantined=0`
  - claude.ai: `ingested=0`, `duplicates=365`, `quarantined=0`
  - Claude Code: `ingested=0`, `duplicates=639`, `quarantined=0`, `files=13`, `lines=1816`
  - Codex: `ingested=0`, `duplicates=11644`, `quarantined=0`, `files=210`, `transcripts=210`
  - ChatGPT: archive `chatgpt/` に JSON export が無く、ユーザー指示通り実 export import は後回し。
- `python ./scripts/personal_lake_sanity.py --db deploy/personal-lake/data/lethe.sqlite3 --github-dump data/github-scratch/github-dump.json --github-source-instance github-personal --claude-conversations-dir D:\userdata\docs\private\claude-source-archive\conversations --claude-source-instance claude-personal`
  - result: `observations=12808`, `claude-ai=365`, `claude-code=639`, `codex=11644`, `github=160`
- `python ./scripts/personal_lake_w0_check.py --timeout-seconds 60 ...`
  - result: `status=ok`; year-first routing keyspec and append-only checks passed.
- Freshness projection production check after fix:
  - `sys:claude-ai=fresh`, `sys:claude-code=fresh`, `sys:codex=fresh`, `sys:chatgpt=unobserved`
  - `sys:chatgpt` は export JSON 不在による期待通りの未観測。
- 2026-07-07 Claude export production run:
  - export request: Claude Privacy -> Export data, Anthropic email download link, zip `data-853e3da4-8afa-4e83-b4ac-69ceacef6264-1783352287-aced0e5a-batch-0000.zip`
  - archive commit: `6eaae97 Archive claude.ai export 2026-07-07`, including today conversation `conversations/bc804247-0bf4-41c3-984b-0594e83016a2.json`
  - import: first `ingested=106`, `duplicates=365`, `quarantined=0`; second `ingested=0`, `duplicates=471`, `quarantined=0`
  - sanity: total observations `12914`, `claude-personal=471`, `expected=471`, `actual=471`
  - freshness after import: `sys:claude-ai=fresh`, `latest_published=2026-07-06T15:34:39.944918Z`, `latest_recorded_at=2026-07-06T15:48:09.457817859Z`; `sys:chatgpt=unobserved`
  - deep health: `status=ok`, all projections healthy
  - scheduler: Windows Task Scheduler task `LETHE Claude Personal Lake Daily Export`, daily `03:30`, state `Ready`, next run `2026-07-07 03:30:00`
- Browser-use public surface:
  - protected resource metadata advertised `mcp:read` and `write:supplemental`
  - public `/health/deep` returned 404, confirming the internal API router is not exposed on Funnel
  - Claude public broad query passed as recorded in F3
  - Auth0 issuer switched to `https://dev-muwlx2h3vvs2z7xt.us.auth0.com/`; public and local protected-resource metadata advertise the new issuer and `["mcp:read","write:supplemental"]`.
  - Claude public write passed after DCR/API grant repair: `sup:86eea51a-03d4-4fa8-b241-3de111ed0ffb` -> `sup:ad779751-43ec-4172-99b6-7b63040b4941`, claim queue terminated projection `stale=false`.
- 2026-07-07 final verification:
  - `cargo test --workspace` passed.
  - `openspec validate cognition-substrate --strict` passed.
  - production `/health/deep` returned `status=ok`, all projections healthy, and `proj:claim-queue` / `proj:freshness` reads returned `stale=false`.

### SHALL 被覆メモ

- SRCH-01: public broad query evidence recorded; no FTS follow-up required.
- MCPW-05: claude.ai public write is complete after Auth0 DCR/API grant repair. ChatGPT public write remains deferred with the ChatGPT export/app work, so C3 remains open for the ChatGPT half only.
- CEXP-01/CEXP-05 operational export jobs: ChatGPT export is explicitly deferred; E1 is complete, E2 remains open, and E3 remains open until the Slack webhook approval/secret exists and a real failure notification is observed.
