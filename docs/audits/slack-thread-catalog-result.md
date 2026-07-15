# Slack thread re-discovery 計算量修正結果

- 対象: 計算量監査 High #2
- 実施日: 2026-07-15 (JST)
- 対象ブランチ: `fix/slack-thread-catalog`
- 本番 selfhost / 実 Slack: 非接触

## 実装結果

SQLite schema version 6 に、`(source_instance, channel_id, thread_ts)` を主キーとする永続 `slack_thread_catalog` と、global discovery high-water / poll generation を保持する `slack_thread_catalog_state` を追加した。

Slack message の durable append と catalog upsert は同一 transaction で処理する。通常 ingest 以外から追加された canonical Observation は、global high-water より後の page だけを読み、page 内の catalog upsert と high-water 更新を同一 transaction で commit する。transaction 失敗時は high-water を進めないため、再実行で同じ tail を安全に処理できる。

thread reply の remote poll は catalog の indexed active/due queue に限定した。新規 reply を返した thread は次 poll も active、空だった thread は idle とし 8 poll generations 後に due に戻す。これにより通常時の過去 thread 全件呼び出しをなくしつつ、古い idle thread に後着した reply も最長 8 poll generations 後に回収する。reply cursor は catalog entry に統合し、poll 完了後だけ進める。途中失敗時は cursor を進めず、再取得を canonical dedup で安全に処理する。

初回起動時に catalog が空なら canonical tail を `append_seq = 0` から一度だけ backfill する。reply が root より先に保存された場合も `thread_ts` から root key を作り、後続 root と同じ主キーへ冪等 upsert する。

## 計算量

記号は監査文書に合わせ、N を canonical Observation 数、C を対象 channel 数、R を既知 thread 数、ΔN を前回 high-water 後の Observation 数、A を当該 poll の active/due thread 数とする。

| 経路 | 修正前 | 修正後 |
|---|---:|---:|
| thread discovery / poll | O(C・N) | O(ΔN + A)（論理処理量） |
| catalog DB 更新 | なし | O(Δthreads log R)（SQLite B-tree upsert） |
| thread remote calls | Θ(R) / poll | Θ(A) / poll |
| 初回 backfill | 毎 poll O(C・N) | 一度だけ O(N + R log R) |

active/due query は全 catalog 列挙ではなく、`source_instance, channel_id, active, next_poll_generation, thread_ts` index を使う active branch と due branch の `UNION ALL` で取得する。`max_sync_items` により一回の poll 対象数も既存 resource limit 内に制限される。

## 正しさと回帰試験

- root → reply、reply → root、同一 root の再発見を同一 catalog key に集約する。
- discovery page の不正な high-water batch は transaction rollback し、catalog / high-water の片側だけを進めない。
- catalog と reply cursor は SQLite 再 open 後も保持する。
- 2 thread 中 1 thread だけが active の poll で remote call が 1 回になることを fixture client の call counter で検証する。
- idle thread への後着 reply を due poll で取得し、最終 Observation timestamp 集合が全 thread 再発見基準と一致することを検証する。
- 同一 reply の再取得で Observation が重複しないことを検証する。

## 品質ゲート

- `cargo fmt --all`: 成功
- `cargo clippy --workspace --all-targets -- -D warnings`: 成功（warning 0）
- `cargo test --workspace`: 成功（exit code 0）

実 Slack API、本番 selfhost、既存本番 SQLite には接続していない。

## Commit 状態

実装・検証後に `fix/slack-thread-catalog` への commit を2回試行したが、worktree の shared Git metadata `D:/userdata/docs/projects/skcollege_database/.git/worktrees/wt-lethe-2/index.lock` を作成する権限がなく、いずれも `Permission denied` で停止した。残存 `index.lock` は存在せず、一時競合ではなく実行環境の filesystem 権限が原因である。変更は worktree index に stage 済みだが、HEAD は起点 `a00e14a` のままである。
