# ReplySLO join index 修正結果

## 状態

実装、回帰テスト、workspace 全体検証は完了している。`fix/replyslo-index` への commit だけが実行環境の Git 管理領域に対する書き込み権限不足で未完了である。working tree の差分は保持している。

## 計算量

- 修正前の rebuild: Observation N 件を page size P で処理する各 page で supplemental S 件を再走査するため O((N/P)·S + N)。
- 修正後の rebuild: hash join index を一度だけ構築して全 page で共有するため期待計算量 O(S + N)、追加メモリ O(S)。
- 通常の Observation 追加: 追加 Observation ΔN 件だけを resident index で投影するため期待計算量 O(ΔN)。
- reply draft / send record 追加: index 更新は期待 O(1)。send record は影響する1 Observation row だけを upsert する。

## 正しさ

- indexed projection、incremental index、全 supplemental replay の結果を比較した。
- earliest sent、複数 send record、未送信、期限超過、遅延送信を検証した。
- page size 1 / 128 の paged rebuild と全再構築の manifest / projection item を比較した。
- Observation delta が既存 ReplySLO row を置換せず、追加 Observation の row だけを insert することを検証した。
- send-record delta が対象1行を更新し、全再構築と一致することを検証した。

## 検証結果

- `cargo fmt --all -- --check`: 成功
- `cargo clippy --workspace --all-targets -- -D warnings`: 成功
- `cargo test --workspace`: 成功（失敗 0、既存の明示 ignored 1）
- `git diff --check`: 成功

## commit blocker

- branch: `fix/replyslo-index`
- base / current HEAD: `a00e14a`
- 実行した commit message: `fix: index ReplySLO supplemental joins`
- 失敗原因: worktree の共通 Git 管理領域 `D:/userdata/docs/projects/skcollege_database/.git/worktrees/wt-lethe-3/index.lock` を作成できず、`Permission denied` になった。この実行環境では `.git` が read-only のため、コード変更では解消できない。
- 権限のある環境で必要な操作: 対象6ファイルを stage し、上記 message で commit する。
