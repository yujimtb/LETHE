# Tasks: personal-lake-ingestion

> 週末ゴール: 「入れ続けられる配管が通った状態」。W1 完了時点で最低限成立、W2 まで到達すれば完了。

## Phase W0 — インスタンス起動(不可逆判断を含むため最初)

- [x] 0.1 selfhost 用 Dockerfile + compose 作成(SQLite / blobs はローカル bind volume、P2)
- [x] 0.2 config.toml 作成: `retention_days = 3650`(P6)、API token 設定(localhost でも設定 — Phase C でそのまま使う)、sources は空
- [x] 0.3 encryption key 生成 → **パスワードマネージャーへ保管**(P7 例外)→ env 投入
- [x] 0.4 keyspec `coarse(year):coarse(month):source:container:fine(published)` で initialize(P1)

**Gate W0:** partition log に `initialize` イベントが永続され、両 keyspec の不変性が DB 側で強制されていること(寮向け Gate P0 と同一条件)。`/health/deep` 応答確認。

## Phase W1 — claude.ai 配管(P3: 先行)

- [x] 1.1 claude.ai で bulk export をリクエスト、zip 取得
- [x] 1.2 private archive repo 作成 + 会話単位展開スクリプト(純関数)+ 初回 commit(P12 / PING-AR-01)
- [x] 1.3 `lethe-import-claude` CLI 実装(`apps/tools`、PING-CL-01 — 配線のみ)
- [x] 1.4 1 会話で e2e(P5)。失敗ケースは U3 issue に property test ケースとして記録してから修正
- [x] 1.5 全履歴 import(約 1 ヶ月分)
- [x] 1.6 同一 zip 再 import → 全件 `Duplicate` であることを確認

**Gate W1:** 1.6 の完全 no-op。projection API 経由で任意の会話が読めること。

## Phase W2 — GitHub 配管

- [x] 2.1 `gh api` dump スクリプト(issues / comments / PRs / reviews / review comments / commits / timeline events、pagination 対応、出力は gitignore された scratch ディレクトリ)(P12)
- [x] 2.2 `lethe-import-github` mapper 実装(PING-GH-01〜03、純関数)
- [x] 2.3 mapper property test: 再 dump no-op / 編集 → 新 Observation / diff 非含有
- [x] 2.4 所有全リポジトリ(private ops 含む)を dump → import(P11)
- [x] 2.5 再 dump → 再 import で no-op 確認

**Gate W2:** 2.5 の no-op。issue L3(U3 検証)の完了条件に足るテストケースが揃っていれば L3 を close。

## Phase W3 — closeout(週末内 or 週明け)

- [x] 3.1 取り込み件数のサニティチェック(会話数 / issue 数を source 側と突合)
- [x] 3.2 本 change を実装結果に合わせて確定、merge
- [x] 3.3 関連 issue 更新: L3(U3)/ L9(前提充足を記録)/ L10(mapper 再利用を記録)
- [x] 3.4 頭の中に残っている未 issue 化項目を最終掃き出し(このセッションの決定事項含む)

## Frozen During This Change

- L9 自動化 / L10 Observer 常駐化 / Phase C 取り出し口 / ダッシュボード Projection / バックアップ自動化
