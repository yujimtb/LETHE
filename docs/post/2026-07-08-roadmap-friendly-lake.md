# LETHE ロードマップ: フレンドリーな個人レイクへ

- Date: 2026-07-08
- Author/Tool: Claude (Claude Code 夜間自律作業セッション)
- Scope: LETHE 全体(検索体験・運用可視性・取り込みパイプライン・セットアップ体験)。正典仕様の変更提案ではなく方向性文書
- Repository revision: 9064e73 (加えて作業ツリーに MCP OAuth 関連の未コミット変更あり)
- Status: Draft

## いまの姿 (2026-07-08 時点)

- append-only Observation Lake + Projection + Governance の Rust workspace。**本番稼働中**(Docker Compose selfhost + Tailscale Funnel + Auth0 OAuth の MCP read port)
- 取り込み済みソース: Slack、Google Slides、ChatGPT(3,415 件)、claude.ai(471 件)、Claude Code(639 件)、Codex(11,644 件)、GitHub
- claude.ai / ChatGPT / Claude Code / Codex の各クライアントから MCP で検索・decision 参照・supplemental 書き込みができる
- 進行中(作業ツリー): MCP OAuth の scope / securitySchemes 対応、再認可運用スクリプト

## 今夜の改善 (2026-07-08 夜間作業)

テーマ「無骨→フレンドリー」。AI と人間の両方にとっての検索・運用の手触り:
1. **複合語検索** — 空白区切りの複数語クエリ(例:「Nanihold ロードマップ」)を全語 AND で検索できるように。従来は生の正規表現の連続一致で、複合語はほぼ確実に 0 件だった。単語 1 語の挙動は後方互換
2. **スニペット修正** — 検索結果の抜粋が常に「本文先頭 240 文字」でヒット箇所を見せていなかった問題を、ヒット位置中心の窓に修正
3. **MCP ツール説明の改善** — search_lake / search_decisions の説明文にクエリの書き方ガイドを明記(接続する全 AI クライアントの検索成功率に直結)
4. **運用可視化** — `scripts/lethe_status.ps1` 新設。/health/deep を人間可読なサマリ(ソース別鮮度・件数、dead-letter、全体判定)で表示
5. `config.example.toml` への説明コメント
6. **import 系 CLI 7 本の `--help` 実装**(従来は `--help` 自体が unknown argument エラーだった)と、引数・環境変数不足エラーの平文化
7. **MCP レスポンスサイズの安全上限** — 大きな応答で MCP セッションが落ちる事象(limit>20+cursor で session expired)への対処。limit をサーバー側で 20 にクランプ(`_meta` に requested/effective を明示)、snippet は省略記号込み最大 240 文字、matched_ranges は 1 レコード最大 20 件

注意: 上記はコード変更のみで、**本番 selfhost にはまだデプロイしていない**(再ビルド・再起動はオーナー判断)。

## 次の一歩(短期: 〜2週間)

1. **今夜の変更のレビューとデプロイ** — 差分確認 → コミット → selfhost 再ビルド・再起動(MCP 経由で複合語検索が効くのはデプロイ後)
2. **取り込みパイプラインを閉じる** — 現状「仕様上は日次、実運用は半接続」:
   - claude.ai 日次エクスポート(03:30 タスク)が `claude_export_browser.mjs failed` で失敗中 → 修理
   - Codex / Claude Code は 03:10 の archive 同期までは成功しているが、Lake への日次 import が未登録 → タスク登録で接続
   - ChatGPT は importer 実装済みだが日次ジョブ未登録 → 同上

## 中期(〜1〜2ヶ月)

- **検索品質の次段階**: 一致語数・新しさによる並び順、期間フィルタ、source_types の使いやすい指定
- **運用ダッシュボード**: lethe_status.ps1 の発展形として、鮮度・失敗・容量をブラウザで見られる管理ビュー(2026-07-04 の管理ダッシュボード構想)
- **ストレージ進化**: PostgreSQL 移行の検討(2026-06-24 の議論)。per-leaf SQLite の容量駆動 split 運用の実測を材料に判断
- **JGX との分担**: Projection / Adapter は JGX 側、lake 基幹は本人、の境界を保った受け入れ整備(Apache-2.0)

## 長期の方向性

「成長し続ける多次元集合」としての汎用個人データ基盤。フラクタル的 routing / sharding 設計(2026-06 の決定台帳 D2〜D9)の実装は、単一ノード運用の限界が見えてから。write-back(M07)は Post-MVP のまま維持。

## やらないこと (non-goals)

- 判断・行動ロジック(抽出・検証・ブリーフィング等は Eos 側。LETHE はデータの真実と提供に徹する)
- 認可サーバ機能(token endpoint、refresh token exchange、DCR、同意画面は実装しない現方針を維持)

## 採用について

この文書は外部作成ロードマップであり自動的に正典にはならない。採用する項目は OpenSpec change に移し、実装判断は `docs/decisions/` または change の `design.md` に反映する(docs/post/README.md の規約どおり)。
