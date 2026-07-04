# Design: personal-lake-ingestion — 決定台帳 P1〜P13

**Status:** すべて LOCKED(2026-07-04 設計セッション)

### P1. routing keyspec = `coarse(year):coarse(month):source:container:fine(published)`

- year 先行。個人 lake の支配クエリは時間的局所性(「直近数週間の横断」)であり、寮 lake の month 先行(季節横断クエリ支配)の選定根拠は個人利用に存在しない。
- source を上位軸にしない: source の同一性は長期で保証されない(サービス改名 / export 形式変更 / アカウント移行)。source は識別子であって編成軸ではない。
- project 軸は D11.4 により routing に入れない(解決済みエンティティ)。Projection 側で持つ。
- keyspec は initialize 後不変(D7)。initialize 時に partition log へ永続されることを Gate W0 で確認する。

### P2. ホスト = 個人 PC + Docker、将来個人 NAS へ移設

- 寮 lake(寮 NAS)とは完全に別インスタンス。consent 境界 = インスタンス境界(FilteringGate のスコープ設計で分離する必要がそもそも生じない)。
- Mac mini は寮備品のため個人データのホストに使用しない。
- SQLite はコンテナから bind した**ローカル** volume(ネットワーク FS 禁止則は寮 NAS と同一)。

### P3. one-shot CLI 2 本(`apps/tools`)、engine 直結

- `lethe-import-claude` を先行、`lethe-import-github` を後続。独立ツール(単一 CLI に統合しない)。
- runtime source 化(poll 常駐)はしない — adapter contract の検討を要するため L10 で実施。
- one-shot は exact idempotency により常に再実行安全。

### P4. GitHub 取り込み範囲 = 全オブジェクト型

issue 本文 / issue コメント / PR 本文 / PR review(総評)/ PR review コメント / commit(出来事スパイン)/ timeline event。実装しやすいケースへのカットオフはしない — 通貨は実装量ではなく判断数であり、判断は P8〜P10 で尽くした。

### P5. claude.ai 初回取り込み = 1 会話 e2e → 全履歴

- importer(tasks 2.2 実装済)の実データ走行実績が不明のため、1 会話で通してから全量。
- 実データで落ちたケース(複数 root / uuid 欠落パターン等)は**そのまま U3 の property test ケースとして記録**する(issue L3)。週末作業が U3 検証を兼ねる。

### P6. `retention_days = 3650`

- `apply_retention` の削除対象は `dead_letters` / `audit_events` のみ(実装確認済み: crates/storage/sqlite/src/persistence/mod.rs)。Observation 本体は設定値に関わらず不変。
- 個人規模では両テーブルとも容量が問題にならず、消して後悔する非対称性のみが存在するため実質恒久保存に設定。

### P7. バックアップ = 見送り(個人 NAS 導入時)

- lake 内容の大半は source から再クロール可能(D12 思想)。回復不能なのは supplemental と claude.ai 由来分のみだったが、後者は P12 の source archive が塞ぐ。
- **例外: encryption key は即時パスワードマネージャーへ保管**(鍵喪失はバックアップの有無と無関係に致命)。

### P8. commit canonical = メタデータ + メッセージ + 変更ファイル一覧(diff 非含有)

- identity: `object_id = {repo}#commit#{sha}`。内容不変のため H(canonical) が完全に安定。
- diff 非含有は境界原理の帰結(proposal.md)であってスコープカットオフではない。git が diff の権威的一次ストア。
- `published` = author date(作業が起きた時刻)。committer date は payload に保持(rebase で変わるのは committer date と sha であり、sha は identity に入っているため歴史書き換えは新 Observation として自然に観測される)。

### P9. PR review コメント: アンカーは構造化 canonical payload

- アンカー(path / line / anchor commit sha)は source が返す生の事実であり解釈ではない → canonical 側(supplemental ではない)。
- identity: `object_id = {repo}#pr_review_comment#{comment_id}`。本文編集は D3b により新 distinct Observation。
- review 本体(approve / request-changes + 総評)は別 Observation 型: `{repo}#pr_review#{review_id}`。

### P10. timeline events = 全イベント型を無差別取り込み

- ホワイトリストは「どれが重要か」という解釈の ingest 側混入であり設計原理に反する。Lake は全部受け、解釈は Projection。
- `object_id = {repo}#issue_event#{event_key}`。ネイティブに append 型のため可変メタデータ(state / labels / title)の観測問題をこれで解消する。通常 event は numeric `id`、GitHub が `committed` timeline event を返す場合は `sha` を `event_key` とする。
- actor / author フィールドは将来の Actor ADR(issue L2)の生帰属としてそのまま使う。

### P11. 対象リポジトリ = 所有全リポジトリ(private ops 含む)

- 選別はキュレーション = 解釈であり ingest 側に持ち込まない(P10 と同一原理)。idempotent のため「後で追加」と「最初から全部」に技術的差はなく、判断を一つ消せる。
- private repo はローカル `gh` 認証で透過的にカバー(P12)。

### P12. fetch / mapping 分離 + source archive の非対称運用

- `gh api` シェルスクリプトで JSON を dump → CLI は dump ファイルを読む純関数 mapper で ObservationDraft 化(Functional Core / Imperative Shell に一致、property test 可能)。
- **GitHub dump = 使い捨て scratch**(一次ストアが耐久・再クロール可能なため。アーカイブは冗長複製)。
- **claude.ai export = private git アーカイブリポジトリへ commit**(一次ストアが再クロール不能なため。耐久一次ストアの自前代替)。export zip は会話単位ファイルに展開(純関数)してから commit し、diff を意味あるものにする。
- archive repo は **source archive であって第二の lake ではない**: 読者は ingest CLI のみ。Projection / 取り出し口は一切こちらを向かない(SHALL NOT)。必ず private。

### P13. 本 change 自体を openspec として管理

外部化プロジェクトの最初の成果物が未文書の実装であることは自己矛盾のため、本 change を先に確定してから実装する。
