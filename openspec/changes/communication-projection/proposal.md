## Why

観測取り込み(`POST /api/import/observation-drafts`)は同一リクエスト内で ledger append → 非 corpus projection materialize → 検索 index catch-up → audit を同期実行する。`classify_non_corpus_delta` は通信メタデータ(`communication_channel_id` / `communication_sender_id` / `communication_thread_ref` / `reply_due_at`)を持つメッセージを `ReplySloRequired` と分類し、**全観測を走査するフルリビルド**(`rebuild_materialized_snapshot_paged`)へ落とす。実測では観測 4.9 万件でメッセージ 1 通の取り込みが 51 秒(append 212ms / materialize 49,542ms / search 964ms / audit 300ms)かかり、O(全観測数) のためデータ増で必然的に悪化する。この遅延がクライアント(nanihold_intercom)の read timeout を招き、2026-07-22 に実メッセージ消失事故を起こした。

reply-SLO は状態計算だけ増分ルールが未実装で、他分類(`FreshnessOnly` / `SlackMessage`)は増分済みである。加えて `UnsupportedSchema` / `EmptyAppend` も通常 append の応答としてフルリビルドへ落ちており、append-only 基盤に対する projection の設計原則(増分 fold)を破っている。

## What Changes

- Append-only 基盤に対し **すべての projection は増分 fold で定義される** ことを規範として明文化し、通常 append の応答でフルリビルド(全観測走査)を行わないことを要件化する。
- `classify_non_corpus_delta` の全フォールバック分岐を廃止する。`ReplySloRequired` は専用 communication projection へ、`EmptyAppend` は no-op へ落とす。`UnsupportedSchema` は「検疫」ではなく、projection が登録済み全 schema の fold 挙動を宣言し起動時に registry との網羅性を検証して不一致なら fail fast する方式へ置き換える(未登録 schema は取り込みエンジンが拒否するため検疫対象は存在せず、旧分岐の実体は registry と私製ホワイトリストの二重管理ドリフトだった)。
- discord/slack メッセージを(チャネル × スレッド)キーで増分 fold し、reply-SLO 状態(未返信・期限・返信済み判定)をメッセージ 1 通あたり O(1) で維持する専用 communication projection を新設する。全履歴再計算と同一結果になる等価性を要件化する。
- 正当なフルリビルド(materialization version 変更時の移行・破損復旧・初回ブートストラップ)は HTTP 応答をブロックせず背景で直列実行し、進行中も古い snapshot を返す(エラーにしない)安全弁として位置づけ直す。
- projection 読み(card-queue・reply-SLO 読み・corpus 検索)には遅延があることを鮮度契約として明文化する。
- 取り込みレイテンシ目標: 登録チャネルメッセージ 1 通の import 応答 p95 < 2 秒。

## Capabilities

### New Capabilities

- `incremental-materialization`: append-only 基盤に対する増分 fold projection の原則、通常 append でのフルリビルド禁止、`classify_non_corpus_delta` 全フォールバック分岐の廃止、正当なフルリビルドの背景化・直列化・読み取り一貫性、projection 読みの鮮度契約、`import_timing` 計測整合、取り込みレイテンシ SLO、新規 projection 追加の受け入れ条件を規定する。
- `communication-projection`: (チャネル × スレッド)キーの reply-SLO 専用 projection のデータモデル、O(1) 増分 fold、時刻依存判定の読み取り時評価、全履歴再計算との等価性、materialization version 移行と自然再構築、card-queue との責務境界を規定する。

### Modified Capabilities

なし。`observation-lake` の append-only 契約、`persistent-search-index` の検索契約、`cognition-projections` の card-queue 契約は変更せず、reply-SLO 計算責務だけを専用 projection へ移す。

## Impact

- 主対象: `apps/selfhost/src/self_host/app/mod.rs`(`classify_non_corpus_delta` / materialize orchestration)、`crates/projections/cognition`(`ReplySloProjector`)、SQLite projection persistence。
- API: wire contract の変更なし。取り込み応答は append 成功で返し、フルリビルドを応答から外す。
- System Laws: Append-Only Law と Replay Law を維持する(canonical Observation から決定的に再構築可能な派生 materialization のみ)。No Direct Mutation Law を維持する。
- 対象外: nanihold_intercom(client)、本番 selfhost デプロイ、既存 `data/`。

## Non-goals

- corpus projection の遅延改善や検索 index の再設計。
- reply-SLO の判定規則(status 定義・latency 計算)そのものの変更。
- send 側(reply-draft@1 / send-record@1 supplemental)の増分 fold の再設計(既に増分済み)。
- card-queue projection の再設計(cognition 側の責務。本 change の対象外)。
- 検索契約・MCP/HTTP wire format の変更。
