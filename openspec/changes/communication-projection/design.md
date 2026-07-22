## Context

観測取り込みは `POST /api/import/observation-drafts` の同一リクエスト内で ledger append → 非 corpus projection materialize → 検索 index catch-up → audit を同期実行する(`apps/selfhost/src/self_host/app/service_support.rs::materialize_after_observation_append`)。materialize は `classify_non_corpus_delta_with_reason`(`apps/selfhost/src/self_host/app/mod.rs:2713`)の分類で分岐する:

- `FreshnessOnly` / `SlackMessage` → 増分 fold(`apply_compact_incremental_delta`)。全観測数に対して O(1)。
- `FullRebuild`(`ReplySloRequired` / `UnsupportedSchema` / `EmptyAppend` / `SlackUserIdMissing`)→ `rebuild_materialized_snapshot_paged`。全観測を page 走査する O(全観測数)。

`ReplySloProjector::project_observations`(`crates/projections/cognition/src/lib.rs:717`)は lake の全 Observation を走査し、通信メタデータ(`communication_channel_id` / `communication_sender_id` / `communication_thread_ref` / `communication/reply_due_at`)を持つ行を抽出して status を計算する。send 側(reply-draft@1 / send-record@1)は `ReplySloJoinIndex` として既に `SupplementalProjectionCache` で増分 fold 済み(`apps/selfhost/src/self_host/app/mod.rs:621`)。フルリビルドが必要なのは着信(観測)側の全走査だけである。

実測(観測 4.9 万件、メッセージ 1 通取り込み)は 51 秒(append 212ms / materialize 49,542ms / search 964ms / audit 300ms)。materialize がボトルネックで、client(nanihold_intercom)の read timeout を招き 2026-07-22 に実メッセージ消失事故を起こした。

canonical Observation は SQLite の append-only ledger が正本であり、projection は破棄・再生可能な派生 materialization である(Append-Only Law / Replay Law / No Direct Mutation Law)。

## Goals / Non-Goals

**Goals:**

- Append-only 基盤に対する「全 projection は増分 fold で定義される」規範を明文化し、通常 append の応答でフルリビルドを行わないことを要件化する。
- reply-SLO の着信側計算を(チャネル × スレッド)キーの専用 projection へ移し、メッセージ 1 通あたり O(1) 増分 fold にする。
- `classify_non_corpus_delta` の全フォールバック分岐(`ReplySloRequired` / `UnsupportedSchema` / `EmptyAppend`)を廃止する。
- 正当なフルリビルド(移行・復旧・ブートストラップ)を背景・直列・古い snapshot 提供の安全弁へ位置づけ直す。
- 登録チャネルメッセージ 1 通の import 応答 p95 < 2 秒を、既存観測数に依存せず満たす。

**Non-Goals:**

- reply-SLO の判定規則(status 定義・latency 計算)そのものの変更。
- send 側 join の再設計(既に増分)。
- corpus projection の遅延改善・検索 index 再設計。
- card-queue projection(cognition 側)の変更。
- MCP / HTTP wire contract、検索契約の変更。

## Decisions

### D1: reply-SLO の着信側を(チャネル × スレッド)キーの専用 projection として増分 fold する

着信メッセージの reply-SLO 素データ(`incoming_observation_id`、`channel_id`、`sender_id`、`thread_ref`、`published`、`due_at`、`sent_at?`)を、(`channel_id`, `thread_ref`)をキーに束ねた map として resident に保持する。append 1 通につき、その 1 件の fact を upsert するだけで済む(O(1))。全観測走査を行う `project_observations` の呼び出しは通常 append 経路から除去する。

send 側は既存の `ReplySloJoinIndex`(`sent_by_observation`)をそのまま入力に使う。send-record@1 の増分 upsert は既に `SupplementalProjectionCache::replace/rollback` で O(1) に維持されており、着信 fact との join もキーで引ける。両者の責務境界: 着信 fact = 観測由来、`sent_at` = supplemental 由来。

### D2: 時刻依存判定(Overdue/Pending)は materialize 時に固定せず読み取り時に評価する

現行 `ReplySloProjector` は `now = built_at`(materialize 時刻)で `Overdue` / `Pending` を確定する。これを踏襲すると「時刻が `due_at` を跨いだだけ」で snapshot が古くなり、再 materialize の誘因になる。専用 projection は `published` / `due_at` / `sent_at` の素データだけを保持し、`status` は読み取り時の評価時刻 T に対して算出する。これにより append なしの時刻経過で再 materialize も全走査も要らない。

**等価性の定義(CP-02):** 「任意に固定した評価時刻 T について、専用 projection の出力 = 現行 `project_records` を同じ T で評価した出力」。現行実装の T は materialize 時刻という運用上の T にすぎないため、同じ T を入れれば結果は bit 一致する。等価性検証は差分テスト(同一入力集合 × 複数 T で新旧を突き合わせ)で担保する。

### D3: classify_non_corpus_delta の全フォールバック分岐を廃止する

**根拠 — 私製ホワイトリストという二重管理が事故の温床だった:** 取り込みエンジンの `ObservationPreparer::prepare`(`crates/engine/src/lake/ingestion.rs:75`)は Step 2 で `self.registry.get_schema(&req.schema)` を照合し、未登録 schema を `IngestResult::Rejected`(`ingestion.rs:83-88`)で拒否する。よって **未登録 schema の Observation はレイクに存在し得ない**。ところが projection 側は registry とは独立に `FRESHNESS_ONLY_SCHEMAS` という私製ホワイトリスト(`apps/selfhost/src/self_host/app/mod.rs:2716`)を持ち、そこに載らない登録済み schema を `UnsupportedSchema` として全観測フルリビルドへ落としていた。つまり `UnsupportedSchema` が実際に検出していたのは「未知 schema」ではなく **registry と projection 内ホワイトリストの二重管理ドリフト**である。ホワイトリストへの追記漏れが、正規に登録された schema を 51 秒のフルリビルドへ落とす事故の温床だった。

したがって検疫(quarantine)は誤った対処であり、正しい対処はドリフトのデプロイ時検出である:

- `ReplySloRequired` → 廃止。通信メタデータを持つメッセージは D1 の増分 fold へ。分類上は `FreshnessOnly` / `SlackMessage` と同じ増分経路に統合される(reply-SLO 更新を増分ステップに追加)。
- `UnsupportedSchema` → 廃止。代わりに projection は「登録済み全 observation schema に対する fold 挙動(増分 fold / freshness-only / communication)」を宣言し、**起動時に registry の schema 集合との網羅性を検証して不一致なら fail fast する**。ドリフトはランタイム対応ではなくデプロイ時検出とする。fold 実行時に宣言外 schema へ遭遇したら(起動検証を素通りしたコード欠陥)警告 + スキップとし、リビルドはしない。
- `EmptyAppend` → 廃止。空 append は no-op(何も再構築しない)。

結果として通常 append の応答経路から `FullRebuild` 分岐が消える。`NonCorpusDeltaKind::FullRebuild` と `NonCorpusDeltaReason` は「背景リビルドの事由」(移行・復旧・ブートストラップ)専用の型へ役割変更する。網羅性検証は registry の observation schema 集合と fold 宣言集合の集合差分で実装し、私製ホワイトリストを唯一の schema 台帳(registry)へ収斂させる。

### D4: 正当なフルリビルドは背景・直列・古い snapshot 提供にする

移行・復旧・ブートストラップのフルリビルドは取り込み応答をブロックしない。append 成功で応答を返し、リビルドは single-flight の背景 task で実行する。進行中の projection 読み(card-queue・reply-SLO・corpus 検索)は直前に公開済みの snapshot を返す(空結果・エラーへ fallible しない)。完了時に新 snapshot を atomic に公開する。この機構は `persistent-search-index` の `SearchIndexManager`(Opening/CatchingUp/Rebuilding/Ready/Failed の single-flight 背景再構築)と同じ設計思想で、非 corpus snapshot 側にも同等の状態機械を持たせる。

初回ブートストラップだけは公開済み snapshot が無いため、その間の読みは「未 ready」を明示する(空結果の silent fallback はしない)。移行・復旧は既存 snapshot を保持したまま背景で差し替える。

### D5: materialization version を更新し既存 snapshot を自然再構築する

`NON_CORPUS_MATERIALIZATION_VERSION`(現在 7)を更新する。起動時に永続 materialization の version 不一致を検知したら D4 の移行フルリビルドで communication projection state を含む snapshot を canonical Observation + supplemental から再構築する。再構築結果は CP-02 の等価性を満たす。専用の追加移行スクリプトは不要で、version bump + 自然再構築で吸収する。

### D6: card-queue は対象外(cognition 側の責務)

card-queue projection(`crates/projections/cognition` の `CardQueueProjector` / `CardQueueReducer`)は supplemental 由来で既に増分 fold 済みであり、reply-SLO とは入力も責務も別である。本 change は card-queue の入力・出力・契約に触れない。communication projection と card-queue はともに「通信」を扱うが、前者は着信の reply-SLO、後者は cognition の card であり責務境界を混同しない。

### D7: 鮮度契約の明文化

projection 読みは canonical Observation に対して遅延を持つ。**確定値: 通常時(増分 fold)の反映遅延は 5 秒以内、背景リビルド中は古い snapshot を返し 60 秒以内の遅延まで許容する。** この契約を API doc と projection 読み経路に明記する。

## 増分 fold 受け入れ規範(IM-08)

**「増分ルールを定義できない projection は設計が誤っている」** を substrate の規範とする。append-only ledger 上の projection は、append 単位で state を更新する fold(reducer)として定義できなければならない。定義できない場合、それは projection の粒度・キー設計・状態表現の誤りであって、通常 append の応答でフルリビルドを許す理由にはならない。将来 projection を追加する際は「append 1 件に対する state 更新規則」を設計成果物として要求する。フルリビルドは移行・復旧・ブートストラップという「materialization 全体の再生成」のためだけの機構であり、通常運用の materialize 手段ではない。

## Risks / Trade-offs

- **[新旧の非等価が潜む]** → CP-02 の差分テスト(同一入力 × 複数 T で新旧突き合わせ)を受け入れ条件にする。特に `overdue` 抽出、tie-break、latency 符号、複数 send-record の最小 `sent_at` 採用(現行 `.min()` 挙動)を網羅する。
- **[読み取り時評価のコスト]** → status 算出は保持行数に対して O(読み取り対象行) で、全観測走査ではない。read はページング済みの reply-SLO 読み契約に載せる。
- **[fold 宣言と registry のドリフト再発]** → 起動時網羅性検証で fail fast し、私製ホワイトリストを registry へ収斂させる。新 schema 登録時は fold 宣言の追加を強制され、追記漏れは起動失敗として即座に検出される。
- **[背景リビルド中の古い snapshot]** → 鮮度契約(IM-05/D7)で明示。空結果・エラーへ fallback しないことを要件化(IM-04)。
- **[version bump による全 instance の初回移行リビルド]** → D4 で背景・非ブロッキング化。本番 selfhost へのデプロイは本 change の対象外(実装後に別途)。

## 確定事項(オーナー決定 2026-07-22)

1. **鮮度契約の具体値**: 通常時 5 秒以内 / 背景リビルド中 60 秒以内で確定(IM-05 / D7 に反映)。
2. **未知 schema の扱い = quarantine 概念の廃止**: 取り込みエンジンが未登録 schema を Rejected で拒否する(`crates/engine/src/lake/ingestion.rs:75`)ため「未知 schema の観測」はレイクに存在し得ず、検疫は不要。旧 `UnsupportedSchema` の正体は registry と私製ホワイトリスト(`FRESHNESS_ONLY_SCHEMAS`)の二重管理ドリフトであり、対処は起動時網羅性検証による fail fast(IM-03 / D3 に反映)。
3. **通信 schema 範囲 = schema 非依存**: `communication_*` メタの有無で判定する現行方式のまま維持し、gmail 等も自然に communication projection の対象となる(CP-01 のキー抽出はメタ由来で schema を問わない)。
4. **移行タイミング = 通常デプロイ**: version bump に伴う初回移行フルリビルドは背景・非ブロッキング(D4)であり、メンテナンス枠は設けず通常デプロイに委ねる。
