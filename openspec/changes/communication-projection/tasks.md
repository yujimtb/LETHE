## 1. 増分 fold 規範と分類分岐の廃止

- [x] 1.1 [Implementer] `incremental-materialization` IM-01/IM-02 に従い、通常 append の応答経路から全観測走査フルリビルドを排除し、フルリビルドを移行・復旧・ブートストラップ専用へ役割変更する(参照: `apps/selfhost/src/self_host/app/service_support.rs::materialize_after_observation_append`)。受入: 対応 schema の append が増分経路のみを通り `rebuild_materialized_snapshot_paged` を呼ばないことのテストが通る。
- [x] 1.2 [Implementer] IM-03 に従い `classify_non_corpus_delta_with_reason`(`mod.rs:2713`)の `ReplySloRequired` / `UnsupportedSchema` / `EmptyAppend` 分岐を廃止し、通信メッセージ→communication projection、空 append→no-op へ変更する。受入: 各事由の分類テスト(既存 `tests.rs` の対応ケース更新)が通る。
- [x] 1.3 [Implementer] IM-03 に従い projection の fold 挙動宣言(登録 schema→増分 fold / freshness-only / communication)と、起動時に registry の observation schema 集合との網羅性を検証し不一致で fail fast する機構を実装する。私製ホワイトリスト `FRESHNESS_ONLY_SCHEMAS`(`mod.rs:2716`)を registry 由来の宣言へ収斂させる。fold 時の宣言外 schema 遭遇は警告+スキップとしリビルドしない。受入: 網羅時は起動継続、ドリフト注入で起動失敗、宣言外遭遇でスキップ+警告のテストが通る。

## 2. communication projection のデータモデルと増分 fold

- [x] 2.1 [Implementer] `communication-projection` CP-01 に従い、(チャネル × スレッド)キーの着信 fact state(`incoming_observation_id`/`channel_id`/`sender_id`/`thread_ref`/`published`/`due_at`/`sent_at?`)を resident に保持する reducer を追加し、既存 `ReplySloJoinIndex`(send 側)を入力に結線する。受入: state が canonical Observation + supplemental から決定的に再構築できる単体テストが通る。
- [x] 2.2 [Implementer] CP-03 に従い着信メッセージ 1 通 append の O(1) upsert と、時刻依存判定(Overdue/Pending)の読み取り時評価を実装する。受入: 1 通 append が他 fact を再走査しないこと、append なしの時刻経過で Pending→Overdue が読み取り時に遷移することのテストが通る。
- [x] 2.3 [Implementer] CP-04 に従い reply-SLO 計算責務を communication projection へ移し、メッセージ系 schema の snapshot 反映(FreshnessOnly/SlackMessage)を維持する。受入: メッセージが引き続き freshness へ反映され、reply-SLO は専用 projection から供給されるテストが通る。

## 3. 等価性検証

- [x] 3.1 [Reviewer] CP-02 に従い、同一の canonical Observation + supplemental 集合と複数の評価時刻 T について、communication projection の出力と現行 `ReplySloProjector::project_records` の出力(`rows`/`overdue`/`status`/`latency_seconds`/並び順)が一致する差分テストを追加する。受入: Pending/Overdue/SentOnTime/SentLate 全 status、tie-break、複数 send-record の最小 `sent_at` 採用を網羅し全一致する。

## 4. 背景フルリビルドと読み取り一貫性

- [x] 4.1 [Implementer] IM-04 に従い、移行・復旧・ブートストラップのフルリビルドを single-flight の背景 task 化し、append 成功で取り込み応答を返す。受入: 背景リビルド進行中の append 応答がブロックされず、リビルド task が同時に一つだけ実行されるテストが通る。
- [x] 4.2 [Implementer] IM-04 に従い、進行中の projection 読みが直前公開の snapshot を返し空結果・エラーへ fallback しないこと、完了時に atomic 公開することを実装する(参照: `persistent-search-index` の状態機械)。受入: リビルド中の読みが古い snapshot を返し、完了後に新 snapshot へ切り替わるテストが通る。

## 5. 移行・鮮度契約・計測整合

- [x] 5.1 [Implementer] CP-05 に従い `NON_CORPUS_MATERIALIZATION_VERSION` を更新し、version 不一致時に既存 snapshot を communication projection state 込みで自然再構築する。受入: 旧 version 起動→移行フルリビルド→CP-02 等価性を満たす snapshot が得られるテストが通る。
- [x] 5.2 [Spec Designer] IM-05/D7 に従い projection 読みの鮮度契約(通常時 5 秒以内 / 背景リビルド中 60 秒以内)を API doc と読み経路に明記する。受入: 鮮度契約の記述が確定値と一致する。
- [x] 5.3 [Implementer] IM-06 に従い `import_timing` ログの新分類での期待値(通信メッセージ append で `non_corpus_materialize_mode=incremental` / `full_rebuild_reason=not_applicable`、宣言外 schema 遭遇時のスキップ警告識別、背景リビルドの識別)を実装・検証する。受入: 各ケースのログフィールド値を検証するテストが通る。

## 6. 性能実証と回帰

- [x] 6.1 [Reviewer] IM-07 に従い、既存観測数を段階的に増やした instance へ登録チャネルメッセージ 1 通を繰り返し取り込み、import 応答 latency の p95 < 2 秒を実測する。受入: 各段階で p95 < 2 秒、応答経路に全観測走査を含まないことを確認する。
- [x] 6.2 [Reviewer] workspace 全テスト、cargo fmt、clippy を実行し、reply-SLO 読み契約・検索契約・card-queue 契約に回帰がないことを確認する。受入: 全コマンド成功、既存テスト全緑。
