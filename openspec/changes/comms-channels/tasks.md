# Tasks: comms-channels

**並列実装の原則:** change ① と同じ。全トラック同時着手可能。各タスク 2 時間以内、implementer=Codex。

## 依存グラフ

```
Track A(チャネルレジストリ)
  A1 → A2
   │
   ├────────────┬────────────┐
   ▼            ▼            ▼
Track B(Slack) Track C(Gmail) Track D(Discord)
  B1 → B2       C1 → C2       D1 → D2
   └────────────┴────────────┘
                ▼
        Track E(統合)
          E1 → E2
```

- B/C/D は A のレコード契約(spec 確定済み)に依存 — A 実装中は fixture レジストリで並行可

## Track A. チャネルレジストリ

- [x] A1 チャネルレコード型・ops 読み込み・未登録 quarantine を実装する
  - Spec: CHRG-01 / 受け入れ: quarantine test、設定反映 test
- [x] A2 consent_scope 付与(IngestionGate 結線)+break-glass 露出+example ops 設定を実装する
  - Spec: CHRG-02, CHRG-03 / 受け入れ: scope 付与 test、FilteringGate 回帰 test

## Track B. Slack

- [x] B1 個人 lake 向け Slack 着信構成(DM / メンション / チャネル)を実装する
  - Spec: CHAD-01 / 受け入れ: 3種 fixture test
- [x] B2 実ワークスペースからの初回取り込み+idempotency 確認
  - Spec: CHAD-01, CHRG-04 / 受け入れ: E2E evidence

## Track C. Gmail

- [x] C1 Gmail 観測写像(identity key、published=Date、スレッド構造保持)を実装する
  - Spec: CHAD-02 / 受け入れ: idempotency test
- [x] C2 get_thread のメールスレッド復元+実データ初回取り込み
  - Spec: CHAD-02 / 受け入れ: スレッド復元 test、E2E evidence

## Track D. Discord

- [x] D1 Discord 観測写像を実装する
  - Spec: CHAD-03 / 受け入れ: idempotency test
- [x] D2 実サーバ/DM からの初回取り込み
  - Spec: CHAD-03, CHAD-04 / 受け入れ: E2E evidence
  - 2026-07-07 本番接続設定: Discord `kana's server` の `#general` (`1507676023314059275`) を対象に、既存 bot `tera` を runtime supervisor connection `discord-primary-tera` として再利用する方針で `deploy/personal-lake/config.toml` / `config.host.toml` に `chan:discord-primary:1507676023314059275` を有効化。LETHE は Discord bot token を持たず、runtime supervisor からの HTTP import を受ける境界を維持する。

## Track E. 統合

- [x] E1 3チャネルを鮮度 projection に編入し、無音検知を確認する
  - Spec: CHAD-05 / 受け入れ: 欠測判定 test
  - 2026-07-07 本番: Discord channel freshness threshold `1800s` を設定し、restart 後の freshness projection で `chan:discord-primary:1507676023314059275=unobserved` を確認。runtime supervisor がまだ observation draft を投入していない状態として期待どおり。
- [x] E2 LETHE に送信系依存が存在しないことの構成 test+SLO 素材の滞留 fold test+SHALL 被覆表
  - Spec: CHAD-04, CHRG-04 / 受け入れ: 被覆表 evidence
  - 2026-07-07 最終検証: `cargo test --workspace` と `openspec validate comms-channels --strict` が成功。
