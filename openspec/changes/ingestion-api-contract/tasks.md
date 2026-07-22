## 1. per-item 応答契約

- [ ] 1.1 [Spec Designer] `ingestion-response-contract` IRC-01 の応答スキーマ(per-item 結果 `{ client_ref, outcome, observation_id?, existing_id?, ticket?, error_code?, reason? }`、summary 併存)を wire 契約として確定し、`observation-lake.md` 4.5 の `IngestResult` との対応表を文書化する。受入: 4 outcome すべての応答例が契約に含まれ、storage の `DurableAppendOutcome` 3 値からの写像(design D1)が一意に定まる。
- [ ] 1.2 [Implementer] IRC-01 に従い `ImportReport` を per-item 結果配列へ拡張し、`append_observations` の per-item `DurableAppendOutcome`(`mod.rs:5494-5510`)を捨てずに写像する。`ObservationDraft` に `client_ref`(任意、既定=index)を追加する。受入: 各 draft に対し入力順で observation_id/existing_id を含む結果が返り、grep 回収なしで ID が取れるテストが通る。

## 2. partial success と error 分類

- [ ] 2.1 [Implementer] IRC-02 に従い `prepare_observation_draft_batch`(`mod.rs:5581`)の早期 return を廃止し、prepare 失敗を per-item 結果へ変換して有効 item のみ append、結果を入力順でマージする。受入: 10 件中 1 件 quarantine で残り 9 件が append され全 item 結果が返り、request 全体が 400 で abort しないテストが通る。
- [ ] 2.2 [Implementer] IRC-03 / design D4 に従い error を `transient` / `validation` / `quarantine` へ分類し、`FailureClass` と `IngestResult` からの写像、安定 `error_code` 語彙、未来時刻の item 別 quarantine 化を実装する。route(`server.rs:879`)の `SelfHostError::Ingestion → 400` 一律変換を item 別失敗経路から外す。受入: 未来時刻 item が request 全体 400 でなく item quarantine で返り、validation/transient が機械判別できるテストが通る。

## 3. サーバ側 canonical identity

- [ ] 3.1 [Spec Designer] `ingestion-idempotency` IDEM-01/02/03 の確定事項(グローバル一意化・新バージョン初日厳格化・collision=quarantine+ticket)を反映し、identity 導出の入力契約(`object_id` + canonical tuple)と client が固定すべき入力・retry 契約を文書化する。受入: グローバル一意・時計非依存・collision 追跡が明文化され、client の retry 固定要件が記載される。
- [ ] 3.2 [Implementer] IDEM-01 に従い取り込み経路で `identity_key(source, object_id, canonical_json)`(`crates/adapters/api/src/idempotency.rs:43`)をサーバ権威で導出/再検証し、`namespace_draft`(`service_support.rs:714`)の client key 素通しを置換する。受入: 同一実体の再送が同一 observation_id の duplicate へ収束し、不一致は `identity_mismatch` で分類されるテストが通る。
- [ ] 3.3 [Implementer] IDEM-02 に従い `CanonicalCollision`(`persistence/mod.rs:2199`)を duplicate と別 outcome/error_code で返し、既存 Observation を上書きしないことを保証する。受入: 同一 identity・異なる canonical 内容が collision として返り上書きされないテストが通る。
- [ ] 3.4 [Implementer] IDEM-03(グローバル一意)に従い、冪等一意性を可変時刻由来のルーティング(leaf / partition 配置)から切り離してグローバル一意へ格上げし、event_time を変えた retry が別 leaf をすり抜けて重複 append されないようにする(storage の UNIQUE スコープ / routing スキーマ移行を含む)。受入: published を変えた retry 3 連続が単一 duplicate へ収束するテストが通る。

## 4. ACK セマンティクスと暗黙契約の開示

- [ ] 4.1 [Spec Designer] IRC-04 に従い応答コード↔台帳状態の対応表を契約化し、派生処理分離が communication-projection / 性能フェーズ依存であるスコープ境界を明記する。受入: `ingested`/`duplicate` が commit 済み・`rejected` が未存在・派生失敗が outcome を反転しない、が表として確定する。
- [ ] 4.2 [Implementer] IRC-04 に従い、append 成功済み item の outcome を後続の materialize/index/audit 失敗で反転させない応答構築を実装する(派生失敗は projection health へ surface)。受入: append 後に materialize が失敗しても item が `ingested` のまま返るテストが通る。
- [ ] 4.3 [Implementer] IRC-05 / design D6 に従い、body 上限(`server.rs:37`)・payload 上限・page limit 上限・clock skew(`values.rs:103`)・identity 構成要素を、超過時に実値と閾値を含む `error_code` 付きエラーで返し、閾値を単一設定源から参照する。受入: 各上限超過が閾値付きエラーで返り、文書に identity 構成と retry 固定要件が記載されるテストが通る。

## 5. 後方互換と検証

- [ ] 5.1 [Spec Designer] IRC-06(確定: API バージョニング)に従い、バージョンの表現形式(パス / header)を選定し、旧バージョンの意味論凍結・新バージョンの初日厳格化・旧バージョンの非推奨化 → 廃止プロセス、および任意 client(Intercom / Nanihold に限らない)の移行パスを文書化する。受入: 旧バージョン凍結と新バージョン厳格化の境界、廃止プロセスが確定する。
- [ ] 5.2 [Implementer] IRC-06 に従い応答フィールド追加を非破壊で導入し、挙動変更(partial success / HTTP 200 / サーバ権威 identity 厳格検証)を新 API バージョンでのみ有効化する。opt-in header では切り替えない。受入: 旧バージョンの既存 client が壊れず、新バージョンで厳格契約が初日から適用されるテストが通る。
- [ ] 5.3 [Reviewer] workspace 全テスト・cargo fmt・clippy を実行し、observation-lake の Append-Only / IngestResult 契約と communication-projection の取り込み経路に回帰がないことを確認する。受入: 全コマンド成功、既存テスト全緑、両 change のマージ順序の衝突がない。
