## ADDED Requirements

### Requirement: Import admission is bounded and fail-fast

v1 と v2 の observation-draft import は設定された `max_concurrent_imports` を超えて同時に処理を開始してはならず、空き permit がない場合は処理を待たずに `import_concurrency_limit` error を返さなければならない。HTTP は 429 とし、error envelope の `details` に設定上限を含めなければならない。

#### Scenario: 同時実行上限を超えた import を reject する

- **WHEN** 設定上限数の import が処理中に新しい v1 または v2 import が到着する
- **THEN** 新しい import は durable append や core materialization を開始せず 429 を返す
- **AND** error code は `import_concurrency_limit` で、上限値が details に含まれる

#### Scenario: permit は import 完了時に解放される

- **WHEN** import が成功、per-item 結果、または validation/storage error で終了する
- **THEN** その import の permit は解放され、後続の import が上限内で開始できる

### Requirement: Draft count is bounded before processing

v1/v2 import は設定された `max_import_drafts` を request の draft 処理開始前に検査しなければならない。v1 の超過時は凍結された request-level bad request envelope を維持する。v2 は設定上限内の items だけを処理し、超過 item は `draft_count_exceeded` と actual/maximum を持つ `rejected` per-item result として返さなければならない。上限超過をv2の全体abortに置き換えてはならず、上限以下の request では既存分類を維持しなければならない。

#### Scenario: 上限超過を fail-fast する

- **WHEN** request の drafts 件数が `max_import_drafts` より大きい
- **THEN** v1 は canonical append、consent update、projection publish を行わず request-level reject する
- **AND** v2 は上限内 item を処理し、超過 item を `rejected`/`draft_count_exceeded` として返す
- **AND** v2 の超過 item details には `actual` と `maximum` が含まれる

#### Scenario: v2 の item 分類を維持する

- **WHEN** drafts 件数が上限以下で一部 item が duplicate または quarantine になる
- **THEN** v2 response は input order/client_ref を維持した per-item result を返す
- **AND** batch 全体を request-level error に変換しない

### Requirement: Materialization publishes at most once per bounded unit

consent snapshot update と append consumer materialization は同一の bounded materialization 境界で処理し、1 request または 1 consumer page/batch について `publish_core_snapshot()` を一回以下にしなければならない。publish counter を test instrumentation として取得できなければならない。

#### Scenario: 通常 import は consent 単独 publish を行わない

- **WHEN** canonical append を含む通常 import が完了する
- **THEN** request は consent snapshot の単独 publish を行わず、append consumer の同一 materialization 境界へ委譲する
- **AND** consumer page ごとの publish は一回以下である

#### Scenario: publish 回数を計測できる

- **WHEN** test が bounded import または consumer を実行する
- **THEN** publish counter から publish 回数を取得できる
- **AND** 1000 件一 request の publish 回数は 2 以下、25 件×40 request 直列の publish 回数は request 数＋固定定数以下である

### Requirement: Search catch-up starts after watermark confirmation

通常 import の search catch-up は append consumer が batch watermark に到達した後に single-flight で起動しなければならない。bulk import session では durable append request の watermark 確定後に request 単位で高々一度起動し、session target watermark が確定した終了境界でも最終 catch-up を一回実行しなければならない。

#### Scenario: bulk session の各 request が catch-up を増殖させない

- **WHEN** active bulk session に複数 request が append する
- **THEN** 各 request の durable watermark 確定後に search catch-up は single-flight で高々一度起動し、observation ごとには起動しない
- **AND** session end の target watermark 確定後に最終 catch-up が実行される

### Requirement: Terminal search jobs are evicted oldest-first

search job record は設定された `max_search_job_records` を上限として保持し、上限超過時は queued/running を除く terminal record を insertion sequence の古い順に削除しなければならない。削除済み job の status 参照は 404/not-found としなければならない。

#### Scenario: 完了 job を古い順に削除する

- **WHEN** completed または failed job が保持上限を超える
- **THEN** 最も古い terminal job から削除される
- **AND** 新しい job と queued/running job は保持される

#### Scenario: eviction 後の status 参照

- **WHEN** client が eviction 済み job id を参照する
- **THEN** status endpoint は not-found error を返す
- **AND** status を成功・失敗のどちらかに推測して返さない

### Requirement: Communication projection does not retain Observation bodies

communication projection state は canonical `Observation` 本体を resident field として保持してはならない。state は communication facts、必要な subject/source-object scalar、privacy reverse index の observation ID、consent/retraction state だけを保持しなければならない。opt-out で fact を除去しても re-consent 用 reverse index は保持し、explicit retraction は target の scalar/reverse index も除去しなければならない。

#### Scenario: serialization/restart 後に Observation 本体がない

- **WHEN** communication projection manifest を serialize して deserialize する
- **THEN** manifest に canonical Observation payload/body の map が存在しない
- **AND** persisted facts と scalar/reverse index のみで state が復元される

#### Scenario: retraction は残留 index を除去する

- **WHEN** communication observation に対する retraction を fold する
- **THEN** fact、subject/source-object scalar、privacy reverse index の target entry が除去される
- **AND** 後続 re-consent で retracted target が復活しない

### Requirement: Re-consent re-materializes by reverse-index page reads

re-consent の遮蔽解除は `observation_privacy_keys` reverse index から対象 observation ID を得て、SQLite の bounded page 読みで canonical 本文を取得し、communication projection を増分再 materialize しなければならない。canonical Observation を projection state に cache してはならない。

#### Scenario: opt-out から re-consent で内容を復元する

- **WHEN** communication observation が opt-out で遮蔽され、その後同じ subject/identifier が unrestricted になる
- **THEN** reverse index から target ID を引き、SQLite から target 本文を読み直す
- **AND** communication/reply-SLO fact が復元される

#### Scenario: 対象外の observation を全件読まない

- **WHEN** re-consent が一つの privacy key に対して発生する
- **THEN** 読み取り対象は reverse index が返す ID の bounded pages に限定される
- **AND** corpus 全件を memory に収集しない

### Requirement: Manifest version guard precedes deserialization

non-corpus/communication manifest version は 11 でなければならない。loader は version を pre-deserialize guard で検査し、10 以下は旧型を `deny_unknown_fields` 型へ deserialize せず canonical/page rebuild に送らなければならない。11 より新しい version や不正な current manifest は明確な error とし、silent fallback/compatibility alias を追加してはならない。

#### Scenario: 実 v10 形状 manifest から再起動する

- **WHEN** 実際の v10 field shape の manifest が persisted projection に存在する
- **THEN** loader は v10 を先に検出して deserialize せず、canonical SQLite page から rebuild する
- **AND** opt-out で遮蔽された内容と re-consent 後に復元された内容が current v11 state で正しく materialize される

#### Scenario: 未知の future version を fail-fast する

- **WHEN** manifest version が 11 より大きい
- **THEN** startup は unsupported/newer format error で失敗する
- **AND** 旧型として silent fallback しない

### Requirement: V13 privacy reverse-index migration is streaming

v13 `observation_privacy_keys` backfill は `append_seq` cursor と固定 page size を使い、一度に page 分を超える Observation JSON を resident にしてはならない。schema semantics、privacy key 集合、append sequence は不変で、migration ledger の記録と index backfill は同一 transaction 境界で commit しなければならない。checkpoint table は追加してはならない。

#### Scenario: 大きめ合成データの migration 結果が等価である

- **WHEN** 複数 page の synthetic observations に v13 migration を実行する
- **THEN** streaming 実装の `observation_privacy_keys` rows は旧実装の全件計算結果と一致する
- **AND** migration ledger と index rows が commit 後に同時に存在する

#### Scenario: migration 中断は再実行安全である

- **WHEN** page 処理中に JSON decode/SQL error が発生する
- **THEN** transaction 全体が rollback され、migration ledger は未記録のままである
- **AND** 次回起動時に同じ migration を最初の cursor から再実行できる

### Requirement: RSS acceptance harness enforces bounded growth

Linux container 向け memory harness は synthetic corpus N 件生成、bulk import、idle/peak RSS または VmHWM 計測を行い、`peak - idle <= constant + O(batch payload)` を判定しなければならない。N は引数化し、CI の小規模実行では publish counter assertion を代替基準として明記しなければならない。

#### Scenario: harness が bounded growth を判定する

- **WHEN** harness を指定 N と batch payload で実行する
- **THEN** idle/peak measurement と publish count を出力する
- **AND** configured bound を超えた場合は non-zero exit になる
