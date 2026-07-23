## 1. Owner decision gate

- [x] 1.1 [Spec Designer][CUT-03/CUT-05] Owner が design 冒頭 Q1-Q3（`source_instance_id` unit、unit/version-bound admission、first v2 `ingested` 後は forward-fix）を確定し、決定が推奨案と異なる場合は proposal/design/spec を実装前に更新する。受入: Q が全て「確定」となり、未決のまま 2.x 以降へ進まない。
- [x] 1.2 [Reviewer][CUT-03] 対象 client/source の `source_instance_id` ownership と credential 共有状態を inventory できる read-only verifier を定義する。受入: client 名の hard-code なしに、共有 producer/credential と切替時 rename を全て blocker として列挙できる。

## 2. Incremental bridge projection

- [x] 2.1 [Implementer][CUT-01] SQLite に bridge candidate、derivation gap、単調 watermark の派生 schema と必要な index を追加する。受入: canonical `observations` を更新・削除せず、同一 batch retry が duplicate row を作らない schema test が通る。
- [x] 2.2 [Implementer][CUT-01] `append_seq` watermark より後ろを bounded batch で読む pure identity derivation と transactional apply を実装する。受入: candidate/gap と watermark が同一 transaction で commit され、注入 failure 時に watermark が進まない test が通る。
- [x] 2.3 [Implementer][CUT-01] bootstrap/steady-state runner を停止位置から再開可能にし、startup/cutover ごとの全 scan と全量 rebuild 経路を設けない。受入: 中断再開 test と、既処理 N 件＋新規 M 件に対して M 件だけ読む instrumentation test が通る。
- [x] 2.4 [Implementer][CUT-02] v2 identity ごとの最小 `append_seq` winner、candidate multiplicity、canonical exact-compare error を返す read-only resolver を実装する。受入: 複数 candidate の入力順に依存せず同じ winner/anomaly を返す replay test が通る。

## 3. Durable cutover control plane

- [x] 3.1 [Implementer][CUT-03/CUT-06] authority・reason・generation を持つ append-only cutover transition log と deterministic state fold を実装する。受入: valid/invalid transition、replay、未 commit event 無視の test が通る。
- [x] 3.2 [Implementer][CUT-03] `source_instance_id` と API version に束縛した credential/admission generation 検証を handler 前に実装する。受入: unit A v2/unit B v1 の同時受理と、A の stale v1 credential が既存 authorization failure になる e2e test が通る。
- [x] 3.3 [Implementer][CUT-03] `draining` transition、new v1 admission close、in-flight v1 完了待ち、`fence_append_seq` 記録を一つの admission barrier として実装する。受入: race 注入下でも fence 後に対象 unit の v1 append が存在しない concurrency test が通る。
- [x] 3.4 [Implementer][CUT-04/CUT-06] watermark coverage、unit gap、exact-compare error、fixture、dry-run を評価する read-only readiness report と fail-closed activation command を実装する。受入: 各 blocker が append sequence/reason 付きで返り、一項目でも失敗なら v2 generation を発行しない。

## 4. v2-only bridge resolution and rollback boundary

- [x] 4.1 [Implementer][CUT-02] v2 append boundary にだけ bridge resolver を接続し、exact match を `duplicate.existing_id`、mismatch を `canonical_collision` へ写像する。受入: v1 append 後の最初の v2 retry が ledger delta 0 で同じ既存 ID を返す e2e test が通る。
- [x] 4.2 [Implementer][CUT-02] bridge miss の v2 item は現行 global registry/append へ進み、並行 v2 request は単一 append に収束させる。受入: bridge lag 中の並行 v2 retry が一件の `ingested` と残り `duplicate` になる test が通る。
- [x] 4.3 [Implementer][CUT-05] unit ごとの first v2 `ingested` を durable transition として記録し、`ingested=0` の pre-commit rollback と `v2_committed` の rollback refusal を実装する。受入: duplicate/rejected/quarantined だけなら v1 復帰でき、一件 append 後は明示 error で拒否する test が通る。
- [x] 4.4 [Reviewer][CUT-02] v1 handler、prepare path、response serialization、HTTP/error mapping、identity key を golden contract で固定する。受入: bridge state/candidate の有無によらず既存 v1 fixtures の wire response と ledger outcome が同一である。

## 5. Verification, scale, and operations

- [x] 5.1 [Reviewer][CUT-01/CUT-02] historical v1 rows、schema-v8 registry rows、v2 rows、missing metadata、legacy duplicate、hash mismatch を含む replay fixture を追加する。受入: bridge projection の再実行で同じ candidates/gaps/winner/watermark が得られる。
- [x] 5.2 [Reviewer][CUT-03/CUT-04] unit A/B の異なる phase、stale producer、in-flight fence、projection lag/gap、dry-run ledger delta を網羅する contract/e2e suite を追加する。受入: same-unit dual admission が不可能で、別 unit の独立移行が継続する。
- [x] 5.3 [Reviewer][CUT-01/CUT-06] 大規模 fixture で bounded bootstrap の resume と steady-state `O(new observations)` を測定する。受入: batch memory が history 総量に比例せず、処理済み prefix を steady-state で再読しないことを query/instrumentation count で確認する。
- [x] 5.4 [Implementer][CUT-06] phase、generation、fence、first v2 append、watermark/lag、candidate/gap/multiplicity/collision、bridge hit、stale v1 rejection を machine-readable health/metrics へ追加する。受入: readiness と rollback 可否を DB 全 scan なしに取得できる。

## 6. Documentation and release gate

- [x] 6.1 [Spec Designer][CUT-03/CUT-04/CUT-05] `docs/development/personal-lake-ingestion.md` に per-unit drain/fence/catch-up/activate、identity 固定値、pre/post-ingested rollback、fail-closed blocker、任意 client 用 runbook を追記する。受入: nanihold_intercom / Nanihold OS を例示しても手順自体は client 名の列挙に依存しない。
- [x] 6.2 [Spec Designer][CUT-02] client contract に v1/v2 identity の具体例と v1 sample→v2 duplicate canary を記載する。受入: operator が期待 key、既存 ID、ledger delta 0 を事前に照合できる。
- [x] 6.3 [Reviewer][CUT-01..CUT-06] `cargo fmt --check`、関連 crate test、selfhost contract/e2e、workspace test、`openspec validate v1-v2-cutover-bridge --strict` を実行する。受入: 全 command 成功、v1 golden contract 無変更、canonical Observation の mutation がない。
- [x] 6.4 [Reviewer][CUT-03/CUT-05] client 切替前 release gate を実施する。受入: owner Q 確定、bridge fence catch-up、zero gap/error、credential 専有、rollback phase、監視 alert が全て evidence 付きで pass し、本番切替は別途明示承認まで行わない。
