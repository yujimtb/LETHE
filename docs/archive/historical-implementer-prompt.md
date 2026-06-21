
**Prompt B**
あなたは DOKP の Implementer です。Rust で Lane B を担当してください。

目的:
Source Adapter 系を並行実装します。対象は M09 Adapter Policy, M10 Slack Adapter, M11 Google Slides Adapter です。

最初に必ず読むもの:
- _index.md
- adapter-policy.md
- slack-adapter.md
- google-slides-adapter.md
- domain-kernel.md
- registry.md
- observation-lake.md
- implementer.md
- R2-02_slack_schema_adapter.md
- R2-06_google_slides_adapter.md

依存条件:
- G2 Lake contract が満たされている前提で進める
- Lane A の public interface を利用し、独自の Lake 実装や Registry 実装は作らない
- Governance の細部が未確定でも、capture path で必要な surface だけに留める

実装スコープ:
- M09: SourceAdapter 共通 trait / contract / retry / heartbeat / idempotency rules
- M10: Slack crawler, Slack event to Observation mapper, schema binding, thread/edit/delete/file handling
- M11: Google Slides crawler, revision snapshot capture, native + rendered hybrid Observation, blob attachment mapping

必須制約:
- Rust で実装する
- Adapter は pure mapping と IO を分離する
- idempotency key を deterministic に生成する
- schemaVersion と adapter version を Observation metadata に残す
- OCR や embedding は adapter に入れない
- credentials は実装しない。config interface だけ定義する
- 実 API 呼び出しは trait 化し、fixture でテスト可能にする

期待する成果物:
1. Adapter 共通 trait / config / error type
2. Slack adapter 実装
3. Google Slides adapter 実装
4. fixture ベース unit tests
5. adapter → lake ingest の integration tests
6. rate limit / retry / duplicate / heartbeat のテスト
7. open question メモ

作業順:
1. Adapter 共通契約を実装
2. Slack mapper と Slack client abstraction を実装
3. Google Slides mapper と Google client abstraction を実装
4. heartbeat Observation を出せるようにする
5. integration test で Lake 連携を確認
6. Lane A の domain / registry / lake API に不要な変更を入れずに完了させる

完了条件:
- Slack と Google Slides の両 adapter が Observation を生成できる
- duplicate 再送で重複しない
- revision snapshot / event capture の違いが型で表現されている
- tests で replay と idempotency を確認できる

**Prompt C**
あなたは DOKP の Implementer です。Rust で Lane C を担当してください。

目的:
Projection と API を構築します。対象は M05 Projection Engine, M06 DAG Propagation, M12 Identity Resolution, M13 Person Page, M14 API Serving です。

最初に必ず読むもの:
- _index.md
- projection-engine.md
- dag-propagation.md
- identity-resolution.md
- person-page.md
- api-serving.md
- domain-kernel.md
- supplemental-store.md
- implementer.md
- R2-01_incremental_propagation_watermark.md
- R2-03_identity_resolution_projection.md
- R2-04_person_page_api.md

依存条件:
- G4 Projection contract が前提
- Lane A の Lake / Supplemental / Registry を利用する
- Lane B の adapter 実装が未完でも、fixture input で projector を作れるようにする

実装スコープ:
- M05: projection spec model, source declaration validator, lineage manifest, deterministic build runner
- M06: watermark state, incremental propagation scheduler, topological order execution
- M12: identity resolution projector, confidence scoring, candidates table, accept/reject surface
- M13: person page projector, self-introduction extraction result の統合, person API payload builder
- M14: read mode resolver, response envelope, filtering middleware, pagination, health endpoint

必須制約:
- Rust で実装する
- projector の core は pure function に近づける
- academic-pinned は deterministic であること
- identity resolution は canonical truth にしない
- filtering-before-exposure を API 手前で必ず通す
- stale fallback と projection metadata を response に含める
- projection materialization を正史として扱わない

期待する成果物:
1. Projection Engine
2. DAG / watermark propagation
3. Identity Resolution projector
4. Person Page projector と API
5. API serving middleware / router / envelope
6. replay tests, integration tests, API contract tests
7. residual risk メモ

作業順:
1. ProjectionSpec と build / lineage 周りを型で固める
2. incremental propagation の state machine を実装
3. identity resolution projector を fixture ベースで実装
4. person page projector を identity output に接続
5. API serving を read mode と filtering 前提で実装
6. same input same output の replay test を追加

完了条件:
- M05, M06, M12, M13, M14 の責務が分離されている
- person-page API が spec どおりに返る
- identity candidate の accept / reject が surface として存在する
- replay test と API contract test が通る

**Prompt D**
あなたは DOKP の Implementer です。Rust で Lane D を担当してください。

目的:
cross-cutting な Governance と Runtime を整えます。対象は M08 Governance, M15 Runtime です。

最初に必ず読むもの:
- _index.md
- governance.md
- runtime.md
- domain-kernel.md
- observation-lake.md
- api-serving.md
- implementer.md
- governance_capability_model.md
- runtime_reference_architecture.md

依存条件:
- D-0 として policy freeze を先に行う
- confidence threshold, DualReference precedence, approval SLA が未確定なら、実装を広げず open question に留める
- 他レーンの public interface を壊さない

実装スコープ:
- M08: minimal policy engine, capability check, consent check, restricted field metadata, review requirement surface, audit event emission hooks
- M15: local runtime topology, config loading, build sandbox boundary, health / heartbeat monitoring, CI / container / local run support

必須制約:
- Rust で実装する
- governance 判定は pure decision service として切り出す
- filtering-before-exposure の最終 gate を明示する
- runtime は semantics subordinate を守る
- build sandbox は network default deny を前提に interface を設計する
- product choice より law compliance を優先する
- MVP は minimal governance と local runtime に留める

期待する成果物:
1. policy decision engine
2. capability / consent / review interfaces
3. audit hook interface
4. runtime config と health model
5. heartbeat / gap detection support
6. local execution 用の最小ランナー
7. law / policy の未確定点メモ

作業順:
1. policy freeze が必要な項目を最初に列挙
2. minimal governance engine を pure function ベースで実装
3. Lake / API / Projection 側に組み込むための interface を定義
4. runtime config, health, heartbeat, gap detection を実装
5. local sandbox / build runner の最小形を用意
6. integration point を薄く保ったまま完了させる

完了条件:
- Allow / Deny / RequireReview が型で扱える
- restricted data を API 前に止める統合点がある
- heartbeat と gap detection の流れがテスト可能
- local runtime と CI の最小セットアップが揃う

**運用メモ**
- Lane A を基盤固定役にして、Lane B/C/D は public interface への依存だけに制限すると衝突が減ります。
- 実際の投げ方は、Lane B と Lane D を先に走らせ、Lane C は M05 の骨格ができた時点で着手させるのが安定します。
- 今のリポジトリ状態に合わせるなら、Lane A は再実装ではなく hardening と gap-fill に寄せるのが正しいです。

必要なら次に、各プロンプトをさらに短くした 実行用ショート版 か、GitHub Copilot / Claude Code / Cursor 向けに最適化した エージェント別版 を作ります。
