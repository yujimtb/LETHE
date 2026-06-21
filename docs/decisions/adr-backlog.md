# LETHE ADR Backlog

## Purpose

この文書は、`design_questions.md` を踏まえて、今後 ADR として確定すべき論点を整理した backlog です。  
`plan.md` の本文に確定仕様だけを残し、未確定事項はここで追跡する前提にします。

---

## Status Legend

| Status | Meaning |
|---|---|
| **Active** | 方針は見えているが、まだ仕様化が必要 |
| **Needs Example** | 決める前に具体例や試作が必要 |
| **Agreed Direction** | 方向性はかなり固まっており、仕様へ昇格できる |
| **Deferred** | 重要だが、kernel 固定後に詰める |

---

## ADR-001 Multimodal Canonicalization Boundary

- **Status:** Ready to Merge
- **Why it matters:** Lake と supplemental の意味境界を決める中核論点。
- **Current direction:** Google Workspace（Docs / Sheets / Slides / Drive / Forms / Photos / Calendar）、Notion、Figma、Canva、Slack などの revisioned SaaS source を列挙した上で、Observation は共通 envelope + `payload.artifact` / `revision` / `native` / `relations` / `rights` / `attachment_roles` で canonical snapshot を表す。
- **User signal:** data source を具体的に列挙し、Observation 構造まで含めて仕様化したい。transcript や OCR は引き続き Projection / supplemental 側に置く。
- **Next decision:** adapter ごとの差分を source contract と schema extension にどう閉じ込めるか、例を増やして補強する。

## ADR-002 High-Frequency Data Capture Policy

- **Status:** Active
- **Why it matters:** Lake の役割を「全部を細粒度で保存する場所」にするか、「再構成可能性を担保する backing layer」にするかが変わる。
- **Current direction:** 頻度で単純分岐するより、Lake は再構成基盤に徹し、Projection では source-native や live endpoint を透過的に読める方が自然。
- **User signal:** センサーや mutable multimodal source では、Projection 実行時に Lake からではなく source-native を読む方がきれいな場合がある。
- **Next decision:** source-native latest read と Lake snapshot read をどう contract に載せるかを具体例で固める。

## ADR-003 Agent Playground and Native Support

- **Status:** Active
- **Why it matters:** 非技術ユーザー向け authoring 体験と安全な自動化の境界を決める。
- **Current direction:** agent は Projection 作成に限定し、Lake に直接触れない playground / sandbox を主戦場にする。
- **User signal:** user-friendly GUI と coding agent の統合が重要。
- **Next decision:** capability tier、review route、publish flow を GUI 前提で具体化する。

## ADR-004 Identity Resolution Placement

- **Status:** Ready to Merge
- **Why it matters:** 名寄せを Lake に入れると canonical truth が解釈依存になりやすい。
- **Current direction:** identity resolution は Projection 寄りとし、Medium / Low は candidate queue に留め、承認済み結果のみを resolved identity view に昇格する。
- **User signal:** 名寄せは基本 Projection の領域。補助情報領域に限定的に置くのは可。
- **Next decision:** OpenSpec M12 / M13 / M08 に反映済み。merge 後は candidate 承認 API の実装へ進む。

## ADR-005 Trust / Confidence Placement

- **Status:** Ready to Merge
- **Why it matters:** trust/confidence を canonical payload に入れると source truth と解釈が混ざる。
- **Current direction:** confidence や verification は Projection または supplemental に置き、Medium confidence は review を経ない限り published/shared Projection に入れない。
- **User signal:** Lake には入れず、必要なら補助情報領域に載せるのがよい。
- **Next decision:** governance_capability_model.md §3.5 と identity spec に反映済み。merge 後は acceptance test 実装へ進む。

## ADR-006 Consent Granularity and Filtering Projections

- **Status:** Ready to Merge
- **Why it matters:** マルチモーダル data では capture と公開を分けて扱う必要がある。
- **Current direction:** filtering projection を中心に据えつつ、年度中は restricted canonical capture を蓄積し、年度末の opt-out 確認後に名寄せ精度の高い filtering basis を固定して experiment 適用を承認する。
- **User signal:** upfront opt-in よりも、データ蓄積後の年度末 opt-out 確認の方が名寄せと filtering の精度を高めやすく、運用上も扱いやすい。
- **Next decision:** 年度末確認 batch と approval trace の record 形を governance spec で具体化する。

## ADR-007 Retraction and Physical Delete

- **Status:** Ready to Merge
- **Why it matters:** append-only 原則の例外を安全に扱う必要がある。
- **Current direction:** 通常撤回、アクセス抑止、physical delete を分離し、physical delete は blob のみを対象にする。
- **User signal:** この方向で概ねよい。削除で DB が壊れない設計が必要。
- **Next decision:** tombstone metadata と DOI 影響時の表示文言を決める。

## ADR-008 Projection Sandbox and Build Governance

- **Status:** Ready to Merge
- **Why it matters:** build isolation がないと deterministic build と安全な agent 利用が両立しにくい。
- **Current direction:** isolated sandbox / container で build し、terminal 非前提の GUI を整える。
- **User signal:** container から Projection の作成・公開までを user-friendly にしたい。
- **Next decision:** MVP で container 必須か、軽量 sandbox から始めるかを決める。

## ADR-009 Lineage Granularity

- **Status:** Needs Example
- **Why it matters:** row-level lineage の常時 materialize は高コストになりやすい。
- **Current direction:** coarse lineage を標準、重要 Projection は row-level を追加。
- **User signal:** まだ具体例が不足している。
- **Next decision:** 画像、動画、複合 Projection の 3 例で lineage の粒度を比較する。

## ADR-010 Schema Openness and Source Contract Evolution

- **Status:** Ready to Merge
- **Why it matters:** open registry を保ちながら source 進化への追従方法を定義する必要がある。
- **Current direction:** adapter version と schema version を binding table で結び、schema major bump には adapter major bump または新 observer contract を要求する。
- **User signal:** Lake へ直接入れるより、source capability の拡張が新 schema 登録の契機になる。
- **Next decision:** registry.md / adapter-policy.md に binding rule を反映済み。adapter 別の concrete example を実装 task で補う。

## ADR-011 Time Model

- **Status:** Ready to Merge
- **Why it matters:** correction や状態系 Projection の意味を決める基盤になる。
- **Current direction:** `published` と `recordedAt` を基本に、valid time は schema 任意フィールドとして持てるようにする。
- **User signal:** この方向で問題なさそう。
- **Next decision:** interval canonical の具体例を 1 つ `plan.md` へ追加する。

## ADR-012 Query / API / Serving Contract

- **Status:** Needs Example
- **Why it matters:** Projection API を透過的かつ安定にするには、latest / pinned / stale fallback の具体例が必要。
- **Current direction:** Projection 同士も API を持ち、利用面では最新版優先。ただし pinned access も契約化する。
- **User signal:** 古いデータでも取得不能よりは良い。具体例が欲しい。
- **Next decision:** operational-latest, academic-pinned, stale cache fallback の 3 例を用意する。

## ADR-013 Security and Secret Handling

- **Status:** Active
- **Why it matters:** filtering timing、secret visibility、export traceability を決める必要がある。
- **Current direction:** 生データ処理前には潰さず、表示前に filtering projection を挟む。secret は capability-based に隠蔽する。
- **User signal:** pre-filtering より pre-display filtering の方が良い。
- **Next decision:** restricted read と export の audit event schema を固める。

## ADR-014 Prioritization After Restructure

- **Status:** Active
- **Why it matters:** architecture kernel を固めた後、どこから実装へ着手するかを決める必要がある。
- **Current direction:** もともとの優先順位は agent support、multimodal boundary、高頻度 data だったが、いまは semantic kernel の固定も最優先に近い。
- **User signal:** 仕様再構成をもう一度行ってから検討したい。
- **Next decision:** `plan.md` 再編後に implementation epics を改めて並べ替える。

## ADR-015 Documentation Restructure and Spec Split

- **Status:** Ready to Merge
- **Why it matters:** 概念仕様、runtime、governance、未確定事項が一枚に混ざると読みづらい。
- **Current direction:** `plan.md` を親仕様にし、domain algebra / runtime / governance / ADR を別紙化する。
- **User signal:** 内容ごとにファイルを分けても問題ない。
- **Next decision:** 今回の doc set を基準に、今後の追記先ルールを固定する。

---

## Immediate Next Moves

優先度順に次の順で固めるのが自然です。

1. ADR-002 High-frequency data capture policy
2. ADR-003 Agent playground and capability model
3. ADR-012 Query / API / serving examples
4. ADR-018 DAG change propagation（incremental propagation の具体実装）
5. ADR-019 MVP scenario（Google Slides + Slack + 名寄せ + 個人ページ）
6. ADR-009 Lineage examples

---

## ADR-016 Concurrency and Conflict Resolution Protocol

- **Status:** Ready to Merge
- **Why it matters:** Writable Projection での同時書き込みと source-native write-back の競合解決を定義する必要がある。
- **Current direction:** 楽観的ロック（OCC）を標準とし、`visibleRowHash` / `baseRevision` で競合検知する。DualReference は stable anchor / lossless inverse / destructive effect の precedence matrix で route を確定する。
- **User signal:** 提案の方向性で合意。仕様化済み。
- **Next decision:** domain_algebra.md §6.4 と write-back.md に反映済み。adapter 別 implementation example を追加したら archive 候補。

## ADR-017 Observer Health and Gap Detection

- **Status:** Agreed Direction
- **Why it matters:** Observer のサイレント停止による暗黙の data gap を検知する手段が必要。
- **Current direction:** Heartbeat Observation + Gap Alert + Projection 側の gapPolicy 宣言の 3 層構成。
- **User signal:** 提案の方向性で合意。仕様化済み。
- **Next decision:** runtime_reference_architecture.md §7.2 に反映済み。alert ルーティングの具体設定を追加する。

## ADR-018 DAG Change Propagation Mechanism

- **Status:** Ready to Merge
- **Why it matters:** Projection DAG の伝播戦略が未定義だと、データ増加時のスケーラビリティと freshness が保証できない。
- **Current direction:** incremental propagation（差分伝播）を第一優先とし、watermark に supplemental version pin を保持する。approved write は approval SLA に従って freshness を監視する。
- **User signal:** 全データ rebuild ではなく更新 record のみの propagation を優先したい。全データ集計型 Projection は設計段階で rebuild コストを考慮する必要がある。
- **Next decision:** dag-propagation.md に watermark / SLA ルールを反映済み。runtime metric wiring を実装 task で固める。

## ADR-019 MVP End-to-End Scenario

- **Status:** Active
- **Why it matters:** 実装に着手するための具体的な end-to-end シナリオが必要。
- **Current direction:** Google Slides + Slack のデータ取り込み → 名寄せ → 個人ページ Projection を MVP シナリオとする。
- **User signal:** 当初の食堂シナリオから変更。Slides + Slack + 名寄せ + 個人ページが望ましい。
- **Next decision:** plan.md §11.4 にシナリオ反映済み。各ステップの具体的な schema / adapter / projector 実装に着手する。

## ADR-020 Supplemental Mutability Policy

- **Status:** Ready to Merge
- **Why it matters:** Supplemental record の AppendOnly / ManagedCache の判定基準を明確にし、academic-pinned の determinism を保証する。
- **Current direction:** 既定は AppendOnly。判定表を domain_algebra.md §4.4 に定義済み。academic-pinned が参照する場合は version-pinned read を必須とし、recordVersion を契約に含める。
- **User signal:** 提案の方向性で合意。
- **Next decision:** supplemental-store.md に recordVersion / consentMetadata 契約を反映済み。merge 後は API 実装へ進む。

## ADR-021 Source-Native Read Contract

- **Status:** Ready to Merge
- **Why it matters:** source-native を直接読む Projection の lineage 表現とフォールバック動作を明確にする。
- **Current direction:** AcademicPinned では禁止、OperationalLatest では許可、ApplicationCached では cache 優先。fallback ladder に加え、Lake 併用時は reconciliation policy を必須にする。
- **User signal:** 提案の方向性で合意。
- **Next decision:** domain_algebra.md §5.5 と projection-engine.md に反映済み。adapter 例を追加したら archive 候補。

## ADR-022 Consent Cascade to Supplemental

- **Status:** Ready to Merge
- **Why it matters:** consent 撤回時に supplemental derivation をどう扱うかの方針が必要。
- **Current direction:** supplemental record は削除せず保持し、`ConsentMetadata` を更新して filtering projection で exposure から除外する。削除すると opt-out 判断の根拠が失われるため。
- **User signal:** filtering projection での除外がクリーン。
- **Next decision:** governance_capability_model.md §7.4 と supplemental-store.md に反映済み。merge 後は filtering query 実装へ進む。

## ADR-023 Lake Physical Sharding

- **Status:** Locked（[Sharding design](../architecture/sharding.md) に D0–D12 を確定済み）
- **Why it matters:** 単一インスタンスの容量・負荷限界を超えて Lake を物理分割するにあたり、dedup・routing・replay・冪等性を壊さない機構を決める中核論点。
- **Current direction:** identity/routing keyspec の initialize-time pin、per-leaf exact index（SQLite UNIQUE）による完全冪等、Patricia trie + lazy split、AP + failover spool/reconcile、per-(projection×leaf) watermark propagation、logical→physical resolver、idempotent re-ingest を唯一の migration primitive とする blue/green keyspec 変更。詳細と根拠・コード接地点は [Sharding design](../architecture/sharding.md)。
- **Supersedes（draft 上書き）:** SimHash routing 不採用、確率的冪等性（Bloom ε）撤回→exact、固定ビット trie→Patricia+split bit、idempotencyKey 粒度を per-message に修正。
- **Next decision:** §3 の実装フェーズ順に従って着手。

---

## Relationship to Other Documents

- 親仕様: [System overview](../architecture/system-overview.md)
- 意味論と law: [Domain algebra](../architecture/domain-algebra.md)
- runtime 参照実装: [Runtime reference](../architecture/runtime-reference.md)
- governance / capability: [Governance capability model](../architecture/governance-capability-model.md)
