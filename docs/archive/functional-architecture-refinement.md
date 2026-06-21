# plan.md 洗練メモ: 関数型アーキテクチャ観点で追加したいこと

## この文書の目的

現行の `plan.md` は、DOKP の思想・要件・レイヤ構成をかなり豊かに表現できています。  
一方で、概念仕様、運用ポリシー、実装候補、未確定論点が同じ層で並んでいるため、今後実装や設計判断に落とすときの「核」がやや見えにくくなっています。

このメモは、`plan.md` をさらに洗練するために、**関数型プログラミングのパラダイム**を参照しながら追加・再編するとよい観点を整理したものです。

---

## 1. 現状の `plan.md` の強み

まず、現行案の強みは明確です。

- Observation / Lake / Projection / Registry の主要概念がすでに定義されている
- mutable source と academic reproducibility の緊張関係を正面から扱っている
- Projection を DAG として扱う発想が明確
- section 13 で、Projection を関数型的に解釈する方向性がすでに示されている
- governance / ethics / lineage / write-back が別論点として見えている

つまり、**発想そのものはかなり良い**です。  
洗練の中心課題は、「よい発想を、実装可能で検証可能な設計核へ落とし込むこと」にあります。

---

## 2. いま足すと効く改善軸

### 2.1 Projection の関数型解釈を「部分」ではなく「全体原理」に引き上げる

現行の `plan.md` では、関数型的な説明は主に section 13 の Projection 周辺に置かれています。  
しかし本当に洗練するなら、Projection だけでなく、システム全体を次の構図で再整理した方がよいです。

> **Functional Core / Imperative Shell**  
> 純粋なドメイン変換を中心に置き、副作用は境界に閉じ込める。

この観点で見ると、DOKP は次のように分解できます。

| 層 | 役割 | 関数型的な見方 |
|---|---|---|
| Domain Kernel | Observation, Entity, Schema, Projection, Command の意味論 | 純粋関数と代数的データ型 |
| Policy Layer | consent, access, write-back, review, retention | 純粋な判定関数 |
| Effect Ports | source fetch, blob save, DB materialize, API call | effect interface / port |
| Adapters | Google, Slack, Figma, sensor, storage, API server | effect interpreter |
| Runtime / Scheduler | crawl, replay, build, cache update | imperative orchestration |

この分解を `plan.md` に明示すると、以後の議論で「これは純粋関数で定義すべきか」「これは adapter 側の責務か」が判断しやすくなります。

### 2.2 ドメイン概念を「文章」だけでなく「代数」として定義する

現状は概念説明が豊富ですが、**どの型が閉じた集合で、どの型が拡張点なのか**がまだ弱いです。  
関数型パラダイムを参考にするなら、最低限次の代数を明示したいです。

- `AuthorityModel = LakeAuthoritative | SourceAuthoritative | DualReference`
- `CaptureModel = Event | Snapshot | ChunkManifest | Restricted`
- `ReadMode = AcademicPinned | OperationalLatest | ApplicationCached`
- `WriteMode = Canonical | Annotation | Proposal`
- `ObservationKind = CanonicalObservation | SupplementalRecord | GovernanceRecord`
- `ProjectionKind = PureProjection | CachedProjection | WritableProjection`
- `CommandResult = Accepted EffectPlan | Rejected PolicyError | NeedsReview ReviewTask`

ポイントは、**用語集を増やすことではなく、分岐の全体像を型として固定すること**です。  
そうすると、仕様の穴が「文章不足」ではなく「型定義不足」として見えるようになります。

### 2.3 純粋関数として定義すべきコア処理を先に列挙する

`plan.md` には「何が起こるか」は多く書かれていますが、「どの処理が純粋であるべきか」はまだ十分に棚卸しされていません。  
以下の関数群を設計上の第一級要素として明示すると、全体が締まります。

#### 純粋関数として扱いたいもの

- `normalizeObservation`
- `validateObservation`
- `classifyStoragePolicy`
- `resolveAuthority`
- `buildProjectionPlan`
- `foldProjectionState`
- `finalizeProjection`
- `deriveWritePlan`
- `evaluateConsentPolicy`
- `evaluateAccessPolicy`
- `computeLineage`
- `checkReplayDeterminism`

#### 副作用として隔離したいもの

- source system からの取得
- blob/object storage への保存
- materialized DB への反映
- source-native API の呼び出し
- DOI 発行
- audit log 永続化
- job scheduling / retry / queue 処理

この切り分けは、実装言語が何であっても有効です。  
重要なのは「再現したい意味」と「現実世界との接続」を分離することです。

### 2.4 Law と invariant を appendix ではなく中核に置く

現行の `plan.md` は考え方の説明は強いですが、**守るべき法則**を system-wide にまとめた章がまだ不足しています。  
関数型設計を採るなら、次の law を中核仕様に昇格させると良いです。

#### 追加したい主要 law

1. **Append-Only Law**  
   Canonical Observation は破壊的更新されない。

2. **Replay Law**  
   pin された同一入力からは同一 Projection 結果が得られる。

3. **Effect Isolation Law**  
   ドメイン解釈は adapter 固有状態に依存しない。

4. **Explicit Authority Law**  
   すべての write は authority model を経由して正当化される。

5. **Provenance Completeness Law**  
   派生物・書き込み・承認は追跡可能でなければならない。

6. **Idempotency Law**  
   同一 command / capture の再送は結果を二重化しない。

7. **Monotone Governance Law**  
   consent / access policy の制約は bypass されず、緩和時も履歴が追える。

8. **Deterministic Interpretation Law**  
   同じ spec と同じ入力集合に対する解釈は決定的である。

Section 13 の consistency law はとても良いので、これを Projection 局所の話に留めず、**システム全体の law セット**として再編すると強くなります。

### 2.5 「Lake / Supplemental / Source-native」の意味境界を型として確定させる

現状の議論では、この三者の役割は見えているものの、どこまでを immutable とみなすか、どこまでを cache とみなすかがまだ曖昧です。  
これは設計破綻の起点になりやすいので、`plan.md` では次の問いに答える章を独立させた方がよいです。

- Lake に保存される最小単位は何か
- Supplemental store は「再計算可能な共有キャッシュ」なのか「準一次的な補助記録」なのか
- mutable を許すのは runtime cache だけか、supplemental record にも許すのか
- Projection が source-native を直接読む場合、その read は lineage にどう現れるのか
- academic-pinned と operational-latest で参照される入力集合はどのように区別されるのか

特に、ユーザー回答にもあるように、transcript や各種補助情報を Lake に直接入れるのか、supplemental 領域に置くのかは重要です。  
ここは文章上のニュアンスではなく、**保存先ごとの意味論**として定義した方がよいです。

### 2.6 Write-back を「逆写像」だけでなく「command algebra」として扱う

現行案では Write Adapter の説明がかなり良いです。  
次の洗練としては、write-back を単なる inverse mapping ではなく、**command algebra** として整理するとさらに安定します。

たとえば UI からの操作は、まず次のような command 型に落とす方がよいです。

- `CreateFact`
- `CorrectFact`
- `RetractFact`
- `AttachAnnotation`
- `SubmitProposal`
- `ApproveProposal`
- `RejectProposal`
- `InvokeSourceNativeChange`

その上で、

`UI action -> Command -> Policy check -> EffectPlan -> Interpreter`

という流れを基本形にすると、write-back の議論が UI 依存でなくなります。  
これは関数型の「値として command を扱い、解釈は後段に回す」発想に対応します。

### 2.7 Runtime topology と semantic model を分離する

現行の `plan.md` は architecture spec として豊かですが、runtime の話と意味論の話が混ざる箇所があります。  
洗練のためには、次の 2 章を明確に分けるのがよいです。

- **Semantic Architecture:** 型、law、状態遷移、read/write の意味
- **Runtime Architecture:** crawler、queue、scheduler、build worker、storage、cache、sandbox

特に technology recommendations は有用ですが、normative spec の途中にあると「本質」と「実装例」が混ざって見えます。  
`plan.md` 本体は意味論中心に寄せ、技術スタックは reference implementation として後段か別紙に分けるのが自然です。

### 2.8 失敗モデルを明示する

関数型パラダイムを参照するなら、成功ケースだけでなく **失敗がどの型に乗るか** も明示したいです。  
いま足したい観点は以下です。

- source fetch failure
- schema validation failure
- policy rejection
- nondeterministic projector detection
- stale base revision による source-native conflict
- consent downgrade による serving refusal
- replay 不能な malformed record

これらを `Error | Rejected | Retryable | NeedsHumanReview` のように分類しておくと、運用と実装の境界が明確になります。

### 2.9 Agent を playground client として位置付けるなら capability model が必要

`design_questions.md` の回答にもある通り、agent は Lake に直接触らず、Projection 作成や sandbox 内の操作に限定する方向が自然です。  
この場合、`plan.md` には capability model を追加した方がよいです。

例:

- `CanReadRegistry`
- `CanSearchCatalog`
- `CanRunProjectionDraft`
- `CanRequestWritePreview`
- `CanSubmitProposal`
- `CannotReadRawSecrets`
- `CannotAppendCanonicalWithoutApproval`

これはセキュリティのためだけでなく、agent を effectful actor としてどこまで interpreter に近づけるかを決めるためにも重要です。

### 2.10 実装の前に「最小核」を決める

現行案は射程が広いので、そのまま実装に入ると境界がぶれやすいです。  
`plan.md` をさらに洗練するなら、まず最小核を次のように宣言したいです。

#### MVP kernel に含めるもの

- Observation append
- Schema validation
- Projection replay
- lineage 記録
- read mode 切替
- proposal / annotation / canonical の三分岐

#### 後で足すもの

- source-native writable multimodal editing
- agent-native auto authoring
- DOI 自動発行
- 高度な governance workflow
- distributed projection execution

これにより、設計の野心を保ったまま、核の一貫性を先に固められます。

---

## 3. `plan.md` の再構成案

以下のように章構成を再編すると、概念仕様としてかなり読みやすくなります。

### 3.1 推奨トップレベル構成

1. **Problem, Scope, Non-Goals**  
   何を解く文書で、何をまだ解かないか

2. **Core Vocabulary and Algebra**  
   Entity, Observation, Projection, Command, Policy, Authority などの型

3. **System Laws and Invariants**  
   replay, immutability, provenance, idempotency, authority

4. **Functional Core / Imperative Shell**  
   純粋関数と副作用境界

5. **Canonical Storage Semantics**  
   Lake / supplemental / source-native の意味境界

6. **Projection Semantics**  
   入力集合、DAG、build、materialization、read mode

7. **Write Semantics**  
   command algebra、write-back、review、proposal

8. **Governance and Capability Model**  
   consent、access、agent capability、retention

9. **Runtime and Reference Implementation**  
   queue、scheduler、storage、sandbox、推奨技術

10. **ADR / Open Questions / Pending Decisions**  
    未確定事項を本文から見える形にする

### 3.2 既存セクションとの対応

| 現行 `plan.md` の主題 | 洗練後の位置づけ |
|---|---|
| 1, 2 | scope / vocabulary / high-level semantic model に再配置 |
| 3, 4 | canonical storage semantics と registry algebra に整理 |
| 5 | projection semantics の中心章として維持 |
| 6, 7, 8, 9 | policy / governance / evolution / lineage に再配置 |
| 10 | user workflow appendix または operational guide に移動 |
| 11 | reference implementation へ移動 |
| 12, 13 | write semantics と functional core の中心章として統合 |
| Appendix B | ADR log として拡張 |

---

## 4. 別紙に切り出すと良いもの

`plan.md` を洗練するためには、何でも 1 ファイルに詰め込むより、役割ごとに別紙を持った方がよいです。

### 候補

1. **adr_backlog.md**  
   未確定論点と意思決定ログ

2. **domain_algebra.md**  
   型定義、state machine、law、error model

3. **runtime_topology.md**  
   worker、queue、cache、storage、sandbox の配置

4. **projection_authoring_guide.md**  
   spec の書き方、write adapter の作法、determinism 要件

5. **governance_capability_model.md**  
   consent / access / agent capability / approval flow

今の `design_questions.md` は「未確定事項の整理」としてかなり有用です。  
次の段階では、そこから **ADR に昇格したもの** と **まだ議論中のもの** を分けるとさらによくなります。

---

## 5. 優先順位付きの refinement backlog

### 優先度 A: 先に固めると全体が安定する

1. Lake / supplemental / source-native の意味論
2. command algebra と write mode の正規化
3. replay determinism の成立条件
4. identity / anchor / revision の型定義
5. policy evaluation の入力と出力

### 優先度 B: 次に必要になる

1. high-frequency data の capture 粒度
2. agent playground の capability と approval flow
3. multimodal source の inverse mapping 条件
4. error taxonomy と retry policy

### 優先度 C: 実装段階で詰めればよい

1. 具体技術スタックの固定
2. worker 分割と deployment topology
3. performance tuning と cache 戦略

---

## 6. `plan.md` に直接追記したい具体項目

もし `plan.md` 自体を次に更新するなら、以下は直接追記候補です。

### 6.1 「Core Algebra」章

少なくとも次の型は、文章ではなく表や疑似コードで定義したいです。

- Observation
- ObservationRef
- ProjectionInput
- ProjectionState
- Command
- EffectPlan
- PolicyDecision
- ReviewStatus
- LineageRecord
- RevisionAnchor

### 6.2 「Effect Boundary」章

次の依存ルールを明文化すると良いです。

- domain kernel は adapter を import しない
- policy layer は IO を起こさない
- adapter は domain 型を解釈するが、意味論を上書きしない
- runtime は effect の順序を管理するが、projection の意味を変えない

### 6.3 「Failure and Recovery」章

最低限、以下を表にする価値があります。

| Failure | 検出点 | 回復方法 |
|---|---|---|
| schema mismatch | ingest | reject or quarantine |
| duplicate capture | ingest | idempotent accept |
| stale revision write | write-back | reject and rebase |
| nondeterministic projection | build | block registration |
| consent violation | serve/write | deny and audit |

### 6.4 「ADR / Pending Decisions」章

`design_questions.md` の論点を、本文から参照できる形にするのが重要です。  
本文で仕様を読んだ人が「何が確定で何が未確定か」を即座に判断できるようになります。

---

## 7. 関数型アーキテクチャとしての最小イメージ

以下のような疑似コードが `plan.md` のどこかにあると、設計の核が非常に伝わりやすくなります。

```text
buildProjection(spec, inputs) =
  inputs
  |> selectByReadMode(spec.readMode)
  |> sortDeterministically
  |> validateInputs(spec)
  |> foldl(applyObservation, initialState(spec))
  |> finalize(spec)

handleCommand(command, context) =
  command
  |> authorize(context.policy)
  |> deriveEffectPlan(context.writeRules)
  |> interpretEffects(context.adapters)
```

この 2 本が安定していれば、

- Projection は純粋変換
- write は command 解釈
- effect は interpreter に隔離

という全体像を一貫して保てます。

---

## 8. 結論

`plan.md` をさらに洗練するには、内容を増やすだけではなく、**仕様の核を関数型的に再配置すること**が重要です。  
特に次の 3 点が効きます。

1. Projection の関数型解釈をシステム全体へ拡張する  
2. 用語を代数的データ型・law・error model として固定する  
3. semantic model と runtime / implementation recommendation を分離する

これができると、DOKP は「豊富なアイデア集」から、「実装可能で検証可能なアーキテクチャ仕様」へ一段進みます。
