# DOKP Open Issues — 横断的論点整理と提案

**Date:** 2026-03-09
**Scope:** 全ドキュメントの横断分析に基づく、次の設計ラウンドで解決すべき論点

---

## 凡例

| ラベル | 意味 |
|---|---|
| **[ARCH]** | アーキテクチャの核に関わる |
| **[OPS]** | 運用・実装に関わる |
| **[GOV]** | ガバナンス・倫理に関わる |
| **[SPEC]** | 仕様文書の整合性・完成度に関わる |
| **Priority: High / Medium / Low** | 実装着手前に解決すべき度合い |

---

## Issue 1: DAG 変更伝播メカニズムが未定義 [ARCH]

**Priority: High**

### 問題

Projection DAG の概念は `plan.md` §5.2 と `domain_algebra.md` §5 で定義されているが、**上流 Observation が追加されたとき下流 Projection にどう伝わるか**の具体的メカニズムがどの文書にも存在しない。

現状は「Projector が検知 → 更新」と書かれているが、検知の手段（push / poll / CDC）、cascade rebuild の戦略（eager / lazy / on-demand）、upstream の breaking change（archive / major bump）時の downstream への影響が未記述。

### 影響

- DB on DBs（P3）の実現性に直結する
- operational-latest の freshness 保証ができない
- build 順序の決定論的な管理が不可能

### 提案

**通知チャネルと rebuild 戦略を 2 軸で定義する。**

通知チャネル:

| Phase | Mechanism | Rationale |
|---|---|---|
| MVP | **Poll-based** — 各 Projection が定期的にソースの最新 revision / Lake の最終 recordedAt を確認する | 実装最小。追加インフラ不要 |
| Growth | **Event-driven** — Lake append 時と Projection publish 時に notification event を発行し、依存 Projection が subscribe する | freshness 改善。必要な Projection だけ rebuild |
| Scale | **CDC + DAG scheduler** — change data capture と DAG-aware scheduler を組み合わせ、cascade rebuild を topological order で実行 | 大規模 DAG での効率的 propagation |

Rebuild 戦略:

| Strategy | When to use |
|---|---|
| **On-demand** | ユーザーまたは API がアクセスした時点で stale 判定し rebuild する。MVP 向き |
| **Scheduled** | cron 等で定期 rebuild。batch workload 向き |
| **Eager cascade** | upstream 更新を検知したら即座に downstream を rebuild。operational-latest 向き |
| **Lazy invalidate** | upstream 更新時は stale フラグだけ付け、次回アクセス時に rebuild。コスト抑制向き |

Upstream breaking change への対応:

- Upstream が **archive** → downstream は最終 snapshot をそのまま保持（degraded read）
- Upstream が **major version bump** → downstream は旧 version pin で動作継続。新 version 対応は明示的 migration
- いずれの場合も Projection Catalog に **health status** を表示する（healthy / stale / degraded / broken）

**関連 ADR:** ADR-002, ADR-012

### ユーザー回答:
良い感じです。ただ、データの更新が行われるたびにprojectinをrebuildしていてはデータが増えた際に不安定になりそうではあるので、方策を考える必要があると思います。
私の中での優先度としては、
1. 更新されたレコードのみをpropagateする
2. scheduled rebuildかlazy invalidetaなど遅延リビルド

になると思います。あまり遅延リビルドはレスポンスの観点から採用したくはありません。
全データを考慮するようなprojectionは設計段階からレスポンスやリビルド頻度に関して考慮する必要があると感じています。



---

## Issue 2: Supplemental Store の mutability 境界が曖昧 [ARCH]

**Priority: High**

### 問題

`domain_algebra.md` §4.1 で `AppendOnly | ManagedCache` の 2 系統が示されているが、**どの derivation をどちらに分類するかの判定基準**が明文化されていない。

ユーザー回答では「immutable で append が基本だが、利点があれば mutable でもよい」とされており、方針が揺れている。

### 影響

- Projection の再現性保証が supplemental の安定性に依存する場合がある
- lineage の追跡が mutability policy によって変わる
- academic-pinned で supplemental を参照する場合の determinism が保証できない

### 提案

**以下の判定表を domain_algebra.md に追加し、既定は append-only とする。**

| 条件 | 分類 | 理由 |
|---|---|---|
| 再計算コストが高く、入力が pin 可能 | **AppendOnly** | lineage が安定し、academic-pinned で参照可能 |
| 再計算コストが高いが、入力が頻繁に変わる | **ManagedCache** + version tag | 最新を上書きしつつ version で辿れるようにする |
| 再計算コストが低い | **ManagedCache** または 都度再計算 | 保存しなくてもよい |
| academic-pinned Projection が参照する | **AppendOnly 必須** | pin された supplemental version を参照できなければ determinism が壊れる |

具体例:

| Derivation | 推奨分類 | 理由 |
|---|---|---|
| ASR transcript | AppendOnly | 再計算コスト高、モデル version pin で再現可能 |
| OCR text | AppendOnly | 同上 |
| Embedding vector | ManagedCache + version tag | モデル更新で上書きしたい場合がある |
| Name resolution candidate | AppendOnly | 判断履歴の追跡が必要 |
| Face/object detection | AppendOnly | モデル version と結果の対応を保持したい |
| Live sensor rollup cache | ManagedCache | 鮮度優先、再計算容易 |

**academic-pinned の Projection が supplemental を読む場合は、必ず version-pinned read とする。** これにより、ManagedCache であっても academic 利用時には特定 version を参照できる。

**関連 ADR:** ADR-001, ADR-

### ユーザー回答:
いいですね。これで行きましょう。

---

## Issue 3: Source-Native 透過的読み取りの契約が不足 [ARCH]

**Priority: High**

### 問題

ユーザーは繰り返し「Projection は Lake からではなく source-native を直接読む構造の方がきれい」「変更が透過的にデータソースに適用される方がよい」と述べている。`plan.md` §2.3 の Read Mode や `domain_algebra.md` §3.4 の `ProjectionInput` で概念的にはサポートされているが、**source-native を直接読む Projection の具体的な契約**がどこにも書かれていない。

### 影響

- operational-latest で source-native を読む Projection の lineage 表現が不明
- source-native が down した場合の fallback 動作が未定義
- academic-pinned でも source-native を読めると誤解されうる

### 提案

**Source-native read の Projection contract を明示する。**

```yaml
# Projection Spec の sources 宣言に source-native read を明示的に書ける
sources:
  - ref: "source-native:sys:google-slides"
    readMode: "operational-latest"
    fallback: "lake-snapshot"          # source 不達時のフォールバック
    freshnessSla: "best-effort"
    lineageCapture: "timestamp-only"   # source から読んだ時刻を記録
```

契約ルール:

| Read Mode | Source-native 直接読み | 条件 |
|---|---|---|
| **academic-pinned** | **禁止** | pin できないため。必ず Lake snapshot または pinned manifest を使う |
| **operational-latest** | **許可** | source が available なら直接読む。lineage には読取時刻を記録 |
| **application-cached** | **Projection cache 優先** | cache miss 時は source-native に fallback 可 |

Fallback ladder:

1. Source-native latest (available なら)
2. Lake の最新 snapshot
3. Projection の前回 cache
4. Stale result + staleness warning

**lineage での表現:** source-native を読んだ場合は、lineage に `sourceNativeRead { system, timestamp, revisionIfKnown }` を記録する。revision が取れない場合はタイムスタンプのみで可。

**関連 ADR:** ADR-002, ADR-

### ユーザー回答:
この仕様で行きましょう。


---

## Issue 4: 並行書き込みと楽観的ロックのプロトコル [ARCH]

**Priority: Medium**

### 問題

`plan.md` §13.7 で `meta.projectionContext.visibleRowHash` が言及されているが、**同一リソースへの同時書き込みが発生した場合の具体的な競合解決プロトコル**が未定義。Source-native write-back ではさらに `baseRevision` の鮮度問題が加わる。

### 影響

- Writable Projection で 2 人が同時に同じ事実を編集した場合の挙動が不明
- Source-native write-back で revision conflict が起きた場合の処理が不明

### 提案

**楽観的ロック（Optimistic Concurrency Control）を標準プロトコルとする。**

Lake-mediated write:

```text
1. User が Projection を読む → visibleRowHash を取得
2. User が編集 → Command 発行（visibleRowHash を添付）
3. Write Gate が Command 受理時に、現在の Projection 状態と visibleRowHash を比較
4. 一致 → accept → Lake append → rebuild
5. 不一致 → ConflictFailure を返す → User に最新状態を提示して再操作を促す
```

Source-native write-back:

```text
1. Write Adapter が baseRevision を添付して source-native API を呼ぶ
2. Source API が revision conflict を返した場合:
   a. Adapter が最新 revision を取得
   b. 自動 merge 可能なら merge → 再送
   c. 自動 merge 不可なら ConflictFailure を User に返す
3. 自動 merge の可否判定: field-level で衝突しなければ merge 可。同一 field への異なる変更は衝突
```

**自動 rebase は annotation mode に限定** し、canonical mode では常にユーザー確認を挟む。

**関連 ADR:** なし（新規 ADR-016 として起票推奨）

### ユーザー回答:
良いと思います。仕様に落としましょう。

---

## Issue 5: Observer 障害検知と Gap Detection [OPS]

**Priority: Medium**

### 問題

`runtime_reference_architecture.md` §7.1 で追跡すべきメトリクスが列挙されているが、**Observer がサイレントに停止した場合の検知手段**が未定義。センサーや crawler が落ちても Lake には何も記録されないため、gap に気づけない。

### 影響

- Projection のデータが暗黙に欠落する
- academic-pinned build で gap のある期間を含んでしまう
- 運用チームが障害に気づくのが遅れる

### 提案

**Heartbeat Observation + Gap Alert の 2 層で対応する。**

1. **Heartbeat Observation:** 各 Observer は定期的に `schema:observer-heartbeat` を Lake に投入する。これにより「最後に生存が確認された時刻」が分かる。

```yaml
- id: "schema:observer-heartbeat"
  name: "Observer Heartbeat"
  version: "1.0.0"
  subject_type: "et:observer"
  payload_schema:
    type: object
    properties:
      status: { enum: ["alive", "degraded", "shutting-down"] }
      last_successful_capture_at: { type: string, format: date-time }
      pending_count: { type: integer }
    required: ["status"]
```

2. **Gap Alert:** monitoring service が heartbeat の途絶を検知し、alert を発行。閾値は Observer ごとに source contract で定義する。

3. **Projection 側の gap awareness:** Projection Spec に `gapPolicy` を宣言できるようにする。

```yaml
spec:
  sources:
    - ref: "lake"
      filter:
        schemas: ["schema:room-entry"]
      gapPolicy:
        action: "warn"           # warn | block | fill-null
        maxGapDuration: "PT1H"   # 1時間以上の gap で発動
```

**関連 ADR:** なし（新規 ADR-017 として起票推奨）

### ユーザー回答:
これも良い設計です。このまま仕様に落としましょう。



---

## Issue 6: MVP End-to-End シナリオの欠如 [OPS]

**Priority: High**

### 問題

`plan.md` §11.2 に Minimal Viable Stack が示され、`runtime_reference_architecture.md` §6.1 に MVP 技術マッピングがあるが、**「最初に何を動かすか」の具体的な end-to-end シナリオ**がない。

### 影響

- 実装に着手できない
- 何が「動いた」の定義が曖昧
- チームメンバーの合意形成が困難

### 提案

**以下の MVP シナリオを定義する。**

### MVP Scenario: 食堂利用データの end-to-end

```
目標: 1つの Observer → 1つの Schema → Lake 格納 → 1つの Projection → API 公開
      を最短で動かす

Step 1: Registry 初期化
  - EntityType: et:person, et:dining-hall を登録
  - Schema: schema:dining-entry を登録
  - Observer: obs:dining-manual を登録（人手入力 or CSV import）

Step 2: Observation 投入
  - 手動フォームまたは CSV から dining-entry Observation を 50 件投入
  - Ingestion Gate が schema validation、dedup、append を実行

Step 3: Projection 構築
  - proj:dining-hourly-summary を定義
    - source: lake, filter: schema:dining-entry
    - engine: DuckDB
    - output: 時間帯別利用者数テーブル
  - sandbox で build → Catalog に登録

Step 4: API 公開
  - FastAPI で /api/projections/dining-hourly-summary を公開
  - GET で最新集計を返す

Step 5: 検証
  - Observation を追追加 → Projection に反映されることを確認
  - lineage: Projection → Lake Observation の追跡を確認

完了条件:
  - 50 件の Observation が Lake に格納されている
  - Projection が正しい集計結果を返す
  - 新規 Observation 追加後に rebuild で反映される
  - lineage query で元 Observation まで辿れる
```

### MVP 後の拡張順序

| Phase | 追加要素 | 目的 |
|---|---|---|
| MVP+1 | Google Slides crawler + document-snapshot schema | mutable SaaS source の取り込み |
| MVP+2 | 2 つ目の Projection + DB on DBs | DAG の動作確認 |
| MVP+3 | Writable Projection + Write Adapter | write-back の検証 |
| MVP+4 | Filtering Projection + consent | governance の検証 |
| MVP+5 | Agent sandbox integration | coding agent の動作確認 |

**関連 ADR:** ADR-014

### ユーザー回答:
MVPシナリオは変更させてください。Google slidesのデータ取り込みと、Slackのデータ取り込みを行い、名寄せと各個人のページ作成をシナリオとしたいです。



---

## Issue 7: Consent 撤回時の Supplemental Derivation の扱い [GOV]

**Priority: Medium**

### 問題

`governance_capability_model.md` §3 で consent と filtering projection の方針が示されているが、**consent 撤回時に Supplemental Derivation Store 内の派生物（transcript、embedding、顔認識結果等）をどう扱うか**が未定義。`plan.md` §7.2 の opt-out も Projection 側の処理しか記述していない。

### 影響

- transcript や embedding に個人情報が含まれる場合、supplemental に残り続ける
- 「Lake は append-only」と「個人情報の削除権」の緊張関係が supplemental 層で再発する

### 提案

**Consent 撤回の cascade 処理を 3 層で定義する。**

| 層 | 対象 | 撤回時の処理 |
|---|---|---|
| **Lake** | Canonical Observation | retraction flag を付与。physical delete は blob のみ（既存方針通り） |
| **Supplemental** | transcript, embedding, face detection 等 | opt_out_policy に従い cascade 処理を実行 |
| **Projection** | materialized views | 既存の filtering projection で除外（既存方針通り） |

Supplemental 層の cascade 処理:

| opt_out_policy | Supplemental での処理 |
|---|---|
| **Drop** | 当該 subject に紐づく supplemental record を soft-delete（参照不可に） |
| **Anonymize** | subject reference を不可逆ハッシュに置換 |
| **Pseudonymize** | subject reference を仮名に置換 |

実装上は、supplemental record が `derivedFrom` で元 Observation を参照しているため、retracted Observation に紐づく supplemental を自動的に特定できる。

**関連 ADR:** ADR-006, ADR-007

### ユーザー回答:
supplementalの部分を削除してしまうと、実際にopt-outすべきデータかどうかがわからなくなってしまう可能性があります。filtering projectionでの除外がきれいなのではないでしょうか。



---

## Issue 8: plan.md と domain_algebra.md の重複整理 [SPEC]

**Priority: Medium**

### 問題

`plan.md` §13 (Functional Projection Model & Writable Views) と `domain_algebra.md` §5–6 の内容が大幅に重複している。コマンド代数、write mode、consistency law が両方に書かれており、どちらが正典かが曖昧。

同様に、`plan.md` §7 (Governance & Ethics) と `governance_capability_model.md` の間にも重複がある。

### 影響

- 更新時の同期漏れが起きる
- 読者がどちらを信頼すべきか分からない

### 提案

**`plan.md` を概念仕様の親文書として維持しつつ、詳細な定義は別紙に委譲する構造を明示する。**

具体的には `plan.md` の以下のセクションを簡潔な要約 + 別紙参照に書き換える:

| plan.md セクション | 現状 | 提案 |
|---|---|---|
| §13 Functional Projection Model | 詳細な write algebra と law | 概要 + 「詳細は domain_algebra.md §5–7 を参照」 |
| §7 Governance & Ethics | consent, access, retention の詳細 | 概要 + 「詳細は governance_capability_model.md を参照」 |
| §11 Technology Recommendations | 技術スタック詳細 | 概要 + 「詳細は runtime_reference_architecture.md §6 を参照」 |

こうすることで `plan.md` は全体の地図（50–60% のボリュームに圧縮）として機能し、各別紙が権威ある詳細仕様になる。

### ユーザー回答:
提案した方向性でドキュメントの整理をしてください。


---

## Issue 9: Entity Relationship の N 項関係 [SPEC]

**Priority: Low**

### 問題

現在の Observation は `subject` (主語) と `target` (対象) の 2 項で関係を表現している。「A が B を C に紹介した」「A と B が C で会議した」のような 3 項以上の関係を表現する標準的な方法がない。

### 影響

- 複雑な社会関係の記録が回りくどくなる
- Projection 側で無理に合成する必要がある

### 提案

**当面は 2 項 + payload 拡張で対応し、必要に応じて将来 participants フィールドを追加する。**

短期（MVP）:

```jsonc
{
  "subject": "person:tanaka",
  "target": "space:meeting-room-1",
  "payload": {
    "action": "meeting",
    "participants": ["person:suzuki", "person:yamamoto"]  // payload 内で追加参加者を記録
  }
}
```

将来（必要になったら）:

```text
Observation =
  { ...
  , subject: EntityRef
  , target: EntityRef?
  , participants: [EntityRef]?   // optional N 項拡張
  , ...
  }
```

**判断基準:** 2 つ以上の Projection が同じ N 項パターンを独自に payload から抽出している場合、共通フィールドへの昇格を検討する。

**関連 ADR:** なし（必要時に起票）

### ユーザー回答:
将来へのロードマップに回します。



---

## Issue 10: Time Zone の正規化方針 [SPEC]

**Priority: Low**

### 問題

`plan.md` のサンプルでは `+09:00` 付きの ISO 8601 が使われているが、**保存時に UTC に正規化するか、ローカルタイム保持かの方針**が明文化されていない。

### 影響

- Projection 間で時刻比較する際にオフセットの不一致が起きうる
- DST のある地域の source との統合で問題になりうる

### 提案

**Lake 保存時は UTC + 元 offset の両方を保持する。**

- `published`: 元の offset 付き ISO 8601 をそのまま保存する（`2026-05-01T08:30:00+09:00`）
- `recordedAt`: システムが UTC で付与する
- Projection が時間計算する際は UTC に正規化して比較する
- 表示時はユーザーのローカルタイムに変換する

schema で timezone-naive な timestamp を使うことは非推奨とし、schema validation で offset の存在を検証する。

### ユーザー回答:
提案で行きましょう。




---

## Issue 11: adr_backlog.md の更新反映 [SPEC]

**Priority: Medium**

### 問題

`adr_backlog.md` の一部の ADR は、`domain_algebra.md` や `governance_capability_model.md` の作成によって**実質的に方向性が確定**しているが、status が更新されていない。

### 提案

以下の status 更新を推奨する。

| ADR | 現在の Status | 推奨 Status | 理由 |
|---|---|---|---|
| ADR-001 | Agreed Direction | **Ready to Merge** | domain_algebra §3.1.1 で SaaS Snapshot Pattern が定義済み |
| ADR-004 | Active | **Agreed Direction** | ユーザー回答 + governance model で方向確定 |
| ADR-005 | Active | **Agreed Direction** | ユーザー回答 + domain_algebra で placement 確定 |
| ADR-006 | Agreed Direction | **Ready to Merge** | governance_capability_model §3 で詳細化済み |
| ADR-007 | Agreed Direction | **Ready to Merge** | governance_capability_model §7 で takedown ladder 定義済み |
| ADR-008 | Agreed Direction | **Ready to Merge** | runtime_reference_architecture §5 で build isolation 定義済み |
| ADR-011 | Agreed Direction | **Ready to Merge** | ユーザー回答で valid time は任意フィールドと確定 |
| ADR-015 | Agreed Direction | **Ready to Merge** | 今回の doc set で実現済み |

また、本ドキュメントで提起した新論点を ADR として追加する:

| 新 ADR | Topic | Status |
|---|---|---|
| ADR-016 | Concurrency and Conflict Resolution Protocol | Active |
| ADR-017 | Observer Health and Gap Detection | Active |
| ADR-018 | DAG Change Propagation Mechanism | Active |
| ADR-019 | MVP End-to-End Scenario Definition | Active |

### ユーザー回答:
statusの更新をお願いします。




---

## 優先順位サマリ

| Priority | Issue | 次のアクション |
|---|---|---|
| **High** | #1 DAG 変更伝播 | plan.md に propagation model セクションを追加 |
| **High** | #2 Supplemental mutability 境界 | domain_algebra.md に判定表を追加 |
| **High** | #3 Source-native 透過的読み取り契約 | plan.md / domain_algebra に source-native read contract を追加 |
| **High** | #6 MVP End-to-End シナリオ | 独立ドキュメントまたは plan.md §10 に追加 |
| **Medium** | #4 並行書き込み楽観的ロック | domain_algebra.md の write semantics に追加 |
| **Medium** | #5 Observer 障害検知 | runtime_reference_architecture.md に追加 |
| **Medium** | #7 Consent cascade to supplemental | governance_capability_model.md に追加 |
| **Medium** | #8 plan.md と別紙の重複整理 | plan.md を圧縮し別紙参照へ |
| **Medium** | #11 ADR backlog status 更新 | adr_backlog.md を更新 |
| **Low** | #9 N 項関係 | 当面は payload 拡張で運用 |
| **Low** | #10 Time zone 正規化 | plan.md に方針を 1 段落追加 |
