# DOKP Open Issues — Round 2

**Date:** 2026-03-10
**Scope:** Round 1 の回答を反映した後に浮上した、次の設計ラウンドで解決すべき論点

---

## 凡例

| ラベル | 意味 |
|---|---|
| **[ARCH]** | アーキテクチャの核に関わる |
| **[IMPL]** | 実装方式に関わる |
| **[GOV]** | ガバナンス・倫理に関わる |
| **[MVP]** | MVP 着手に直結する |
| **Priority: High / Medium / Low** | 実装着手前に解決すべき度合い |

---

## Issue R2-1: Incremental Propagation の具体的な Watermark 管理 [ARCH]

**Priority: High**

### 問題

plan.md §5.2.1 で incremental propagation（差分伝播）を第一優先の伝播戦略として定義したが、**watermark の管理方法と incremental apply が不可能な場合の判定基準**が具体化されていない。

具体的に未定義な点:

- watermark は `recordedAt` か `id` か `published` のどれを基準にするか
- 複数 source を持つ Projection の watermark はどう管理するか
- incremental apply の正しさをどう検証するか（full rebuild との差分検証）
- 集計系 Projection で incremental apply が不可能な場合の標準的代替パターン

### 影響

- 差分伝播の信頼性が不明
- Projection 開発者が incremental apply の実装方法を判断できない
- full rebuild との整合性が保証されない

### 提案

**Watermark 基準:**

- 既定は `recordedAt` + `id` の複合 watermark を使用する
- `recordedAt` で大まかなフィルタリングを行い、`id`（UUID v7）でバイト順の確定的な切り点を定める
- Late arrival（`published` < watermark だが `recordedAt` > watermark）は incremental apply で自然に拾える

**複数 source の watermark:**

```yaml
spec:
  incrementalState:
    watermarks:
      - source: "lake:schema:room-entry"
        lastId: "019577a0-..."
        lastRecordedAt: "2026-05-01T08:30:00.123Z"
      - source: "proj:person-directory-2026"
        lastVersion: "1.2.0"
        lastBuildId: "build-0042"
```

**Incremental / Full Rebuild の判定:**

| Projection パターン | Incremental Apply | 推奨戦略 |
|---|---|---|
| Append-only 集約（新規レコードの追加のみ） | 容易 | watermark 以降の record を追加適用 |
| Window 集計（時間窓別カウント等） | 可能（影響窓のみ再計算） | 影響する window のみ再集計 |
| Global 集計（全体平均、全体ランキング等） | 困難 | scheduled full rebuild + incremental cache の併用 |
| Graph 構築（ノード・エッジ追加） | 可能（差分のみ追加） | 新規ノード・エッジの追加適用 |
| Identity resolution（名寄せ） | 部分的 | 新規候補の追加は incremental、merge 判定は scheduled rebuild |

**整合性検証:**

- 公開系 Projection は定期的に full rebuild と incremental 結果の差分検証を行う
- 差分が検知された場合は `DeterminismFailure` として報告し、full rebuild を実行する

**関連 ADR:** ADR-018

### ユーザー回答：
この仕様で良いと思います。


---

## Issue R2-2: MVP の Slack Schema と Adapter 設計 [MVP]

**Priority: High**

### 問題

MVP シナリオ（plan.md §11.4）で Slack のデータ取り込みを定義したが、**Slack 用の schema、adapter、capture 戦略**が未定義。Slack は以下の特性を持ち、Google Slides とは異なる設計が必要:

- メッセージは基本的に immutable（edit / delete はあるが主流はappend）
- thread / reply の階層構造がある
- reaction / file attachment がある
- channel / DM / group の scope がある
- Bot message と human message の区別がある

### 影響

- MVP の実装に着手できない
- 名寄せに必要な Slack user identity の取得方法が不明

### 提案

**Schema 設計:**

```yaml
- id: "schema:slack-message"
  name: "Slack Message"
  version: "1.0.0"
  subject_type: "et:person"          # 送信者
  target_type: "et:slack-channel"    # 送信先 channel
  payload_schema:
    type: object
    properties:
      message_type: { enum: ["message", "reply", "bot-message"] }
      text: { type: string }
      ts: { type: string }           # Slack の message timestamp (unique id)
      thread_ts: { type: string }    # thread parent の ts (reply の場合)
      reactions: { type: array }
      file_refs: { type: array }
      edited: { type: boolean }
    required: ["message_type", "text", "ts"]
```

**Capture 戦略:**

| 方式 | 説明 | 推奨 |
|---|---|---|
| **初回 full export** | conversations.history API で全メッセージを取得 | MVP 初回 |
| **差分 crawl** | oldest パラメータで前回 cursor 以降のみ取得 | 継続運用 |
| **Event subscription** | Slack Events API で real-time にメッセージを受信 | Growth phase |

**Authority model:** `lake-authoritative`（取り込み後は Lake が正史）

**Observer 登録:**

```yaml
- id: "obs:slack-crawler"
  observer_type: "crawler"
  source_system: "sys:slack"
  schemas: ["schema:slack-message", "schema:slack-channel-snapshot"]
  authority_model: "lake-authoritative"
  capture_model: "event"
  owner: "infra-team"
  trust_level: "automated"
```

**名寄せ用の identity 情報:**

- Slack user id (`U...`) をメッセージの actor として記録
- Slack user profile（display name, email, real name）を別途 `schema:slack-user-profile` として取り込む
- Google account と Slack の email を突合キーにして名寄せする

### ユーザー回答：
この仕様で良いと思いますが、先にsource adopterのポリシーを定めてから開発に進んだ方が混乱が少なくなると思います。
次回のopen_issuesでadopterポリシーを提案してください。




---

## Issue R2-3: 名寄せ Projection の具体的な設計 [MVP]

**Priority: High**

### 問題

MVP シナリオで名寄せ（Identity Resolution）が中核的な役割を果たすが、**名寄せ Projection の具体的な設計（入力、アルゴリズム、出力 schema、confidence の扱い）** が未定義。

### 影響

- 個人ページ Projection が構築できない
- 複数 source をまたいだデータの紐づけが仕様上不明

### 提案

**名寄せ Projection の構成:**

```yaml
apiVersion: "dokp/v1"
kind: "Projection"
metadata:
  id: "proj:person-resolution"
  name: "Person Identity Resolution"
  version: "1.0.0"
spec:
  sources:
    - ref: "lake"
      filter:
        schemas: ["schema:slack-user-profile"]
    - ref: "lake"
      filter:
        schemas: ["schema:workspace-object-snapshot"]
        payload_filter: { "artifact.provider": "google" }
    - ref: "supplemental"
      filter:
        derivations: ["ocr-text-person-mention"]
  engine: "duckdb"
  readModes:
    - name: "operational-latest"
```

**名寄せの段階:**

| Step | 入力 | 方法 | 出力 |
|---|---|---|---|
| 1. Email 突合 | Slack email + Google account email | 完全一致 | high-confidence link |
| 2. Display name 突合 | Slack display name + Google display name | fuzzy match | medium-confidence candidate |
| 3. Content mention 抽出 | OCR text / Slide author / Slack message 内の人名 | NER + fuzzy match | low-confidence candidate |
| 4. Human review | medium/low-confidence candidates | 人手確認 GUI | confirmed / rejected |

**出力 schema:**

```yaml
# resolved_identities テーブル
- canonical_person_id: "person:tanaka-2026"
  sources:
    - system: "slack"
      external_id: "U1234567"
      confidence: "high"
      method: "email-match"
    - system: "google"
      external_id: "tanaka@example.jp"
      confidence: "high"
      method: "email-match"
  display_name: "田中太郎"
  resolved_at: "2026-05-01T10:00:00Z"
  resolution_status: "confirmed"  # confirmed | candidate | rejected
```

**Candidate の保存先:** Supplemental（AppendOnly）。判断履歴の追跡が必要なため。

**Human review の仕組み:**

- medium/low-confidence の candidate を一覧表示する GUI
- reviewer が confirm / reject を選択 → annotation observation として記録
- confirmed candidate は次回 build で resolved identity に昇格
### ユーザー回答：
MVPとしてはこれで良いと思います。
将来、ソース横断トラッキングを強化したいので、名前や自己紹介の単語情報のレーベンシュタイン距離からエントロピーを最小化するような名寄せメカニズムを実装したいと考えています。
将来実装のロードマップに記入しておいてください。



---

## Issue R2-4: 個人ページ Projection の API 設計 [MVP]

**Priority: High**

### 問題

MVP の最終出力である個人ページ Projection の **API 設計、表示項目、DB on DBs としての source 依存関係**が未定義。

### 影響

- MVP の完成条件が曖昧
- GUI チームが API 契約をもとに開発を始められない

### 提案

**Projection 定義:**

```yaml
apiVersion: "dokp/v1"
kind: "Projection"
metadata:
  id: "proj:person-page"
  name: "Person Page"
  version: "1.0.0"
spec:
  sources:
    - ref: "proj:person-resolution"
      version: ">=1.0.0"
    - ref: "lake"
      filter:
        schemas: ["schema:workspace-object-snapshot", "schema:slack-message"]
  engine: "duckdb"
  readModes:
    - name: "operational-latest"
      sourcePolicy: "source-native-preferred"
```

**API エンドポイント:**

| Endpoint | Method | Response |
|---|---|---|
| `/api/persons` | GET | 名寄せ済み人物一覧（id, display_name, source_count） |
| `/api/persons/{person_id}` | GET | 個人詳細ページ |
| `/api/persons/{person_id}/slides` | GET | 関連 Slides 一覧 |
| `/api/persons/{person_id}/messages` | GET | 関連 Slack メッセージ一覧 |
| `/api/persons/{person_id}/timeline` | GET | Activity timeline |

**個人詳細ページのデータ構造:**

```json
{
  "person_id": "person:tanaka-2026",
  "display_name": "田中太郎",
  "identities": [
    { "system": "slack", "external_id": "U1234567" },
    { "system": "google", "external_id": "tanaka@example.jp" }
  ],
  "related_slides": [
    {
      "document_id": "gslide:deck-abc123",
      "title": "プロジェクト企画書",
      "role": "editor",
      "last_seen_revision": "rev-017"
    }
  ],
  "recent_messages": [
    {
      "channel": "general",
      "text": "明日の会議について...",
      "ts": "2026-05-01T10:30:00+09:00"
    }
  ],
  "activity_summary": {
    "total_slides_related": 5,
    "total_messages": 128,
    "last_activity": "2026-05-01T10:30:00+09:00"
  }
}
```
### ユーザー回答：
大体良い感じです。
使用しているGoogle slidesは自己紹介スライドなので、そこの個人の情報をもとに個人ページを作成する感じで良いと思います。
別のディレクトリに、Google Slidesの画像から個人の情報を抽出し、Notionに転帰するプログラムがあるので適宜使用する言語に書き換えしつつ転用してもらえればと思います。


---

## Issue R2-5: Projection の rebuild コスト見積もりと閾値設定 [ARCH]

**Priority: Medium**

### 問題

plan.md §5.2.1 で「全データを対象とする集計型 Projection は設計段階で rebuild コストを明示しなければならない」と定義したが、**コスト見積もりの方法と acceptable な閾値**が未定義。

### 影響

- Projection 開発者が「この Projection は incremental apply が必要か」を判断できない
- SLA 違反の rebuild を事前に検知できない

### 提案

**Projection Spec に rebuild 見積もりセクションを追加:**

```yaml
spec:
  rebuildEstimate:
    inputScale: "~10K observations"
    fullRebuildTime: "~30s"
    incrementalApplyTime: "~1s per 100 records"
    strategy: "incremental-preferred"
    scheduledRebuildInterval: "P1D"    # daily full rebuild for drift correction
    maxAcceptableLatency: "PT5M"       # 5 minutes
```

**閾値ガイドライン:**

| 分類 | Full Rebuild Time | 推奨戦略 |
|---|---|---|
| Lightweight (< 1 min) | incremental + 必要時 full rebuild | 特別な制限なし |
| Medium (1-10 min) | incremental 必須 + scheduled daily rebuild | rebuild 中の stale serving を許可 |
| Heavy (10 min - 1 hour) | incremental 必須 + scheduled weekly rebuild | precomputed cache の利用を推奨 |
| Very Heavy (> 1 hour) | incremental 必須 + on-demand full rebuild のみ | build isolation と resource limit を厳格に設定 |

### ユーザー回答：
良いと思います。
重いprojectionを実装する際にはデータの取得日時を区切って頻繁にrebuildを起こさないように作成者に向けてポリシーを作るのも良いと思います。



---

## Issue R2-6: Google Slides Adapter の実装仕様 [MVP]

**Priority: High**

### 問題

MVP シナリオで Google Slides の取り込みが必要だが、**Slides API の具体的な利用方法、rate limit 対策、capture 粒度**が未定義。

### 影響

- adapter 実装に着手できない
- API quota を超過するリスクがある

### 提案

**API 利用方針:**

| API | 用途 | Rate Limit 対策 |
|---|---|---|
| `presentations.get` | native structure 取得 | 1 deck あたり 1 call。batch 実行 |
| `drive.revisions.list` | revision 履歴取得 | deck ごとに差分チェック |
| `drive.export` | PDF/PPTX export | render snapshot 用。必要時のみ |

**Capture 粒度:**

- 初回: 対象 deck の全 revision を snapshot として取り込む
- 継続: 前回 capture 以降の新 revision のみを差分取得する
- revision が変わっていない deck は skip する

**revision 検知:**

```python
# pseudo-code
last_known_revision = get_watermark(deck_id)
current_revision = drive.revisions.list(deck_id, fields="revisions(id,modifiedTime)")
if current_revision.latest.id != last_known_revision:
    capture_snapshot(deck_id, current_revision.latest)
    update_watermark(deck_id, current_revision.latest.id)
```

**OAuth scope:**

- `https://www.googleapis.com/auth/presentations.readonly`
- `https://www.googleapis.com/auth/drive.readonly`

**Error handling:**

| Error | 対応 |
|---|---|
| 403 Rate Limit | exponential backoff + retry |
| 404 Not Found | deck が削除された場合、archive observation を生成 |
| 401 Unauthorized | token refresh → 失敗なら alert |

### ユーザー回答：
これで良いですね。n+1問題を起こさないようにAPIコール数を減らすようにしてください。
slackの項でも指摘しましたが、sourceを作成する際のポリシーを先に定めてから開発に進むと良いと思います。


---

## Issue R2-7: Agent Sandbox の最小構成 [IMPL]

**Priority: Medium**

### 問題

ADR-003 で agent playground の方向性は示されているが、**MVP で必要な最小限の sandbox 構成**が具体化されていない。MVP シナリオでは agent 利用は必須ではないが、MVP+4 に向けた設計は今から考えておく必要がある。

### 影響

- MVP+4 の実装見積もりが立たない
- sandbox なしで Projection 開発を始めると、後から sandbox を導入する際の移行コストが高い

### 提案

**MVP（sandbox なし）:**

- Projection の開発は Git + CLI + local DuckDB で行う
- spec lint は CLI ツールとして提供
- dry-run build は local 環境で実行

**MVP+4（最小 sandbox）:**

| Component | 実装 | 目的 |
|---|---|---|
| Spec editor | Web UI with YAML validation | 非技術ユーザーの Projection 作成 |
| Dry-run runner | 隔離された local container | 安全な試行 |
| Build log viewer | Web UI | build 状態の確認 |
| Agent connector | API endpoint for coding agent | agent からの spec 生成・lint |

**sandbox の capability 制限:**

- Network: default deny（source-native read は明示的に許可した endpoint のみ）
- Storage: ephemeral（build 完了後に破棄）
- CPU/Memory: upper bound 設定
- Duration: timeout 設定

### ユーザー回答：
これで良い気がします。


---

## Issue R2-8: Projection Catalog の Discovery UX [IMPL]

**Priority: Medium**

### 問題

Projection Catalog は DB on DBs を実現するための重要なインフラだが、**ユーザーが既存の Projection を発見し、自分の Projection の source として利用するための具体的な UX**が未定義。

### 影響

- Projection の再利用が促進されない
- DB on DBs の実際の利用体験が不明

### 提案

**最小限の Catalog API:**

| Endpoint | Method | Response |
|---|---|---|
| `/api/catalog/projections` | GET | 全 Projection 一覧（id, name, tags, status, version） |
| `/api/catalog/projections/{id}` | GET | 詳細（sources, outputs, readModes, lineage） |
| `/api/catalog/projections/{id}/dependents` | GET | この Projection を source にしている下流 Projection |
| `/api/catalog/search?q=...` | GET | tag / name / description でのフリーテキスト検索 |
| `/api/catalog/dag` | GET | DAG 全体の構造（nodes + edges） |

**表示すべき情報:**

- Projection の目的と内容の説明
- 入力 source（Lake / Supplemental / 他 Projection）
- 出力テーブルとカラム定義
- 対応 read mode
- 最終 build 時刻と health status
- downstream dependency count
- DOI（付与済みの場合）

### ユーザー回答：
MVPとしてはこれで良い感じだと思います。


---

## 優先順位サマリ

| Priority | Issue | 次のアクション |
|---|---|---|
| **High** | R2-1 Incremental Propagation watermark 管理 | domain_algebra.md に watermark 仕様を追加 |
| **High** | R2-2 Slack Schema / Adapter 設計 | plan.md に schema 追加、adapter 実装着手 |
| **High** | R2-3 名寄せ Projection 設計 | MVP 実装の中核。spec + projector の試作 |
| **High** | R2-4 個人ページ Projection API | API 契約を確定し GUI チームと連携 |
| **High** | R2-6 Google Slides Adapter 実装 | adapter 実装着手 |
| **Medium** | R2-5 Rebuild コスト見積もり閾値 | Projection Spec の拡張定義 |
| **Medium** | R2-7 Agent Sandbox 最小構成 | MVP+4 の設計検討 |
| **Medium** | R2-8 Projection Catalog Discovery UX | Catalog API の仕様策定 |
