# Issue R2-3: 名寄せ Projection の具体的な設計

**Labels:** [MVP]
**Priority:** High
**Status:** Approved

---

## 問題

MVP シナリオで名寄せ（Identity Resolution）が中核的な役割を果たすが、**名寄せ Projection の具体的な設計（入力、アルゴリズム、出力 schema、confidence の扱い）** が未定義。

## 影響

- 個人ページ Projection が構築できない
- 複数 source をまたいだデータの紐づけが仕様上不明

## 提案

### Projection 定義

```yaml
apiVersion: "lethe/v1"
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

### 名寄せの段階

| Step | 入力 | 方法 | 出力 |
|---|---|---|---|
| 1. Email 突合 | Slack email + Google account email | 完全一致 | high-confidence link |
| 2. Display name 突合 | Slack display name + Google display name | fuzzy match | medium-confidence candidate |
| 3. Content mention 抽出 | OCR text / Slide author / Slack message 内の人名 | NER + fuzzy match | low-confidence candidate |
| 4. Human review | medium/low-confidence candidates | 人手確認 GUI | confirmed / rejected |

### 出力 schema

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

### Candidate の保存先

Supplemental（AppendOnly）。判断履歴の追跡が必要なため。

### Human review の仕組み

- medium/low-confidence の candidate を一覧表示する GUI
- reviewer が confirm / reject を選択 → annotation observation として記録
- confirmed candidate は次回 build で resolved identity に昇格

---

## ユーザー回答

MVPとしてはこれで良いと思います。
将来、ソース横断トラッキングを強化したいので、名前や自己紹介の単語情報のレーベンシュタイン距離からエントロピーを最小化するような名寄せメカニズムを実装したいと考えています。
将来実装のロードマップに記入しておいてください。

---

## 次のアクション

- MVP: spec + projector の試作
- Future: レーベンシュタイン距離 + エントロピー最小化ベースの名寄せメカニズムをロードマップに追加
