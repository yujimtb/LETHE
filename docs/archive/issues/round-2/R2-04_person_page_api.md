# Issue R2-4: 個人ページ Projection の API 設計

**Labels:** [MVP]
**Priority:** High
**Status:** Approved

---

## 問題

MVP の最終出力である個人ページ Projection の **API 設計、表示項目、DB on DBs としての source 依存関係**が未定義。

## 影響

- MVP の完成条件が曖昧
- GUI チームが API 契約をもとに開発を始められない

## 提案

### Projection 定義

```yaml
apiVersion: "lethe/v1"
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

### API エンドポイント

| Endpoint | Method | Response |
|---|---|---|
| `/api/persons` | GET | 名寄せ済み人物一覧（id, display_name, source_count） |
| `/api/persons/{person_id}` | GET | 個人詳細ページ |
| `/api/persons/{person_id}/slides` | GET | 関連 Slides 一覧 |
| `/api/persons/{person_id}/messages` | GET | 関連 Slack メッセージ一覧 |
| `/api/persons/{person_id}/timeline` | GET | Activity timeline |

### 個人詳細ページのデータ構造

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

---

## ユーザー回答

大体良い感じです。
使用しているGoogle slidesは自己紹介スライドなので、そこの個人の情報をもとに個人ページを作成する感じで良いと思います。
別のディレクトリに、Google Slidesの画像から個人の情報を抽出し、Notionに転帰するプログラムがあるので適宜使用する言語に書き換えしつつ転用してもらえればと思います。

---

## 次のアクション

- API 契約を確定し GUI チームと連携
- 既存の Slides→Notion 転記プログラムを参照・転用
