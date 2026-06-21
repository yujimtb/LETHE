# Issue R2-6: Google Slides Adapter の実装仕様

**Labels:** [MVP]
**Priority:** High
**Status:** Approved (source adapter policy を先行策定)

---

## 問題

MVP シナリオで Google Slides の取り込みが必要だが、**Slides API の具体的な利用方法、rate limit 対策、capture 粒度**が未定義。

## 影響

- adapter 実装に着手できない
- API quota を超過するリスクがある

## 提案

### API 利用方針

| API | 用途 | Rate Limit 対策 |
|---|---|---|
| `presentations.get` | native structure 取得 | 1 deck あたり 1 call。batch 実行 |
| `drive.revisions.list` | revision 履歴取得 | deck ごとに差分チェック |
| `drive.export` | PDF/PPTX export | render snapshot 用。必要時のみ |

### Capture 粒度

- 初回: 対象 deck の全 revision を snapshot として取り込む
- 継続: 前回 capture 以降の新 revision のみを差分取得する
- revision が変わっていない deck は skip する

### revision 検知

```python
# pseudo-code
last_known_revision = get_watermark(deck_id)
current_revision = drive.revisions.list(deck_id, fields="revisions(id,modifiedTime)")
if current_revision.latest.id != last_known_revision:
    capture_snapshot(deck_id, current_revision.latest)
    update_watermark(deck_id, current_revision.latest.id)
```

### OAuth scope

- `https://www.googleapis.com/auth/presentations.readonly`
- `https://www.googleapis.com/auth/drive.readonly`

### Error handling

| Error | 対応 |
|---|---|
| 403 Rate Limit | exponential backoff + retry |
| 404 Not Found | deck が削除された場合、archive observation を生成 |
| 401 Unauthorized | token refresh → 失敗なら alert |

---

## ユーザー回答

これで良いですね。n+1問題を起こさないようにAPIコール数を減らすようにしてください。
slackの項でも指摘しましたが、sourceを作成する際のポリシーを先に定めてから開発に進むと良いと思います。

---

## 次のアクション

- source adapter policy を策定（R2-2 / R2-6 共通の前提条件）
- n+1 回避の batch 戦略を明示
- adapter 実装着手
