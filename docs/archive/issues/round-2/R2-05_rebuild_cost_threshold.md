# Issue R2-5: Projection の rebuild コスト見積もりと閾値設定

**Labels:** [ARCH]
**Priority:** Medium
**Status:** Approved

---

## 問題

plan.md §5.2.1 で「全データを対象とする集計型 Projection は設計段階で rebuild コストを明示しなければならない」と定義したが、**コスト見積もりの方法と acceptable な閾値**が未定義。

## 影響

- Projection 開発者が「この Projection は incremental apply が必要か」を判断できない
- SLA 違反の rebuild を事前に検知できない

## 提案

### Projection Spec に rebuild 見積もりセクションを追加

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

### 閾値ガイドライン

| 分類 | Full Rebuild Time | 推奨戦略 |
|---|---|---|
| Lightweight (< 1 min) | incremental + 必要時 full rebuild | 特別な制限なし |
| Medium (1-10 min) | incremental 必須 + scheduled daily rebuild | rebuild 中の stale serving を許可 |
| Heavy (10 min - 1 hour) | incremental 必須 + scheduled weekly rebuild | precomputed cache の利用を推奨 |
| Very Heavy (> 1 hour) | incremental 必須 + on-demand full rebuild のみ | build isolation と resource limit を厳格に設定 |

---

## ユーザー回答

良いと思います。
重いprojectionを実装する際にはデータの取得日時を区切って頻繁にrebuildを起こさないように作成者に向けてポリシーを作るのも良いと思います。

---

## 次のアクション

- Projection Spec の拡張定義
- 重い Projection 向けの rebuild 抑制ポリシーを策定
