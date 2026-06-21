# Issue R2-1: Incremental Propagation の具体的な Watermark 管理

**Labels:** [ARCH]
**Priority:** High
**Status:** Approved
**Related ADR:** ADR-018

---

## 問題

plan.md §5.2.1 で incremental propagation（差分伝播）を第一優先の伝播戦略として定義したが、**watermark の管理方法と incremental apply が不可能な場合の判定基準**が具体化されていない。

具体的に未定義な点:

- watermark は `recordedAt` か `id` か `published` のどれを基準にするか
- 複数 source を持つ Projection の watermark はどう管理するか
- incremental apply の正しさをどう検証するか（full rebuild との差分検証）
- 集計系 Projection で incremental apply が不可能な場合の標準的代替パターン

## 影響

- 差分伝播の信頼性が不明
- Projection 開発者が incremental apply の実装方法を判断できない
- full rebuild との整合性が保証されない

## 提案

### Watermark 基準

- 既定は `recordedAt` + `id` の複合 watermark を使用する
- `recordedAt` で大まかなフィルタリングを行い、`id`（UUID v7）でバイト順の確定的な切り点を定める
- Late arrival（`published` < watermark だが `recordedAt` > watermark）は incremental apply で自然に拾える

### 複数 source の watermark

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

### Incremental / Full Rebuild の判定

| Projection パターン | Incremental Apply | 推奨戦略 |
|---|---|---|
| Append-only 集約（新規レコードの追加のみ） | 容易 | watermark 以降の record を追加適用 |
| Window 集計（時間窓別カウント等） | 可能（影響窓のみ再計算） | 影響する window のみ再集計 |
| Global 集計（全体平均、全体ランキング等） | 困難 | scheduled full rebuild + incremental cache の併用 |
| Graph 構築（ノード・エッジ追加） | 可能（差分のみ追加） | 新規ノード・エッジの追加適用 |
| Identity resolution（名寄せ） | 部分的 | 新規候補の追加は incremental、merge 判定は scheduled rebuild |

### 整合性検証

- 公開系 Projection は定期的に full rebuild と incremental 結果の差分検証を行う
- 差分が検知された場合は `DeterminismFailure` として報告し、full rebuild を実行する

---

## ユーザー回答

この仕様で良いと思います。

---

## 次のアクション

- domain_algebra.md に watermark 仕様を追加
