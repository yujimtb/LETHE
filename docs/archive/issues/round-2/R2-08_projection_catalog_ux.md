# Issue R2-8: Projection Catalog の Discovery UX

**Labels:** [IMPL]
**Priority:** Medium
**Status:** Approved

---

## 問題

Projection Catalog は DB on DBs を実現するための重要なインフラだが、**ユーザーが既存の Projection を発見し、自分の Projection の source として利用するための具体的な UX**が未定義。

## 影響

- Projection の再利用が促進されない
- DB on DBs の実際の利用体験が不明

## 提案

### 最小限の Catalog API

| Endpoint | Method | Response |
|---|---|---|
| `/api/catalog/projections` | GET | 全 Projection 一覧（id, name, tags, status, version） |
| `/api/catalog/projections/{id}` | GET | 詳細（sources, outputs, readModes, lineage） |
| `/api/catalog/projections/{id}/dependents` | GET | この Projection を source にしている下流 Projection |
| `/api/catalog/search?q=...` | GET | tag / name / description でのフリーテキスト検索 |
| `/api/catalog/dag` | GET | DAG 全体の構造（nodes + edges） |

### 表示すべき情報

- Projection の目的と内容の説明
- 入力 source（Lake / Supplemental / 他 Projection）
- 出力テーブルとカラム定義
- 対応 read mode
- 最終 build 時刻と health status
- downstream dependency count
- DOI（付与済みの場合）

---

## ユーザー回答

MVPとしてはこれで良い感じだと思います。

---

## 次のアクション

- Catalog API の仕様策定
