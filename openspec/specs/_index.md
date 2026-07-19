# LETHE Module Spec Index

**Version:** 1.0
**Date:** 2026-03-10

## Purpose

この文書は LETHE の仕様をモジュール単位に分割した一覧と、モジュール間の依存関係、マルチエージェント開発における担当マッピングを定義する。

各モジュール仕様は `openspec/specs/` に配置され、OpenSpec の change workflow から参照される。

---

## Module Map

| # | Module | Spec File | Scope | MVP? |
|---|---|---|---|---|
| M01 | Domain Kernel | [domain-kernel.md](domain-kernel.md) | 型・law・failure model | ✓ |
| M02 | Registry | [registry.md](registry.md) | EntityType / Schema / Observer / Catalog | ✓ |
| M03 | Observation Lake | [observation-lake.md](observation-lake.md) | Canonical capture / ingestion / storage | ✓ |
| M04 | Supplemental Store | [supplemental-store.md](supplemental-store.md) | 派生情報ストア | ✓ |
| M05 | Projection Engine | [projection-engine.md](projection-engine.md) | Projection 意味論 / spec / lifecycle | ✓ |
| M06 | DAG Propagation | [dag-propagation.md](dag-propagation.md) | Incremental propagation / watermark | ✓ |
| M07 | Write-Back | [write-back.md](write-back.md) | Command algebra / writable projections | — |
| M08 | Governance | [governance.md](governance.md) | Consent / access / review / retention | ✓(min) |
| M09 | Adapter Policy | [adapter-policy.md](adapter-policy.md) | Source adapter 共通パターン | ✓ |
| M10 | Slack Adapter | [slack-adapter.md](slack-adapter.md) | Slack source adapter (MVP) | ✓ |
| M11 | Google Slides Adapter | [google-slides-adapter.md](google-slides-adapter.md) | Google Slides source adapter (MVP) | ✓ |
| M12 | Identity Resolution | [identity-resolution.md](identity-resolution.md) | 名寄せ Projection (MVP) | ✓ |
| M13 | Person Page | [person-page.md](person-page.md) | 個人ページ Projection & API (MVP) | ✓ |
| M14 | API Serving | [api-serving.md](api-serving.md) | API layer / read modes / serving | ✓ |
| M15 | Runtime | [runtime.md](runtime.md) | Topology / sandbox / reference stack | ✓(min) |
| M16 | Platform Generalization | [platform-generalization.md](platform-generalization.md) | ドメイン分離 / plugin / storage port | — |
| M17 | Platform Robustness | [platform-robustness.md](platform-robustness.md) | authn / 冪等 / 失敗隔離 / 運用品質 | — |
| M18 | Operational Event Ledger | [operational-event-ledger.md](operational-event-ledger.md) | Nanihold event / DataSpace / backend cutover | ✓ |
| M19 | Personal History Ingestion | [history-ingestion.md](history-ingestion.md) | Native conversation import / receipt / bounded Nanihold read | ✓ |

---

## Dependency DAG

```
M01 Domain Kernel ─────────────────────────────────────────────────
  │                                                                 
  ├──► M02 Registry ──────────────────────────────────────────────  
  │      │                                                          
  │      ├──► M03 Observation Lake ──────────────────────────────  
  │      │      │                                                   
  │      │      ├──► M04 Supplemental Store                        
  │      │      │                                                   
  │      │      └──► M09 Adapter Policy ──┬──► M10 Slack Adapter   
  │      │                                └──► M11 GSlides Adapter 
  │      │                                                          
  │      └──► M05 Projection Engine ─────────────────────────────  
  │             │                                                   
  │             ├──► M06 DAG Propagation                           
  │             │                                                   
  │             ├──► M07 Write-Back                                
  │             │                                                   
  │             ├──► M12 Identity Resolution ──► M13 Person Page   
  │             │                                                   
  │             └──► M14 API Serving                               
  │                                                                 
  ├──► M08 Governance (cross-cutting)                              
  │                                                                 
  ├──► M15 Runtime (cross-cutting)                                 
  │                                                                 
  ├──► M16 Platform Generalization (cross-cutting; depends on M01/M02/M09)
  │                                                                 
  ├──► M17 Platform Robustness (cross-cutting; depends on M03/M08/M14/M15)
  │
  └──► M18 Operational Event Ledger (depends on M01/M03/M08/M14)
          │
          └──► M19 Personal History Ingestion (depends on M18)
```

### 依存関係の読み方

- 矢印の先（dependent）は、矢印の元（dependency）の仕様が確定していることを前提とする
- **cross-cutting** なモジュール（Governance, Runtime）は全モジュールから参照される
- 同じ深さのモジュールは **並行開発可能**

---

## Parallel Development Lanes

マルチエージェントで並行開発するための推奨レーン分割:

**開始条件:** Lane B/C に入る前に、M01 Domain Kernel と M08 Governance の **normative contract** を凍結する。

### Lane A: Platform Foundation

```
M01 → M02 → M03 → M04
```

| Phase | Module | Agent | Deliverable |
|---|---|---|---|
| A-1 | M01 Domain Kernel | Spec Designer | 型定義・law 確定 |
| A-2 | M02 Registry | Implementer | Registry DB + API |
| A-3 | M03 Observation Lake | Implementer | Lake append + ingestion gate |
| A-4 | M04 Supplemental Store | Implementer | Supplemental CRUD |

### Lane B: Source Adapters (after M03)

```
M09 → M10 (Slack)
M09 → M11 (Google Slides)
```

| Phase | Module | Agent | Deliverable |
|---|---|---|---|
| B-1 | M09 Adapter Policy | Spec Designer | 共通 adapter contract |
| B-2a | M10 Slack Adapter | Implementer | Slack crawler + Observation 生成 |
| B-2b | M11 GSlides Adapter | Implementer | GSlides crawler + Observation 生成 |

### Lane C: Projection & API (after M03, M05)

```
M05 → M06
M05 → M12 → M13
M05 → M14
```

| Phase | Module | Agent | Deliverable |
|---|---|---|---|
| C-1 | M05 Projection Engine | Implementer | Projector runner + catalog |
| C-2a | M06 DAG Propagation | Implementer | Watermark + incremental apply |
| C-2b | M12 Identity Resolution | Implementer | 名寄せ projector |
| C-3 | M13 Person Page | Implementer | Person page API |
| C-2c | M14 API Serving | Implementer | FastAPI serving layer |

### Lane D: Governance & Runtime (phase-0 freeze + parallel implementation)

```
M08 Governance
M15 Runtime
```

| Phase | Module | Agent | Deliverable |
|---|---|---|---|
| D-0 | M08 Governance | Spec Designer | confidence threshold、DualReference precedence、approval SLA を固定 |
| D-1 | M08 Governance | Implementer | 最小 policy engine |
| D-2 | M15 Runtime | Implementer | Docker / sandbox / CI |

---

## Parallel Merge Gates

各レーンは「着手」は並行に行えても、**merge は以下の handoff gate を満たしてから** とする。

| Gate | Required deliverable | Unblocked modules |
|---|---|---|
| G0 Semantic freeze | M01 law / failure routing + M08 confidence policy / DualReference precedence / write approval SLA | 全レーン |
| G1 Registry contract | M02 の Schema Registry、Observer / Source Contract API、adapter-version binding | M03, M05, M09 |
| G2 Lake contract | M03 の append API、`since` / watermark API、temporal validation、governance quarantine surface | M04, M05, M10, M11 |
| G3 Supplemental contract | M04 の version-pinned read、consent metadata、retraction linkage | M05, M12, M13 |
| G4 Projection contract | M05 の source declaration validator、reconciliation policy、lineage manifest | M06, M07, M12, M13, M14 |
| G5 Identity contract | M12 の `resolved_persons` / `resolution_candidates` 境界、confidence approval rule | M13, M14 |

---

## MVP Implementation Order

```
1. M01 Domain Kernel         ← 型を確定 (前提)
2. M08 Governance (spec)     ← policy matrix / approval SLA を固定
3. M02 Registry              ← 最小 Registry
4. M03 Observation Lake      ← append API + quarantine gate
5. M09 Adapter Policy        ← adapter contract
6. M10 Slack Adapter   ┐
   M11 GSlides Adapter ┘    ← 並行実装可能
7. M04 Supplemental Store    ← OCR/transcript 保存 + version pin
8. M05 Projection Engine     ← projector runner + reconciliation
9. M12 Identity Resolution   ← 名寄せ
10. M14 API Serving          ← serving layer
11. M06 DAG Propagation      ← incremental 伝播
12. M13 Person Page          ← 個人ページ API
13. M08 Governance (engine)  ← internal-only + audit
14. M15 Runtime (min)        ← local sandbox
15. M07 Write-Back           ← post-MVP / contract freeze 後
16. M16 Platform Generalization ← domain vocabulary / storage port / adapter contract
17. M17 Platform Robustness     ← authn / resumable sync / audit / resource limits
```

---

## Agent Role Mapping

| Module | Spec Designer | Implementer | Reviewer |
|---|---|---|---|
| M01 Domain Kernel | ✓ 型・law 定義 | — | law 整合性検証 |
| M02 Registry | ✓ schema 設計 | DB + API 実装 | 制約検証 |
| M03 Observation Lake | ✓ ingestion 契約 | append + gate 実装 | append-only 検証 |
| M04 Supplemental Store | ✓ mutability policy | CRUD 実装 | lineage 検証 |
| M05 Projection Engine | ✓ spec format | runner 実装 | replay 検証 |
| M06 DAG Propagation | ✓ watermark 仕様 | propagation 実装 | determinism 検証 |
| M07 Write-Back | ✓ command algebra | adapter 実装 | authority 検証 |
| M08 Governance | ✓ policy 設計 | policy engine 実装 | exposure 検証 |
| M09 Adapter Policy | ✓ 共通契約 | — | contract 検証 |
| M10 Slack Adapter | ✓ schema + contract | API client 実装 | idempotency 検証 |
| M11 GSlides Adapter | ✓ capture 仕様 | API client 実装 | snapshot 検証 |
| M12 Identity Resolution | ✓ 名寄せ spec | projector 実装 | confidence 検証 |
| M13 Person Page | ✓ API 契約 | endpoint 実装 | exposure 検証 |
| M14 API Serving | ✓ read mode 契約 | serving 実装 | mode 検証 |
| M15 Runtime | ✓ topology 設計 | infra 実装 | isolation 検証 |

---

## Cross-Reference Rules

1. 各モジュール spec は冒頭に `Dependencies` セクションを持ち、依存モジュールを明記する
2. System Law への参照は M01 Domain Kernel を正規参照先とする
3. Failure model への参照は M01 Domain Kernel を正規参照先とする
4. Governance policy への参照は M08 Governance を正規参照先とする
5. runtime 詳細への参照は M15 Runtime を正規参照先とする
6. 親仕様文書（`docs/architecture/`）は authoritative overview として保持する

---

## Relationship to Parent Documents

| Parent Document | Extracted Modules | Status |
|---|---|---|
| `docs/architecture/system-overview.md` | M02, M03, M05, M07, M09-M14 | 親仕様として保持。モジュール spec が詳細を持つ |
| `docs/architecture/domain-algebra.md` | M01, M04, M06, M07 | 意味論の正規参照。モジュール spec が実装寄り詳細を持つ |
| `docs/architecture/governance-capability-model.md` | M08 | policy の正規参照 |
| `docs/architecture/runtime-reference.md` | M15 | runtime の正規参照 |
| `docs/archive/issues/round-2/R2-*.md` | M06, M10, M11, M12, M13 | 解決済みIssueの履歴 |
