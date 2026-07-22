## Why

監査(`docs/development/principles-audit-20260722.md`)の**プライバシーフェーズ**。個人データレイクの信頼の根幹である consent 境界・撤回・契約明示が実装で形骸化している。

- **B-09:** Schema Registry の payload 検証が実質素通し。Claude/Slack/Gmail/GitHub 等の schema がほぼ `{"type":"object"}` のみで required/型/format 制約も source contract もない(`registry.rs:424`)。validator は動くが入力 schema が空疎(`ingestion.rs:470`)。契約明示性の原理に反し、`user_id` 欠落のような不正 Observation が canonical 化され下流に防御分岐を強いる。
- **B-10:** append 前 consent gate が常に `Role::SystemAdmin` / `AccessScope::Internal` / `ConsentStatus::RestrictedCapture` の定数で評価される(`ingestion.rs:168`)。`IngestRequest` に caller consent field すらない(`ingestion.rs:23`)。opt-out・失効・channel 固有 status が append 前 policy に反映されず quarantine 契約が形骸化する。
- **B-11:** retraction が projection に反映されない。Slack delete は `meta.retracts` に Observation ID でなく subject 文字列を入れ(`mapper.rs:80`)、PersonalAllText は `meta.retracts` も `observation.consent` も参照しない(`corpus/lib.rs:588`)。canonical を消さない原理は守れても公開 projection から撤回対象が消えない。
- **C-13:** `personal_all_text` は `read:corpus` scope だけで全文へ到達し、record 単位の consent/retraction filtering が CorpusProjector に組み込まれていない。
- **B-12(限定):** consent/retraction/blob 認可判定の**監査証跡**のみを対象とする。durability 機構は append-commit-and-lock-split(ADC-01〜03)の責務で重複しない。

## What Changes

- 各 observation schema に宣言フィールドの実 payload JSON Schema を持たせる。宣言必須の欠落・型違反は item エラー、宣言外の余剰フィールドは受理・保存し projection が宣言フィールドのみ読む(厳格性を取り込みゲートから projection 契約へ移す)。過去レイクデータは再検証せず version-gated・API バージョニングで新 version から適用する。
- append 前 consent gate を定数評価から実 consent-decision 記録(既存 supplemental kind)評価へ置き換え、consent 変更の反映鮮度を契約する。
- `meta.retracts` を typed metadata(Observation ID / object ID 逆 index)にし、retraction を corpus/検索/通信 projection へ増分反映(物理削除でなく遮蔽)し、遮蔽の完全性を検証する。
- 可視性モデルを **consent scope 単位**で本 change が正として定義し(indexed-keyset-reads C2Q5 の委譲を解決)、可視 blob 表のキーとする。
- consent/retraction/blob 認可判定の監査証跡**内容**を規定し、durability は append-commit の ADC に委譲する。

各設計判断の原理導出は design.md に明記する。原理から導出できず運用選択に委ねた項はオーナー確定済み(design.md「確定事項」、2026-07-23)。

## Capabilities

### New Capabilities

- `observation-schema-strictness`: 宣言フィールド検証(欠落・型違反は item エラー)+ 宣言外余剰フィールドの受理・保存 + version-gated 移行(B-09)。
- `consent-gate-evaluation`: 実 consent-decision 評価と反映鮮度契約(B-10)。
- `retraction-projection-shielding`: typed retraction の projection 増分遮蔽と完全性検証(B-11 / C-13)。
- `consent-scoped-blob-visibility`: consent scope 単位の可視性モデルを正として定義(C-13 / indexed-keyset C2Q5)。
- `privacy-decision-audit-trail`: consent/retraction/blob 判定の監査証跡内容(B-12 限定、durability は ADC 委譲)。

### Modified Capabilities

なし。`registry` の payload_schema/version 規則、`governance` の consent model / filtering-before-exposure、`corpus-projection` の filter 契約、`supplemental-store` の consent cascade を変更せず、prose 契約を取り込み・projection・認可経路で強制可能にする新規 capability を定義する。

## Impact

- 主対象: `apps/selfhost/src/self_host/registry.rs`、`crates/engine/src/lake/ingestion.rs`、`crates/adapters/slack/src/slack/mapper.rs`、`crates/projections/corpus/src/lib.rs`、`crates/policy/src/governance/`。
- System Laws: Append-Only Law(canonical 保持・projection 遮蔽)、Filtering-before-Exposure Law(公開前 filtering)、Replay Law(可視表・遮蔽状態は canonical + consent から再構築可能)、Explicit Authority Law(strict 契約)を維持・強化する。
- 対象外: client 実装、本番 selfhost デプロイ、既存 `data/` の再検証。

## Non-goals

- 性能改修(lock 分割・索引 — append-commit-and-lock-split / indexed-keyset-reads)。
- 取り込み応答契約(per-item 応答・identity・partial success — ingestion-api-contract)。
- audit durability 機構そのもの(append-commit-and-lock-split ADC)。本 change は監査**内容**のみ。
- 可視 blob 参照表の索引実装・O(1) 認可経路(indexed-keyset-reads BAI)。本 change はその**キー=consent scope**と retraction 連動を定義する。
- 外部共有・マルチユーザー化・年度末 opt-out batch。
