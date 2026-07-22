## 1. schema strict 化(B-09)

- [ ] 1.1 [Spec Designer] `observation-schema-strictness` SSV-01/SSV-03 に従い、既存 registered schema(`registry.rs:424`)ごとに projection が読む宣言フィールドの required・型・format・source contract を実 payload JSON Schema として設計する。宣言外の余剰フィールドは受理・保存(確定 Q1)とし拒否/隔離要件を設けない。受入: Slack/Claude/GitHub/Gmail 等の各 schema に空疎でない宣言フィールド契約が定まり、通信 metadata の required(C 章 14)が明示される。
- [ ] 1.2 [Implementer] SSV-01 に従い取り込み経路(`ingestion.rs:470`)で宣言必須の欠落・型違反を item エラーにし、宣言外の余剰フィールドは受理・保存する。余剰の発生を `import_timing` 等で計測する。受入: 宣言必須欠落 item がエラーで append されず、余剰フィールド付き payload は保存され、余剰が計測されるテストが通る。
- [ ] 1.3 [Implementer] SSV-02 に従い strict schema を新 version として登録し(rollout は API バージョニング、確定 Q6)、過去 Observation を再検証せず書込時 schemaVersion で保持する(registry version 規則)。受入: 新 strict version が新規取り込みのみに適用され既存データを遡及検証しないテストが通る。

## 2. consent gate 実評価(B-10)

- [ ] 2.1 [Spec Designer] `consent-gate-evaluation` CGE-01 に従い、`IngestRequest`(`ingestion.rs:23`)に subject/channel consent 解決経路を通す契約を確定する。受入: 定数評価(`ingestion.rs:168`)を廃止し実 consent-decision を解決する gate 署名が定まる。
- [ ] 2.2 [Implementer] CGE-01 に従い append 前 gate を実 consent-decision(`schema:consent-decision` supplemental)評価へ置き換え、最新 decision を正・未登録既定 `restricted_capture`・違反を明示 quarantine にする。受入: opt-out subject の Observation が quarantine され、consent 未登録が restricted_capture で評価されるテストが通る。
- [ ] 2.3 [Implementer] CGE-02(確定 Q2)に従い公開 projection への consent delta 反映を通常時 5 秒以内(communication-projection IM-05 と統一)で実装・契約明示する。受入: capture gate の評価時最新性と 5 秒鮮度が分離して満たされるテストが通る。

## 3. retraction の projection 遮蔽(B-11 / C-13)

- [ ] 3.1 [Implementer] `retraction-projection-shielding` RPS-01 に従い、`meta.retracts` を target Observation ID / object ID の typed metadata にし、Slack mapper の subject 文字列直入れ(`mapper.rs:80`)を廃止する。受入: retract が typed に表現され逆 index 可能になるテストが通る。
- [ ] 3.2 [Implementer] RPS-01/RPS-02 に従い CorpusProjector(`corpus/lib.rs:588`)に consent / retraction filtering を組み込み、retraction を corpus / 検索 / 通信 projection へ増分反映して対象 record を遮蔽する。受入: `read:corpus` で personal_all_text を検索しても retract 対象が返らないテストが通る(C-13 解決)。
- [ ] 3.3 [Reviewer] RPS-02/RPS-03(確定 Q3/Q5)に従い遮蔽の完全性(全公開経路から到達不能)を毎 commit 増分検証 + on-demand full 検証で確認し、決定的再構築・un-retract 不採用・crypto-erasure 不採用を検証する。受入: retract 対象が corpus・検索・通信・可視 blob のいずれからも到達不能で、遮蔽状態が canonical + retraction から再構築でき、un-retract が honored されないことを確認する。

## 4. consent scope 可視性モデル(C-13 / C2Q5)

- [ ] 4.1 [Spec Designer] `consent-scoped-blob-visibility` CBV-01(確定 Q4)に従い record/subject 細粒度の可視性モデルを正として確定し、indexed-keyset-reads C2Q5 を record/subject 細粒度で解決する。受入: 可視 blob 参照表のキー粒度が record/subject 細粒度で確定し indexed-keyset 側と整合する。
- [ ] 4.2 [Implementer] CBV-02 に従い可視 blob 参照表を record/subject 細粒度(consent scope 下)でキー化し、consent delta・retraction と同一 commit で upsert/delete する(索引実装は indexed-keyset-reads BAI に委譲)。受入: consent/retraction 変更が同一 commit で可視表に反映され決定的に再構築できるテストが通る。

## 5. privacy 判定の監査証跡(B-12 限定)

- [ ] 5.1 [Implementer] `privacy-decision-audit-trail` PDA-01 に従い consent gate / retraction 遮蔽 / blob 認可判定に actor・subject/scope・decision・rule・timestamp の監査証跡内容を付与する(`crates/policy/src/governance/`)。受入: 3 判定経路が規定内容の監査証跡を生成するテストが通る。
- [ ] 5.2 [Reviewer] PDA-02 に従い durability 機構(fail-closed・mirror 廃止・commit 境界)を append-commit-and-lock-split ADC-01/02/03 に委譲し重複定義がないことを確認する。受入: 本 change が監査内容のみを定義し ADC と重複しないことを確認する。

## 6. 検証と回帰

- [ ] 6.1 [Reviewer] SSV/CGE/RPS/CBV/PDA の各 spec について registry・governance・corpus-projection・supplemental-store の既存契約に回帰がないことを確認する。受入: 既存 spec の意味論が保たれ、strict 化・consent 評価・遮蔽が非破壊であることを確認する。
- [ ] 6.2 [Reviewer] workspace 全テスト、cargo fmt、clippy を実行する。受入: 全コマンド成功、既存テスト全緑。
