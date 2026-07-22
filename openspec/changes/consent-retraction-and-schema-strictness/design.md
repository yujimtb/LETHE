## Context

監査 A 章の第一原理と B/C 章の違反を、プライバシー境界に絞って照合する。本 change の全設計判断は次の 2 原則から導出し、原則から導出できず運用選択に委ねる項はオーナー確定済み(下記「確定事項」)。

**原則 (1) LETHE 第一原理**(A 章):

- **A-1 Append-Only**: canonical Observation は更新・削除しない。訂正 `meta.corrects`、撤回 `meta.retracts`、opt-out は consent ledger + filtering projection。
- **A-2 Lake 正 / Projection 派生**: projection は破棄・再生可能。
- **A-3 決定論的 fold・増分伝播**: projection は watermark 以降のみ処理する純粋 fold。
- **A-6 契約明示性**: Registry と strict validation で payload/version/source contract を明示。全 Observation が schema に適合しなければならない。
- **A-9 Consent 両境界**: policy/consent 違反は append 前に quarantine、restricted data は公開前に filtering projection を通す。
- **A-10 台帳リプレイ**: 状態は台帳・永続 materialization から再構築でき、hidden mutable state に依存しない。

**原則 (2) スケーラビリティ原則**:

- 運用上の自由選択(既定値・鮮度・cadence 等)は**スケールで判断**する。
- 製品境界を厳格に保つ。**クライアントは任意接続前提**であり、可視性・consent は client 個別合意でなく projection と consent scope で強制する。

**現状(実コード):**

- **B-09:** registry の各 schema がほぼ `{"type":"object"}`、source_contracts 空(`registry.rs:424`)。JSON Schema validator は動くが入力 schema が空疎(`ingestion.rs:470`)。
- **B-10:** append 前 policy は定数 `SystemAdmin`/`Internal`/`RestrictedCapture` で評価、`IngestRequest` に consent field なし(`ingestion.rs:23/168`)。
- **B-11:** Slack delete が `meta.retracts` に subject 文字列(`mapper.rs:80`)、PersonalAllText は `meta.retracts`/`observation.consent` 非参照(`corpus/lib.rs:588`)。deploy は `personal_all_text`(`config.toml:84`)。
- **C-13:** `personal_all_text` は `read:corpus` だけで全文到達(C 章 13)。
- **既存資産:** consent-decision は `schema:consent-decision` の supplemental kind で `unrestricted`/`restricted_capture`/`opted_out` を明示、既定 `restricted_capture`、最新 decision が正(governance §11)。supplemental の consent cascade(保持 + filtering、`supplemental-store` §5)。過去 Observation は書込時 schemaVersion を永久保持(registry §3.4)。

## Goals / Non-Goals

**Goals:** strict payload 検証(B-09)、実 consent 評価(B-10)、retraction の projection 増分遮蔽と完全性(B-11/C-13)、consent scope 可視性モデルの確定(C2Q5)、consent/retraction/blob 判定の監査証跡内容(B-12 限定)。

**Non-Goals:** 性能(append-commit-and-lock-split / indexed-keyset-reads)、取り込み応答契約(ingestion-api-contract)、audit durability 機構、可視表の索引実装、外部共有・マルチユーザー化。

## Decisions

各判断に導出原則を明記する。

### D1: 宣言フィールド検証 + 宣言外フィールド受理(SSV-01)← A-1 + A-6

厳格性の置き場所を取り込みゲートから projection 契約へ移す(オーナー確定 Q1「Projection でどうにかなる」)。schema は projection が読む宣言フィールドの required・型・format を定義し、(a) 宣言必須の欠落・型違反は item エラー(fold が安全に読めないため canonical append しない)、(b) 宣言外の余剰フィールドは受理・保存し拒否も隔離もしない、(c) 余剰の発生を `import_timing` 等で計測して可観測化する。projection は宣言フィールドのみを読む(IM-03 の fold 宣言と整合)。**A-1 append-only**(データは受けて失わない)と **A-6 契約明示性**(読む側が契約を宣言する)の両立。空疎な `{"type":"object"}` は廃止するが、それは余剰拒否のためでなく宣言フィールドに実制約を与えるため。quarantine 領域の新設はしない。

### D2: version-gated 厳格化と既存データ非再検証(SSV-02)← A-1 + A-6

strict schema は**新 version**として登録し、過去 Observation はその書込時 schemaVersion のまま再検証しない(**A-1 append-only**: canonical を遡って書き換えない、registry §3.4「過去 Observation は schemaVersion を永久保持」)。新規取り込みのみ新 strict version で検証する。**A-6** は「今後の契約を明示せよ」であり過去の遡及検証は要求しない。strict 版の rollout は API バージョニングで行う(オーナー確定 Q6、既定どおり)。移行は schema major/minor bump 規則(registry §3.4)に従う。

### D3: 通信 metadata 等の必須 field 契約(SSV-03)← A-6

channel kind・source instance・external ID・sender/thread metadata など、欠落が quarantine または下流不整合になる field(C 章 14)を strict schema の required にする。**A-6**: base JSON Schema で必須化されていなかった暗黙契約を明示契約へ引き上げる。

### D4: append 前 gate が実 consent-decision を評価(CGE-01)← A-9 + A-10

append 前 policy を定数評価から、subject/channel の実 consent-decision(`schema:consent-decision` supplemental)評価へ置き換える。最新 decision を正とし(governance §11)、未登録は既定 `restricted_capture`。consent 違反・opt-out は明示 quarantine とする。**A-9 capture 時境界**の直接適用。最新 decision を正とするのは **A-10 台帳リプレイ**(consent-decision ledger の fold 結果が状態)。`IngestRequest` に consent 解決経路を通す。

### D5: consent 変更の反映鮮度契約(CGE-02)← A-9 + A-3

consent 変更の反映を 2 境界で契約する。(a) **capture gate**: 評価時点で解決済みの最新 decision を使う(**A-9**、correctness 境界のため鮮度は「評価時最新」)。(b) **公開 projection 反映**: consent delta を watermark 増分で反映する(**A-3** 増分 fold)。projection 反映の許容 staleness は communication-projection IM-05 と統一して**通常時 5 秒以内**で確定する(オーナー確定 Q2)。

### D6: typed retraction と projection 増分遮蔽(RPS-01)← A-1 + A-3

`meta.retracts` を typed metadata(target Observation ID / source object ID の逆 index)にし、subject 文字列直入れ(`mapper.rs:80`)を廃止する。retraction 記録を corpus / 検索 / 通信 projection へ watermark 増分で反映し、対象 record を同一 commit で projection から遮蔽する。**A-1**「物理削除でなく filtering projection で撤回を表現」+ **A-3** 増分 fold の直接適用。canonical Lake の Observation は保持する。撤回は projection 遮蔽方式のまま採り crypto-erasure 等の物理消去は採用しない(オーナー確定 Q5)。un-retract(遮蔽解除)は認めず、公開再開は新たな明示 consent 記録の追記として別途表現する(**A-1** append-only: 遮蔽も復活も追記で表現)。

### D7: 遮蔽の完全性検証(RPS-02)← A-9 + A-2

retract 対象が corpus record・検索 index・通信 projection・可視 blob の全公開経路から到達不能であることを検証する。**A-9 公開時境界**(filtering-before-exposure)+ **A-2**(projection は canonical + retraction から決定的に再構築可能)。CorpusProjector に consent/retraction filtering を組み込み、`read:corpus` だけで撤回対象へ到達する C-13 を塞ぐ。検証 cadence は**毎 commit の増分検証 + on-demand の full 検証**で確定し、全観測走査の定期(日次)full 検証はスケールしないため行わない(オーナー確定 Q3)。

### D8: record/subject 細粒度の可視性モデルを正として定義(CBV-01)← A-9 + スケーラビリティ(製品境界)

可視性モデルを consent scope(人物・artifact・space・group・external partner、governance §4.1)を基礎に **record / subject 細粒度を最初から**備える形で定義し、これを本 change の**正**とする(オーナー確定 Q4)。indexed-keyset-reads の C2Q5(可視 blob 参照表の粒度)を record / subject 細粒度で解決し、scope 単位のみで開始して後から細分化する段階設計は採らない。**A-9**: 可視性は consent 境界に一致する。**スケーラビリティ原則(製品境界厳格・client 任意接続前提)**: 可視性は接続 client 個別でなく projection 側で record / subject 細粒度に強制する。

### D9: 可視 blob 表を record/subject 細粒度でキー化し retraction と連動(CBV-02)← A-9 + A-2

可視 blob 参照表を record / subject 細粒度(consent scope 下)でキー化し、consent delta・retraction と同一 commit で upsert/delete する。**表の索引実装・O(1) 認可経路は indexed-keyset-reads(BAI-01/02)の責務**であり、本 change はその表の**キー(=record/subject 細粒度)と retraction/consent 連動の意味論**を正として定義する。**A-9** filtering-before-exposure + **A-2** 再構築可能性(canonical + consent 状態から決定的に再構築)。

### D10: consent/retraction/blob 判定の監査証跡内容(PDA-01)← A-9 + governance(auditable decisions)

consent gate 判定・retraction 遮蔽・blob 認可判定は、actor・対象 subject/scope・decision・適用 rule・timestamp を含む監査証跡を生成する(governance §6.3/§9)。**A-9** consent 境界の追跡可能性 + governance「auditable decisions」。

### D11: durability は append-commit ADC へ委譲(PDA-02)← 依存明記

監査証跡の durable 化・fail-closed・in-memory mirror 廃止は append-commit-and-lock-split の ADC-01/02/03 の責務であり、本 change はそれに**依存**し重複させない。本 change は「何を記録するか(内容)」、ADC は「どう durable に記録するか(機構)」を定義する。

## Risks / Trade-offs

- **[strict 化が既存取り込みを壊す]** → version-gated(D2)で新 version のみ厳格。既存データ・旧 version 取り込みは意味論凍結。宣言外の余剰は受理・保存(確定 Q1)のため既存 payload を壊さない。rollout は API バージョニング(確定 Q6)。
- **[consent 評価の追加コスト]** → subject/channel decision lookup は O(1)〜O(log C)。索引は indexed-keyset-reads に整合。
- **[retraction 逆 index 維持]** → 増分維持は append-commit-and-lock-split の consumer / indexed-keyset-reads の索引に載る。本 change は遮蔽の意味論と完全性を定義。
- **[可視表キー変更が index に及ぶ]** → キー=record/subject 細粒度(確定 Q4)の確定は indexed-keyset C2Q5 の前提。表実装は同 change、本 change はキー意味論。
- **[余剰フィールドの下流負荷]** → 余剰は projection が読まないため fold は宣言フィールドのみで安定。余剰発生を `import_timing` 計測で可観測化(確定 Q1(c))。

## Dependencies / スコープ重複の回避

- **append-commit-and-lock-split:** audit durability 機構(ADC-01/02/03)・commit 境界(CAB)・派生 consumer 経路を提供。本 change は consent/retraction/blob 判定の監査**内容**(PDA)と projection 遮蔽の増分反映**意味論**(RPS)を載せ、durability を重複させない。
- **indexed-keyset-reads:** 可視 blob 参照表(BAI-01/02)の索引・O(1) 認可・再構築可能性を提供。本 change はその表の**キー=consent scope**(CBV)と retraction 連動を正として定義し C2Q5 を解決する。
- **ingestion-api-contract:** 取り込み応答契約・identity。B-09 strict validation は同 change では error 分類として**参照**のみ。strict schema 本体の定義は本 change(SSV)。
- **communication-projection:** reply-SLO / 通信 projection のデータモデルは同 change。本 change はその projection へ retraction 遮蔽(RPS)を課すのみ。
- **corpus-projection(spec):** CORPUS-06 filtering-before-exposure / CORPUS-07 personal_all_text。本 change は consent/retraction filtering を CorpusProjector に組み込む契約(RPS-01)で C-13 を解決する。
- **registry / governance / supplemental-store(spec):** payload_schema・version 規則 / consent model・filtering-before-exposure・audit events / consent cascade を変えず、取り込み・projection・認可経路で強制可能にする。

## 確定事項(オーナー確定 2026-07-23)

以前の Open Questions は全てオーナー確定した。各項の原則導出も併記する。残余の Open Question はない。

1. **Q1 宣言外フィールドの扱い(重要な設計転換):** 未知の余剰フィールドは拒否も隔離もせず**受理・保存**する(オーナー:「Projection でどうにかなる」)。厳格性の置き場所を取り込みゲートから projection 契約へ移す。宣言必須の欠落・型違反のみ item エラー、余剰は保存、projection は宣言フィールドのみ読む、余剰発生は計測で可観測化。quarantine 領域の新設はしない。→ D1 / SSV-01。**A-1 append-only(受けて失わない)+ A-6 契約明示性(読む側が宣言)**の両立として導出。
2. **Q2 consent 反映鮮度:** 通常時 **5 秒以内**(communication-projection IM-05 の鮮度契約と統一)。→ D5 / CGE-02。capture gate の評価時最新性は A-9 から導出、projection 反映値はスケール判断で IM-05 と統一。
3. **Q3 遮蔽完全性検証 cadence:** **毎 commit の増分検証 + on-demand の full 検証**。全観測走査の定期(日次)full 検証は「スケールしない」ため不採用。→ D7 / RPS-02。スケーラビリティ原則で導出。
4. **Q4 可視性粒度:** **record / subject 細粒度を最初から**設計する。scope 単位開始→後から細分化はしない。→ D8 / CBV-01/02。A-9 consent 境界を細粒度で導出。
5. **Q5 撤回方式・un-retract:** crypto-erasure は採用せず **projection 遮蔽方式(現行 RPS)のまま**。un-retract は認めず、復活は新たな明示 consent 記録として別途表現。→ D6 / RPS-03。A-1 append-only で導出(遮蔽・復活とも追記で表現)。
6. **Q6 rollout:** **API バージョニング**(既定どおり)。→ D2 / SSV-02。契約明示性 + 既定移行方式。
