# LETHE Sharding Refactor — Decision Ledger & Implementation Plan

**Status:** D0–D12 すべて確定（LOCKED）。本ドキュメントは実装の正典。
**目的:** LETHE を大きめのワークスペース（寮 Slack）に投入するにあたり、複数インスタンス（物理 Lake 分割）を、ハードコードな mapping に頼らず・再現性（replay）を保ったまま・完全な冪等性を保証して導入する。重複排除（dedup）と routing を核に据える。
**正典との関係:** 意味論の正典は [System overview](system-overview.md) / [Domain algebra](domain-algebra.md) / [Runtime reference](runtime-reference.md) / [ADR backlog](../decisions/adr-backlog.md)。本ドキュメントはそれらに対する sharding 観点の確定事項であり、矛盾する旧記述（draft の確率的冪等性・SimHash routing・固定ビット trie 等）を上書きする。

---

## 0. このドキュメントの使い方

- **§1 横断不変条件**と**§2 確定事項（D0–D12）**は LOCKED。実装はこれに従う。各決定に根拠（なぜそう決めたか）とコード上の接地点（`file:line`）を併記してある。
- §1 の不変条件を破る変更は却下（破ると LETHE の System Laws が壊れる）。
- 現コードで確認できる事実はコードを読んで確認すること。設計意図は本ドキュメントが正典。
- 実装順序は §3（D12 フェーズ）に従う。

---

## 1. 横断不変条件（破ってはいけない）

1. **route は「現在 tree」の純関数。** タイミング・履歴・ingestion 時刻に依存しない。再取り込みされた同一メッセージは必ず現在の home leaf に着地 → dedup が割れない。
2. **routing key の時刻成分はすべて `published`（event time）。** `recordedAt`（ingestion time）は使わない。再 export を別の月/年に処理すると recordedAt が変わり別 leaf に route → 偽「新規」→ 重複挿入。published はメッセージ固有の不変値。
3. **identity は content hash を含む。** 無変更再取り込みは dedup、編集（同 object_id・新 content）は新 distinct Observation。偽 merge（silent loss）も偽 split（取りこぼし）も同時に塞ぐ。
4. **keyspec（routing/identity）はセットアップ時に確定し initialize で pin、以後不変。** 変更は in-place 不可、全件 re-ingest の migration（D12）。
5. **placement は routing 軸（coarse prune + 容量 balance）のみ。** 比較・結合・横断が必要な軸、および**解決を要する派生軸**（person, subject, project）は placement にも leaf-prune にも入れない。read-side の projection / supplemental index に追い出す（placement は1つ、index/projection は多数）。
6. **冪等性は exact（決定的）。** per-leaf exact index が正規判定（`Duplicate(existing_id)` を返す）。Bloom は冪等性の構成要素ではない（あっても負パス最適化のみ）。silent drop なし。
7. **dedup は routing key の一意性に依存しない。** identity（exact index）が dedup を、routing が placement を担う完全分離。
8. **watermark / propagation 検出の cursor は leaf-local の単調 append sequence（`append_seq`）。** cursor は physical append 順にのみ単調であればよく、`recordedAt` にも `published` にも依存しない。`recordedAt` を cursor にすると rehome（D8 drain / split 再配置 / blue-green）が元の古い `recordedAt` を保持して挿入したとき watermark frontier 下に沈み silent loss（「append 順 = recordedAt 順」は live fresh ingest でのみ成立し、rehome で破れる）。published も backfill で過去になり同様に沈む。→ **routing は published、watermark は leaf-local `append_seq`** を使い分ける。`recordedAt` は provenance として保持するが cursor には使わない。
9. **projection は可換かつ冪等な fold に限る。** 順序依存 fold は core がサポートしない。順序依存な導出（各時点スナップショット等）は read 時に materialized な可換集合の上で projection 作者が自前で組む（core はその状態を持たない）。
10. **stored Observation の rehome が唯一の migration primitive。** backfill / split 再配置 / failover drain / blue-green keyspec 変更はすべてこれ一つに還元される。rehome は stored Observation の `id` / `published` / `recordedAt` / `consent` を保持して現 tree へ再 route し、着地 leaf の identity UNIQUE に通す内部 append であり、adapter→fresh ingest（新 id・新 recordedAt を付与する通常経路）ではない（D12.1 の 2 レール契約）。

### 1.1 System Laws との対照

| Law (`domain_algebra §7.1`) | 本リファクタでの扱い |
| --- | --- |
| Append-Only | 保存（各 leaf も append-only、split は再配置で更新でない、blue/green は旧 retire） |
| Replay | 保存・強化（`tree(L)` 復元 + 全 leaf の merge-sort で決定的再現）。failover window は control-plane sequence で境界再現（D8）。 |
| Effect Isolation | 保存（routing/解決は Imperative Shell、Kernel は論理 lake のみ） |
| No Direct Mutation | 保存 |
| Filtering-before-Exposure | 影響なし（Projection 層で従来通り） |
| **Idempotency** | **強化**。確率的冪等性を撤回し、per-leaf exact index で完全（決定的）冪等に。衝突判定は full observation でなく canonical tuple のみを exact compare（D9.4）。 |
| Provenance Completeness | 保存（silent drop が無いので欠損が記録不能にならない） |

---

## 2. 確定事項（Decision Ledger D0–D12）

### D0 — スコープ：実シャーディングを今やる
分割機構まで実装する（contract 凍結だけで分割を後回しにしない）。split の transient window（D6）と failover（D8）は hard-correctness。

### D1 — 観測粒度：append 系は per-message
- 追記・チャット・AI 会話・ログ: 1 メッセージ/turn = 1 Observation。根拠: 会話丸ごと 1 Observation だと追記のたび content hash が変わり会話全体が新 snapshot として再保存され、full-dump 反復取り込みで保存量が会話長の二乗。per-message なら再取り込みで既存は dedup、新規のみ append → 保存量は distinct メッセージ数に線形。
- revision 文書（Slides/Docs/Sheets/Notion/Figma）: revisioned snapshot 維持（revision 数 ≪ メッセージ数）。
- sensor/高頻度: chunk-manifest（位置で同一性）。
- 会話のまとまり: Observation 単位でなく `conversation_id` でグループ化する Projection/subject。
- claude.ai 再 export の欠落は per-message append なら無害（既存は残る、欠けた分は no-op）。

### D2 — dedup 意味論：exact（決定的）
- `IngestResult = Ingested(id) | Duplicate(existing_id) | Rejected(...) | Quarantined(...)`。silent drop なし。現行も同型（[crates/engine/src/lake/ingestion.rs](../../crates/engine/src/lake/ingestion.rs)）。
- 正規判定は per-leaf exact index（D9）。

### D3 — identity key 契約
- `identity_key = source : object_id : H(canonical_content)`（**per-message**）。
- **object_id**: adapter が抽出を宣言。Slack `channel:ts`。claude.ai message `uuid`（欠落時は `conversation_uuid` + `parent_message_uuid` チェーン上の位置から決定的に導出）。generic source は adapter 宣言。
- **H**: sha256。exact index が content 実体を保持し hash 一致時に exact 比較（衝突時も偽 merge しない）。
- **編集 = 新 distinct Observation**。無変更再取り込み → 同 key → dedup。編集 → 新 content → 新 key → 新 Observation。両版は object_id で linkable、現在版は Projection の latest-by-published。identity をネイティブ ID 単独にすると Slack 編集が「duplicate」で捨てられる（silent 欠損）ため content hash 必須。

### D3b — canonical_content の境界
- **支配原則: 正規化は最小・保守的。迷ったら false-split（余分な版を1つ append、append-only 下で無害）を選び、false-merge（silent loss、取り返しつかない）は絶対回避。**
- **include:** `sender` / `body`（transport ノイズのみ正規化、ユーザ可視空白は保つ） / `event_time`（RFC3339 UTC 固定精度） / 添付の sha256（本体は CAS）。構造アンカー（parent/thread）は content でなく object_id 側。
- **exclude:** reactions 等の独立変化 side-state（別 Observation 列） / 編集 wrapper メタ / ingestion メタ（recordedAt, crawler cursor, export run id, claude.ai `updated_at`）。
- **正規化するのは transport ノイズだけ:** NFC / CRLF→LF / JSON canonical / timestamp 表記統一。body 内のユーザ可視空白は畳まない。
- adapter が object_id 抽出と canonical タプル生成を宣言、core は固定 serialization を hash するだけ。**この固定 serialization（canonical タプル実体）は stored column として保持し、hash 衝突時の唯一の比較対象になる（D9.4）。** full observation（reactions / 編集 wrapper / ingestion メタを含む）との比較はしない。

### D4 — routing key
- `routing_key = coarse(published month) : coarse(published year) : source : container(workspace, channel) : fine(published)`。
- 時刻成分は全て `published`（不変条件2）。
- **軸順 month:year**（year:month でない）。根拠: 寮 Slack は年またぎ同月の「串刺し」検索が支配的読み → month を最上位 split 軸にして「全年の同月」を co-locate。per-workspace 設定可。
- 上位 routing に暗号 hash 不使用。最下位 balance も hash でなく `fine(published)`（projection 可視・leaf 内時系列順序・負荷を単調 spread）。routing に hash は一切無し。
- placement 原則: 季節 coarse prune + 容量 balance のみ。person/project/意味軸は read-side へ（不変条件5）。
- 2年在籍上限が効く: year 軸は最大2値 → 月部分木が year で割れても高々2 leaf → 串刺し union = bounded。
- 副次効果: published-time-window クエリは prune できる（late-arrival でも published で route 済み）。ただし month:year 軸順では連続時間範囲は lexicographic range にならず、window が覆う `(month, year)` bucket 集合へ展開してその部分木 union を prune する（範囲スキャンではない、D11.1）。在籍 ≤2 年で year 次元が最大2値なので bucket 集合は bounded。代償は incremental propagation（D10）に移る。
- `routing_keyspec` は per-workspace・version pin、変更 = 全件 re-route（D12）。

### D5 / D6 — trie 構造 & split
- **Patricia trie**（固定ビット trie でない）。選んだ split bit を log に記録（D7）。
- split は容量駆動（content 分布でない）。インスタンス増加 ≪ データ増加。
- **lazy split:** 容量到達まで浅いまま太らせ、割る時は leaf の中身ごと全再配置（次の discriminating bit で子へ）。route は常に現在 tree の純関数。
- **split atomic cutover protocol（R9）。** route は「現在 tree」の純関数（不変条件1）なので親→子の切替は不可分でなければならない。2 フェーズ（partition log の `split_prepare`/`split_commit`, D7）:
  1. `split_prepare`: 親の snapshot から子 leaf を build（route はまだ親を指す＝read/route は一貫して親を見る）。
  2. catch-up: prepare 後に親へ入った新規 ingest の差分を子へ rehome（mode (a)）。残差が小さくなったら親の該当 key レンジに **write barrier（短い freeze）** を張り最終差分を catch-up。
  3. `split_commit`: route を子へ原子的に切替（log の `event_seq` で確定、`bit_index` を記録）、親を retire、freeze 解除。
  - これを欠くと: (i) split を先に公開すると route が子を見て親の未移動データを取りこぼし、(ii) rehome 中の親への新規 ingest が移送漏れ。両方を 2 フェーズ + barrier で塞ぐ。availability 重視は snapshot+catch-up で freeze 最小化、単純さ重視は全レンジ freeze、どちらも安全（D12.3 blue/green と同パターン）。propagation への影響は D10.7。
- 「上から埋める／新データだけ下層へ」は不採用（home がタイミング依存になり dedup が割れる）。
- locality（浅い prefix）と balance（容量起因の局所 split）は別 depth に同居。

### D7 — partition rule log schema
- イベント: `initialize` / `split_prepare` / `split_commit` / `failover` / `recover`。**全イベントが control-plane 単調 `event_seq`（+ optional control-plane timestamp）を持つ**（後述）。`failover`/`recover` の `event_seq` が failover window の境界（D8）、`split_prepare`/`split_commit` が split window の境界（D6）。
- `initialize` に routing_keyspec と identity_keyspec を pin（軸順・encoding・version、canonical_content 規則・object_id rule・normalization・version）。両 keyspec は不変、変更は migration（D12）。
- `split_commit` に `bit_index` 必須（Patricia は深さ≠ビット位置なので、どのビットで分岐したか記録しないと `tree(L)` が一意復元できない）。`reason`（容量）も記録。prepare だけで commit されなかった split（crash 等）は `tree(L)` に影響しない（tree は initialize + `split_commit` の fold）。
- tree は時刻で版管理しない。route は「最新 tree」+ split は遡及全再配置。**全 partition event は単調 `event_seq`（control-plane）+ optional control-plane timestamp を持ち、`published` 由来の `at` は廃止**（split/failover は event-time を持たない control-plane event だから、R5/R10）。`event_seq` は window 境界・順序付け用で、tree 選択は initialize + `split_commit` の fold で決まる（時刻ではない）。
- leaf id は不透明（`lake:<uuid>`）。path/責任ビット範囲は log から計算。
- **単一インスタンスで開始してよい（leaf=root のみ、ディレクトリ分割不要）が、keyspec だけは完全形で確定必須**（分割は後付け可、keyspec は後付け不可＝後からだと全件 re-route）。`plan.md §11` と一致。

### D8 — failover：AP + reconcile
- leaf 喪失中も ingest を止めず、L の key レンジを受ける（可用性優先）。L 復旧時に identity_key で重複検出 → merge/dedup。
- 安全条件:
  1. redirect 先は近傍 leaf 直書きでなく **専用 failover spool**（L レンジ専用・append-only）。近傍 leaf の exact index を汚さない。
  2. reconcile = spool を現 tree へ再 route して **rehome**（down 中に L が split しても現 home に着地）。reconcile は「stored Observation の rehome」（D12.1 Rail-2、id/published/recordedAt 保持の内部 append）に還元され、adapter→fresh ingest 経路は通さない。
  3. transient dup は発生しうるが排出時に exact index で決定的に除去（`Duplicate`）。最終状態に dup は残らない。
- window 境界を partition log の control-plane 単調 marker（event sequence / control-plane 時刻）で log 記録し、spool 内の排出順は `spool_seq`（D9.6）で固定する → replay が window・排出順を再現、最終状態決定的。`published` は障害窓の境界に使わない（failover は ingestion 時点の物理状態で、late arrival / backfill では published が過去になり、ある ingest が障害窓に入ったか再現できない。物理状態は物理/コントロールプレーン順序で記録する、不変条件8 と同方針）。
- durability（replica からの L 復元）は別レイヤー据え置き。
- spool の物理形は D9 参照。

### D9 — exact index / spool 永続化 / crash recovery 〔grill 確定〕
**コード接地:** 現行 SQLite スキーマ（[crates/storage/sqlite/src/persistence/mod.rs](../../crates/storage/sqlite/src/persistence/mod.rs)）は exact index の土台を既に持つ。リファクタで **`append_seq` を `INTEGER PRIMARY KEY AUTOINCREMENT`（= rowid alias、SQLite が INSERT で自動採番・AUTOINCREMENT で delete 後も再利用しない）として watermark cursor に据え**、旧 PK の `id` を `TEXT NOT NULL UNIQUE` に降格、`idempotency_key` 列を `identity_key`（NOT NULL UNIQUE）に再定義、`canonical_json`（衝突比較対象, R4）を足す：
```sql
CREATE TABLE observations (
    append_seq INTEGER PRIMARY KEY AUTOINCREMENT,  -- ★ leaf-local 単調 commit cursor（rowid alias）。INSERT で自動採番、AUTOINCREMENT で delete 後も再利用なし = watermark cursor（D10.1, R8）
    id TEXT NOT NULL UNIQUE,                        -- UUIDv7。get / tie-break / 監査
    identity_key TEXT NOT NULL UNIQUE,             -- exact index = この UNIQUE B-tree（source:object_id:H(canonical)、旧 idempotency_key）。Observation.idempotency_key の denormalize＝observation_json 内にも在り adapter なしで rebuild 可（D9.3, R12）
    canonical_json TEXT NOT NULL,                  -- hash 入力になった canonical タプル実体（派生物・observation_json には含めない）。衝突時の唯一の比較対象（D9.4）
    recorded_at TEXT NOT NULL,                     -- provenance（cursor には使わない、不変条件8）
    observation_json TEXT NOT NULL                 -- 全実体を保持＝再構築可能
);
```
（`append_seq INTEGER NOT NULL` 単体では SQLite が採番せず `WHERE append_seq > ?`（D10）の前提が崩れる。`PRIMARY KEY AUTOINCREMENT` か、明示 sequence table/trigger + `UNIQUE`/index が必須。rowid alias が最小実装、R8。）

- **D9.1 exact index = `identity_key` カラムの UNIQUE B-tree。** 別テーブルの index は作らない。行 INSERT 1 本で index 更新も原子的（SQLite 保証、複数文 tx 不要）。`existing_id` は違反時に follow-up SELECT で取得。リファクタは「カラムに何を入れるか」を変えるだけ（free-form → `source:object_id:H(canonical_content)`、optional → NOT NULL）。同 INSERT で `append_seq`（leaf-local commit cursor, D10.1）は SQLite が AUTOINCREMENT で自動採番し、`canonical_json`（衝突比較対象, D9.4）保存も原子的に行う（アプリ側 `MAX()+1` は不要・競合なし、R8）。
- **D9.2 SQLite-authoritative per leaf。** 現行の「in-memory `LakeStore`（Vec + dedup HashMap）が authoritative・SQLite は補償ロールバックで同期」（[apps/selfhost/src/self_host/app/](../../apps/selfhost/src/self_host/app/mod.rs), [crates/engine/src/lake/store.rs](../../crates/engine/src/lake/store.rs)）は多 leaf で RAM に収まらず破綻するため、**per-leaf SQLite を authoritative に**し、in-memory `LakeStore` は非永続 per-leaf キャッシュ（起動時 `load_observations` から再構築 or 廃止）へ降格。dedup 判定は SQLite UNIQUE に一本化。持続層では補償ロールバック不要（INSERT が原子的）。
- **D9.3 index は派生物・再構築可能（disaster rebuild の依存を明確化, R12）。** 通常 crash recovery = SQLite 再オープン（UNIQUE index intact、再構築不要）。index 破損・行健全なら stored `identity_key` column から UNIQUE B-tree を直接 rebuild（再導出不要）。**`identity_key` は Observation の `idempotency_key` フィールド（＝`observation_json` 内）の denormalize** なので、column を失っても `observation_json` から **adapter なし**で引き直せる。一方 **`canonical_json` は Observation 本体に含まれない派生物**（hash 入力の canonical タプル実体・`observation_json` に入れない）で、その column を失うと canonical 規則での再導出（= D12.1 mode (b) の adapter 経路）が要る。store は disposable（D12.2）なので無理に再導出せず、**`canonical_json` / `observation_json` が読めない破損は fail-fast → re-crawl** を既定にする（つまり disaster rebuild が依存するのは「`observation_json` 内の `identity_key`」と「`canonical_json` column」の可読性）。split 全再配置時は再配置ループ内で各子 index を build（親 index 破棄、追加 scan 不要）。disaster rebuild は `append_seq` を振り直すので当該 leaf の watermark は無効化され全再配送になる（correctness は D10.5 冪等 apply で保たれる＝D10.7 と同じ扱い、`append_seq` は物理 cursor で `observation_json` に含めない）。
- **D9.4 hash 衝突時の扱い。** content-hash identity 下では「同 identity_key・異 canonical」⟺ sha256 衝突。`Conflict → Quarantined` 経路（[crates/engine/src/lake/ingestion.rs](../../crates/engine/src/lake/ingestion.rs)）は維持するが、**比較対象は hash 入力になった canonical タプルのみ**にする。現行の [crates/engine/src/lake/store.rs](../../crates/engine/src/lake/store.rs) `same_idempotent_observation` は `payload` / `attachments` / `meta` 全体を比較するため、canonical から除外した reactions / 編集 wrapper / ingestion メタ（D3b exclude 群）の変化で偽 `Conflict` を起こす。SQLite-authoritative 化後は UNIQUE 違反時に stored `canonical_json` と incoming canonical タプルだけを exact compare する契約に置換する（`identity_key` と同じく stored column なので adapter 不要・再導出不要、D9.3 と同方針）。等しい→`Duplicate` / 異なる→`Conflict`（真の sha256 衝突のみ）。silent false-merge を絶対に作らない（D3b）。
- **D9.5 Bloom は α＝入れないで始める。** bounded leaf では index が SQLite page cache に収まり負パスも O(log n)。profiling で ingest が IO bound と判明したら β（負パス最適化）として追加。Bloom は冪等性の構成要素ではない。
- **D9.6 failover spool の物理形。** 喪失 leaf の failover エピソード専用の独立 append-only SQLite ストア（`spool:<failover-id>`）。spool 行は observations 行と同形で **`identity_key` / `canonical_json` / `id` / `published` / `recorded_at` / `observation_json` を保持**し、さらに spool 内の append 順を固定する **`spool_seq INTEGER PRIMARY KEY AUTOINCREMENT`** を持つ。spool 内では `identity_key` の UNIQUE 制約と leaf `append_seq` の単調性は持たない（spool 自身は dedup しない＝失敗窓中は append のみ・index 維持しない）が、drain は `ORDER BY spool_seq` で決定的に行う（R15）。**`observation_json` だけ保存すると drain 時に adapter 再計算が要る＝mode (a) の前提が崩れる**ので、`identity_key`・`canonical_json` を必ず持たせる（R13）。drain = spool 行を 1 件ずつ **rehome**（D12.1 Rail-2: stored `identity_key` を信頼して現 tree へ再 route → 着地 leaf に id/published/recorded_at 保持で内部 append → 着地 leaf で新 `append_seq` 採番、着地 leaf の `identity_key` UNIQUE が `Duplicate` を決定的に弾く）。**通常 ingest（fresh ingest）パイプラインは通さない**（新 id・新 recorded_at を付与してしまい dedup と propagation cursor が壊れるため、不変条件10 / R1）。drain 後に spool を retire。エピソード境界は partition log の `failover`/`recover` の control-plane sequence から取る。

### D10 — watermark ベース incremental propagation × 分割 〔grill 確定〕
**コード接地:** 現行 watermark は単一グローバル Vec への `usize` position（projection ごと、[crates/engine/src/propagation/watermark.rs](../../crates/engine/src/propagation/watermark.rs), [crates/engine/src/propagation/scheduler.rs](../../crates/engine/src/propagation/scheduler.rs)）。単一グローバル Vec が前提で、分割するとグローバル position は消える。

- **D10.1 watermark = per-(projection × leaf) の `append_seq` cursor、control-plane 永続。** watermark は「projection P が leaf L をどこまで消費したか」。projection ごとに消費速度が違い失敗時は該当 watermark だけ据え置くので per (projection, leaf) が必要。各 leaf は physical append 順に単調な `append_seq`（D9 schema）を持つので `WHERE append_seq > ?` でその leaf の tail だけ読める。**`recorded_at` を cursor にしない**: rehome（D8 drain / split / blue-green）は元の古い `recorded_at` を保持して挿入するので、`(recorded_at, id)` cursor だと frontier 通過後の挿入が下に沈み silent loss（不変条件8）。`append_seq` は rehome append でも必ず新採番されて frontier を超えるため取りこぼさない。`id`(UUIDv7) は監査・tie-break 用に併記可。partition log と同じ control-plane に永続化（現行 in-memory は durable 化）。
- **D10.2 検出と適用を分離。** 検出順 = leaf-local `append_seq`（per-leaf tail、取りこぼさない＝不変条件8）。適用は **可換 fold なので順序非依存**で per-leaf delta を到着順に流せる（published / recordedAt はデータとして参照、適用順序には依存しない）。
- **D10.3 projection は可換かつ冪等に限る（core サポート範囲）。** 順序依存 fold は core がサポートしない（不変条件9）。順序依存な導出は D11 の published 順ストリームを projection 作者が自前で読んで内部実装する。core はその状態を持たない＝backfill で壊れる心配を core が負わない。
- **D10.4 apply に merge-sort 不要。** `(published, recorded_at, id)` の merge-sort は read 側（D11）専用。
- **D10.5 配送は at-least-once + 冪等 apply。** apply→watermark commit の順（crash で再配送＝at-least-once）。可換なだけの fold（count++ 等）は二重計上するので projection 契約に**冪等性も含める**（count なら distinct observation_id の集合濃度＝集合ベース）。core は単純なまま、冪等性は projection 作者の責務（不変条件9 と一直線）。
- **D10.6 通知:** 当面 per-leaf poll、後に leaf append の event 発行。
- **D10.7 split 後の propagation 契約（R11）。** rehome は子 leaf に新 `append_seq` を振るので、per-(projection×leaf) watermark（D10.1）では split 後に子 leaf の全 Observation が「新規 tail」に見え、全 projection へ leaf 全量が再配送される。correctness は D10.5 冪等 apply で保たれる（set ベース fold は再 add が no-op）。**baseline = この全量再配送を許容**（split は容量駆動で稀＝インスタンス増 ≪ データ増（D5）、コストは leaf 容量 × projection 数 ×（稀な）split 回数で bounded）。D9.5 Bloom と同じ α/β 方針: profiling で split 再配送が重いと判明したら **β: watermark frontier 子移管**（rehome を親 `append_seq` 昇順で行い、各 projection の親 watermark `W_P` を超えない最大の子 `append_seq` を (projection×子leaf) watermark に seed → 既消費分を再配送しない）を追加。β は split を projection watermark 集合に結合させるので baseline では採らない。

### D11 — runtime の logical→physical 解決 〔grill 確定〕
**コード接地:** `crates/runtime/src/runtime/` は config/health/heartbeat のみ。leaf 解決は未実装（現行 read = 単一 Vec 線形 filter [crates/engine/src/lake/store.rs](../../crates/engine/src/lake/store.rs)）。greenfield。

- **D11.1 `candidate_leaves(filter, log)`。** filter の routing 軸制約を現在 tree + split log（D7）から prune（tree から計算するので常に最新・staleness なし）。published time-window → **覆う `(month, year)` bucket 集合へ展開し各 bucket の部分木 union を prune**（month 軸が先頭なので連続時間範囲は lexicographic range にならない＝単純な範囲スキャン不可。在籍 ≤2 年で year 次元が最大2値なので bucket 集合は bounded）／source・container（equality）→ 安定 encode で prune／fine = 無制約は配下全 touch。
- **D11.2 read primitive = touch leaf 群の streaming k-way merge by `(published, recorded_at, id)`**（Law S8）。唯一の論理 read。D10 から外した merge-sort はここに棲む。
- **D11.3 Effect Isolation。** resolver は Imperative Shell、Kernel は論理 lake のみ。
- **D11.4 解決済みエンティティは leaf prune にも index にも使わない（不変条件5 の尖鋭化）。** person は名寄せ projection の出力で改訂され得る（merge/split＝非可換）、subject/project も「lake から取り出しただけでは不完全」＝派生物。これらは**それを materialize した projection の出力ストアが query 面**になる（「person P を見せろ」は person projection 出力を読む）。base read で絞れるのは routing 軸 + 観測に存在するプリミティブ属性（native sender id, schema, object_id 成分, attachment hash 等）のみ。名寄せ projection 自身は propagation 時に全 leaf の tail を読む（prune できない側）が incremental なので量は bounded。
- **D11.5 secondary index は投入時ゼロ、supplemental 層へ後付け。** 必要になったら supplemental record（kind 例 `"leaf-locator"`、`Mutability::ManagedCache`、version = lake frontier、`derived_from` は全件列挙せず frontier で lineage を表す）として、propagation-time 維持の projector が materialize（[crates/core/src/domain/supplemental.rs](../../crates/core/src/domain/supplemental.rs)）。**派生データ（解決済み person 等）を index してよい**が、その場合は ManagedCache・rebuild 前提・version に upstream projection version を含め（lake frontier + 名寄せ version）、**加速器であって正しさの依存先ではない**（正しさは base read = placement-prune + read 時 filter が担保）。**index が stale な場合の挙動は暗黙 full-scan fallback にしない**: 呼び出し側が宣言する明示 read mode（base/full-scan を opt-in、または stale-with-marker を許容）か fail-fast に倒す（repo の明示宣言型 fallback ladder / `stale-fallback`＋`X-LETHE-Stale` と整合、resolver 内の暗黙 fallback は作らない）。upstream 改訂時の rebuild は projection 作者の責務（split は subtractive で可換維持できないが、それは core の incremental propagation の対象外＝D10 と矛盾しない）。index するのはプリミティブ → leaf であり、`person → native_sender_id` の解決は名寄せ projection が read 時に合成。

### D12 — migration path（単一 → 分割、keyspec 変更、フェーズ順）〔grill 確定〕
**コード接地:** adapter framework は既存（[crates/adapters/api/src/traits.rs](../../crates/adapters/api/src/traits.rs) `to_observations` が `idempotency_key` を産出、slack/gslides 実装あり）。現行キーは content-hash 非含有（[crates/adapters/slack/src/slack/mapper.rs](../../crates/adapters/slack/src/slack/mapper.rs)）。claude.ai zip importer は未実装。

- **D12.1 stored Observation の rehome = 唯一の migration primitive（不変条件10）。** `stored Observation → identity_key を得る → 現 routing keyspec + 現 tree で route → 着地 leaf に内部 append（leaf identity UNIQUE が Duplicate を弾く）`。**2 レール契約**（混同が R1 の silent loss の元）:
  - **Rail-1 fresh ingest**（adapter → `IngestRequest` → [crates/engine/src/lake/ingestion.rs](../../crates/engine/src/lake/ingestion.rs) が `new_id()`＋新 `recorded_at` を付与）＝**初出キャプチャ専用**。migration では使わない。
  - **Rail-2 rehome**（stored Observation を再 route → 着地 leaf に内部 append）＝split / failover drain / blue-green / 既存観測 backfill。**stored の `id` / `published` / `recorded_at` / `consent` を保持**し、変えるのは着地 leaf と（mode b のみ）keyspec 由来の identity 表現（`identity_key` column、`canonical_json` column、`observation_json.idempotency_key`）だけ。新採番されるのは着地 leaf の `append_seq`（D10.1）のみ。**専用の内部 append/rehome API が必要**（fresh ingest 経路を通すと id/recorded_at が失われ dedup と propagation cursor が壊れる、R1/R2）。
  - rehome の 2 モード:
    - **(a) stored identity_key を信頼**（keyspec 不変: split・failover drain・同 keyspec rebuild）＝純粋な routing 操作、adapter 不要。
    - **(b) canonical_content から再計算**（identity_keyspec 変更: blue/green）＝adapter の canonical タプル再生成が必要（id/published/recorded_at は依然保持、変わるのは `identity_key` column / `canonical_json` column / `observation_json.idempotency_key`）。`identity_key` は `Observation.idempotency_key` の denormalize（D9.3）なので、column だけを新 key にして `observation_json` 内を旧 key のまま残してはならない。mode (b) rehome は Observation JSON を新 keyspec の identity 表現で再シリアライズして保存する（R14）。**新フォーマット観測のみ対象**（旧フォーマット payload を読む状況は作らない）。
- **D12.2 既存 LETHE ストアは disposable → re-crawl。** 中身は dev/MVP データで保存価値が低いため破棄し、source から再取得して新パイプラインで取り込む。旧→新の後方互換 shim は作らない。寮 Slack 本体の初回取り込みは migration でなく通常 ingest。
- **D12.3 keyspec 変更 = blue/green。** 新 keyspec で新構造を立て、全観測を rehome（mode b、id/published/recorded_at 保持、identity 表現は新 keyspec で再シリアライズ）で新構造へ（exact dedup で安全・冪等）、read を cutover、旧を retire。**cutover 後は旧物理 leaf を削除**（全観測は新構造に append 済み＝新構造の replay で現状態を決定的再現、Append-Only/Replay 保存）。**旧 keyspec + partition log の version は metadata として履歴保持**。移行中の書き込みは rehome primitive 上で iterative catch-up（bulk → 差分反復 → 短い freeze → cutover）か freeze、どちらも安全（詳細は実施時）。in-place mutation は禁止。
- **D12.4 adapter は本リファクタに合わせて作り直してよい。** 新契約 = adapter が (object_id, canonical タプル) を宣言、core が H して identity_key を組立。

**実装フェーズ順は §3 を参照。**

---

## 3. 実装フェーズ順（D12.フェーズ）

依存関係上、identity 計算は adapter の canonical タプル生成に依存するため、identity 契約と adapter を**縦スライス**で通す。

1. **keyspec 凍結**（routing + identity、完全形）— 単一 leaf=root（D7）。
2. **3部 identity 契約を Slack 1本で縦に通す**: adapter が (object_id, canonical タプル) を宣言 → core が H して identity_key 組立 → NOT NULL UNIQUE で exact dedup（D3/D9）→ per-message 化（D1）→ **rehome primitive をここで実装**（D12.1: stored Observation を id/published/recorded_at 保持で再 route する内部 append、fresh ingest と別レール）。
3. **adapter 横展開**: gslides を canonical-タプル宣言に作り直し → **claude.ai zip importer 新規** → generic 化。
4. **多 leaf 化**: partition rule log + Patricia + lazy split + 全再配置（= rehome mode (a) を子へ）（D5/D6/D7）。
5. **resolver（D11）+ per-(proj×leaf) watermark propagation（D10）**。
6. **failover spool + drain（D8）**（= rehome mode (a)）。
7. **CDC/Merkle**（内部編集される大型文書専用）— 別の content-model マイルストーンとして最後。

---

## 4. draft / 旧記述の上書き点

- SimHash routing は不採用 → content-derived published 階層（D4）。
- 確率的冪等性（Bloom で ε 取りこぼし）は撤回 → per-leaf exact index で完全（決定的）冪等（D2/D9）。
- 固定ビット trie → Patricia + split bit 記録（D5/D7）。
- idempotencyKey 粒度を会話全体 → per-message に修正（D1/D3）。
- `domain_algebra.md` の Idempotency Law を完全（exact）冪等として精緻化。Observation の `idempotencyKey` を optional → 必須・高エントロピー・解決可能に格上げ。
- watermark / propagation 検出 cursor を global `usize` position → per-(projection×leaf) の leaf-local `append_seq` に変更（`recordedAt` も `published` も cursor に使わない、R2/不変条件8）。

---

## 5. 用語

- **logical lake / physical leaf**: Projection/Kernel が見る単一 Lake / 実際に観測を持つ物理インスタンス（trie の葉）。
- **identity_key**: `source:object_id:H(canonical_content)`。dedup の唯一の判定キー。per-message。
- **routing_key**: placement 用。`coarse(published month):coarse(published year):source:container:fine(published)`。dedup には使わない。
- **identity_keyspec / routing_keyspec**: それぞれの導出規則（version pin、initialize で固定、変更 = migration）。
- **exact index**: per-leaf の `identity_key` カラムの UNIQUE B-tree。冪等性の正規判定。
- **append_seq（leaf-local commit cursor）**: 各 leaf の physical append 順に単調な整数列。watermark / propagation 検出 cursor の唯一の根拠（`recorded_at`/`published` は使わない）。rehome append も必ず新採番される。
- **canonical tuple compare**: identity_key UNIQUE 違反時に hash 入力になった canonical タプル（stored `canonical_json`）だけを exact 比較する衝突判定。full observation 比較はしない（reactions/meta の独立変化で偽 Conflict にしないため）。
- **failover spool**: failover 中に喪失 leaf レンジ宛を溜める専用 append-only SQLite。復旧時に `spool_seq` 順で現 tree へ rehome して排出。window 境界は control-plane sequence。
- **lazy split**: 容量到達まで割らず、割る時は中身ごと全再配置。route は現 tree の純関数。
- **rehome（内部再配置 append）**: stored Observation を現 tree へ再 route し、`id`/`published`/`recorded_at`/`consent` を保持して着地 leaf に内部 append する単一 migration primitive。backfill / split / failover drain / blue-green を統一（mode (a) stored identity_key 信頼 / mode (b) canonical 再計算 + `observation_json.idempotency_key` 再シリアライズ）。fresh ingest（新 id・新 recorded_at）とは別レール（D12.1）。
- **串刺し（cross-year same-month）read**: 寮 Slack の支配的読み。month:year 軸順で co-locate。
- **placement 原則**: routing 軸 + プリミティブ属性のみで prune。解決を要する軸（person/subject/project）は projection / supplemental index へ。

---

## 6. 改訂ログ（Codex レビュー反映 R1–R15）

中核方針（exact dedup / identity↔routing 分離 / Patricia + split log / SQLite authoritative）は維持。R1–R6 は該当 D 決定の契約精緻化であり、上書きではない（R7）。R2（append_seq）と R5（control-plane window）は同根＝**物理/コントロールプレーンの順序を event-time（published / recordedAt）と混同しない**。**R8–R11 は 2 巡目レビューの反映**（append_seq の SQLite 実装・split atomic cutover・partition `event_seq` 統一・split 再配送契約）。**R12–R13 は 3 巡目**（disaster rebuild の保存場所整合・failover spool の column 明示）。**R14–R15 は 4 巡目**（mode b rehome の JSON/column 整合・spool drain 順序）。

| R | 指摘 | 対象 | 変更 | 理由（壊れる不変条件） |
| --- | --- | --- | --- | --- |
| R1 | P1-A | 不変条件10 / D8.2 / D9.6 / D12.1 | drain/reconcile/migration を fresh ingest でなく **rehome レール**（id/published/recorded_at 保持の内部 append）に分離 | fresh ingest は毎回 `new_id()`＋新 recorded_at を付与（[ingestion.rs:57](../../crates/engine/src/lake/ingestion.rs),[:209](../../crates/engine/src/lake/ingestion.rs)）→ stored 移送で identity が壊れ dedup/propagation が割れる |
| R2 | P1-B | 不変条件8 / D9 schema / D10.1–10.2 | watermark cursor を `(recorded_at, id)` → **leaf-local `append_seq`**。`observations` に `append_seq` 列追加 | rehome は古い recorded_at を保持 → recorded_at cursor だと frontier 下に沈み silent loss（不変条件8） |
| R3 | P1-C | D4 / D11.1 | time-window prune を lexicographic range → **`(month,year)` bucket 集合展開** | month:year 軸順では連続時間範囲が range にならず取りこぼし |
| R4 | P1-D | D3b / D9.4 / D9 schema | 衝突判定を full observation 比較 → **canonical タプルのみ exact compare**。`canonical_json` 列追加 | `same_idempotent_observation` は payload/meta 全体比較（[store.rs:151](../../crates/engine/src/lake/store.rs)）→ canonical 除外の reactions/meta 変化で偽 Conflict（Idempotency） |
| R5 | P2-A | D7 / D8 | failover window 境界を `published` → **control-plane 単調 sequence** | published は backfill で過去化 → 物理障害窓を再現不能（Replay 決定性） |
| R6 | P2-B | D11.5 | stale index 時の **暗黙 full-scan fallback を撤回** → 明示 read mode / fail-fast | 明示要求のない fallback は repo 方針（明示宣言型 fallback ladder）に反する |
| R7 | 補足 | §1.1 / §2 / §6 | 中核方針は妥当＝上書きでなく精緻化、と明記 | — |
| R8 | P1 (2巡目) | D9 schema / D9.1 / D9.3 | `append_seq` を **`INTEGER PRIMARY KEY AUTOINCREMENT`**（id は UNIQUE へ降格）→ SQLite が自動採番し `WHERE append_seq > ?` が成立 | `INTEGER NOT NULL` 単体では採番されず watermark（D10）が機能しない |
| R9 | P1 (2巡目) | D6 / D7 | **split atomic cutover protocol** を明文化（`split_prepare`/`split_commit` + write barrier + snapshot/catch-up/freeze） | split を rehome 前に公開すると未移動データ取りこぼし、rehome 中の親 ingest が移送漏れ |
| R10 | P2 (2巡目) | D7 | 全 partition event を **`event_seq` + optional control-plane timestamp** に統一、`published` 由来 `at` を廃止 | split/failover は event-time を持たない control-plane event（R5 と整合） |
| R11 | P2 (2巡目) | D10.7（新設） | split 後の **全量再配送を baseline で許容**（β で watermark frontier 子移管）を明記 | 子 leaf の新 `append_seq` で全 Observation が新規 tail に見え再配送される前提が未定義だった |
| R12 | P2 (3巡目) | D9.3 / D9 schema | disaster rebuild の依存を明確化: `identity_key` は `observation_json` 内（adapter 不要で復元可）、`canonical_json` は派生 column で喪失時は **re-derive(adapter) か fail-fast→re-crawl** | `observation_json` 再走査で adapter 不要に引ける、と「別 column・JSON 非包含」前提が矛盾していた |
| R13 | P3 (3巡目) | D9.6 | failover spool 行に **`identity_key` / `canonical_json` を保持**と明示 | drain = rehome mode (a) は stored `identity_key` を信頼するので、spool が `observation_json` だけだと adapter 再計算が要る |
| R14 | P1 (4巡目) | D12.1 / D12.3 / 用語 | mode (b) rehome では `identity_key` column / `canonical_json` column だけでなく **`observation_json.idempotency_key` も新 keyspec で再シリアライズ** | `identity_key` は `Observation.idempotency_key` の denormalize なので、column と JSON が乖離すると disaster rebuild / export / replay が旧 key に戻る |
| R15 | P3 (4巡目) | D8 / D9.6 / 用語 | failover spool に **`spool_seq`** を追加し、drain は `ORDER BY spool_seq` と明記 | replay が「排出順」まで再現すると主張するなら、spool-local append 順序が必要 |
