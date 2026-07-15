# 永続・増分 Corpus 検索インデックス設計

更新日: 2026-07-15  
対象: `lethe-selfhost` の検索 v2 と、検索に必要な Corpus Projection

## 目的と境界

Corpus を検索するたびに全レコードから trigram index を作る処理と、selfhost 起動時に全 Observation / Corpus を常駐させる処理を廃止する。canonical data の正本は引き続き SQLite の append-only Observation であり、検索 index はそこから破棄・再生できる派生 materialization とする。

本設計は検索 v2 の request / response、NFKC、regex、複合語 AND、filter、date order、cursor、snippet、matched range を変更しない。ranking、形態素解析、embedding、fuzzy search、canonical store の置換は対象外である。

## 構成

`lethe-search-index` crate が Tantivy 0.26 を所有し、selfhost の `SearchIndexManager` が lifecycle と SQLite からの再生を担当する。HTTP / MCP は manager の ready handle だけを読み、index が利用できないときに SQLite 全走査や旧 snapshot へ切り替えない。index format version は2である。

```text
SQLite append-only Observation
    │ finite page / append_seq watermark
    ▼
CorpusProjector ── record_id delta ──► Tantivy generation
                                         │
                                         ├─ grep / corpus page / summary
                                         └─ get_record / thread / link resolve
```

検索対象 Corpus は `ProjectionSnapshot` と resident `LakeStore` に複製しない。検索要求が保持するのは Tantivy reader snapshot、候補 page、exact match の検査中レコード、返却する `limit + 1` 件だけである。

## 永続形式と公開境界

`corpus.index_dir` の配置は次のとおりである。

```text
<index_dir>/
├─ CURRENT
└─ generations/
   ├─ <UUIDv7>/       # 公開中の Tantivy index
   └─ <UUIDv7>/       # 再構築中または reader 解放待ちの世代
```

新しい世代は `generations/<UUIDv7>` に構築する。commit、再 open、schema / metadata、Tantivy checksum、record count、smoke count を検証した後、`CURRENT.tmp` を flush して `CURRENT` へ atomic replace する。構築途中の世代を検索へ公開しない。

実行中に世代を切り替えた場合、旧世代は in-flight reader の `Arc` を `Weak` で監視し、最後の reader が解放された後にだけバックグラウンドで削除する。そのため再構築時は一時的に二世代分のディスク容量が必要である。起動時に残った非公開世代は、公開世代を検証した後に清掃する。

commit metadata は次を同じ Tantivy commit 境界に保存する。

- index format version と Tantivy schema fingerprint
- Corpus 設定 / projector fingerprint
- 反映済み SQLite `append_seq` と Observation 件数
- projection watermark と record count
- source type 別件数
- workspace filtering に必要な Form-linked Sheet ID

metadata、schema、count のいずれかが一致しなければ、その世代を ready として開かない。

## Schema と検索候補

表示と exact 判定に必要な field は stored とし、filter / read API に必要な field は indexed / fast field を併用する。NFKC 済み本文は専用 tokenizer `lethe_ngram_1_3_v1` で 1〜3-gram を index 化する。NFKC本文と複合sort keyは検索応答に不要なstored重複を持たず、候補通過後にstored原文からNFKC本文とsort keyを再計算する。

literal term は 1文字なら unigram、2文字なら bigram、3文字以上なら trigram を必須候補として抽出する。各term内でdocument frequencyが最小の必須n-gramを一つずつ決定的に選び、複合語ではtermごとの選択結果をANDする。同じterm内の全n-gram postings重複走査は避け、各termのexact matchが必ず含む一つだけを使うためfalse negativeはない。安全なliteral n-gramを抽出できないregexは `AllQuery` を候補源にする。Tantivyの候補一致を最終結果には使わず、保存原文に対する既存 `PreparedGrepQuery` のregex / NFKC / filter / range / snippet処理で必ずexact判定するため、増え得るfalse positiveは結果へ漏れない。

SQLite の重複判定用列は canonical JSON 本文そのものではなく SHA-256 digest を保持する。digest一致時は保存済み Observation JSON の `meta.canonical_json` と入力を完全比較し、digest衝突を Duplicate と誤認しない。canonical JSON は API / 再構築の正本として Observation JSON metadata に残る。schema version 5 はこの形式を要求し、旧列形式への互換レイヤや silent fallback は持たない。

`from` / `to` は `timestamp_nanos` の inclusive `RangeQuery`、source types と channel / container は `TermSetQuery` として本文候補 query と交差する。これらは stored document の読込と regex 判定より前に Tantivy の候補段階で効き、post-filter のために全件を走査・materialize しない。

検索 deadline は候補 collector の前後と各 stored document の判定前に検査する。一回の Tantivy collector 呼出しそのものは途中中断しないため、安全なliteral候補がある検索は `limit + 1` 件（最大128件）、候補がないregexは128件のpageで走査する。

性能 harness の合成 Corpus は約4年と4 channel に決定的に分散し、各段階の prefix でも日付・channel・本文 term が偏らないよう timestamp slot を permutation する。検索 workload は次の二群を別々に warm-up・集計する。

- 実効群: 直近1年相当の `from` / `to`、channel と source type、両者の複合 filter、半角・全角空白の複合語 AND。10 case を各4回、計40 request。
- 全体検索群: filter を指定しない共通 literal、選択的 literal、半角・全角空白 AND。5 case を各4回、計20 request。

合格条件は実効群 p95 2秒以下（1秒以下を headroom 目標）、両群の warm-up / 計測 failure 0、peak RSS 2.5 GiB以下、swap / OOM 0 とする。絞り込み不能な全体検索の p95 は参考値として報告し、latency threshold には含めない。

## 順序と cursor

公開順序は次の契約を維持する。

- `date_asc`: `(timestamp ASC, record_id ASC)`
- `date_desc`: `(timestamp DESC, record_id ASC)`

Unix nanoseconds を符号順に bias した固定長 hex と `record_id` を連結した `sort_asc`、timestamp bit を反転した `sort_desc` を cursor range 用の STRING field として index 化する。TopDocs は static fast field の `(timestamp_nanos ASC|DESC, record_id ASC)` tuple で並べ、動的な複合文字列 sort を避ける。複合 key は sort 用 fast field や stored field に重複保持しない。cursor の timestamp / record_id から複合 key を再構成し、exclusive lower bound として適用するため、同時刻の tie-break と cursor 後の新規挿入があっても既取得レコードを再返却しない。

## 増分更新と crash window

canonical append を先に durable commit し、その後で新しい `append_seq` の有限 page だけを Corpus record へ投影する。同じ Tantivy commit 内で `delete_term(record_id)` と `add_document` を行うため、同一 Observation または crash recovery による再処理でも record は一件に保たれる。durable store が duplicate-only と判定した batch は index を変更しない。

SQLite commit 後に index commit が失敗しても canonical Observation は巻き戻さない。manager を unavailable にし、保存 metadata の `last_append_seq` より後を catch-up するか、検証不能なら新世代を再構築する。SQLite と Tantivy を擬似的な分散 transaction にはしない。

workspace filtering は初回再構築の一巡目で Form-linked Sheet ID の小さい集合を集め、二巡目で各 Observation を投影する。増分で新しい link が判明した場合は、既存 Sheet record の invalidation も同じ delta に含める。

## 起動と破損復旧

manager の状態は `Opening | CatchingUp | Rebuilding | Ready | Failed` である。

1. 起動時に `CURRENT` の世代を open、checksum / schema / metadata を検証する。
2. 有効なら保存 `append_seq` より後だけを catch-up し、`Ready` にする。全件再構築はしない。
3. `CURRENT` 不在、schema 不一致、metadata / segment 破損なら `Rebuilding` とし、単一のバックグラウンド task が固定 page で新世代を構築する。
4. 構築と公開に成功すれば `Ready`、失敗すれば診断 detail を保持した `Failed` にする。

commit metadata に数値 `index_format_version` がない旧 Corpus index も現行世代として開かず、metadata 不一致として同じ単一 full rebuild 経路へ送る。canonical Observation 以外の旧 index や全件 scan へ fallback しない。

同じ ready generation について複数要求が破損を報告しても、generation ID と epoch の compare-and-set で rebuild は一つだけ開始する。切替済み旧 generation から遅れて届いたエラーは新 generation を壊れた状態へ戻さない。

`Opening`、`CatchingUp`、`Rebuilding` 中の HTTP 検索は `503 search_index_rebuilding`、`Failed` 中は `503 search_index_failed` を返す。MCP は対応する明示的 internal error を返す。空配列、SQLite 全走査、旧 index の silent fallback はない。health / readiness は状態と detail を公開する。

## 検索以外の materialization とメモリ上限

全 Observation 常駐の原因は Corpus 以外の再投影にもあったため、selfhost の full materialization も固定 high-water の二段 page 処理へ変更した。

`proj:person-page` manifest は起動時に version gate を先に通す。数値 `format_version` がない、非数値、または `7` 未満なら legacy と判定し、旧 manifest の内容を読取用 state として受理せず canonical Observation と supplemental から同期 full rebuild する。rebuild は v7 manifest と keyed items を staging から atomic publish し、次回起動は v7 manifest を厳格検証して増分経路へ進む。`7` より大きい認識可能な版と JSON object でない manifest は、旧 binary が未知の将来 state を上書きしないよう fail-fast する。

- 一巡目: identity / consent、canonical fingerprint、freshness、answer log など再投影に必要な compact state を作る。
- 二巡目: 最終 identity を使って person / ReplySLO を作り、projection item を page 単位で staging owner へ書く。
- 完了時: target items の置換、manifest commit、staging 消費を一つの SQLite transaction で行う。

page が過大、空なのに high-water 未到達、`append_seq` が非単調、最終件数 / high-water が不一致なら fail-fast し、既存 target materialization を保持する。Person Message と ReplySLO の全行は `ProjectionSnapshot` に常駐させず、SQLite row store から必要な行だけ読む。

ReplySLO の二巡目では、全 supplemental S 件から `draft_id -> observation_id` と `observation_id -> earliest_sent_at` の hash join index をループ開始前に一度だけ構築し、全 Observation page で共有する。従来は N 件の Observation を page size P で処理するたびに supplemental 全件を走査していたため O((N/P)·S + N) だったが、現在の rebuild は期待計算量 O(S + N)、追加メモリ O(S) である。earliest sent は同一 observation に複数の send record がある場合も index 更新時に最小値を維持する。

通常の canonical append は resident join index を参照して追加 Observation ΔN 件だけを ReplySLO row に変換し、既存行を再投影しないため期待計算量 O(ΔN) である。reply draft / send record の append は同じ index を O(1) で更新し、send record が影響する1 Observation row だけを upsert する。supplemental または projection の永続 commit が失敗した場合は専用 rollback token で index と supplemental store を元に戻し、全件再構築への silent fallback は行わない。

supplemental write は supplemental append、strict projection item delta、manifest を一つの SQLite transaction で commit する。insert-existing、update-missing、delete-missing、同一 key の競合操作は拒否する。commit 成功後にだけ in-memory compact state を交換するため、途中失敗で DB と公開 state が分離しない。

supplemental 由来の非 corpus projection は resident の kind-routed reducer を使う。通常 write は新旧レコード 1 件を順序付き cache、CardQueue draft/event state、ReplySLO join index へ適用し、ClaimQueue は claim/transition/verification/decision kind が変わった場合だけ再投影する。ClaimQueue の結果は resume snapshot と plan state へ同一インスタンスを共有する。CardQueue は draft ごとの event replay と `expires_at -> draft_id` index を持ち、期限切れ候補を全 card scan で探索しない。通常の Observation materialization も supplemental 全件の list/sort/fingerprint を行わず、これらの cache と index を再利用する。

supplemental fingerprint は materialization format v5 から、各 current record の domain-separated SHA-256 digest を 256 bit 加算した順序非依存 accumulator とする。append は digest 1 件の加算、ManagedCache replacement は旧 digest の減算と新 digest の加算で更新する。rollback token は store、fingerprint、件数、reducer state を同時に元へ戻す。起動時と明示 full rebuild では current record 全件から同じ accumulator と projection を再生し、persisted projection と reducer replay が違えば fail-fast する。

計算量は、S を current supplemental 数、C を card 数、A を変更 draft に属する approval/send event 数、D を当該時刻までに期限到来した pending draft 数とすると次の通りである。

| 経路 | 修正前 | 修正後 |
|---|---:|---:|
| supplemental fingerprint | write ごとに O(S log S) | record delta O(record bytes)、manifest serialization は O(S) |
| ClaimQueue 利用 | 同一 write で最大4回 full replay | 影響 kind で1回 O(S log S + edges)、結果を cognition へ共有 |
| ClaimQueue ancestry / supersedes | 最悪 O(S²) | 当該 graph fold は memoization / path compression により O(S + edges) |
| CardQueue replay | O(S log S + C・S) | record apply O(log S + A)、snapshot 化 O(C + D) |
| cognition / ReplySLO | materialize ごとに supplemental 全件を複数 replay | activity kind cache O(activity + claims + decisions)、ReplySLO join O(observations) |

したがって CardQueue による 1 write O(S²)、逐次 S writes O(S³) の経路は消滅する。現在の全量 manifest 契約により 1 write の下限は snapshot serialization O(S) であり、逐次投入全体は O(S²) だが、projection 計算自体は概ね O(S)（card の局所 reducer は O(log S + affected)）である。full replay は起動時検証、起動時の format migration、明示 rebuild に限定し、通常 write から silent fallback しない。

外部の bulk import request はさらに `IMPORT_PROCESS_BATCH_SIZE = 512` 件ずつへ分割して draft を準備する。各内部 batch の一時 Vec を解放し、request 内で準備した最大10,000件だけを一度の durable bulk append へ渡す。新規追加された Observation だけを request-local Vec に集めて非コーパス materialization を一度だけ行い、その後に検索 index を一度だけ catch-up する。したがって 10,000 件の HTTP request でも、500k 件全体の Observation / Corpus Vec や index writer 入力を resident にしない。全件重複なら materialization と index catch-up を行わず、入力が空でも暗黙の再構築へ進まない。

`rebuild_page_size` は index の rebuild / catch-up における SQLite Observation page（benchmark では 4,096）、`IMPORT_PROCESS_BATCH_SIZE` は selfhost bulk import の draft / append / 投影処理（512）をそれぞれ上限とする別の境界である。いずれも corpus 全体の件数に応じて増加しない。

ただし、SQLite の observation JSON、Tantivy segment、commit / merge の一時 page は永続 storage の resident page となる。したがって import のアプリケーション Vec を有界化しても、tmpfs のファイル内容と cgroup memory limit の合計が4GiBに収まることまでは保証しない。前回の4GiB・swap 0・DB全体tmpfs実測では500k import中にOOMとなった。選択肢2としてDBをWSL native ext4のSSD-backed VHDXへ配置した再計測では500k importと検索まで完走したが、VmHWM 3,870,176 KiBで2.5GiB peak RSS headroomは超過した。詳細な4 stageの表、前回tmpfsとの比較、指紋、再現・cleanup手順は `openspec/changes/persistent-search-index/result.md` を正とする。ext4計測はNAS実機に近いディスクI/Oを含むが、速度は開発SSD / VHDXに依存し、NAS実機確認は別途行う。

## 必須設定

`[corpus]` の次の三値は必須であり、環境変数や database path からの暗黙 default はない。

| key | 制約 | 用途 |
|---|---:|---|
| `index_dir` | 空でない path | index root |
| `writer_heap_bytes` | 15,000,000以上 | Tantivy writer heap |
| `rebuild_page_size` | 1以上 | rebuild / catch-up の Observation page |

設定例は `config.example.toml`、`deploy/personal-lake/`、専用 benchmark config を同時に更新している。

## 検証と運用上の注意

実装の contract は `lethe-search-index` の reference-engine 比較、順序 / cursor / snippet、upsert / catch-up / generation / checksum test、selfhost の lifecycle / corruption test、HTTP / MCP e2e で検証する。全体の検証コマンドは次である。

```powershell
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
pwsh -NoProfile -File scripts/check_dependency_layers.ps1
$env:PYTHONDONTWRITEBYTECODE='1'
python -m unittest discover -s scripts/tests -p "test_*.py" -v
```

性能実証の固定条件と実行手順、実測値または実行不能理由は `openspec/changes/persistent-search-index/result.md` を正とする。本変更では本番 selfhost、既存 `data/`、デプロイ、再起動を実施しない。
