## Context

検索 v2 は `ProjectionSnapshot.corpus: Vec<CorpusRecord>` を全件保持し、各要求で全件を `GrepRecord` へ clone した後、`GrepEngine` が filter 済み全件から `TrigramIndex` を作り直している。selfhost 起動は SQLite の `load_observations()` で全 Observation を `LakeStore` へ読み、Corpus を含む全 Projection を再構築する。bulk import も取込 batch ごとに同じ全量再構築を行う。Wave 2 の 10k / 4 GiB 測定は検索 p95 4.104 秒で 2 秒 SLO を hard-stop した。

対象環境は Synology DS920+（Celeron J4125、実効 RAM 3.7 GiB）、本番 corpus は最大約 66.5 万投稿である。検索 v2 の regex / NFKC / AND / filter / order / cursor / snippet と HTTP / MCP wire contract は変更できない。canonical Observation は SQLite の append-only store が正本であり、検索 index は破棄・再生可能な派生 materialization でなければならない。

## Goals / Non-Goals

**Goals:**

- 検索ごとの index 構築、全 Corpus clone、全 Observation / Corpus 常駐を検索経路から除去する。
- 現行検索 v2 と同じ match 集合、順序、cursor、snippet をオンディスク index で返す。
- durable append 後に watermark 以降だけを冪等 upsert し、通常取込で全量再構築しない。
- 初回・schema / projector fingerprint 変更時だけ、SQLite を有限 page で読み直して再構築する。
- 破損を検知した瞬間から検索を 503 にし、single-flight のバックグラウンド再構築で回復する。
- 4 GiB 制限下で 10k〜500k を実測し、日付・channel/source・複合語 AND の実効クエリ p95 2 秒・peak RSS 2.5 GiB の見通しを確認する。絞り込み不能な全体検索は群を分けて参考値を残す。

**Non-Goals:**

- ranking、形態素解析、embedding、fuzzy search の追加。
- canonical Observation、SQLite、既存 governance filter の置換。
- regex の意味論または検索 v2 wire format の変更。
- PostgreSQL 移行、本番デプロイ、既存 `data/` の移行実行。

## Decisions

### D1: Tantivy 0.26 系を Corpus の派生 materialization に使う

独立した `lethe-search-index` crate に Tantivy を導入し、`default-features = false`、mmap と LZ4 圧縮に必要な feature だけを有効にする。writer は 1 thread / 32 MiB heap、reader は manual reload とする。commit 成功後に同期 `reload()` して read-your-write を保証する。stored fieldは応答に必要な原文だけを保持し、NFKC本文とsort keyは候補確認時に再計算してRAM/file cacheの重複を抑えるため、index format versionを2へ更新する。

独自 postings file や SQLite FTS5 も候補だが、segment commit、checksum、mmap reader、delete + add の upsert、長期運用実績を自作せず得られる Tantivy を採用する。Tantivy の query score / ranking は API に露出させない。

### D2: 1〜3-gram は候補だけに使い、現行 regex を最終判定にする

schema は stored の表示 field に加え、NFKC 済み本文を `NgramTokenizer(1, 3, false)` で index 化する。安全な literal term は、1文字なら unigram、2文字なら bigram、3文字以上なら trigram を必須候補として抽出する。各literal termからdocument frequencyが最小の必須n-gramを一つずつ決定的に選び、複合語ではtermごとの選択結果をANDする。同じliteralの全n-gramを交差すると全件共通語で同じpostingsを重複走査するため、各termの一つだけを使う。exact matchは選んだn-gramを必ず含むのでfalse negativeは発生せず、増え得るfalse positiveは最終regex判定で除く。regex meta characterを含むterm、またはnormalization noneなどindexで完全性を証明できないqueryだけを `AllQuery` から読む。

`from` / `to` は indexed `timestamp_nanos` の inclusive `RangeQuery`、source types と channel / container は indexed field の `TermSetQuery` として同じ Boolean query に積む。stored document を読んで本文を照合する前に Tantivy がこれらを交差するため、post-filter のために全 Corpus を materialize しない。

候補 document の stored `text` へ、現行 `regex` crate の `QueryMatcher`、filter、range、snippet 処理を必ず適用する。NFKC本文はstored原文から再計算する。これにより index の false positive は許しても false negative と regex 意味論変更を許さない。旧インメモリ trigram 構築経路は削除する。

### D3: 公開順序専用の複合 key を持つ

契約順は `date_desc = (timestamp DESC, record_id ASC)`、`date_asc = (timestamp ASC, record_id ASC)` である。timestamp fast field 単独の sort は同時刻を Tantivy DocAddress 順にして契約を壊す。

そこで fixed-width の biased Unix nanoseconds hex と `record_id` を結合した `sort_asc`、timestamp bit を反転して同様に結合した `sort_desc` を STRING / FAST field として index 化し、cursor の exclusive range に使う。複合 key は sort 用 fast field や stored field に重複保持しない。TopDocs の並べ替えは動的な複合文字列比較を避け、static fast field の `(timestamp_nanos ASC|DESC, record_id ASC)` tuple を使う。候補は小さい page で反復し、exact match が `limit + 1` 件になるまで stored document を読む。

### D4: index commit 内 metadata を整合性境界にする

各 commit は Corpus document の delete / add と同じ Tantivy commit payload に、index format version、Tantivy schema fingerprint、Corpus config / projector fingerprint、反映済み SQLite `append_seq`、projection watermark、公開 document 数を記録する。record upsert は同一 commit 内の `delete_term(record_id)` → `add_document` とする。

SQLite append が先、index commit が後である。両者を偽の分散 transaction にせず、crash window は次回起動または直後 catch-up で `observation_page(last_append_seq, page_size)` を再生する。再生は record_id upsert なので冪等である。index 失敗時は canonical append を巻き戻さず index を unavailable にする。

### D5: 全量再構築は generation と `CURRENT` pointer で公開する

index root を明示設定し、`generations/<uuid>/` に新 index を page streaming で構築する。commit 後に再 open、schema / payload、`validate_checksum()`、document 数、smoke query を検証する。検証済み generation 名だけを書いた `CURRENT.tmp` を flush して `CURRENT` へ atomic rename し、ready handle を新 generation へ交換する。

既存 generation の directory rename は mmap と Windows file lock の相性が悪いため行わない。旧 handle は in-flight request の `Arc` が消えるまで生存させ、その後 cleanup する。`CURRENT` が指す破損 generation を旧 generation や全件 scan へ fallback してはならない。

### D6: lifecycle manager を selfhost imperative shell に置く

`SearchIndexManager` は `Opening | CatchingUp | Rebuilding | Ready | Failed` と last error を保持する。Tantivy open / metadata / checksum / query / stored document decode / commit の corruption 系 error は manager を即 `Rebuilding` にし、atomic single-flight flag を獲得した一 task だけが SQLite から再構築する。Rebuilding / Failed 中の HTTP は 503 `search_index_rebuilding` / `search_index_failed`、MCP は明示的 internal error を返す。

`AppService` は manager を `ProjectionSnapshot` と分離して所有する。grep、corpus page、source summary、get_record、get_thread、resolve_link は manager だけを読む。`ProjectionSnapshot.corpus` と request ごとの `Vec<GrepRecord>` は削除する。再構築は storage port の paged API を使い、全件 `load_observations()` を呼ばない。

### D7: Corpus 投影を page / Observation 単位へ分解する

`CorpusProjector` に単一 Observation を record delta へ変換する API を設ける。workspace mode の Form response Sheet 除外は、初回構築で linked Sheet ID だけを一 pass で収集し、二 pass 目を投影する。incremental では linked ID を index metadata field と manager の小さい set に保持し、新しい link が既存 Sheet を無効化すると source object ID term で該当 document を削除する。personal mode は各 Observation を独立投影する。

SQLite の append result が AppCore の全件 cache に依存しないよう、duplicate / collision の判定は durable store を唯一の判定元にする。検索対象 Observation は index commit 後に resident `LakeStore` へ複製しない。検索以外の Projection が必要とする state はその materialization のみに保持し、検索のために canonical Observation を常駐させない。

### D8: 設定は明示必須とし fallback を置かない

`[corpus]` に `index_dir`、`writer_heap_bytes`、`rebuild_page_size` を必須追加する。環境変数や database path からの暗黙 default、旧インメモリ検索、index 不在時の SQLite scan は設けない。example、deploy fixture、test config を同時更新する。

## Risks / Trade-offs

- **[任意 regex は候補を絞れない]** → contract を優先して index 全 document を順序 page で走査し、timeout を超えた場合は明示エラーにする。性能 gate は実運用で先行する日付・channel/source filter と複合語 AND を実効群にし、絞り込み不能な全体検索は failure 0 を要求した上で latency を参考値として分離する。
- **[Tantivy と regex の Unicode offset が異なる]** → Tantivy は候補 ID だけを決め、matched_ranges と snippet は保存原文に対する既存実装だけで作る。
- **[SQLite commit 後に index が失敗する]** → index を即 unavailable にし、保存 append_seq から catch-up / rebuild する。canonical data を戻さない。
- **[破損判定が rebuild storm を起こす]** → compare-and-set の single-flight と generation ID を使い、一つの rebuild だけを許す。
- **[初回 500k build の disk / CPU 負荷]** → 1 writer thread、32 MiB heap、有限 page、generation build で online process の RSS と現行 generation を保護する。
- **[index directory の容量増加]** → 新 generation 検証中は最大二世代分を要することを運用文書へ明記し、成功後だけ旧 generation を削除する。失敗 generation は診断情報を残して Failed にする。
- **[他 Projection が canonical 全件を要求する]** → Corpus 検索経路から切り離し、各 Projection の既存 materialization を利用する。全件 `LakeStore` を再導入して検索要件を満たしたことにはしない。

## Migration Plan

1. 新しい必須 `corpus.index_dir` 等を staging config に設定し、書込権限と二世代分空き容量を事前確認する。
2. 初回起動で API process を開始し、Corpus readiness を `rebuilding` として公開する。canonical SQLite を page 読取して generation を構築する。
3. checksum / count / smoke query 検証後だけ `CURRENT` を公開し、検索 readiness を healthy にする。
4. 旧 binary への rollback は新 index を正本として扱わず、停止後に旧 binary と旧設定へ戻す。canonical SQLite は変更しない。新 index directory は後で安全に破棄できる。
5. 本変更では本番 migration / deploy を実行しない。合成データの 4 GiB gate と全テスト合格後に別の運用承認を必要とする。

## Open Questions

なし。性能 gate で未達が出た場合は dataset や filter selectivity を都合よく縮めず、candidate plan、collector、segment、stored field を調整して同じ実効群と全体検索群を再測定する。
