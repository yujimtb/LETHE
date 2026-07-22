# LETHE設計原理監査(2026-07-22, sol xhigh)

実測事故(観測取り込み51秒フルリビルド→クライアントタイムアウト→メッセージ消失)を起点に、第一原理と実装の矛盾を横断監査した結果。監査者: Codex gpt-5.6-sol xhigh(読み取り専用)。

## A. 抽出した第一原理

1. **Lake は append-only の canonical capture である**

   Canonical Observation は更新・削除せず、訂正は `meta.corrects`、撤回は `meta.retracts`、オプトアウトは Consent Ledger と filtering projection で表現する。[system-overview.md:784](/D:/userdata/docs/projects/skcollege_database/docs/architecture/system-overview.md:784)、[observation-lake.md:140](/D:/userdata/docs/projects/skcollege_database/openspec/specs/observation-lake.md:140)

2. **Lake が正で、Projection は破棄・再生可能な派生物である**

   Lake は canonical、projection materialization は replaceable で Ground Truth ではない。[domain-algebra.md:286](/D:/userdata/docs/projects/skcollege_database/docs/architecture/domain-algebra.md:286)、[system-overview.md:813](/D:/userdata/docs/projects/skcollege_database/docs/architecture/system-overview.md:813)

3. **Projection は決定論的な fold であり、通常伝播は増分である**

   Projection は決定的順序の入力に純粋な `foldl(applyInput)` を適用する。通常更新は watermark 以降だけを処理し、アクセス時 rebuild は非推奨。[domain-algebra.md:348](/D:/userdata/docs/projects/skcollege_database/docs/architecture/domain-algebra.md:348)、[dag-propagation.md:11](/D:/userdata/docs/projects/skcollege_database/openspec/specs/dag-propagation.md:11)

   本日確定した規範では、増分規則を定義できない projection は状態表現・キー設計が誤っていると明文化されている。[communication-projection/design.md:72](/D:/userdata/docs/projects/skcollege_database/openspec/changes/communication-projection/design.md:72)

4. **期待計算量はデータ全量ではなく差分・返却量に依存する**

   文書とオーナー指定レンズから、以下が導かれる。

   - append: 償却 O(1)/件
   - projection更新: O(Δ)
   - 既知キー読み: O(1)～O(log N)
   - cursor読み: O(返却件数)
   - rebuild O(N): 移行・復旧・bootstrapのみ

   append-sequence watermark と per-leaf tail は設計文書でも明示されている。[sharding.md:140](/D:/userdata/docs/projects/skcollege_database/docs/architecture/sharding.md:140)

5. **取り込みは exact idempotent である**

   正規キーは `source:object_id:H(canonical_content)`。同じキー・同じcanonical内容は既存IDを返し、同じキー・異なる内容はcollisionとして拒否する。[domain-algebra.md:552](/D:/userdata/docs/projects/skcollege_database/docs/architecture/domain-algebra.md:552)、[sharding.md:61](/D:/userdata/docs/projects/skcollege_database/docs/architecture/sharding.md:61)

6. **契約は Registry と strict validation で明示する**

   Schema Registry が payload形式・version・source contractを一元管理し、全ObservationがSchemaに適合しなければならない。[registry.md:63](/D:/userdata/docs/projects/skcollege_database/openspec/specs/registry.md:63)、[system-overview.md:248](/D:/userdata/docs/projects/skcollege_database/docs/architecture/system-overview.md:248)

7. **取り込み結果はID・重複・quarantineを明示する**

   `Ingested { id }`、`Duplicate { existingId }`、`Rejected`、`Quarantined` が正式契約である。[observation-lake.md:128](/D:/userdata/docs/projects/skcollege_database/openspec/specs/observation-lake.md:128)

8. **Effect IsolationとACK境界**

   取り込みゲートはprojection materializationを直接更新しない。DB書き込みやsourceアクセスはfoldの外側に置く。[observation-lake.md:76](/D:/userdata/docs/projects/skcollege_database/openspec/specs/observation-lake.md:76)、[domain-algebra.md:365](/D:/userdata/docs/projects/skcollege_database/docs/architecture/domain-algebra.md:365)

   「canonical commit成功を、後続projection/index失敗で取り込み失敗に見せてはならない」は単独のLawとしては未記載。ただしLake authoritative、IngestResult契約、検索indexの「SQLite appendは戻さない」という決定から必然的に導かれる。[persistent-search-index/design.md:47](/D:/userdata/docs/projects/skcollege_database/openspec/changes/persistent-search-index/design.md:47)

9. **Consent/privacyはcapture時と公開時の両境界を持つ**

   policy/consent違反はappend前にquarantineし、restricted dataは公開前にfiltering projectionを通す。[platform-robustness/spec.md:22](/D:/userdata/docs/projects/skcollege_database/openspec/specs/platform-robustness/spec.md:22)、[system-overview.md:74](/D:/userdata/docs/projects/skcollege_database/docs/architecture/system-overview.md:74)

10. **状態は台帳・永続materializationから再構築できる**

    hidden mutable stateに意味論を依存させず、同じ入力から同じ状態を再生する。[domain-algebra.md:533](/D:/userdata/docs/projects/skcollege_database/docs/architecture/domain-algebra.md:533)

11. **並行性はstorage能力を不必要に潰さない**

    SQLite writerの直列化は許容されるが、他storageはmulti-writerを許容できる設計とし、長時間処理は再開可能jobに置く。[platform-robustness/spec.md:73](/D:/userdata/docs/projects/skcollege_database/openspec/specs/platform-robustness/spec.md:73)

---

## B. 原理違反の指摘一覧

### B-01. canonical appendのACKが派生処理完了まで返らない

- **原理・実装・判定**: Lake authoritative、Effect Isolation、明示的IngestResultに違反する。SQLite append後、非corpus materialization、audit、検索index catch-upを同期実行し、その失敗をHTTP失敗として返す。[mod.rs:5478](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/mod.rs:5478)、[mod.rs:5512](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/mod.rs:5512)、[mod.rs:5534](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/mod.rs:5534)、[server.rs:399](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/server.rs:399)。**確証**。
- **計算量**: 期待はappend O(B)とID返却。実装は `O(B + COUNT(N) + projection Δ + manifest全量S + index catch-up Δ + fsync)`。既知のフルリビルド分岐はこの評価から除外しても全量要因が残る。
- **破綻**: canonical append済みなのに、後続失敗・timeoutでクライアントは未保存と判断する。再送がDuplicateになった場合、`request_appended_observations` が空なので失敗したmaterializationを再実行しない。取り込み結果とcanonical事実が分離する。
- **修正方向**: canonical append、最小durable audit/outbox、ID結果を一つのcommit境界にする。Projection/indexはappend-seqをconsumeする非同期jobとし、失敗はprojection healthでsurfaceする。
- **重大度**: **高**

### B-02. AppCore・primary storage・OELが巨大な排他ロックで直列化される

- **原理・実装・判定**: `AppCore`、primary persistence、OEL、history projectionがすべて単一`Mutex`。[mod.rs:1320](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/mod.rs:1320)。通常importはbulk-operation lockとAppCore lockをリクエスト全体で保持する。[mod.rs:5447](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/mod.rs:5447)。OELのappend/page/by-id/blobも同じOEL mutexを使う。[mod.rs:5172](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/mod.rs:5172)。PostgreSQLも内部で単一`Client`をmutex化する。[postgres/lib.rs:19](/D:/userdata/docs/projects/skcollege_database/crates/storage/postgres/src/lib.rs:19)。**実装は確証、47秒待ち・並行2件ハングとの因果は強い推定**。
- **計算量**: 期待は各読み `O(log N+k)` かつ相互独立。実装の応答時間は `O(自処理 + 先行する全critical sectionの総時間)`。並行数Cに対してtail latencyが概ねC倍まで増える。
- **破綻**: 1件の長いimport/history query/blob I/Oが、既知ID読み・cursor page・別projection読みまで停止させる。`spawn_blocking` はasync workerを保護するだけでstorage並行性を増やさない。
- **修正方向**: immutable snapshotの`Arc`公開、短時間のwriter lock、SQLite read connection pool、PostgreSQL poolを分離する。I/O中にAppCore lockを保持しない。
- **重大度**: **高**

### B-03. 差分処理の境界確認が毎回 `COUNT(*)` でO(N)

- **原理・実装・判定**: 増分materializationの直前に`observation_stats()`を呼び、SQLiteは`COUNT(*), MAX(append_seq)`を実行する。[service_support.rs:245](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/service_support.rs:245)、[persistence/mod.rs:138](/D:/userdata/docs/projects/skcollege_database/crates/storage/sqlite/src/persistence/mod.rs:138)。検索catch-upは空tail時を含め同じstatsを最大2回読む。[search-index/index.rs:669](/D:/userdata/docs/projects/skcollege_database/crates/search-index/src/index.rs:669)。OEL statsも同じ構造。[persistence/mod.rs:1988](/D:/userdata/docs/projects/skcollege_database/crates/storage/sqlite/src/persistence/mod.rs:1988)。**確証**。
- **計算量**: 期待は保存済みcount/high-waterをO(1)で読むこと。SQLiteの現行`COUNT(*)`はO(N)。通常1件appendでも複数回O(N)を払う。
- **破綻**: データ量に比例して「差分があるか確かめるだけ」の時間が増え、projection自体をO(Δ)にしてもappend latencyがN依存のまま残る。
- **修正方向**: transaction内で単調count/high-water行を更新するか、`MAX(PK)`と保存済みcountを使う。通常catch-upで全件count整合性検証をしない。
- **重大度**: **高**

### B-04. appendごとにpartition log全体をreplayする

- **原理・実装・判定**: Observation appendは毎回`load_partition_tree()`を呼び、全`partition_log`を読み`PartitionTree::from_events`する。[persistence/mod.rs:190](/D:/userdata/docs/projects/skcollege_database/crates/storage/sqlite/src/persistence/mod.rs:190)、[persistence/mod.rs:1142](/D:/userdata/docs/projects/skcollege_database/crates/storage/sqlite/src/persistence/mod.rs:1142)。OEL appendにも同じ処理が入る。[persistence/mod.rs:1827](/D:/userdata/docs/projects/skcollege_database/crates/storage/sqlite/src/persistence/mod.rs:1827)。**確証**。
- **計算量**: 期待はroute O(tree depth)＋append O(B)。実装はO(P+B)、Pはpartition control event総数。
- **破綻**: split/failover履歴が増えるほど、無関係な通常appendとOEL appendが遅くなる。
- **修正方向**: 起動時にpartition treeを再生してimmutable snapshot化し、partition event追加時だけ差分適用・atomic交換する。
- **重大度**: **高**

### B-05. ClaimQueue更新とmanifest永続化が「差分のふりをした全量処理」

- **原理・実装・判定**: ClaimQueue影響kindでは、全supplemental recordを`project_ordered_records`へ渡す。[mod.rs:685](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/mod.rs:685)、[mod.rs:2426](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/mod.rs:2426)。さらに全writeでanswer log、ClaimQueue、CardQueue等を含むmanifest全体をJSON化して上書きする。[mod.rs:969](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/mod.rs:969)、[persistence/mod.rs:1283](/D:/userdata/docs/projects/skcollege_database/crates/storage/sqlite/src/persistence/mod.rs:1283)。**確証**。開発文書も1 writeの下限O(S)、逐次投入O(S²)を認めている。[persistent-index-design.md:128](/D:/userdata/docs/projects/skcollege_database/docs/development/persistent-index-design.md:128)
- **計算量**: 期待はaffected recordに対するO(Δ log S)。ClaimQueueはO(S log S+edges)、manifestは毎回O(S)。逐次S件で少なくともO(S²)。
- **破綻**: supplementalが増えるほど、小さなdecision/claim/card writeが遅くなり、AppCoreとDB lockの保持時間も伸びる。
- **修正方向**: Claim/Decisionをkeyed reducerと逆indexへ分解する。manifestはscalar metadataと個別row stateへ分割し、変更rowだけtransactional upsertする。
- **重大度**: **高**

### B-06. OELにcorrelation/causation/event-type検索契約がない

- **原理・実装・判定**: モデルには`correlation_id`と`causation_id`があるが、storage traitはcursor、stream、event-idしか提供しない。[storage/api/lib.rs:24](/D:/userdata/docs/projects/skcollege_database/crates/storage/api/src/lib.rs:24)、[storage/api/lib.rs:128](/D:/userdata/docs/projects/skcollege_database/crates/storage/api/src/lib.rs:128)。SQLiteはevent_type列を持つが、indexはstream用だけで、correlation/causationはJSON内にしかない。[schema.rs:28](/D:/userdata/docs/projects/skcollege_database/crates/storage/sqlite/src/persistence/schema.rs:28)。HTTP surfaceも同様。[server.rs:58](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/server.rs:58)。**確証**。
- **計算量**: 期待は既知correlation/causation/typeでO(log N+k)。現状はcursor 0からO(N) page scan＋クライアント側JSON filter。
- **破綻**: 監査traceが台帳量に比例し、単一OEL mutexのため他read/writeも数分停止する。Naniholdの3～6分実測と整合する。
- **修正方向**: correlation、causation、event_typeを列・複合index化し、keyset cursor付きfilter endpointをstorage traitから追加する。
- **重大度**: **高**

### B-07. 取り込みAPIがObservation IDを返さない

- **原理・実装・判定**: 正式契約はIngested IDまたはDuplicate existing ID返却だが、`ImportReport`は件数だけ。[observation-lake.md:128](/D:/userdata/docs/projects/skcollege_database/openspec/specs/observation-lake.md:128)、[mod.rs:146](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/mod.rs:146)。storage outcomeのIDは件数へ捨てられる。[mod.rs:5494](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/mod.rs:5494)。**確証**。
- **計算量**: 期待はappend結果と同時にO(B)でIDを返すこと。現状のID回収はgrepで最悪O(N)、曖昧一致なら追加照合も必要。
- **破綻**: timeout時にevent-id lookupで成否確認できず、再送・grep・重複の連鎖になる。
- **修正方向**: request itemごとにclient correlation key、`Ingested{id}`、`Duplicate{existing_id}`、`Quarantined{ticket}`を同じ順序で返す。
- **重大度**: **高**

### B-08. 冪等キーの正規契約をサーバが検証しない

- **原理・実装・判定**: adapter utilityには正しい`source:object_id:H(canonical_json)`生成がある。[idempotency.rs:27](/D:/userdata/docs/projects/skcollege_database/crates/adapters/api/src/idempotency.rs:27)。しかしimportはcaller提供keyを`source_instance_id`でprefixするだけ。[service_support.rs:714](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/service_support.rs:714)。storageもkeyとcanonical JSONの対応を再計算せず、そのままUNIQUE判定する。[persistence/mod.rs:2131](/D:/userdata/docs/projects/skcollege_database/crates/storage/sqlite/src/persistence/mod.rs:2131)。**確証**。
- **計算量**: 正しい再送はindex lookup O(log N)でDuplicateになるべき。クライアントがevent_time/keyを再生成すると新規append O(1)が繰り返され、後処理・重複解消はO(N)以上になる。
- **破綻**: client retryごとに時刻を作り直すとcanonical hashも変わる。publishedがpartitionを跨げばper-leaf UNIQUEも効かない。ユーザー観測の3重複を説明できる。
- **修正方向**: stable source object IDとcanonical tupleを入力契約にし、サーバでkeyを導出または厳密再検証する。retry用client operation IDも独立して持つ。
- **重大度**: **高**

### B-09. Schema Registryが実質「JSON objectなら何でも可」

- **原理・実装・判定**: Claude/ChatGPT/GitHub/coding-agent/Slack/Gmail/Discord/workspace/heartbeatのschemaがほぼ`{"type":"object"}`のみで、required fieldも型も追加属性制約もない。source contractsも空。[registry.rs:424](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/registry.rs:424)。JSON Schema validator自体は動作しているが、入力schemaが空疎。[ingestion.rs:470](/D:/userdata/docs/projects/skcollege_database/crates/engine/src/lake/ingestion.rs:470)。**確証**。
- **計算量**: 原理上はO(payload) validationで欠落を止める。現状はO(1)に近い形式確認だけで通し、下流でmissing key、rebuild、検索不能、手動復旧O(N)へ転嫁する。
- **破綻**: Slack `user_id`欠落のような不正Observationがcanonical化され、全projectionが防御分岐を持つか停止する必要が生じる。
- **修正方向**: schema/versionごとのrequired fields、型、format、`additionalProperties`方針、observer/source contractを実データ契約として定義する。
- **重大度**: **高**

### B-10. consent gateが実際のconsentを見ず定数で評価する

- **原理・実装・判定**: append前policyは常に`Role::SystemAdmin`、`AccessScope::Internal`、`ConsentStatus::RestrictedCapture`で評価される。[ingestion.rs:168](/D:/userdata/docs/projects/skcollege_database/crates/engine/src/lake/ingestion.rs:168)。channel contextと実際のConsentRef適用はその後。[ingestion.rs:201](/D:/userdata/docs/projects/skcollege_database/crates/engine/src/lake/ingestion.rs:201)。`IngestRequest`自体にcaller consent fieldもない。[ingestion.rs:23](/D:/userdata/docs/projects/skcollege_database/crates/engine/src/lake/ingestion.rs:23)。**確証**。
- **計算量**: 期待はsubject/channelのconsent lookup O(1)～O(log C)。実装はO(1)の定数判定だが、境界として正しくない。
- **破綻**: consent失効・opt-out・channel固有statusがappend前policyへ反映されず、quarantine契約が形骸化する。
- **修正方向**: channel/subject consentを先に解決し、その実値でpolicyを評価する。未解決は明示quarantineとする。
- **重大度**: **高**

### B-11. generic retractionとpersonal corpusのprivacy反映が実装されていない

- **原理・実装・判定**: Slack deleteは`meta.retracts`へObservation IDではなく`message:slack:...`というsubject文字列を入れる。[mapper.rs:80](/D:/userdata/docs/projects/skcollege_database/crates/adapters/slack/src/slack/mapper.rs:80)。PersonalAllTextは新Observation IDから新しいcorpus record IDを作り、`meta.retracts`も`observation.consent`も参照しない。[corpus/lib.rs:588](/D:/userdata/docs/projects/skcollege_database/crates/projections/corpus/src/lib.rs:588)。personal deploymentは実際に`personal_all_text`。[config.toml:84](/D:/userdata/docs/projects/skcollege_database/deploy/personal-lake/config.toml:84)。**PersonalAllTextとgeneric retractionについて確証**。
- **計算量**: 期待はtarget ID/object IDの逆indexをO(1)～O(log N)で更新し、projection rowを差分削除・置換すること。現状はoriginal recordが残り、手動探索はO(N)、完全な撤回はできない。
- **破綻**: canonical Lakeを消さないという原理は守れても、公開projectionから撤回・opt-out対象が消えず、privacy境界が破れる。
- **修正方向**: corrects/retractsをtyped metadataにし、active-version/tombstone projectionを増分管理する。Corpus indexは同一commitで対象recordをdeleteし、consent changeも逆index経由で反映する。
- **重大度**: **高**

### B-12. Auditが同期ボトルネックなのにdurabilityはfail-open

- **原理・実装・判定**: 全認証・write・filter判定でprimary persistence lockを取りaudit insertするが、lock取得失敗・serialization失敗・DB失敗をログだけで握り潰す。[mod.rs:5388](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/mod.rs:5388)。その後、無制限`Vec`のInMemoryAuditLogにも追加する。[audit.rs:18](/D:/userdata/docs/projects/skcollege_database/crates/policy/src/governance/audit.rs:18)。**確証**。
- **計算量**: 期待はO(1)のdurable audit appendまたは明示失敗。実装はO(primary-lock待ち＋insert)を全requestに課しつつ、失敗時はdurable記録なし。メモリは監査件数Aに対しO(A)。
- **破綻**: heavy import中は認証だけでも待たされる一方、DB障害時には保護操作が監査なしで成功する。再起動でin-memory auditは消える。
- **修正方向**: canonical transaction/outbox内のmandatory auditに統合し、失敗時は保護操作も失敗させる。in-memory全履歴mirrorを削除し、永続台帳をpage queryする。
- **重大度**: **高**

## C. append-commit-and-lock-split 実装後の状態 (2026-07-23)

性能フェーズ第1弾で、B-01〜B-05およびB-12の対象経路を修正した。canonical Observation append、per-item outcome、監査イベント登録、append-seq high-water更新はSQLiteの同一transactionで確定し、projection materializeと検索index追随は応答後のconsumerへ分離した。consumer失敗はACKを反転せず、projection healthと永続監査イベントで可視化する。

AppCoreは`ArcSwap`のimmutable snapshotを読み取りへ公開し、canonical writer、derived consumer、read laneを分離した。SQLiteはwriter/read connectionを分け、PostgreSQL operational ledgerはwriter clientとread client poolを分けている。blob/page/network I/OではAppCore mutexを保持しない。

`observation_stats`とoperational-event statsは保存済みscalarを読むようにし、partition treeは起動時replay後のimmutable snapshotを通常appendで再利用する。projection manifestはscalar metadataとper-field stateに分割し、変更fieldだけをtransactional upsertする。supplemental fingerprint/countはresident reducerの増分更新を使う。監査の診断mirrorは固定長に制限し、履歴取得は永続page queryを使う。

受入確認には、durable appendとaudit enqueueの同一transaction rollback、duplicate後のappend-seq consumer、writer lane保持中のread lane非ブロック、派生失敗後もcanonical appendを成功とするテスト、および5,000件波形テストを含める。

### B-13. persistent search後も任意regexは全document scan

- **原理・実装・判定**: safe literal n-gramを抽出できないregexは`AllQuery`になり、128件ずつ全documentを読みregex判定する。[search.rs:178](/D:/userdata/docs/projects/skcollege_database/crates/search-index/src/search.rs:178)。timeoutは固定500ms。[grep.rs:12](/D:/userdata/docs/projects/skcollege_database/crates/api/src/api/grep.rs:12)。設計文書でもこのtrade-offを認識している。[persistent-search-index/design.md:75](/D:/userdata/docs/projects/skcollege_database/openspec/changes/persistent-search-index/design.md:75)。**確証**。
- **計算量**: literal/filter検索はO(postings＋candidate)。絞り込み不能regexはO(N)、しかもN増加時は結果ではなくtimeout率が増える。
- **破綻**: ID回収をgrepに依存する現在のクライアント契約と組み合わさると、取り込みreconciliation自体が500msで失敗する。500k実測もpeak RSS約3.87GiBでmemory gate未達、検索max 714秒のtailを記録している。[result.md:64](/D:/userdata/docs/projects/skcollege_database/openspec/changes/persistent-search-index/result.md:64)
- **修正方向**: exact metadata/object-id検索を別API・indexにする。任意regexは非同期search job、明示的cost class、必須filter等として通常SLOから分離する。
- **重大度**: **高**

### B-14. ページングAPIが返却件数ではなく全集合・offsetに比例する

- **原理・実装・判定**:
  - person一覧は全personをcollectして毎回sortする。[projection_api.rs:150](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/projection_api.rs:150)
  - ClaimQueue/CardQueueは全集合をfilter・cloneしてからoffset sliceする。[projection_api.rs:660](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/projection_api.rs:660)、[projection_api.rs:901](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/projection_api.rs:901)
  - lineage生成は全supplemental IDをcollect・sort・hashする。[service_support.rs:813](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/service_support.rs:813)
  - Corpusのoffset pageは先頭から`offset`件を再びskipする。[search-index/read.rs:658](/D:/userdata/docs/projects/skcollege_database/crates/search-index/src/read.rs:658)
  - person messages/slides/timelineとReplySLOはpaginationなしで全行を返す。[projection_api.rs:122](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/projection_api.rs:122)、[projection_api.rs:196](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/projection_api.rs:196)

  すべて**確証**。
- **計算量**: 期待はcursor page O(k)。実装はperson O(P log P)、claim/card O(C)、lineage O(S log S)、深いcorpus page O(offset+k)、person detail O(当該人物全履歴)。
- **破綻**: limit=20でも母集合が増えるほど遅くなる。offset pageは後半ほど悪化し、AppCore lock保持時間も伸びる。
- **修正方向**: persisted sort key＋keyset cursor、filter複合index、保存済みlineage digest/countを使う。detail系にもcursorを必須化する。
- **重大度**: **中**

### B-15. 既知BlobRefの認可が全person scan

- **原理・実装・判定**: blob hashが既知でも、全`person_components`とslide refsを`any()`で走査して参照可否を判定する。[service_support.rs:389](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/service_support.rs:389)。**確証**。
- **計算量**: 期待は`blob_ref -> visible projection owner` indexによるO(1)～O(log N)。実装はO(person数＋全blob ref数)。
- **破綻**: person/slide増加と画像並行取得が重なると、各画像要求がAppCore lockを長く保持して相互ブロックする。
- **修正方向**: projection materializationにvisible blob reference tableを持たせ、consent deltaと同時にupsert/deleteする。
- **重大度**: **中**

### B-16. batch quarantine契約が全体abortへ潰される

- **原理・実装・判定**: future timestampやpolicy quarantineは`Quarantined{ticket}`で返す契約だが、import batchでは最初の1件を`SelfHostError::Ingestion`へ変換し、残りも含め全requestを400で停止する。[mod.rs:5581](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/mod.rs:5581)、[server.rs:879](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/server.rs:879)。partial success要件にも反する。[platform-robustness/spec.md:47](/D:/userdata/docs/projects/skcollege_database/openspec/specs/platform-robustness/spec.md:47)。**確証**。
- **計算量**: 期待はO(B)で全itemを分類し結果を返すこと。現状も単回はO(B)だが、クライアント再送によりO(retry回数×B)へ増幅し、正常itemも進まない。
- **破綻**: 10分を超えるclock skew 1件がrequest全体を止め、quarantine件数・ticketも返らない。クライアントはtransient errorとvalidation errorを区別しにくい。
- **修正方向**: item単位結果を保持し、invalid/quarantineだけ隔離してvalid itemをappendする。clock-skewは構造化error codeとticketで返す。
- **重大度**: **中**

### B-17. 永続化済みsync状態を再起動時に復元しない

- **原理・実装・判定**: `sync_metrics`テーブルへ記録する実装はあるが、AppCore生成時に`last_sync_at=None`、error=None、metrics=defaultへ戻す。[schema.rs:150](/D:/userdata/docs/projects/skcollege_database/crates/storage/sqlite/src/persistence/schema.rs:150)、[mod.rs:1084](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/mod.rs:1084)。healthはこのin-memory値を返す。[service_support.rs:8](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/service_support.rs:8)。**確証**。
- **計算量**: 期待は起動時O(source数)の復元、または台帳cursorからの再構築。実装はO(1)だが偽の初期状態。
- **破綻**: 再起動直後に「sync実績なし・metricsゼロ」と見え、監査・運用判断が実際の台帳と不一致になる。
- **修正方向**: persisted metrics/last-sync recordを起動時に厳密ロードし、欠損・不整合は明示する。
- **重大度**: **中**

---

## C. 暗黙の契約一覧

クライアントが知らないと事故につながる契約は以下。

1. **HTTP成功前にcanonical append済みの可能性がある**

   importはappend後のprojection/index処理で400/500/503やtimeoutになり得る。HTTP失敗を「未保存」と解釈できない。

2. **Observation import結果は件数しか返らない**

   `ingested/duplicates/quarantined`のみで、Observation ID、existing ID、item別失敗理由は返らない。

3. **`source_instance_id`は冪等identityの一部**

   同じsource dataでも`source_instance_id`が変わると別keyになる。[service_support.rs:714](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/service_support.rs:714)

4. **retryではpublished・object ID・canonical JSON・idempotency keyを完全固定する必要がある**

   event_timeをretry時刻で作り直すと別版として保存される。OELではさらにObservation IDとrecorded_atを含むevent envelope全体を同一bytesで再送する必要がある。[operational-event-ledger.md:99](/D:/userdata/docs/projects/skcollege_database/docs/architecture/operational-event-ledger.md:99)

5. **未来時刻許容幅は固定10分**

   `published > recordedAt + 10m`はquarantine対象だが、draft importでは構造化quarantineでなく400になる。[values.rs:103](/D:/userdata/docs/projects/skcollege_database/crates/core/src/domain/values.rs:103)

6. **personal deploymentのpage上限は500**

   `max_page_size=500`で、それを超えるOEL/projection limitは拒否される。[config.toml:75](/D:/userdata/docs/projects/skcollege_database/deploy/personal-lake/config.toml:75)

7. **OELの`after_cursor`と`limit`は両方必須**

   defaultはない。cursor 0は全履歴の先頭を意味する。

8. **cursor形式がAPIごとに異なる**

   - OEL: 数値keyset cursor
   - Claim/Card: 数値文字列offset
   - Corpus grep: opaque encoded keyset cursor
   - Persons/Corpus records: offset pagination

   cursorを共通抽象として扱うクライアントは壊れる。

9. **grepのdefault limitは100、時間制限は固定500ms**

   regex meta characterやindexで完全性を証明できないnormalizationではAllQueryになり、件数増加時は途中結果でなくtimeout errorになる。

10. **通常append中は検索indexが`CatchingUp`になり、全検索が503になり得る**

    ready handleは`Ready`以外を拒否する。[search_index.rs:269](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/search_index.rs:269)、[search_index.rs:685](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/app/search_index.rs:685)

11. **import request body上限は128MiB、1 payloadのpersonal設定上限は1MiB**

    body上限はrouteに直書きされている。[server.rs:37](/D:/userdata/docs/projects/skcollege_database/apps/selfhost/src/self_host/server.rs:37)。文書は最大10,000 draftsを前提にするが、取り込み関数にはdraft件数の明示的上限検査がなく、全prepared observationsを保持する。

12. **bulk import session中はsession IDの伝播が必須**

    sessionがactiveなら通常import・supplemental write・syncとの排他契約が発生し、session終了まで一部projectionがstaleになる。

13. **`personal_all_text`は`read:corpus` scopeだけで全文へ到達する**

    record単位のObservation consent/retraction filteringはCorpusProjectorに組み込まれていない。

14. **通信Observationには登録channelと通信metadataが必要**

    channel kind、source instance、external ID、sender/thread metadataの欠落はquarantineまたは下流projection不整合になるが、base JSON Schemaでは必須化されていない。

15. **Claim/Card APIのdefault limitは20、MCPもdefault 20**

    一方grep HTTPはdefault 100であり、同じ「検索/一覧」でも既定値が異なる。

16. **person detail/messages/slides/timelineとReplySLOは無制限全件応答**

    大きな人物履歴・reply履歴には安全なcursor契約がない。

17. **audit永続化失敗はクライアントへ返らない**

    成功応答でもmandatory auditがDBに存在しない可能性がある。

---

## D. 総評

共通パターンは、**canonical事実・派生状態・運用補助状態の境界が一つの同期critical sectionへ押し込まれていること**である。

特に以下が繰り返されている。

- 差分処理なのに、整合性確認・manifest・lineage・paginationで全集合を再計算する。
- `spawn_blocking`を並行化とみなしているが、その内部は単一mutex・単一connectionで直列化される。
- APIがIDや検索キーを返さず、クライアントにcursor 0 scanやgrepを強いる。
- schema/consent/idempotencyをserver contractで保証せず、下流projectionやクライアント再試行へ責任を転嫁する。
- derived subsystem失敗をcanonical append失敗として返し、実際の台帳状態を曖昧にする。
- correctness metadataを保存済みscalar/indexとして持たず、毎回全件から再証明しようとする。

### 優先して直すべき上位3件

1. **canonical commit/ACK境界とlock scopeの再設計**

   append＋per-item ID＋最小audit/outboxだけで成功を確定し、projection/indexをappend-seq consumerへ分離する。AppCore/OEL/primary persistenceの巨大mutexを同時に解体する。

2. **取り込み契約の正常化**

   per-item Observation IDを返し、server側でcanonical identityを導出・検証し、strict JSON Schemaと実consentでappend前に判定する。これでtimeout→再送→重複→grep回収の連鎖を止める。

3. **OELとprojection readを本物のkeyset/index queryへする**

   correlation/causation/type indexを追加し、offset・全sort・全lineage生成・全person/blob scanを廃止する。cursor pageの計算量を実際にO(k)へ固定する。

既知の51秒フルリビルドは単独事故ではなく、**「全量から整合性を再証明してから応答する」設計癖の最も大きく表面化した例**と判断する。

---

## E. indexed-keyset-reads 実装フォローアップ (2026-07-23)

性能フェーズ第2弾では、OEL の actor/correlation/causation/type/stream/occurred_at 索引付き keyset query、v2 projection cursor、可視 BlobRef 表、exact 検索経路、regex search job、persisted sync state 復元を実装した。SQLite は schema v11 へ移行し、PostgreSQL も同じ OEL scalar/index 契約と read pool を持つ。

新しい一覧・履歴 API は `/api/v2` の opaque cursor 契約に限定し、既存 v1 の cursor semantics は変更していない。検証では同一 sort key を跨ぐページ境界、SQLite `EXPLAIN QUERY PLAN` の OIQ/projection 索引、exact object-id、BlobRef 可視表、sync state 再起動、regex job lifecycle を固定した。
