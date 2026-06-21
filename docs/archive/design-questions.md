# DOKP 追加設計論点シート

このファイルは、現行の [plan.md](../architecture/system-overview.md) を踏まえて、追加で設計判断が必要な事項を整理した回答用シートです。

使い方:
- 各セクションの「決めること」を読んでください。
- `回答:` の下に自由に記入してください。
- 箇条書き、文章、例示、未定メモのいずれでも構いません。
- 回答後、このファイルをもとにチャットで追加質問や整理を依頼してください。

---

## 1. マルチモーダルデータの canonical 化方針

### 決めること
- Google Slides 以外に、どの種類のマルチモーダルデータを一次対象にするか
- 各データ種別について、何を canonical data とし、何を derived data とするか
- 時間・空間・構造の anchor をどの粒度で持つか
- write-back を許可するデータ種別と、その条件

### 主な候補データ
- 音声
- 動画
- PDF / Office 文書
- 画像集合
- チャットログ
- 画面録画
- ホワイトボード写真
- センサー付き映像

### 具体的に決めたい観点
- 音声なら: speaker segment, transcript, diarization, raw waveform のどれが canonical か
- 動画なら: raw video, keyframe, shot boundary, detected object track のどれが canonical か
- PDF / 文書なら: native structure, rendered page image, OCR text のどれを canonical とするか
- 画像なら: 原画像、EXIF、領域 annotation、caption、embedding のどこまでを一次資料とみなすか
- チャットなら: message event, edit history, thread structure, reaction, attachment の扱いをどうするか
- anchor なら: timestamp, time range, page number, slide object id, bounding box, speaker id などを標準化するか

### 回答:

たたき台案:
- 一次対象は、音声、動画、PDF / Office 文書、画像、チャットログ、画面録画を優先する。
- canonical は原則として「source-native に近い表現」と「人間が見た/聞いたレンダリング」の 2 系統を持てるものは両方保持する。
- derived は OCR、ASR、caption、embedding、要約、分類、物体検出、話者推定などの意味付け結果とする。

推奨ルール:
- 音声: raw waveform と source metadata を canonical、transcript と diarization は derived。
- 動画: raw video と container metadata を canonical、keyframe は derived だが再計算コストが高い場合は cached derived として保持可。
- PDF / Office 文書: native structure が取れるなら canonical、同時に rendered page image も canonical に含めてよい。OCR text は derived。
- 画像: 原画像と EXIF を canonical、bounding box annotation は人手作成なら annotation 系 canonical 候補、caption と embedding は derived。
- チャットログ: message event、edit history、thread structure、reaction、attachment link を canonical。要約やトピック推定は derived。
- 画面録画: raw video を canonical、UI 要素認識や操作要約は derived。

anchor の共通モデル案:
- time anchor: timestamp または time range
- spatial anchor: bounding box / polygon / region id
- structural anchor: page number, object id, DOM-like path, message id, thread id
- actor anchor: speaker id, participant id, device id

write-back 方針案:
- default read-only。
- canonical write-back は source-native anchor があり、lossless inversion できる場合のみ許可。
- 画像や動画への自由編集要求は canonical にせず、annotation または proposal に落とす。

未確定:
- 人手アノテーションを canonical 扱いする境界。
- keyframe や waveform feature を reusable canonical cache とみなすか。

### ユーザー回答:
補助情報をlakeに含めるかの論点になりそう。運用ポリシーの観点からはtranscriptは基本的にlakeに含めるべきではなく、transcriptはprojectionとすべきだが、いちいち書き起こしていては計算がもったいない。補助情報領域と1次情報領域を書き分け、解釈情報などを保存しておくのは良いかもしれない。補助情報領域はimmutableで変更はappend形式にすると整合性がとれるが、利点があればmutableでも良い
データソースとしては、googleのコンシューマ向けサービスを網羅する。あとはfigma、slackのデータなど。
想定している運用だと、追加したいデータを収集するクローラーを作成し、それらがlakeに自動で追加しておくのが良いのではないk。



---

## 2. 高頻度更新データの Lake 収録ポリシー

### 決めること
- どの頻度・どの意味密度までを 1 Observation として Lake に入れるか
- どこから raw store / chunk manifest に切り替えるか
- 研究用と運用監視用で保持方針を分けるか
- 遅延到着、欠損、再送、順不同到着の扱いをどうするか

### 判断軸として使えそうなもの
- Hz や update/sec の頻度
- 1 点ごとの意味の強さ
- 後から再解析する可能性
- 保存コスト
- リアルタイム性の要求
- 倫理・同意上の制約

### 決めておくと良い具体項目
- 低頻度の閾値: 例 1 sample = 1 Observation とする上限
- 中頻度の閾値: rollup を canonical にする条件
- 高頻度の閾値: raw chunk manifest のみを Lake に置く条件
- chunk サイズ: 時間単位、件数単位、容量単位のどれを基準にするか
- late arrival 許容窓: 何分・何時間まで再並べ替えを許すか
- correction の適用方針: 欠損補完や異常値除外を canonical にしないか

### 回答:

たたき台案:
- 頻度だけでなく、意味密度と再解析価値で tiering する。
- Lake 本体には「意味的イベント」「状態遷移」「区間」「raw chunk manifest」を置き、連続 raw 本体は外部 raw store に置く。

推奨 tier:
- 低頻度: おおむね 1分未満に 1件程度以下、または 1件ごとに意味が独立しているものは 1 sample = 1 Observation。
- 中頻度: 秒単位更新だが個々の点の意味が弱いものは、1分または5分 rollup を canonical 候補にする。
- 高頻度: 1Hz 超、あるいは高頻度で将来再解析が必要なものは raw chunk manifest のみを Lake に置く。

chunking 案:
- 基本は時間窓で chunk 化する。
- 目安は 1分または 5分単位。高負荷時は容量上限も併用する。
- chunk ごとに sample_count、sampling_hz、sequence range、欠損率を記録する。

late arrival / duplicate 案:
- published を event time、recordedAt を ingest time として分離。
- late arrival 許容窓はまず 24時間を標準にし、用途別に override 可。
- duplicate は idempotencyKey で排除。
- out-of-order は Projection 側で event time 順に再構成。

研究用と運用用の分離案:
- 研究用: raw を長期保持し、chunk manifest を canonical にする。
- 運用監視用: raw は短期保持、rollup と alert event を優先する。

補正方針案:
- 欠損補完、異常値除外、平滑化は canonical にしない。
- 必要なら correction / annotation 系 Projection として保持する。

未確定:
- 周波数の閾値を固定値にするか、schema ごとに registry 管理にするか。

### ユーザー回答:
lakeの構造含め、ここはもう一度考えたいところではある。頻度ごとにlakeに保存するのは自然な発想だが、場合分けが頻発しそうで破綻が起きそうな気がする。lakeはデータが必ず保存され、後から再構成することさえできればいいので、実運用上はデータとprojectionは透過的であるのがいいかも; センサーのデータはlakeに保存されるが、実際projectionを行う際にはlakeから取らず、センサーのデータを直接取る構造のほうがきれい。同じことはmutableな他のマルチモーダルデータにも言えて、合成されたDBでAPIから直接変更できることはそのまま変更が透過的にデータソースに適用される方が混乱は少ない気がする。

---

## 3. コーディングエージェントのネイティブ対応

### 決めること
- エージェントを単なる利用者として扱うか、Projection 構築の一級クライアントとして扱うか
- エージェントにどこまで自動生成を許すか
- エージェント用の control plane / API を設けるか
- 人間レビューを必須にする境界をどこに置くか

### 想定ユースケース
- 新しい Projection Spec の雛形生成
- schema / entity / source の候補提示
- SQL / projector / write adapter の生成支援
- lineage や schema を参照しながら build 失敗を診断
- 既存 Projection を読んで DB on DBs の合成案を出す
- proposal mode の入力を canonical schema に変換する候補提案

### 必要になりそうな機能
- Schema Registry 検索 API
- Projection Catalog 検索 API
- lineage 参照 API
- dry-run build / validation API
- spec lint / compatibility check
- 変更差分の説明生成
- 承認フロー連携
- 権限付き write-back 実行

### 設計上の論点
- エージェントが直接 Observation を append してよいか
- canonical mode の write-back は常に人間承認が必要か
- エージェントがアクセスできる情報範囲をどう絞るか
- プロンプトや会話ログを監査対象に含めるか
- 生成コードの provenance をどう残すか

### 回答:

たたき台案:
- エージェントは単なる利用者ではなく、Projection authoring と inspection の一級クライアントとして設計する。
- ただし、canonical 変更は人間承認付きにし、agent の自動権限は read、proposal 生成、draft build までを基本とする。

アーキテクチャ案:
- Agent Gateway を設け、Schema Registry、Projection Catalog、Lineage、Build Runner への統一入口にする。
- Agent は直接 DB や Lake に触れず、必ず capability-scoped API を経由する。
- Projection 作成は、spec draft 生成 -> lint -> dry-run build -> human review -> register の流れにする。

agent に許す操作案:
- Schema / Projection 検索
- 既存 spec の読解と候補生成
- SQL / projector / write adapter の雛形生成
- dry-run build とエラー解釈
- proposal mode Observation の生成案提示

agent に原則禁止する操作案:
- 人間承認なしの canonical write-back
- 生の credential 参照
- unrestricted な全件 Lake 読み出し
- 外部ネットワークへ自由に出る build 実行

必要 API 案:
- registry search
- schema compatibility check
- projection spec lint
- projection build dry-run
- lineage explain
- write-back preview
- approval request submit

監査方針案:
- agent が生成した spec、コード、write request には provenance を残す。
- prompt、tool call summary、承認者、実行結果ハッシュを監査ログに残す。

未確定:
- 会話ログ全文を保存するか、構造化 audit summary のみ保存するか。
- agent ごとの trust tier を持つか。

### ユーザー回答:
基本的にはprojection作成に限定したい。原則としてlakeに触れないplaygroundのようなものを用意し、それに対しユーザーが自由に構築できるような形式が良い。DBからのwrite-backに関してはポリシーを厳密に決めて、行動指針とするのが良いだろう。


---

## 4. Entity identity と同一性解決

### 決めること
- 同一人物・同一部屋・同一文書・同一機器をどう判定するか
- merge / split / alias をどのように表現するか
- 匿名 ID と実 ID の対応をどこまで管理するか

### 典型的なケース
- 年度を跨いで person id が変わる
- センサー交換で device id が変わる
- Google Drive 上で文書のコピーが作られる
- 同一空間に複数の命名ゆれがある

### 回答:

たたき台案:
- entity 自体は immutable id を持ち、同一性の再解釈は alias / merge / split event で表現する。
- 「同じものとみなす判断」と「現在の代表 id」は分離する。

推奨モデル:
- canonical entity id は一度発行したら再利用しない。
- 別 id を同一と判断した場合は `same-as` または `merged-into` 系 Observation を追加する。
- split が必要な場合は `split-from` を持つ新 entity を作る。
- 表示や Projection では preferred id を引く。

運用案:
- person は年度ローカル id と永続 person id を分ける。
- device は physical device id と deployment instance id を分ける。
- document は source document id、copy lineage、snapshot revision を分ける。
- space は alias 名を registry で管理する。

匿名対応案:
- 実 ID と研究用 pseudonym の対応は別管理領域に隔離し、Lake 本体には置かない。

未確定:
- merge / split の権限を誰に持たせるか。

### ユーザー回答:
名寄せなどは基本的にprojectionの領域(解釈が入るため)。しかし、1で提案したように補助情報領域の設定によって限定的に名寄せできるようにするのが良い気がする


---

## 5. Source trust / confidence モデル

### 決めること
- Source 単位の trust_level だけで十分か
- Observation 単位の confidence, verification status を持つか
- 複数 source が矛盾したときの優先順位をどう決めるか

### 例
- 自動センサー値と人手修正が食い違う
- OCR 結果と native structure が食い違う
- LLM によるラベル付けをどの程度信用するか

### 回答:

たたき台案:
- source 単位 trust_level だけでは粗すぎるため、Observation 単位の confidence / verification status を持つ。

推奨モデル:
- Source Registry には base trust を保持する。
- 各 Observation には optional に `meta.confidence`、`meta.verificationStatus`、`meta.derivedFrom` を持てるようにする。
- derived 系 Projection は、元データの confidence を継承または集約できるようにする。

verification status 案:
- raw
- automated
- human-reviewed
- disputed
- superseded

矛盾時の原則:
- raw native source を優先
- 次に human-verified
- 次に automated inference
- 矛盾は上書きせず、両方保持して Projection 側で resolution policy を適用する

未確定:
- confidence を数値にするか、離散ラベルにするか。

### ユーザー回答:
これも基本的にprojection側で判断すべき領域。lakeに入れるべきではない。入れたいならば補助情報領域


---

## 6. Consent の粒度拡張

### 決めること
- 人物以外の subject にも consent / restriction が必要か
- 1 つの artifact に複数人が含まれる場合をどう扱うか
- 音声・動画・写真の incidental capture をどう扱うか
- 将来の二次利用やモデル学習への同意をどう分けるか

### 回答:

たたき台案:
- person subject 以外にも、artifact rights、space policy、group-level restriction を導入できるようにする。
- 特にマルチモーダルでは 1 artifact に複数人が含まれるため、subject 単体同意だけでは不足する。

推奨拡張:
- consent は subject-centric だけでなく artifact-centric restriction を持てる。
- incidental capture を区別し、主要対象、付随映り込み、匿名第三者で扱いを分ける。
- 二次利用は「研究利用」「教育利用」「公開展示」「モデル学習」で分ける。

運用案:
- 写真 / 動画 / 音声は、可能なら capture 時に involved persons を後付けできるようにする。
- involved persons が未確定の間は restricted 扱いにする。
- external publication や model training は別同意を要求する。

未確定:
- group photo や雑踏音声のように全員特定できないケースの既定値。

### ユーザー回答:
データを取り出し利用するのはprojection領域。フィルタリング用projectionで解決するのがいいか。例えばgoogle photosの顔認識機能はかなり優秀で、こちらでの除外が現実的な気がする。


---

## 7. 削除要求・例外的な取り下げ対応

### 決めること
- append-only 原則の例外をどこまで認めるか
- retraction だけでなく、blob の物理削除や暗号鍵破棄を認めるか
- 著作権、肖像権、個人情報、IRB 条件違反のときの緊急対応フローをどうするか

### 回答:

たたき台案:
- 原則は append-only を維持するが、例外的に access suppression、crypto-shred、blob takedown の 3 段階を持つ。

推奨段階:
- 通常撤回: retraction Observation を追加し、Projection から除外する。
- 機微事故: blob 参照を停止し、アクセス制御で即時遮断する。
- 法的削除要求: blob 実体削除または暗号鍵破棄を許可する。

運用案:
- 削除要求には reason code を付与する。
- DOI 付き Projection に影響する場合は tombstone を残し、消えた理由を lineage 上で説明可能にする。
- 緊急対応フローは admin と ethics 担当の承認ルートを分ける。

未確定:
- physical delete の適用範囲を blob のみとするか、metadata にも及ぼすか。

### ユーザー回答:
これで良い気がする。削除によってDBが壊れない設計にする必要はある。
physical deleteはblobのみで良い


---

## 8. Projection runtime / sandbox / build governance

### 決めること
- Projection build をどこで実行するか
- 任意コード実行をどう隔離するか
- 再現性のために依存関係をどこまで固定するか
- build resource 制限をどうするか

### 具体項目
- container 実行にするか
- Python / SQL / notebook をどこまで許すか
- ネットワークアクセスを build 時に禁止するか
- DOI 付き Projection の build 環境を固定スナップショット化するか

### 回答:

たたき台案:
- Projection build は隔離された runner で実行し、少なくとも DOI 対象 build は再現可能な container image 上で動かす。

推奨方針:
- spec とコードは Git 管理。
- build は ephemeral container / sandbox で実行。
- ネットワークは default deny。必要な source のみ明示許可。
- CPU / memory / storage / runtime に上限を設ける。

言語サポート案:
- SQL、Python は正式サポート。
- notebook は探索用に限定し、公開 Projection の build entrypoint には使わない。

DOI 付き Projection 追加条件案:
- lockfile 必須
- build image digest 固定
- source version pin 必須
- build log と artifact hash 保存

未確定:
- 学生チームの MVP で container を必須にするか、後期フェーズからにするか。

### ユーザー回答:
コーディングエージェントの項と若干内容が重複するが、隔離されたsandbox環境等で構築されるのが良いだろう。
ただ、黒いターミナル画面に慣れていない人のために、コーディングエージェントを組み込んだユーザーフレンドリーなGUIを作成する必要はあるだろう。containerからprojectionの作成・公開まで簡単にできるようにしなければならない。



---

## 9. Lineage の保存粒度

### 決めること
- 行単位 lineage を materialize するか、必要時に再計算するか
- 中間 Projection をまたぐ lineage をどこまで保持するか
- blob 内部の領域や time range まで辿れるようにするか

### 回答:

たたき台案:
- lineage は 2 層で保持する。通常は coarse-grained、重要 Projection は row-level lineage を持つ。

推奨モデル:
- Projection 単位では source refs と version を必須保持。
- テーブル / file 単位では build manifest を保持。
- 行単位 lineage は、研究成果物、公開データ、write-back 対象 Projection に優先して付与する。

blob anchor 案:
- 画像は region anchor、音声 / 動画は time range anchor、文書は page / object anchor を辿れるようにする。

運用案:
- 全件 row-level lineage の常時 materialize は高コストなので default では不要。
- 必要時再計算可能な projector には lazy lineage を許す。

未確定:
- row-level lineage をどの保存形式で持つか。

### ユーザー回答:
情報が少ないので、具体例が欲しい。



---

## 10. Schema / Entity の開放性と統制のバランス

### 決めること
- 誰でも自由登録できる原則を保ちながら、重複や乱立をどう防ぐか
- deprecated, alias, owner, reviewer を持たせるか
- 命名規則やレビュー基準をどの程度強制するか

### 回答:

たたき台案:
- 誰でも自由登録の原則は維持するが、registry object に owner、status、alias、deprecated、review note を持たせる。

推奨ルール:
- 新規登録は self-service でよい。
- ただし global に共有される型や schema は review 済みフラグを持つ。
- 類似項目が既にある場合は alias または extension を推奨する。

status 案:
- draft
- active
- reviewed
- deprecated
- superseded

運用案:
- 命名規則は prefix と説明責務だけ最低限強制。
- 完全な中央審査ではなく、catalog 上で discoverability と重複警告を強くする。

未確定:
- review 権限を中央チームだけにするか、分野別 maintainer 制にするか。

### ユーザー回答:
明確にしておきたいのだが、基本的にlakeに直接入れることはあまり想定されていない。
このため、entityが新規登録される際にはデータソースが増えたり、データソースの取り扱えるデータが増えた場合になるだろう。ここで考えなければいけないのが、すでに使用しているデータソースのデータ形式が拡張されたり変更された場合にどうスキーマを設計するかである

---

## 11. 時間モデルの厳密化

### 決めること
- `published` と `recordedAt` 以外に、valid time や effective interval を持つか
- 後から判明した事実をどの時点に効かせるか
- 状態系データを event と interval のどちらで canonical にするか

### 回答:

たたき台案:
- 現行の `published` と `recordedAt` に加えて、必要な schema では valid time interval を持てるようにする。

推奨整理:
- published: 事象発生時刻
- recordedAt: 取り込み時刻
- valid_from / valid_to: その状態や主張が有効な期間

運用案:
- 状態系データは interval canonical を優先する。
- 瞬間イベントは event canonical のまま扱う。
- 後から判明した事実は correction Observation で追加し、必要なら valid interval を過去に遡らせる。

未確定:
- valid time を Observation 共通属性にするか、schema 任意フィールドにするか。

### ユーザー回答:
これで大丈夫な気がする。valid timeは任意フィールドで良い。


---

## 12. Query / API / serving 契約

### 決めること
- Projection を他者が使うときの安定 API を持つか
- freshness, snapshot version, compatibility の契約をどう定めるか
- DB on DBs のソースとして使うときに、どの粒度で version pin させるか

### 回答:

たたき台案:
- Projection には catalog 登録時に serving contract を持たせる。
- DB on DBs を安定化するため、少なくとも schema contract、freshness、version pin の 3 点を明示する。

推奨項目:
- access method
- freshness SLA または更新周期
- backward compatibility policy
- source pinning rule
- snapshot identifier

versioning 案:
- 他 Projection を source とする場合、major version pin を必須にする。
- DOI 付き利用は immutable snapshot pin を必須にする。

未確定:
- Native Query をどこまで正式契約に含めるか。

### ユーザー回答:
APIとしての公開はlakeとユーザー作成sandboxを隔離するうえで必須。開発の可能性も広がるため、projection同士もAPIを持たせた方が良さそう。同時にAPIも透過的である必要がある。
データの利用は基本的には最新版を返す方が混乱は少ないだろう
データが古くなったとしても渡したほうがデータが取得できないよりマシ
もう少し深めたいので具体例をください

---

## 13. セキュリティと秘密情報管理

### 決めること
- source credential をどう管理・ローテーションするか
- Projection ごとの最小権限をどう定義するか
- 機微 blob の暗号化、鍵管理、監査ログをどう設計するか

### 回答:

たたき台案:
- credential は source owner が直接保持せず、secret manager 経由にする。
- Projection と agent には capability-based な最小権限を与える。

推奨構成:
- source credential は vault 系ストアで保管
- token rotation を定期実施
- blob は分類に応じて暗号化レベルを変える
- 監査ログは read、write、export、approval を最低限残す

運用案:
- public / internal / restricted / highly-sensitive の 4 区分を持つ。
- restricted 以上の blob は暗号化とアクセス審査を必須にする。
- export は誰が何を持ち出したか記録する。

未確定:
- MVP でどこまで secret manager を入れるか。

### ユーザー回答:
データ処理前にフィルタリングしてしまうと構築の難易度が上がったり、変異してしまう可能性がある。
生データを表示する前にフィルタリングプロジェクションを用意したほうが良さげ

---

## 14. 優先順位付け

### いま最優先で詰めたい項目
- 第1優先: コーディングエージェントのネイティブ対応
- 第2優先: マルチモーダル canonical 化方針
- 第3優先: 高頻度更新データの tiering policy

### 理由

- エージェント対応を先に決めると、今後の Projection 追加や spec 記述の運用負荷が大きく下がる。
- マルチモーダル canonical 化を先に決めないと、何を Lake に入れて何を derived に落とすかが毎回ぶれる。
- 高頻度データ policy を決めないと、保存コスト、再現性、クエリ性の設計が固まらない。


---

## 15. 補足メモ

自由記述欄です。まだ整理できていない違和感、追加したいユースケース、懸念事項があれば書いてください。

### 回答:

追加のたたき台メモ:
- 学生チーム運用を考えると、初期フェーズでは厳密性より「後から tightening できる拡張余地」を優先した方がよい。
- その意味で、registry と projection spec は最初から少し冗長でもよいので、review status や provenance の置き場を確保しておく価値が高い。
- 一方で、Lake 本体の Observation 契約は早めに固めた方がよい。ここがぶれると後からの移行コストが高い。

### ユーザー回答:
多少の仕様変更や再構成が必要な箇所があるので、もう一度練り直してからこちらを検討する。
