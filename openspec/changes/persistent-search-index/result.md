# persistent-search-index 実測結果

実装確認日: 2026-07-15 JST  
最終ベンチ実測: 2026-07-15 JST（WSLログ時刻）  
対象 branch: `feature/persistent-search-index`  
commit: 実施していない（依頼どおり dirty worktree のまま）  
500k測定時の HEAD: `c194e14c82d536cfcc12449d499b434b7e03c190`  
500k測定時の dirty-tree SHA-256: `ac998b920d20969682aafd84fba2d4ed6a316e66aa1cbd8f0ceadb4ade68d377`

## 判定

import 経路は、draft の準備を内部512件単位に分割し、外部 bulk request ごとに最大10,000件だけを durable append へ渡す形へ変更した。検索 index の rebuild page、重複判定用の canonical JSON digest 化、候補 query の先行絞り込みも実装した。

今回の選択肢2（SQLite / Tantivy DBをWSL native ext4上のbench dirへbind、datasetもnative ext4）では、10k / 50k / 100k / 500kのimportと検索計測が完了した。500kの実効検索p95は1.152609秒で、今回の合格条件2秒以下を満たしたが、目標1秒以下は未達である。warm-up / 計測 failure、swap、OOMは全stageで0だった。

一方、OpenSpecに記載した peak RSS headroom 2.5GiBは500kで未達（VmHWM 3,870,176 KiB）となり、ハーネスの総合 `passed` は `false` である。したがって、**検索p95・500k完走・OOMなしの今回の主目的は達成、2.5GiB RSS headroomを含む総合性能ゲートは未達** と判定する。値を推測して合格扱いにはしない。

## 実装した最適化

- `apps/selfhost` の observation draft import は `IMPORT_PROCESS_BATCH_SIZE = 512` 件ずつ準備する。外部 request 内の最大10,000件だけを append 用に保持し、materialization と index catch-up は append 後に一度だけ行う。500k全体の Observation / Corpus Vec は作らない。
- `crates/search-index` の rebuild は固定 page で処理し、page の不要な再コピーを除去した。benchmark の `rebuild_page_size` は4,096である。
- SQLite schema version 5 では重複判定用の canonical JSON 列を SHA-256 digest とし、digest一致時だけ保存済み Observation の canonical JSON を完全比較する。canonical JSON 自体は Observation JSON の metadata に保持するため、重複・collision の契約は維持する。旧 schema への互換 fallback は実装していない。
- `from` / `to` は timestamp の Tantivy inclusive `RangeQuery`、`source_types` は `TermSetQuery`、channel / container も indexed term query として本文候補 query と交差させる。stored document の読込と本文判定より前に候補を絞る。
- 複合語は各 literal term の必須 n-gram から最小 document frequency の一つを選び、term ごとの選択結果を AND する。同一 term の n-gram postings を全て走査しない。
- literal 候補の collector は `limit + 1`（最大128）、候補を作れない regex は128件単位とし、stored load と regex warm-up 超過を抑えた。
- ベンチハーネスは実効群（date range / channel+source / date+channel+source / compound AND）と全体検索群を分離し、各 mode の requests / warm-up / failure / p95 / mean / max を出力する。

## 品質検証

| コマンド | 結果 |
|---|---|
| `cargo fmt --all -- --check` | 成功 |
| `cargo clippy --workspace --all-targets -- -D warnings` | 成功、warning 0 |
| `cargo test --workspace` | 成功、失敗 0、ignored 1 |
| `python -m pytest scripts/tests/test_persistent_index_benchmark.py -q` | 成功、17件 |
| `pwsh -NoProfile -File scripts/check_dependency_layers.ps1` | 成功 |
| `python scripts/check_markdown_links.py` | 成功 |
| `openspec validate persistent-search-index --type change --strict --no-interactive --json` | 成功、issue 0 |
| `git diff --check` | whitespace error 0 |

検索 v2 の `from` / `to` / `source_types` / order / cursor / limit、全角スペースを含む複合語 AND、snippet / matched ranges、HTTP / MCP の契約テストは Rust テストで維持する。

測定後、ハーネスの `run` に source HEAD / dirty-tree SHA-256 の必須指定を追加し、指紋未指定時のローカルgit fallbackを除去した。これはfail-fastな実行前検証であり、selfhostのimport / 検索実装や測定済みコンテナの挙動は変更しない。変更後もPythonテスト17件を再実行して成功している。

## ハーネス固定条件

| 項目 | 条件 |
|---|---:|
| dataset | 500,000 records、seed `20260713`、本文384 bytes以上 |
| drafts | 1,046,062,500 bytes、SHA-256 `b69245b7060a545e2ce9683747b0f53c69e4764f4f698a52a90ad11a029f92c7` |
| staged sizes | 10,000 / 50,000 / 100,000 / 500,000 |
| import batch | 10,000（内部準備 batch 512） |
| rebuild page | 4,096 |
| warm-up | 各 query case 2 round |
| 計測 request | 実効40 + 全体20（合計60、各群20回以上） |
| query cases | 実効10 cases、全体検索5 cases |
| 並列 | 2 |
| limit | 20 |
| container | CPU4、memory 4GiB、memory+swap 4GiB、swap増分0 |
| filesystem | WSL native ext4 上の benchRoot、`db/` を `/var/lib/lethe` へRW bind（datasetもnative ext4） |

今回のDBは開発SSD-backed VHDX上のディスクであり、ディスクI/O込みでNAS実機に近い。ただし速度は開発SSD / VHDX に依存するため、NAS実機の最終確認は別途行う。

## 実測値

### 今回: 選択肢2（DBをnative ext4ディスクへbind）の実測

実行環境は WSL2 Ubuntu の `/dev/sdf` native ext4（`/`）で、benchRoot は `/home/user/lethe-bench-20260715-native-ext4`、DB本体は `db/` をコンテナの `/var/lib/lethe` へRW bindした。datasetも同じnative ext4上に置き、tmpfsはコンテナ内 `/tmp`だけである。Docker制限は memory 4GiB、memory+swap 4GiB、swap増分0、CPU4で固定した。

| stage | import 秒 | 実効 p95 秒 | 実効 mean / max 秒 | 全体検索 p95 秒 | 全体検索 mean / max 秒 | peak RSS / cgroup peak | swap / OOM | p95ゲート |
|---:|---:|---:|---:|---:|---:|---|---|---|
| 10k | 14.571658250 | 0.779371948 | 0.642808963 / 1.140365350 | 0.722424682 | 0.628884787 / 0.816996821 | VmHWM 255,952 KiB / 352,378,880 bytes | 0 / 0 | 合格 |
| 50k | 77.159307200 | 0.880713341 | 0.644307774 / 1.244078977 | 0.828061906 | 0.551089222 / 0.883610747 | VmHWM 449,488 KiB / 831,836,160 bytes | 0 / 0 | 合格 |
| 100k | 232.048741217 | 0.736202983 | 0.631211430 / 0.844523405 | 0.729555547 | 0.619067133 / 0.794892698 | VmHWM 835,212 KiB / 1,231,110,144 bytes | 0 / 0 | 合格 |
| 500k | 7,939.521106356 | 1.152608572 | 21.716605993 / 714.697568391 | 0.866089213 | 0.704114547 / 0.950031422 | VmHWM 3,870,176 KiB / 4,295,180,288 bytes | 0 / 0 | 合格（目標1秒は未達） |

500kは40 batch、import完了後に実効40 request・全体20 request（各warm-up 2 round）を実行した。全stageでcursor sanity、warm-up failure、計測 failureは0。500k stageの `slo_search_pass` / `slo_swap_pass` / `slo_oom_pass` はtrueだが、`slo_memory_pass` はpeak RSS 2.5GiB基準超過のためfalseであり、stageの総合 `passed` はfalseとなった。監視中の累積Block I/Oは約21GB規模で、ホスト画面のカクつきは観測しなかった。

この計測はDBディスクI/O込みでNAS実機に近いが、開発SSD-backed VHDXの速度に依存する。NAS実機のlatency / I/O / memoryの最終確認は別途行う。

### 前回: data全体tmpfsの実測（比較用）

| stage | import 秒 | 実効 p95 秒 | 実効 mean / max 秒 | 全体検索 p95 秒 | 全体検索 mean / max 秒 | peak RSS / cgroup peak | swap / OOM |
|---:|---:|---:|---:|---:|---:|---|---|
| 10k | 6.561100302 | 0.006029098 | 0.004526763 / 0.007269966 | 0.004665152 | 0.003545350 / 0.005085656 | VmHWM 262,196 KiB / 360,308,736 bytes | 0 / 0 |
| 50k | 26.529175993 | 0.006759316 | 0.005411425 / 0.008404780 | 0.005207852 | 0.004233635 / 0.006145050 | VmHWM 468,528 KiB / 871,768,064 bytes | 0 / 0 |
| 100k | 33.754081444 | 0.006923062 | 0.005954839 / 0.010028913 | 0.006884413 | 0.005015199 / 0.007156127 | VmHWM 527,356 KiB / 1,255,223,296 bytes | 0 / 0 |
| 500k | 未完了 | 未取得 | 未取得 | 未取得 | 未取得 | 終了前最終観測 cgroup current 4,146,810,880 / peak 4,148,289,536 bytes、終了後VmHWM unavailable | Docker OOMKilled=true / exit137、swap0 |

10k / 50k / 100k の検索は、各 stage で実効40 request、全体20 request、warm-up failure 0、計測 failure 0だった。500k は100k検索完了後の import 中に停止したため、500kの実効 p95・全体検索 p95・peak RSSは未取得である。

### 前回tmpfs条件での500k未達の証拠と原因の範囲

最終実測は 500k request の途中で `LETHE request failed for POST /api/import/observation-drafts: Remote end closed connection without response` となった。専用コンテナの終了状態は次のとおりである。

| 項目 | 値 |
|---|---|
| container state | exited |
| Docker OOMKilled | `true` |
| exit code | `137` |
| restart count | `0` |
| cgroup limit | memory 4GiB / memory+swap 4GiB |
| swap | 増分0 |
| 500k検索 | import前のため未取得 |

内部512件化と request batch 変更により全体を一括保持する import 経路は除去できた。前回tmpfs条件ではプロセス VmHWM は100k時点で527,356 KiBだった一方、tmpfs上の SQLite / Tantivy の resident page と commit / merge の一時圧力を含む cgroup は500k途中で4GiBへ達した。これは500kの検索レイテンシが遅いことではなく、tmpfs storage + memory制約でimport完了まで到達できない未達だった。今回のext4条件の結果は前節に分離して記録する。

## 前回tmpfs条件の注意

benchRoot は WSL の `$HOME` 配下、dataset と `data/` は WSL native ext4 / tmpfs 上に置いた。Docker デーモンが別 mount namespace を持つため、data の tmpfs はホスト名前空間と Docker デーモン名前空間の双方へ明示的に mount してから bind mount した。`findmnt -T /var/lib/lethe` が `tmpfs` であることを確認している。

RAM-backed計測は開発PC上限値であり、ディスク I/O の影響を除いた楽観的な検索レイテンシである。tmpfsのファイル内容もコンテナ4GiB cgroupのメモリ圧力になり得るため、前回のOOMを無視してはいけない。無制限設定やsilent fallbackは追加していない。今回の選択肢2ではDB本体をtmpfsに置いていない。

## 今回のディスク配置計測の注意

SQLite / Tantivy DB本体はWSL native ext4のSSD-backed VHDX上に置き、`/var/lib/lethe`へbindした。datasetも同じext4上で、計測中にDockerコンテナの`/tmp`だけをtmpfsとした。したがって、500kはDB容量をcgroup RAMへ丸ごと常駐させずに完走したが、SQLite journal、Tantivy segment merge、fsyncによるI/O時間は含む。開発SSD / VHDXの速度に依存し、NAS実機の性能を保証する値ではない。

## 再現手順

WSL の native ext4 で実行する。`/mnt/d` は source checkout の参照にのみ使い、benchRoot は WSL `$HOME` 配下に置く。Docker サービスの再起動は不要である。

`LETHE_BENCH_SOURCE_HEAD` と `LETHE_BENCH_SOURCE_TREE_SHA256` は、source checkout の git を扱える環境（この worktree では Windows PowerShell）で取得し、WSL shell へ明示的に渡す。WSL の `/mnt/d` worktree に対する git fingerprint の暗黙 fallback は使わない。

```bash
cd /mnt/d/userdata/docs/projects/_mission_20260712_nanihold/wt-lethe-idx
benchRoot="$HOME/lethe-bench-$(date +%Y%m%d-%H%M%S)"
export LETHE_BENCH_ROOT="$benchRoot"
export LETHE_BENCH_HTTP_PORT="18080"
export LETHE_BENCH_SOURCE_HEAD="<PowerShellで取得したHEAD SHA>"
export LETHE_BENCH_SOURCE_TREE_SHA256="<PowerShellで取得したdirty tree SHA-256>"
: "${LETHE_BENCH_SOURCE_HEAD:?LETHE_BENCH_SOURCE_HEAD is required}"
: "${LETHE_BENCH_SOURCE_TREE_SHA256:?LETHE_BENCH_SOURCE_TREE_SHA256 is required}"
export LETHE_BENCH_STORAGE_ENCRYPTION_KEY="$(openssl rand -hex 32)"
export LETHE_BENCH_READ_TOKEN="$(openssl rand -hex 16)"
export LETHE_BENCH_WRITE_TOKEN="$(openssl rand -hex 16)"
export LETHE_BENCH_HEALTH_TOKEN="$(openssl rand -hex 16)"

python3 scripts/persistent_index_benchmark.py prepare \
  --work-dir "$benchRoot" --records 500000 --seed 20260713 --body-bytes 384

findmnt -T "$benchRoot" -o FSTYPE,SOURCE,TARGET
test "$(findmnt -n -T "$benchRoot" -o FSTYPE)" = ext4

cd deploy/persistent-index-benchmark
docker compose up -d --build
container="$(docker compose ps -aq lethe-selfhost)"
cd ../..
export LETHE_BENCH_READ_TOKEN LETHE_BENCH_WRITE_TOKEN
python3 scripts/persistent_index_benchmark.py run \
  --work-dir "$benchRoot" --base-url "http://127.0.0.1:$LETHE_BENCH_HTTP_PORT" \
  --read-token-env LETHE_BENCH_READ_TOKEN \
  --write-token-env LETHE_BENCH_WRITE_TOKEN \
  --docker-container "$container" --source-instance-id persistent-index-benchmark \
  --sizes 10000,50000,100000,500000 --batch-size 10000 \
  --warmup-rounds 2 --search-requests 60 --search-concurrency 2 \
  --search-limit 20 --timeout-seconds 1800 --report "$benchRoot/result.json" \
  --source-head "$LETHE_BENCH_SOURCE_HEAD" --source-tree-sha256 "$LETHE_BENCH_SOURCE_TREE_SHA256"

cd deploy/persistent-index-benchmark
docker compose down -v
cd ../..
test "$(realpath "$benchRoot")" = "$HOME/$(basename "$benchRoot")"
rm -rf -- "$benchRoot"
```

実測後は専用 Composeを`down -v`し、検証済みの専用benchRootだけを削除する。今回dataset用tmpfsは使っていないため、tmpfsアンマウントは不要である。本番selfhost、既存`data/`、本番volume、WSL / Dockerサービスは操作しない。

## 要判断

今回の選択肢2で500k importと検索計測は完了した。次の判断事項は、OpenSpecの2.5GiB peak RSS headroomを受入条件として維持するなら、Tantivy writer / mergeの常駐メモリを追加で削減する必要がある点である。memory+swapの増量、swap許可、無制限設定、silent fallbackは今回追加していない。NAS実機でのlatency / I/O / memoryの最終確認は別途必要である。
