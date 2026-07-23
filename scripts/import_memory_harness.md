# v15.1 import memory harness

`import_memory_harness.py` は Linux container 専用です。指定された selfhost を直接起動し、その subprocess PID の `/proc/<pid>/status` から `VmRSS` を読みます。ハーネス自身の RSS や、終了後の対象プロセスの RSS は測定しません。

手順は次のとおりです。

1. synthetic corpus (`N=10,000`) を作成する。
2. `--server-command` で selfhost を起動する。コマンドは shell wrapper ではなく selfhost を直接 exec し、PID を対象プロセスにする。
3. `--seed-command` を一度実行し、`--settle-seconds` 待機して consumer/rebuild の収束を待つ。
4. seed 後の対象 PID の `VmRSS` を baseline として読み取る。
5. duplicate-only を warmup 2回実行し、再度対象 PID の `VmRSS` を測定 baseline とする。
6. duplicate-only を測定 10回実行し、各コマンド完了直後に `VmRSS` を記録する。
7. 各 import の出力に `ingested=0` と `duplicates=batch` があることを検査し、後半サンプルの線形傾きと最終差分を上限判定する。

既定の CI 縮小設定は `N=10,000`、`batch=25`、warmup=2、測定=10、傾き `≤2 MiB/batch`、最終差分 `≤64 MiB` です。全ての上限は引数で変更できます。`--measure-batches` は 8〜12 に制限しています。

コマンドテンプレートでは以下の placeholder を利用できます。seed/duplicate command には `{corpus}` が必須です。

- `{corpus}`: seed または duplicate の synthetic JSONL パス
- `{count}`: corpus 件数
- `{batch_size}`: duplicate batch 件数
- `{batch_index}`: 実行中の batch 番号

例:

```text
python3 scripts/import_memory_harness.py \
  --server-command 'target/debug/lethe-selfhost --config /tmp/test-config.toml' \
  --seed-command 'python3 tools/import_once.py --input {corpus}' \
  --duplicate-command 'python3 tools/import_once.py --input {corpus} --batch {batch_size}'
```

seed/duplicate command は JSON または `key=value` 形式で、少なくとも `ingested=<整数>` と `duplicates=<整数>` を stdout/stderr に出してください。ハーネスは `.env` を読み取らず、本番 endpoint へ接続する設定も生成しません。server command が必要とする設定・認証情報は、テスト専用の明示的な command/config として呼び出し側で指定します。
