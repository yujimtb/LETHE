# v15 import memory harness

`import_memory_harness.py` は Linux container 専用の受入 harness です。`N` 件の synthetic JSONL を一時ディレクトリに作り、`--command` の `{corpus}` にパスを差し込み、`/usr/bin/time -v` の VmHWM と harness 自身の idle RSS の差を判定します。

```text
python3 scripts/import_memory_harness.py 10000 \
  --batch-size 1000 \
  --constant-bytes 268435456 \
  --per-batch-byte-budget 104857600 \
  --command 'your-import-command --input {corpus} --batch-size {batch_size}'
```

CI では小さい `N` と publish counter assertion を代替基準にできます。import command の stdout に `publish_count=<integer>` を出し、`--ci --max-publishes <n>` を指定します。実 corpus・本番 endpoint・秘密ファイルは harness の入力にしません。
