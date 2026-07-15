FROM rust:1-slim-bookworm AS builder

WORKDIR /workspace
COPY . .
RUN cargo build --release --locked -p lethe-selfhost

FROM debian:bookworm-slim

ARG LETHE_BENCH_SOURCE_HEAD
ARG LETHE_BENCH_SOURCE_TREE_SHA256

LABEL com.hlab.lethe.benchmark-source-head="${LETHE_BENCH_SOURCE_HEAD}"
LABEL com.hlab.lethe.benchmark-source-tree-sha256="${LETHE_BENCH_SOURCE_TREE_SHA256}"

RUN useradd --system --create-home --home-dir /var/lib/lethe lethe
COPY --from=builder /workspace/target/release/lethe-selfhost /usr/local/bin/lethe-selfhost

USER lethe
WORKDIR /var/lib/lethe
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/lethe-selfhost"]
