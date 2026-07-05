FROM rust:1-slim-bookworm AS builder

WORKDIR /workspace
COPY . .
RUN cargo build --release -p lethe-selfhost -p lethe-import-claude -p lethe-import-claude-code -p lethe-import-github

FROM debian:bookworm-slim

RUN useradd --system --create-home --home-dir /var/lib/lethe lethe
COPY --from=builder /workspace/target/release/lethe-selfhost /usr/local/bin/lethe-selfhost
COPY --from=builder /workspace/target/release/lethe-import-claude /usr/local/bin/lethe-import-claude
COPY --from=builder /workspace/target/release/lethe-import-claude-code /usr/local/bin/lethe-import-claude-code
COPY --from=builder /workspace/target/release/lethe-import-github /usr/local/bin/lethe-import-github

USER lethe
WORKDIR /var/lib/lethe
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/lethe-selfhost"]
