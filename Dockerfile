# syntax=docker/dockerfile:1

FROM rust:1.89.0-bookworm AS builder
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libclang-dev cmake ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# Copy workspace manifests first for better layer caching where possible
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY conduit/Cargo.toml conduit/Cargo.toml
COPY conduit-config/Cargo.toml conduit-config/Cargo.toml
COPY conduit-macros/Cargo.toml conduit-macros/Cargo.toml
COPY xtask/Cargo.toml xtask/Cargo.toml
COPY conduit/src conduit/src
COPY conduit-config/src conduit-config/src
COPY conduit-macros/src conduit-macros/src
COPY xtask/src xtask/src
COPY .cargo .cargo

# Build the Conduit binary. Default omits RocksDB to reduce build memory/time (important for CI/Render builds).
# Override with: --build-arg CARGO_FEATURES="backend_turso,backend_sqlite,backend_rocksdb,conduit_bin"
ARG CARGO_FEATURES="backend_turso,backend_sqlite,conduit_bin"
ARG CARGO_BUILD_JOBS=2
ENV CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS}
RUN cargo build --release -p conduit --no-default-features --features "${CARGO_FEATURES}"

FROM debian:bookworm-slim AS runtime
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl \
  && rm -rf /var/lib/apt/lists/* \
  && useradd --system --create-home --home-dir /var/lib/matrix-conduit --shell /usr/sbin/nologin conduit

COPY --from=builder /app/target/release/conduit /usr/local/bin/conduit
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh \
  && mkdir -p /var/lib/matrix-conduit \
  && chown -R conduit:conduit /var/lib/matrix-conduit

# Render injects PORT. Set CONDUIT_SERVER_NAME, DB/Turso settings, and secrets via env vars.
ENV CONDUIT_ADDRESS=0.0.0.0 \
    CONDUIT_DATABASE_PATH=/var/lib/matrix-conduit

EXPOSE 6167
USER conduit
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
