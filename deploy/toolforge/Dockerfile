# Toolforge runtime image for wiki-economics.
#
# Toolforge's Build Service (Cloud Native Buildpacks) only supports one
# non-Node runtime per image; this repo needs Rust (compiled CLI) + Node
# (admin server + Observable site build) + the DuckDB CLI at runtime, so a
# Dockerfile is used instead and pushed with `toolforge build` / `docker
# build` + registry push. See deploy/toolforge/README.md for the full
# runbook.
#
# The image intentionally does NOT run `npm run build` — the Observable site
# build reads parquet outputs via the DuckDB CLI (site/data-build/*.sh),
# which don't exist until a refresh has run. Site build stays part of the
# scripts/refresh.sh flow at container runtime, same as Cloud VPS today.

# ---- Rust builder -----------------------------------------------------
FROM rust:1-bookworm AS rust-builder
WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY vendor/ vendor/
COPY src/ src/

RUN cargo build --release

# ---- Runtime ------------------------------------------------------------
FROM debian:bookworm-slim AS runtime
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libssl3 \
    curl \
    unzip \
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*

# Pinned DuckDB CLI — needed at site-build time by site/data-build/*.sh,
# which shell out to `duckdb :memory: -json -c "..."` against parquet
# output. Match the version site/package.json pins for the duckdb npm
# bindings so query behavior stays consistent between the two.
ARG DUCKDB_VERSION=1.4.4
RUN curl -fsSL -o /tmp/duckdb.zip \
      "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-amd64.zip" \
    && unzip /tmp/duckdb.zip -d /usr/local/bin \
    && rm /tmp/duckdb.zip \
    && chmod +x /usr/local/bin/duckdb

COPY --from=rust-builder /build/target/release/wiki-econ /usr/local/bin/wiki-econ
COPY scripts/ scripts/
COPY site/ site/
COPY Cargo.toml ./

RUN cd site && npm ci

ENV WIKI_ECON_ENV=production
ENV WIKI_ECON_BIN=/usr/local/bin/wiki-econ
ENV WIKI_ECON_ADMIN_BIND_HOST=0.0.0.0
# Toolforge's webservice port convention varies by webservice type; confirm
# the real value at deploy time and override via jobs.yaml if it isn't 8000.
ENV WIKI_ECON_ADMIN_PORT=8000

EXPOSE 8000

CMD ["node", "site/admin-server.cjs"]
