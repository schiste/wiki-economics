# Toolforge runtime image for wiki-economics.
#
# Toolforge's Build Service (Cloud Native Buildpacks) only supports one
# non-Node runtime per image; this repo needs Rust (compiled CLI) + Node
# (admin server + Observable site build) at runtime, so a Dockerfile is used
# instead and pushed with `toolforge build` / `docker build` + registry
# push. See deploy/toolforge/README.md for the full runbook.
#
# The image intentionally does NOT run `npm run build` — the Observable site
# build reads parquet outputs via the site/data-build/*.cjs generators,
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
    nodejs \
    npm \
    && rm -rf /var/lib/apt/lists/*

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
