# syntax=docker/dockerfile:1.7
#
# FluxMQ image: broker (`fluxmq`, which also serves the web console), `fluxmq-bench`,
# `fluxmq-demo` and `fluxmq tls gen-dev`.
#
#   docker build -t fluxmq:dev .
#   docker run --rm -p 37420:7420 -p 37421:7421 -v fluxmq-data:/var/lib/fluxmq fluxmq:dev
#
# Stages: `ui` (pnpm + Vite -> /ui/dist), `build` (cargo release with BuildKit cache mounts
# for the registry and target dir), `runtime` (debian slim, non-root). See deploy/README.md.

ARG RUST_IMAGE=rust:1-slim-bookworm
ARG NODE_IMAGE=node:22-bookworm-slim
ARG RUNTIME_IMAGE=debian:bookworm-slim

# ---------------------------------------------------------------------------------------
FROM ${NODE_IMAGE} AS ui
ENV CI=true \
    PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1 \
    PNPM_STORE_DIR=/pnpm-store
# pnpm version pinned to ui/package.json "packageManager".
RUN npm install -g pnpm@9.2.0 --no-fund --no-audit --loglevel=error
WORKDIR /ui
COPY ui/package.json ui/pnpm-lock.yaml ./
RUN --mount=type=cache,id=fluxmq-pnpm-store,target=/pnpm-store \
    pnpm install --frozen-lockfile --store-dir /pnpm-store
COPY ui/ ./
RUN pnpm build && test -f dist/index.html

# ---------------------------------------------------------------------------------------
FROM ${RUST_IMAGE} AS build
RUN apt-get update \
 && apt-get install -y --no-install-recommends cmake make g++ \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
# The console is built into the broker binary (rust-embed reads ui/dist at compile time).
COPY --from=ui /ui/dist ./ui/dist
# The sources are touched before building: the target dir is a shared cache mount and cargo's
# freshness check is mtime-based, so a context with older mtimes (git archive of another
# commit) must never reuse artifacts compiled from different sources.
# No debug info / stripped symbols in the image (the workspace profile keeps `debug = 1`
# for local profiling). Nothing in Cargo.toml changes.
# Extra features, comma-separated (`FLUXMQ_FEATURES=profiling,jemalloc`). Profiling image:
# `--build-arg FLUXMQ_FEATURES=profiling` (`GET /api/debug/pprof/profile`); symbol names are
# then kept unless RELEASE_STRIP says otherwise (a stripped profile shows only libc frames).
ARG FLUXMQ_FEATURES=""
ARG RELEASE_STRIP=""
ENV CARGO_PROFILE_RELEASE_DEBUG=0 \
    CARGO_INCREMENTAL=0 \
    CARGO_TERM_COLOR=never
RUN --mount=type=cache,id=fluxmq-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=fluxmq-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=fluxmq-target,target=/src/target,sharing=locked \
    find crates ui/dist -type f -exec touch {} + \
 && case ",$FLUXMQ_FEATURES," in *,profiling,*) strip=none ;; *) strip=symbols ;; esac \
 && export CARGO_PROFILE_RELEASE_STRIP="${RELEASE_STRIP:-$strip}" \
 && cargo build --release --locked -p fluxmq-server -p fluxmq-client --bins \
      ${FLUXMQ_FEATURES:+--features "$(echo "$FLUXMQ_FEATURES" | sed 's|[^,]*|fluxmq-server/&|g')"} \
 && mkdir -p /out \
 && cp target/release/fluxmq target/release/fluxmq-bench target/release/fluxmq-demo /out/

# ---------------------------------------------------------------------------------------
FROM ${RUNTIME_IMAGE} AS runtime
ARG FLUXMQ_UID=10001
# curl: container healthcheck; ca-certificates: TLS to public endpoints (webhooks, S3, notifiers).
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl tini \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid ${FLUXMQ_UID} fluxmq \
 && useradd --system --uid ${FLUXMQ_UID} --gid fluxmq --home-dir /var/lib/fluxmq --shell /usr/sbin/nologin fluxmq \
 && mkdir -p /var/lib/fluxmq /certs \
 && chown fluxmq:fluxmq /var/lib/fluxmq /certs
COPY --from=build /out/fluxmq /out/fluxmq-bench /out/fluxmq-demo /usr/local/bin/
COPY deploy/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
COPY deploy/healthcheck.sh /usr/local/bin/fluxmq-healthcheck

ENV FLUXMQ_DATA_DIR=/var/lib/fluxmq \
    FLUXMQ_CLIENT_ADDR=0.0.0.0:7420 \
    FLUXMQ_HTTP_ADDR=0.0.0.0:7421 \
    FLUXMQ_RAFT_ADDR=0.0.0.0:7422 \
    FLUXMQ_REPLICATION_ADDR=0.0.0.0:7423

USER fluxmq
WORKDIR /var/lib/fluxmq
VOLUME ["/var/lib/fluxmq"]
# 7420 client protocol, 7421 HTTP/WS + console, 7422 Raft/cluster RPC, 7423 replication.
EXPOSE 7420 7421 7422 7423
HEALTHCHECK --interval=5s --timeout=3s --start-period=20s --retries=5 CMD ["fluxmq-healthcheck"]
ENTRYPOINT ["tini", "--", "docker-entrypoint.sh"]
CMD ["fluxmq"]
