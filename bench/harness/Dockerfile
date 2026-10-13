# syntax=docker/dockerfile:1.7
#
# mq-harness image: the common FluxMQ / Kafka / RabbitMQ load harness (bench/harness).
# Build context = repository root (the harness uses crates/fluxmq-client by path):
#   docker build -f bench/harness/Dockerfile -t fluxmq-harness:dev .
# librdkafka is compiled from source by rdkafka's `cmake-build` feature (no system librdkafka;
# its 2.12 sources include curl headers, hence libcurl in both stages). Its TLS (the Kafka
# `security.protocol=ssl` rows) links the distribution's OpenSSL: libssl-dev to build,
# libssl3 at run time.

ARG RUST_IMAGE=rust:1-slim-bookworm
ARG RUNTIME_IMAGE=debian:bookworm-slim

FROM ${RUST_IMAGE} AS build
RUN apt-get update \
 && apt-get install -y --no-install-recommends cmake make g++ pkg-config libcurl4-openssl-dev libssl-dev \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY bench/harness ./bench/harness
ENV CARGO_INCREMENTAL=0 CARGO_TERM_COLOR=never
WORKDIR /src/bench/harness
RUN --mount=type=cache,id=fluxmq-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=fluxmq-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=fluxmq-harness-target,target=/src/bench/harness/target,sharing=locked \
    find /src/crates /src/bench/harness/src -type f -exec touch {} + \
 && cargo build --release \
 && mkdir -p /out && cp target/release/mq-harness /out/ \
 && (cp Cargo.lock /out/ 2>/dev/null || true)

FROM ${RUNTIME_IMAGE}
ARG FLUXMQ_UID=10001
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates libcurl4 libssl3 \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid ${FLUXMQ_UID} fluxmq \
 && useradd --system --uid ${FLUXMQ_UID} --gid fluxmq --no-create-home --shell /usr/sbin/nologin fluxmq \
 && mkdir -p /results && chown fluxmq:fluxmq /results
COPY --from=build /out/ /usr/local/share/mq-harness/
RUN ln -s /usr/local/share/mq-harness/mq-harness /usr/local/bin/mq-harness
# Same uid as the broker image, so the gen-dev keys (0600) in the shared certs volume are readable.
USER fluxmq
ENTRYPOINT ["mq-harness"]
