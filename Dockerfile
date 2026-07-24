# Build stage
FROM rust:1-bookworm AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config \
        protobuf-compiler \
        libgstreamer1.0-dev \
        libgstreamer-plugins-base1.0-dev \
        libglib2.0-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN cargo build --release --workspace

# Demo build stage: reuse the primary build's cache and additionally compile
# the `demo` example (only pulled in by the `demo` image target below, so the
# production image never pays for it).
FROM build AS demo-build
RUN cargo build --release -p transonic-client --example demo

# Demo runtime: the TTS web demo is a gRPC client + web server, so it needs no
# GStreamer plugins — just the binary and the bundled PCM samples.
FROM debian:bookworm-slim AS demo
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=demo-build /src/target/release/examples/demo /usr/local/bin/transonic-demo
COPY testdata /data
EXPOSE 8093
ENTRYPOINT ["/usr/local/bin/transonic-demo"]
CMD ["--listen", "0.0.0.0:8093", "--transform", "http://127.0.0.1:15330", "--data", "/data"]

# Runtime stage (default target: the production service image).
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        libgstreamer1.0-0 \
        gstreamer1.0-plugins-base \
        gstreamer1.0-plugins-good \
        gstreamer1.0-plugins-bad \
        gstreamer1.0-plugins-ugly \
        gstreamer1.0-libav \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /src/target/release/transonic /usr/local/bin/transonic
COPY --from=build /src/target/release/transonic-client /usr/local/bin/transonic-client
COPY config /etc/transonic

# Cap glibc's per-thread malloc arenas.  On a high-core host the default
# (up to 8x CPU count) balloons the resident set with per-thread arenas that
# are never returned to the OS; capping at 2 keeps a tight footprint (peak
# RSS ~100 MiB vs ~600 MiB under a concurrency soak) with no measurable
# throughput cost.
ENV MALLOC_ARENA_MAX=2 \
    TRANSONIC_CONFIG_PATH=/etc/transonic/service_config_sample.json \
    TRANSONIC_ENDPOINT=0.0.0.0:5000 \
    PROMETHEUS_PORT=0.0.0.0:8080

EXPOSE 5000 8080

ENTRYPOINT ["/usr/local/bin/transonic"]
