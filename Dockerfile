# Build stage
FROM rust:1-bookworm AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        protobuf-compiler \
        libgstreamer1.0-dev \
        libgstreamer-plugins-base1.0-dev \
        libglib2.0-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN cargo build --release --workspace

# Runtime stage
FROM debian:bookworm-slim

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

ENV TRANSONIC_CONFIG_PATH=/etc/transonic/service_config_sample.json \
    TRANSONIC_ENDPOINT=0.0.0.0:5000 \
    PROMETHEUS_PORT=0.0.0.0:8080

EXPOSE 5000 8080

ENTRYPOINT ["/usr/local/bin/transonic"]
