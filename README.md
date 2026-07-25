# Transonic

Transonic is a library and gRPC-based service for running GStreamer media
transformation pipelines as a service.  The name blends *transform*, *tonic*
(the gRPC stack it is built on) and *sonic*.

Clients pick a preconfigured pipeline and stream media through a bidirectional
gRPC call; the service renders the pipeline, transcodes the stream, applies
flow control, and streams the encoded result back.  The wire protocol
(`transonic.v1`), the JSON configuration schema, the pipeline template
language, the flow-control semantics and the command-line surface are all
stable.

## Architecture

```mermaid
flowchart TB
    subgraph CL["clients"]
        direction LR
        A1["transonic-client<br/>(CLI)"]
        A2["demo example<br/>(TTS web demo)"]
        A3["your service"]
    end

    subgraph SV["transonic-server · tonic"]
        direction LR
        S1["TransformService<br/>bidi gRPC stream"]
        S2["HealthService<br/>moving average"]
        S3["Prometheus<br/>/metrics"]
    end

    subgraph CO["transonic-core · engine"]
        direction LR
        C1["deserialize job<br/>+ deduce params"]
        C2["render pipeline<br/>from config bins"]
        C3["piped transform<br/>processor"]
        C4["AppSource<br/>flow control"]
        C5["AppSinkProcessor<br/>aggregate + meta"]
    end

    subgraph GS["GStreamer pipeline · one per job, in-process"]
        direction LR
        G1(["appsrc"]) --> G2["parse /<br/>decode"] --> G3["convert ·<br/>resample"] --> G4["encode<br/>lame · opus"] --> G5["mux"] --> G6(["appsink"])
    end

    CFG[("config JSON<br/>pipeline templates")]
    PLUG["transonic-plugins<br/>mixer · silence-trim ·<br/>demux · passthrough"]
    META["transonic-meta<br/>per-buffer metadata<br/>silence + input offset"]

    CL ==>|"transonic.v1.Transform · PCM in"| S1
    S1 --> C1 --> C3
    CFG --> C2 --> C3
    C3 --> C4 ==> G1
    G6 ==> C5 --> C3
    C3 -. builds and runs .-> GS
    PLUG -. register .-> GS
    META -. tag .-> C4
    META -. read .-> C5
    C3 ==>|"encoded out · flow control<br/>media props · completed"| S1
    S1 ==>|"MP3 / Opus stream"| CL
    S1 -. outcomes .-> S2

    classDef client fill:#3b82f6,stroke:#1e40af,color:#fff;
    classDef server fill:#8b5cf6,stroke:#5b21b6,color:#fff;
    classDef core fill:#10b981,stroke:#047857,color:#04241b;
    classDef gst fill:#f59e0b,stroke:#b45309,color:#3a2600;
    classDef side fill:#64748b,stroke:#334155,color:#fff;
    class A1,A2,A3 client;
    class S1,S2,S3 server;
    class C1,C2,C3,C4,C5 core;
    class G1,G2,G3,G4,G5,G6 gst;
    class CFG,PLUG,META side;
```

Request path (top → bottom): a client opens a `Transform` bidi stream and
sends `TransformConfig` then PCM `InputChannelPayload`s; the engine deduces
the job parameters, renders the pipeline from the configured bins, and feeds
the bytes into `appsrc`.  Response path (bottom → top): `appsink` output is
aggregated into `OutputChannelPayload`s and streamed back, interleaved with
`FlowControlNotification` (NEED/ENOUGH_DATA), `MediaProperties` and a final
`TransformCompleted`.  Everything runs in-process — no subprocess, one
GStreamer pipeline per job.

Crates:

| Crate                | Purpose                                                                    |
| -------------------- | -------------------------------------------------------------------------- |
| `transonic-proto`    | Generated `transonic.v1` + `grpc.health.v1` protobuf/gRPC types.           |
| `transonic-meta`     | The per-buffer custom audio meta (silence flag, input stream offset).      |
| `transonic-core`     | Config loading, pipeline templates, jobs and the pipeline engine.          |
| `transonic-plugins`  | Custom GStreamer elements (see below), loadable statically or as a cdylib. |
| `transonic-server`   | The `transonic` service binary: gRPC server, health service, metrics.      |
| `transonic-client`   | The `transonic-client` CLI, plus the `demo`, `loadtest` and `backpressure` examples. |

Examples (under `crates/transonic-client/examples/`):

| Example        | Purpose                                                                       |
| -------------- | ----------------------------------------------------------------------------- |
| `demo`         | TTS streaming web demo (PCM in, MP3/WebM-Opus out; MSE playback).             |
| `loadtest`     | In-process concurrency load test (aggregate throughput, latency percentiles). |
| `backpressure` | Observes appsrc flow control (ENOUGH_DATA / NEED_DATA) under overload.         |

## Custom elements

| Element                        | Notes                                                        |
| ------------------------------ | ------------------------------------------------------------ |
| `backgroundaudiomixer`         | Background track mixing with volume / fade-in / fade-out.    |
| `startsilencecutter`           | RMS-based leading-silence detection and trimming.            |
| `smartvadstartsilencetrimmer`  | VAD-based variant; ships an energy VAD (see `transonic-plugins/src/vad.rs`). |
| `demuxtomultichannel`          | Interleaved multichannel → many mono streams.                |
| `passthroughdecoder`           | Transitional passthrough "decoder" + typefinder.             |

## Quick start (Docker Compose)

The whole stack — the transform service, the TTS web demo, Prometheus and a
provisioned Grafana — comes up with one command:

```sh
docker compose up --build
```

Then open:

* `http://localhost:8093` — the TTS streaming demo (pick a sample, an encoding
  and a pace; the *Bursty (TTS-like)* pace shows the back-pressure signal
  toggling between `ENOUGH_DATA` and `NEED_DATA`);
* `http://localhost:3000` — Grafana (anonymous admin), dashboard **Transonic**;
* `http://localhost:9090` — Prometheus.

The compose file uses host networking (Linux), so the shipped `deploy/` configs
work unchanged.  The service runs the `tts_demo.json` pipeline set; edit
`docker-compose.yml` to point `TRANSONIC_CONFIG_PATH` at another config.

## Building

System dependencies (Debian/Ubuntu):

```sh
scripts/install-build-dependencies.sh    # gstreamer dev headers, protoc
scripts/install-runtime-dependencies.sh  # codec plugin packs for runtime
cargo build --release
```

Or build the container image (bundles all runtime codecs):

```sh
docker build -t transonic .
```

Tagged releases (`git tag v0.1.0 && git push origin v0.1.0`) publish a
production image to `ghcr.io/npuichigo/transonic` via GitHub Actions:

```sh
docker pull ghcr.io/npuichigo/transonic:latest
```

## Running

```sh
# Options (environment variables in parentheses):
#   TRANSONIC_CONFIG_PATH, TRANSONIC_ENDPOINT, TRANSONIC_LOG_LEVEL,
#   TRANSONIC_TEMP_STORAGE_DIRECTORY, TRANSONIC_SHUTDOWN_DELAY_SEC,
#   PROMETHEUS_PORT, PROMETHEUS_METRICS_PREFIX
transonic -c config/service_config_sample.json 0.0.0.0:5000
```

Transform a file with the CLI client:

```sh
transonic-client -p multi_any_audio/single_format_audio \
    -I multi_any -O riff-16khz-16bit-mono-pcm \
    -i input.mp3 -o output.wav \
    http://127.0.0.1:5000
```

Prometheus metrics are exposed at `http://0.0.0.0:8080/metrics`, and the
standard `grpc.health.v1.Health/Check` method reports the service health
(driven by a moving-average filter over transform outcomes).

On a high-core host, set `MALLOC_ARENA_MAX=2` in the environment (the Docker
image does this by default): glibc otherwise creates up to 8× the CPU count of
per-thread malloc arenas that inflate the resident set and are not returned to
the OS.  Capping arenas keeps a tight footprint (peak RSS ~100 MiB vs ~600 MiB
under a concurrency soak) with no measurable throughput cost.

### Monitoring stack (Prometheus + Grafana)

`docker compose up` (see [Quick start](#quick-start-docker-compose)) already
wires this together.  To run it by hand instead, `deploy/` ships a ready-to-run
Prometheus scrape config and a provisioned Grafana dashboard.  With the service
exposing metrics on `127.0.0.1:18330`:

```sh
# Prometheus — scrapes the service (shares the host network to reach it)
docker run -d --name transonic-prom --network host \
  -v "$PWD/deploy/prometheus.yml:/etc/prometheus/prometheus.yml" \
  prom/prometheus

# Grafana — Prometheus datasource + the "Transonic" dashboard, auto-provisioned
docker run -d --name transonic-grafana --network host \
  -e GF_AUTH_ANONYMOUS_ENABLED=true -e GF_AUTH_ANONYMOUS_ORG_ROLE=Admin \
  -v "$PWD/deploy/grafana/provisioning:/etc/grafana/provisioning" \
  -v "$PWD/deploy/grafana/dashboards:/etc/grafana/dashboards" \
  grafana/grafana
```

Then open Grafana at `http://localhost:3000` → dashboard **Transonic**
(active jobs, transcodes completed, throughput ops/s, error ratio, and
throughput-by-outcome / concurrency time series).

## TTS streaming demo

A web demo simulating the TTS scenario — a producer emits raw PCM (from
`testdata/`, in 20 ms chunks, optionally paced at 1× realtime like a live
TTS engine), Transonic transcodes it to MP3 or WebM/Opus at a selectable
sample rate and bitrate, and the browser plays the stream while it is still
being generated.

The encoded audio reaches the browser over a **WebSocket**: binary frames
carry the audio and interleaved JSON frames carry the same events the gRPC
bidi stream produces — progress, media properties and the
`FlowControlNotification` back-pressure signals (`ENOUGH_DATA` / `NEED_DATA`),
which the page surfaces live.  Pick *As fast as possible* to flood the
pipeline and watch it push back.  Codecs the browser can't play through MSE
fall back to a plain chunked-HTTP `<audio src>` with polled metrics.

`docker compose up` serves this at `http://localhost:8093`.  To run it
standalone:

```sh
# Terminal 1: the transform service with the TTS pipeline set
transonic -c config/tts_demo.json 127.0.0.1:15330

# Terminal 2: the demo web server (a client example)
cargo run --release --example demo -- \
    --listen 127.0.0.1:8093 --transform http://127.0.0.1:15330 --data testdata
# then open http://127.0.0.1:8093
```

Inputs cover s16le/f32le PCM at 8/16/24/48 kHz (`pcm_s16_*` / `pcm_f32_*`
bins); outputs cover MP3 at 16/24/44.1 kHz and WebM/Opus at 16/24/48 kHz,
with the bitrate supplied per request through the
`$(pipeline_parameters.bitrate,...)` template placeholder.

## Performance

Numbers from the container image (`docker build -t transonic .`) run with a
pinned vCPU budget (`--cpus`), driven by the bundled `loadtest` example from
the host.  Each request streams a 30 s mono 24 kHz `s16le` PCM clip and
transcodes it to MP3 (24 kHz, 96 kbit/s); the service runs glibc with
`MALLOC_ARENA_MAX=2` and the `config/tts_bench.json` config (2 MiB input
buffer).  The host is shared, so treat these as representative, not lab-grade.

"clips/s" is the request rate — each request is one complete 30 s-clip
transcode — so 90 clips/s means 90 full clips finished per second, i.e.
90 × 30 s = 2670 audio-seconds encoded per wall-second ("aggregate").

| vCPUs | concurrency | throughput     | aggregate      | p50     | p99     |
| ----: | ----------: | -------------: | -------------: | ------: | ------: |
| 8     | 1           | 7 clips/s      | 210× realtime  | 157 ms  | 224 ms  |
| 8     | **8**       | **50 clips/s** | **1490× realtime** | 173 ms  | 196 ms  |
| 8     | 16          | 43 clips/s     | 1290× realtime | 378 ms  | 502 ms  |
| 8     | 32          | 25 clips/s     | 760× realtime  | 1293 ms | 1792 ms |
| 16    | 1           | 7 clips/s      | 223× realtime  | 156 ms  | 165 ms  |
| 16    | **16**      | **90 clips/s** | **2670× realtime** | 178 ms  | 262 ms  |
| 16    | 32          | 52 clips/s     | 1550× realtime | 579 ms  | 977 ms  |
| 16    | 64          | 27 clips/s     | 815× realtime  | 2352 ms | 2581 ms |

Takeaways:

* **Throughput peaks at concurrency ≈ vCPU count** — about 180× realtime and
  ~5.7 clips/s per core — and scales near-linearly from 8 to 16 vCPUs
  (50 → 90 clips/s).  A single stream transcodes a 30 s clip in ~155 ms
  (~190× realtime).
* **Oversubscribing trades throughput for latency.**  Past the core count p50
  climbs steeply and aggregate throughput falls, so size the client's
  concurrency near the available vCPUs rather than firing unbounded requests.
* **WebM/Opus**, a heavier encoder, runs at roughly half the MP3 throughput:
  16 vCPU / concurrency 16 → ~43 clips/s, 1290× realtime, p50 387 ms; a single
  Opus stream is ~104 ms.
* **Footprint stays small** — peak RSS ~150 MiB under the concurrency-16 soak,
  thanks to the arena cap (glibc otherwise balloons to several hundred MiB on
  a many-core host).

Reproduce:

```sh
docker run -d --name transonic-bench --cpus=16 -p 5000:5000 \
    -e TRANSONIC_CONFIG_PATH=/etc/transonic/tts_bench.json transonic
cargo run --release --example loadtest -- \
    http://127.0.0.1:5000 testdata/librivox-austen-24kHz-s16le-mono.raw \
    pcm_s16_24k mp3-24khz 30.0 16 320
```

## Configuration

Configuration files are JSON documents matching the
`transonic.v1.ServiceConfiguration` schema (see
`config/service_config_sample.json`, `config/dev_env.json`,
`config/tts_demo.json` and `config/tts_bench.json`).  Pipeline bins are
`gst-launch`-style fragments with
`$(pipeline_parameters.key,default)` placeholders substituted per channel at
request time.

## Tests

```sh
cargo test --workspace
```

The test suite covers:

* unit tests for the pipeline template language, configuration validation,
  job validation, the VAD and the health moving-average filter;
* engine integration tests (`transonic-core/tests/engine.rs`) covering
  passthrough, concat, file-sink draining, flow control and the error
  taxonomy;
* element tests (`transonic-plugins/tests/elements.rs`) for the custom
  GStreamer elements;
* end-to-end gRPC tests (`transonic-server/tests/grpc.rs`) that start a real
  server on an ephemeral port and drive it over the wire.

Integration tests require a GStreamer installation (core elements are
sufficient for most of them).
