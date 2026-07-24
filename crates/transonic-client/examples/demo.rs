//! TTS streaming web demo.
//!
//! Simulates a TTS engine: raw PCM utterances (from the bundled test data)
//! are streamed into the Transonic service over gRPC, transcoded to MP3 or
//! WebM/Opus at a selectable sample rate and bitrate, and the encoded stream
//! is forwarded to the browser and the `<audio>` element plays it
//! progressively — audio starts before "synthesis" finishes.
//!
//! Two transports carry the encoded stream to the browser:
//! * `GET /ws?...` — a WebSocket that pushes encoded audio (binary frames)
//!   interleaved with JSON events (progress, media properties and, crucially,
//!   the gRPC `FlowControlNotification` back-pressure signals).  This is the
//!   primary path and mirrors the bidirectional gRPC stream one-to-one.
//! * `GET /stream?...` — a plain chunked HTTP response used as a fallback for
//!   codecs the browser can't play through MSE; server-side metrics are then
//!   polled from `GET /api/progress`.
//!
//! Endpoints:
//! * `GET /`            — the demo page.
//! * `GET /api/catalog` — available PCM samples and output formats.
//! * `GET /ws?...`      — the transcoded audio stream + events (WebSocket).
//! * `GET /stream?...`  — the transcoded audio stream (chunked HTTP fallback).
//! * `GET /api/progress`— server-side metrics for the fallback path.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::{header, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use bytes::Bytes;
use clap::Parser;
use futures::{SinkExt, StreamExt};
use serde_json::json;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info};

use transonic_proto::v1 as proto;

#[derive(Debug, Parser)]
#[command(name = "transonic-demo", about = "TTS streaming demo for Transonic")]
struct Options {
    /// The address to serve the demo on.
    #[arg(long, default_value = "127.0.0.1:8090")]
    listen: SocketAddr,

    /// The Transonic transform-service gRPC endpoint.
    #[arg(long = "transform", default_value = "http://127.0.0.1:5000")]
    transform_endpoint: String,

    /// The directory holding the raw PCM samples.
    #[arg(long, default_value = "testdata")]
    data: PathBuf,
}

///////////////////////////////////////////////////////////////////////////
// catalog
///////////////////////////////////////////////////////////////////////////

/// One raw PCM sample: what a TTS engine would have produced.
#[derive(Clone, serde::Serialize)]
struct Sample {
    id: &'static str,
    label: &'static str,
    file: &'static str,
    /// The matching Transonic input bin.
    input_bin: &'static str,
    sample_rate: u32,
    /// Bytes per PCM frame (mono): 2 for s16le, 4 for f32le.
    bytes_per_frame: u32,
    sample_format: &'static str,
    #[serde(skip)]
    data: Bytes,
}

/// One output encoding preset.
#[derive(Clone, serde::Serialize)]
struct OutputFormat {
    id: &'static str,
    label: &'static str,
    /// The matching Transonic output bin.
    output_bin: &'static str,
    mime: &'static str,
    /// Selectable bitrates; MP3 uses kbit/s, Opus uses bit/s.
    bitrates: &'static [u32],
    default_bitrate: u32,
}

fn output_formats() -> Vec<OutputFormat> {
    vec![
        OutputFormat {
            id: "mp3-16k",
            label: "MP3 · 16 kHz",
            output_bin: "mp3-16khz",
            mime: "audio/mpeg",
            bitrates: &[32, 64, 128],
            default_bitrate: 64,
        },
        OutputFormat {
            id: "mp3-24k",
            label: "MP3 · 24 kHz",
            output_bin: "mp3-24khz",
            mime: "audio/mpeg",
            bitrates: &[48, 96, 160],
            default_bitrate: 96,
        },
        OutputFormat {
            id: "mp3-44k",
            label: "MP3 · 44.1 kHz",
            output_bin: "mp3-44khz",
            mime: "audio/mpeg",
            bitrates: &[96, 128, 192],
            default_bitrate: 128,
        },
        OutputFormat {
            id: "opus-16k",
            label: "WebM/Opus · 16 kHz",
            output_bin: "opus-webm-16khz",
            mime: "audio/webm; codecs=\"opus\"",
            bitrates: &[16000, 24000, 32000],
            default_bitrate: 24000,
        },
        OutputFormat {
            id: "opus-24k",
            label: "WebM/Opus · 24 kHz",
            output_bin: "opus-webm-24khz",
            mime: "audio/webm; codecs=\"opus\"",
            bitrates: &[24000, 32000, 64000],
            default_bitrate: 32000,
        },
        OutputFormat {
            id: "opus-48k",
            label: "WebM/Opus · 48 kHz",
            output_bin: "opus-webm-48khz",
            mime: "audio/webm; codecs=\"opus\"",
            bitrates: &[32000, 64000, 96000],
            default_bitrate: 64000,
        },
    ]
}

// Speech PCM samples standing in for a TTS engine's output.  These are ~30 s
// clips of public-domain LibriVox audiobook recordings (see testdata/README.md
// for provenance), decoded to raw mono PCM at the rates/formats a TTS backend
// typically emits.
fn sample_definitions() -> Vec<Sample> {
    let empty = Bytes::new();
    vec![
        Sample {
            id: "austen-24k",
            label: "Austen narration · 24 kHz s16le",
            file: "librivox-austen-24kHz-s16le-mono.raw",
            input_bin: "pcm_s16_24k",
            sample_rate: 24_000,
            bytes_per_frame: 2,
            sample_format: "s16le",
            data: empty.clone(),
        },
        Sample {
            id: "austen-24k-f32",
            label: "Austen narration · 24 kHz f32le (model output)",
            file: "librivox-austen-24kHz-f32le-mono.raw",
            input_bin: "pcm_f32_24k",
            sample_rate: 24_000,
            bytes_per_frame: 4,
            sample_format: "f32le",
            data: empty.clone(),
        },
        Sample {
            id: "suntzu-16k",
            label: "Sun Tzu narration · 16 kHz s16le",
            file: "librivox-suntzu-16kHz-s16le-mono.raw",
            input_bin: "pcm_s16_16k",
            sample_rate: 16_000,
            bytes_per_frame: 2,
            sample_format: "s16le",
            data: empty.clone(),
        },
        Sample {
            id: "suntzu-8k",
            label: "Sun Tzu narration · 8 kHz s16le",
            file: "librivox-suntzu-8kHz-s16le-mono.raw",
            input_bin: "pcm_s16_8k",
            sample_rate: 8_000,
            bytes_per_frame: 2,
            sample_format: "s16le",
            data: empty,
        },
    ]
}

struct AppState {
    transform_endpoint: String,
    samples: HashMap<&'static str, Sample>,
    outputs: HashMap<&'static str, OutputFormat>,
    /// Per-session server-side streaming metrics, keyed by the page-supplied
    /// session id.  Used to observe the transform-service leg independently of the
    /// browser/network leg.
    progress: std::sync::Mutex<HashMap<String, Arc<std::sync::Mutex<ProgressCell>>>>,
}

/// Server-side metrics for one streaming session.
#[derive(Clone, serde::Serialize)]
struct Progress {
    /// "starting" | "streaming" | "done" | "error"
    state: String,
    /// Demo-server time from issuing the Transform RPC to receiving the
    /// first encoded chunk from the transform service (excludes browser and page network).
    transform_first_chunk_ms: Option<f64>,
    /// Milliseconds since the Transform RPC was issued.
    elapsed_ms: f64,
    /// Encoded bytes received from the transform service so far.
    output_bytes: u64,
    /// Encoded payload chunks received from the transform service so far.
    output_chunks: u64,
    /// PCM bytes fed to the transform service so far.
    input_bytes_sent: u64,
    /// Total PCM bytes of the selected sample.
    input_bytes_total: u64,
    /// Nominal encoded bitrate in bit/s (for media-time estimation).
    bitrate_bps: u64,
    /// Source media duration in seconds.
    source_seconds: f64,
    termination: Option<String>,
}

struct ProgressCell {
    started: std::time::Instant,
    data: Progress,
}

impl ProgressCell {
    fn new(input_bytes_total: u64, bitrate_bps: u64, source_seconds: f64) -> Self {
        ProgressCell {
            started: std::time::Instant::now(),
            data: Progress {
                state: "starting".to_string(),
                transform_first_chunk_ms: None,
                elapsed_ms: 0.0,
                output_bytes: 0,
                output_chunks: 0,
                input_bytes_sent: 0,
                input_bytes_total,
                bitrate_bps,
                source_seconds,
                termination: None,
            },
        }
    }
}

///////////////////////////////////////////////////////////////////////////
// shared transform plumbing
///////////////////////////////////////////////////////////////////////////

/// How the producer paces PCM into the pipeline.
#[derive(Clone, Copy, Debug)]
enum Pace {
    /// Push everything as fast as the pipeline accepts it.
    Fast,
    /// Push at a steady multiple of realtime (1.0 = live TTS).
    Realtime(f64),
    /// Push a burst, pause, repeat — like a TTS model that emits an utterance,
    /// thinks, then emits the next.  Bursts overrun the input soft limit
    /// (ENOUGH_DATA) and the pauses let the pipeline starve (NEED_DATA), so the
    /// back-pressure signal visibly alternates.
    Burst,
}

/// Parses the `pace` query parameter into a [`Pace`].
fn parse_pace(value: Option<&str>) -> Result<Pace, ()> {
    match value {
        None | Some("off") => Ok(Pace::Fast),
        Some("burst") => Ok(Pace::Burst),
        Some(other) => other.parse::<f64>().map(Pace::Realtime).map_err(|_| ()),
    }
}

/// Builds the `TransformConfig` for one TTS request: a single PCM input
/// channel and a single encoded output channel with the bitrate substituted
/// into the pipeline template.
fn build_config(input_bin: &str, output_bin: &str, bitrate: u32) -> proto::TransformConfig {
    proto::TransformConfig {
        pipeline_name: "tts".to_string(),
        inputs: vec![proto::ChannelSpecifier {
            channel_id: "tts-pcm".to_string(),
            pipeline_bin_id: input_bin.to_string(),
            pipeline_parameters: Default::default(),
            metadata: Default::default(),
        }],
        outputs: vec![proto::ChannelSpecifier {
            channel_id: "encoded".to_string(),
            pipeline_bin_id: output_bin.to_string(),
            pipeline_parameters: HashMap::from([("bitrate".to_string(), bitrate.to_string())]),
            metadata: Default::default(),
        }],
        parameter_overrides: vec![],
    }
}

/// Spawns the producer task: emits 20 ms PCM chunks like a streaming TTS
/// engine, optionally paced at a realtime multiple.  `on_sent` is invoked with
/// the running count of PCM bytes handed to the request stream.
fn spawn_producer(
    request_tx: tokio::sync::mpsc::Sender<proto::TransformRequest>,
    sample: Sample,
    pace: Pace,
    on_sent: impl Fn(u64) + Send + 'static,
) {
    // Burst mode: emit roughly this many PCM bytes at full speed (enough to
    // overrun the demo's input soft limit) then pause before the next burst.
    const BURST_BYTES: usize = 350_000;
    const BURST_PAUSE: Duration = Duration::from_millis(600);

    tokio::spawn(async move {
        let chunk_bytes = (sample.sample_rate * sample.bytes_per_frame / 50) as usize;
        let chunk_media_time = Duration::from_millis(20);
        let mut offset = 0usize;
        let mut burst_sent = 0usize;
        while offset < sample.data.len() {
            let end = (offset + chunk_bytes).min(sample.data.len());
            let piece_len = end - offset;
            let request = proto::TransformRequest {
                channel_id: "tts-pcm".to_string(),
                request: Some(proto::transform_request::Request::Payload(
                    proto::InputChannelPayload {
                        chunk_list: vec![sample.data.slice(offset..end)],
                    },
                )),
            };
            if request_tx.send(request).await.is_err() {
                return; // Listener went away.
            }
            offset = end;
            on_sent(offset as u64);
            match pace {
                Pace::Fast => {}
                Pace::Realtime(multiple) if multiple > 0.0 => {
                    tokio::time::sleep(chunk_media_time.div_f64(multiple)).await;
                }
                Pace::Realtime(_) => {}
                Pace::Burst => {
                    burst_sent += piece_len;
                    if burst_sent >= BURST_BYTES {
                        burst_sent = 0;
                        tokio::time::sleep(BURST_PAUSE).await;
                    }
                }
            }
        }
        let _ = request_tx
            .send(proto::TransformRequest {
                channel_id: "tts-pcm".to_string(),
                request: Some(proto::transform_request::Request::ChannelCompleted(
                    proto::ChannelCompleted {},
                )),
            })
            .await;
    });
}

///////////////////////////////////////////////////////////////////////////
// handlers
///////////////////////////////////////////////////////////////////////////

async fn index() -> Html<&'static str> {
    Html(include_str!("index.html"))
}

async fn catalog(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    #[derive(serde::Serialize)]
    struct SampleInfo<'a> {
        #[serde(flatten)]
        sample: &'a Sample,
        duration_seconds: f64,
        size_bytes: usize,
    }
    let mut samples: Vec<SampleInfo> = state
        .samples
        .values()
        .map(|sample| SampleInfo {
            duration_seconds: sample.data.len() as f64
                / f64::from(sample.sample_rate * sample.bytes_per_frame),
            size_bytes: sample.data.len(),
            sample,
        })
        .collect();
    samples.sort_by_key(|info| info.sample.id);
    let mut outputs: Vec<&OutputFormat> = state.outputs.values().collect();
    outputs.sort_by_key(|output| output.id);
    axum::Json(serde_json::json!({ "samples": samples, "outputs": outputs }))
}

#[derive(serde::Deserialize)]
struct StreamParams {
    sample: String,
    output: String,
    bitrate: Option<u32>,
    /// Page-supplied session id for server-side metrics correlation.
    session: Option<String>,
    /// TTS generation pace: "off" (as fast as possible), "burst" (bursty,
    /// TTS-like) or a realtime multiple such as "1", "2", "5".
    pace: Option<String>,
}

async fn stream(
    State(state): State<Arc<AppState>>,
    Query(params): Query<StreamParams>,
) -> Result<Response, (StatusCode, String)> {
    let sample = state
        .samples
        .get(params.sample.as_str())
        .ok_or((StatusCode::NOT_FOUND, "unknown sample".to_string()))?
        .clone();
    let output = state
        .outputs
        .get(params.output.as_str())
        .ok_or((StatusCode::NOT_FOUND, "unknown output format".to_string()))?
        .clone();
    let bitrate = params.bitrate.unwrap_or(output.default_bitrate);
    if !output.bitrates.contains(&bitrate) {
        return Err((StatusCode::BAD_REQUEST, "unsupported bitrate".to_string()));
    }
    let pace = parse_pace(params.pace.as_deref())
        .map_err(|_| (StatusCode::BAD_REQUEST, "invalid pace".to_string()))?;

    // Register server-side metrics for this session.
    let bitrate_bps = if output.mime == "audio/mpeg" {
        u64::from(bitrate) * 1000
    } else {
        u64::from(bitrate)
    };
    let source_seconds =
        sample.data.len() as f64 / f64::from(sample.sample_rate * sample.bytes_per_frame);
    let cell = Arc::new(std::sync::Mutex::new(ProgressCell::new(
        sample.data.len() as u64,
        bitrate_bps,
        source_seconds,
    )));
    if let Some(session) = &params.session {
        let mut registry = state.progress.lock().unwrap();
        // Basic eviction so the registry can't grow without bound.
        if registry.len() > 64 {
            registry.retain(|_, cell| {
                cell.lock().unwrap().started.elapsed() < Duration::from_secs(900)
            });
        }
        registry.insert(session.clone(), Arc::clone(&cell));
    }

    let request_id = format!("demo-{}", uuid::Uuid::new_v4());
    info!(
        request = %request_id,
        sample = sample.id,
        output = output.id,
        bitrate,
        pace = ?pace,
        "starting transform"
    );

    // Connect to the transform service.
    let channel = tonic::transport::Endpoint::from_shared(state.transform_endpoint.clone())
        .map_err(internal)?
        .connect()
        .await
        .map_err(internal)?;
    let mut client = proto::transform_service_client::TransformServiceClient::new(channel);

    // Request stream: config, paced PCM payloads, channel completion.
    let (request_tx, request_rx) = tokio::sync::mpsc::channel::<proto::TransformRequest>(16);
    let config = build_config(sample.input_bin, output.output_bin, bitrate);
    request_tx
        .send(proto::TransformRequest {
            channel_id: String::new(),
            request: Some(proto::transform_request::Request::Config(config)),
        })
        .await
        .map_err(internal)?;

    // Producer task: emit 20 ms PCM chunks like a streaming TTS engine.
    let producer_cell = Arc::clone(&cell);
    spawn_producer(request_tx, sample.clone(), pace, move |sent| {
        producer_cell.lock().unwrap().data.input_bytes_sent = sent;
    });

    let mut request = tonic::Request::new(ReceiverStream::new(request_rx));
    request
        .metadata_mut()
        .insert("request_id", request_id.parse().map_err(internal)?);
    let mut responses = client
        .transform(request)
        .await
        .map_err(internal)?
        .into_inner();

    // Forward encoded payload chunks to the HTTP response body, recording
    // Transform-service-side metrics as they arrive.
    let stream_cell = Arc::clone(&cell);
    let body_stream = async_stream::stream! {
        loop {
            match responses.message().await {
                Ok(Some(response)) => match response.response {
                    Some(proto::transform_response::Response::Payload(payload)) => {
                        {
                            let mut cell = stream_cell.lock().unwrap();
                            let elapsed = cell.started.elapsed().as_secs_f64() * 1000.0;
                            if cell.data.transform_first_chunk_ms.is_none() {
                                cell.data.transform_first_chunk_ms = Some(elapsed);
                                cell.data.state = "streaming".to_string();
                            }
                            cell.data.output_chunks += payload.chunk_list.len() as u64;
                            cell.data.output_bytes += payload
                                .chunk_list
                                .iter()
                                .map(|chunk| chunk.len() as u64)
                                .sum::<u64>();
                        }
                        for chunk in payload.chunk_list {
                            yield Ok::<Bytes, std::io::Error>(chunk);
                        }
                    }
                    Some(proto::transform_response::Response::TransformCompleted(done)) => {
                        info!(
                            request = %request_id,
                            reason = ?done.termination_reason(),
                            "transform completed"
                        );
                        let mut cell = stream_cell.lock().unwrap();
                        cell.data.termination = Some(format!("{:?}", done.termination_reason()));
                    }
                    _ => {}
                },
                Ok(None) => {
                    let mut cell = stream_cell.lock().unwrap();
                    cell.data.state = "done".to_string();
                    cell.data.elapsed_ms = cell.started.elapsed().as_secs_f64() * 1000.0;
                    break;
                }
                Err(status) => {
                    error!(request = %request_id, status = %status, "transform failed");
                    let mut cell = stream_cell.lock().unwrap();
                    cell.data.state = "error".to_string();
                    cell.data.elapsed_ms = cell.started.elapsed().as_secs_f64() * 1000.0;
                    cell.data.termination = Some(status.to_string());
                    break;
                }
            }
        }
    };

    Response::builder()
        .header(header::CONTENT_TYPE, output.mime)
        .header(header::CACHE_CONTROL, "no-store")
        .body(axum::body::Body::from_stream(body_stream))
        .map_err(internal)
}

#[derive(serde::Deserialize)]
struct ProgressParams {
    session: String,
}

async fn progress(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ProgressParams>,
) -> Result<axum::Json<Progress>, (StatusCode, String)> {
    let cell = state
        .progress
        .lock()
        .unwrap()
        .get(&params.session)
        .cloned()
        .ok_or((StatusCode::NOT_FOUND, "unknown session".to_string()))?;
    let snapshot = {
        let cell = cell.lock().unwrap();
        let mut data = cell.data.clone();
        // Elapsed keeps ticking while live and freezes at completion.
        if data.state != "done" && data.state != "error" {
            data.elapsed_ms = cell.started.elapsed().as_secs_f64() * 1000.0;
        }
        data
    };
    Ok(axum::Json(snapshot))
}

///////////////////////////////////////////////////////////////////////////
// websocket transport
///////////////////////////////////////////////////////////////////////////

/// Upgrades to a WebSocket that streams the transcoded audio and events.
/// Parameters are validated before the upgrade so a bad request still yields a
/// normal HTTP error.
async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    Query(params): Query<StreamParams>,
) -> Response {
    let sample = match state.samples.get(params.sample.as_str()) {
        Some(sample) => sample.clone(),
        None => return (StatusCode::NOT_FOUND, "unknown sample").into_response(),
    };
    let output = match state.outputs.get(params.output.as_str()) {
        Some(output) => output.clone(),
        None => return (StatusCode::NOT_FOUND, "unknown output format").into_response(),
    };
    let bitrate = params.bitrate.unwrap_or(output.default_bitrate);
    if !output.bitrates.contains(&bitrate) {
        return (StatusCode::BAD_REQUEST, "unsupported bitrate").into_response();
    }
    let pace = match parse_pace(params.pace.as_deref()) {
        Ok(pace) => pace,
        Err(_) => return (StatusCode::BAD_REQUEST, "invalid pace").into_response(),
    };
    ws.on_upgrade(move |socket| ws_session(socket, state, sample, output, bitrate, pace))
}

/// A JSON progress event mirroring the fields the fallback `/api/progress`
/// endpoint returns, so the page can drive the same metric widgets.
#[allow(clippy::too_many_arguments)]
fn progress_event(
    first_chunk_ms: Option<f64>,
    elapsed_ms: f64,
    output_bytes: u64,
    output_chunks: u64,
    input_bytes_sent: u64,
    input_bytes_total: u64,
    bitrate_bps: u64,
    source_seconds: f64,
    enough_count: u64,
    need_count: u64,
) -> Message {
    Message::Text(
        json!({
            "type": "progress",
            "transform_first_chunk_ms": first_chunk_ms,
            "elapsed_ms": elapsed_ms,
            "output_bytes": output_bytes,
            "output_chunks": output_chunks,
            "input_bytes_sent": input_bytes_sent,
            "input_bytes_total": input_bytes_total,
            "bitrate_bps": bitrate_bps,
            "source_seconds": source_seconds,
            "enough_count": enough_count,
            "need_count": need_count,
        })
        .to_string(),
    )
}

fn error_event<E: std::fmt::Display>(error: &E) -> Message {
    Message::Text(json!({ "type": "error", "message": error.to_string() }).to_string())
}

/// Runs one transform over a WebSocket: opens the gRPC bidi stream, forwards
/// encoded audio as binary frames and every other response (progress, flow
/// control, media properties, completion, errors) as JSON text frames.
async fn ws_session(
    socket: WebSocket,
    state: Arc<AppState>,
    sample: Sample,
    output: OutputFormat,
    bitrate: u32,
    pace: Pace,
) {
    let (mut sink, mut stream) = socket.split();

    let bitrate_bps = if output.mime == "audio/mpeg" {
        u64::from(bitrate) * 1000
    } else {
        u64::from(bitrate)
    };
    let source_seconds =
        sample.data.len() as f64 / f64::from(sample.sample_rate * sample.bytes_per_frame);
    let input_bytes_total = sample.data.len() as u64;
    let request_id = format!("demo-{}", uuid::Uuid::new_v4());
    info!(
        request = %request_id,
        sample = sample.id,
        output = output.id,
        bitrate,
        pace = ?pace,
        "starting transform (ws)"
    );

    // Drives the transform and writes all frames into the WebSocket sink.
    let produce = async {
        let channel =
            match tonic::transport::Endpoint::from_shared(state.transform_endpoint.clone()) {
                Ok(endpoint) => match endpoint.connect().await {
                    Ok(channel) => channel,
                    Err(e) => {
                        let _ = sink.send(error_event(&e)).await;
                        return;
                    }
                },
                Err(e) => {
                    let _ = sink.send(error_event(&e)).await;
                    return;
                }
            };
        let mut client = proto::transform_service_client::TransformServiceClient::new(channel);

        let (request_tx, request_rx) = tokio::sync::mpsc::channel::<proto::TransformRequest>(16);
        let config = build_config(sample.input_bin, output.output_bin, bitrate);
        if request_tx
            .send(proto::TransformRequest {
                channel_id: String::new(),
                request: Some(proto::transform_request::Request::Config(config)),
            })
            .await
            .is_err()
        {
            return;
        }

        let input_sent = Arc::new(AtomicU64::new(0));
        {
            let input_sent = Arc::clone(&input_sent);
            spawn_producer(request_tx, sample.clone(), pace, move |sent| {
                input_sent.store(sent, Ordering::Relaxed);
            });
        }

        let mut request = tonic::Request::new(ReceiverStream::new(request_rx));
        if let Ok(value) = request_id.parse() {
            request.metadata_mut().insert("request_id", value);
        }
        let mut responses = match client.transform(request).await {
            Ok(response) => response.into_inner(),
            Err(status) => {
                let _ = sink.send(error_event(&status)).await;
                return;
            }
        };

        let started = std::time::Instant::now();
        let mut output_bytes = 0u64;
        let mut output_chunks = 0u64;
        let mut first_chunk_ms: Option<f64> = None;
        let mut enough_count = 0u64;
        let mut need_count = 0u64;
        let mut termination: Option<String> = None;
        // Throttle progress pushes; encoded payloads can arrive far faster
        // than the page needs to repaint.
        let mut last_progress = started;

        loop {
            match responses.message().await {
                Ok(Some(response)) => match response.response {
                    Some(proto::transform_response::Response::Payload(payload)) => {
                        if first_chunk_ms.is_none() {
                            first_chunk_ms = Some(started.elapsed().as_secs_f64() * 1000.0);
                        }
                        for chunk in payload.chunk_list {
                            output_chunks += 1;
                            output_bytes += chunk.len() as u64;
                            if sink.send(Message::Binary(chunk.to_vec())).await.is_err() {
                                return;
                            }
                        }
                        if last_progress.elapsed() >= Duration::from_millis(100) {
                            last_progress = std::time::Instant::now();
                            let event = progress_event(
                                first_chunk_ms,
                                started.elapsed().as_secs_f64() * 1000.0,
                                output_bytes,
                                output_chunks,
                                input_sent.load(Ordering::Relaxed),
                                input_bytes_total,
                                bitrate_bps,
                                source_seconds,
                                enough_count,
                                need_count,
                            );
                            if sink.send(event).await.is_err() {
                                return;
                            }
                        }
                    }
                    Some(proto::transform_response::Response::FlowControlNotification(fc)) => {
                        let kind = match fc.notification_type() {
                            proto::FlowControlNotificationType::EnoughData => {
                                enough_count += 1;
                                "enough_data"
                            }
                            proto::FlowControlNotificationType::NeedData => {
                                need_count += 1;
                                "need_data"
                            }
                        };
                        let event = json!({
                            "type": "flow_control",
                            "kind": kind,
                            "enough_count": enough_count,
                            "need_count": need_count,
                        })
                        .to_string();
                        if sink.send(Message::Text(event)).await.is_err() {
                            return;
                        }
                    }
                    Some(proto::transform_response::Response::MediaProperties(media)) => {
                        if let Some(proto::media_properties::Properties::AudioProperties(audio)) =
                            media.properties
                        {
                            let event = json!({
                                "type": "media_properties",
                                "encoding": audio.encoding,
                                "sample_rate": audio.sample_rate,
                                "sample_type": audio.sample_type,
                                "channels": audio.channels,
                            })
                            .to_string();
                            if sink.send(Message::Text(event)).await.is_err() {
                                return;
                            }
                        }
                    }
                    Some(proto::transform_response::Response::TransformCompleted(done)) => {
                        termination = Some(format!("{:?}", done.termination_reason()));
                    }
                    _ => {}
                },
                Ok(None) => {
                    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
                    let _ = sink
                        .send(progress_event(
                            first_chunk_ms,
                            elapsed_ms,
                            output_bytes,
                            output_chunks,
                            input_sent.load(Ordering::Relaxed),
                            input_bytes_total,
                            bitrate_bps,
                            source_seconds,
                            enough_count,
                            need_count,
                        ))
                        .await;
                    let event = json!({
                        "type": "completed",
                        "termination": termination,
                        "elapsed_ms": elapsed_ms,
                    })
                    .to_string();
                    let _ = sink.send(Message::Text(event)).await;
                    let _ = sink.send(Message::Close(None)).await;
                    return;
                }
                Err(status) => {
                    error!(request = %request_id, status = %status, "transform failed (ws)");
                    let _ = sink.send(error_event(&status)).await;
                    return;
                }
            }
        }
    };

    // Resolves when the browser closes the socket, so we can abort the
    // transform promptly instead of running it to completion unseen.
    let closed = async {
        while let Some(Ok(message)) = stream.next().await {
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
    };

    tokio::select! {
        _ = produce => {}
        _ = closed => info!(request = %request_id, "client closed the stream"),
    }
}

fn internal<E: std::fmt::Display>(error: E) -> (StatusCode, String) {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

///////////////////////////////////////////////////////////////////////////
// main
///////////////////////////////////////////////////////////////////////////

#[tokio::main]
async fn main() -> Result<(), String> {
    tracing_subscriber::fmt()
        .with_max_level(tracing_subscriber::filter::LevelFilter::INFO)
        .init();
    let options = Options::parse();

    // Load the PCM samples into memory.
    let mut samples = HashMap::new();
    for mut sample in sample_definitions() {
        let path = options.data.join(sample.file);
        match std::fs::read(&path) {
            Ok(data) => {
                sample.data = Bytes::from(data);
                samples.insert(sample.id, sample);
            }
            Err(e) => {
                error!(file = %path.display(), error = %e, "skipping unavailable sample");
            }
        }
    }
    if samples.is_empty() {
        return Err(format!(
            "no samples found under '{}'",
            options.data.display()
        ));
    }

    let state = Arc::new(AppState {
        transform_endpoint: options.transform_endpoint.clone(),
        samples,
        outputs: output_formats()
            .into_iter()
            .map(|output| (output.id, output))
            .collect(),
        progress: std::sync::Mutex::new(HashMap::new()),
    });

    let app = axum::Router::new()
        .route("/", axum::routing::get(index))
        .route("/api/catalog", axum::routing::get(catalog))
        .route("/api/progress", axum::routing::get(progress))
        .route("/ws", axum::routing::get(ws_handler))
        .route("/stream", axum::routing::get(stream))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(options.listen)
        .await
        .map_err(|e| format!("failed to bind {}: {e}", options.listen))?;
    info!(listen = %options.listen, transform = %options.transform_endpoint, "TTS demo ready");
    axum::serve(listener, app)
        .await
        .map_err(|e| format!("server error: {e}"))
}
