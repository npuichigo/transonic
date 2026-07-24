//! TTS streaming web demo.
//!
//! Simulates a TTS engine: raw PCM utterances (from the bundled test data)
//! are streamed into the Transonic service over gRPC, transcoded to MP3 or
//! WebM/Opus at a selectable sample rate and bitrate, and the encoded stream
//! is forwarded to the browser as a chunked HTTP response that the `<audio>`
//! element plays progressively — audio starts before "synthesis" finishes.
//!
//! Endpoints:
//! * `GET /`            — the demo page.
//! * `GET /api/catalog` — available PCM samples and output formats.
//! * `GET /stream?...`  — the transcoded audio stream.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::{header, StatusCode};
use axum::response::{Html, IntoResponse, Response};
use bytes::Bytes;
use clap::Parser;
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

// Synthetic PCM samples (generated with GStreamer's `audiotestsrc`) that
// stand in for a TTS engine's output — no third-party audio is bundled.
fn sample_definitions() -> Vec<Sample> {
    let empty = Bytes::new();
    vec![
        Sample {
            id: "tone-24k",
            label: "Sine tone A4 · 24 kHz s16le",
            file: "tone-a4-24kHz-s16le-mono.raw",
            input_bin: "pcm_s16_24k",
            sample_rate: 24_000,
            bytes_per_frame: 2,
            sample_format: "s16le",
            data: empty.clone(),
        },
        Sample {
            id: "drone-24k-f32",
            label: "Saw drone · 24 kHz f32le (model output)",
            file: "drone-saw-24kHz-f32le-mono.raw",
            input_bin: "pcm_f32_24k",
            sample_rate: 24_000,
            bytes_per_frame: 4,
            sample_format: "f32le",
            data: empty.clone(),
        },
        Sample {
            id: "tone-16k",
            label: "Triangle tone E4 · 16 kHz s16le",
            file: "tone-e4-16kHz-s16le-mono.raw",
            input_bin: "pcm_s16_16k",
            sample_rate: 16_000,
            bytes_per_frame: 2,
            sample_format: "s16le",
            data: empty.clone(),
        },
        Sample {
            id: "ticks-16k",
            label: "Metronome ticks · 16 kHz s16le",
            file: "ticks-16kHz-s16le-mono.raw",
            input_bin: "pcm_s16_16k",
            sample_rate: 16_000,
            bytes_per_frame: 2,
            sample_format: "s16le",
            data: empty.clone(),
        },
        Sample {
            id: "noise-8k",
            label: "Pink noise · 8 kHz s16le",
            file: "noise-pink-8kHz-s16le-mono.raw",
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
    /// TTS generation pace: "off" (as fast as possible) or a realtime
    /// multiple such as "1", "2", "5".
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
    let pace_multiple: Option<f64> = match params.pace.as_deref() {
        None | Some("off") => None,
        Some(value) => Some(
            value
                .parse()
                .map_err(|_| (StatusCode::BAD_REQUEST, "invalid pace".to_string()))?,
        ),
    };

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
        pace = ?pace_multiple,
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
    let config = proto::TransformConfig {
        pipeline_name: "tts".to_string(),
        inputs: vec![proto::ChannelSpecifier {
            channel_id: "tts-pcm".to_string(),
            pipeline_bin_id: sample.input_bin.to_string(),
            pipeline_parameters: Default::default(),
            metadata: Default::default(),
        }],
        outputs: vec![proto::ChannelSpecifier {
            channel_id: "encoded".to_string(),
            pipeline_bin_id: output.output_bin.to_string(),
            pipeline_parameters: HashMap::from([("bitrate".to_string(), bitrate.to_string())]),
            metadata: Default::default(),
        }],
        parameter_overrides: vec![],
    };
    request_tx
        .send(proto::TransformRequest {
            channel_id: String::new(),
            request: Some(proto::transform_request::Request::Config(config)),
        })
        .await
        .map_err(internal)?;

    // Producer task: emit 20 ms PCM chunks like a streaming TTS engine.
    let producer_sample = sample.clone();
    let producer_cell = Arc::clone(&cell);
    tokio::spawn(async move {
        let chunk_bytes =
            (producer_sample.sample_rate * producer_sample.bytes_per_frame / 50) as usize;
        let chunk_media_time = Duration::from_millis(20);
        let mut offset = 0usize;
        while offset < producer_sample.data.len() {
            let end = (offset + chunk_bytes).min(producer_sample.data.len());
            let request = proto::TransformRequest {
                channel_id: "tts-pcm".to_string(),
                request: Some(proto::transform_request::Request::Payload(
                    proto::InputChannelPayload {
                        chunk_list: vec![producer_sample.data.slice(offset..end)],
                    },
                )),
            };
            if request_tx.send(request).await.is_err() {
                return; // Listener went away.
            }
            offset = end;
            producer_cell.lock().unwrap().data.input_bytes_sent = offset as u64;
            if let Some(multiple) = pace_multiple {
                if multiple > 0.0 {
                    tokio::time::sleep(chunk_media_time.div_f64(multiple)).await;
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
