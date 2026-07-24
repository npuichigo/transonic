//! Command line client for the Transonic transform service.
//!
//! Reads media from a file (or stdin), streams it through the `Transform` RPC
//! and writes the transformed output to a file (or stdout).

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use clap::Parser;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info, trace};

use transonic_proto::v1 as proto;

const DEFAULT_INPUT_BIN: &str = "multi_pcm_16";
const DEFAULT_OUTPUT_BIN: &str = "riff-16khz-16bit-mono-pcm";

/// Flow control enforcement mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
enum FlowControlMode {
    Block,
    Error,
    Notify,
    /// NOTIFY on the server plus manual client-side pacing.
    Clientside,
}

#[derive(Debug, Parser)]
#[command(name = "transonic-client", about = "Transonic media transform client")]
struct Options {
    /// Set pipeline output buffer size.  Default 0 (no buffering).
    #[arg(short = 'b')]
    output_buffer: Option<u32>,

    /// Flow control enforcement policy.
    #[arg(short = 'e', value_enum)]
    flow_control: Option<FlowControlMode>,

    /// Input file.  Default stdin.
    #[arg(short = 'i')]
    input: Option<PathBuf>,

    /// Input bin type.
    #[arg(short = 'I', default_value = DEFAULT_INPUT_BIN)]
    input_bin: String,

    /// Maximum audio duration in milliseconds, 0 unlimited.
    #[arg(short = 'l')]
    length_limit: Option<u32>,

    /// Number of input channels (all use the same input file and bin).
    #[arg(short = 'n', default_value_t = 1, value_parser = clap::value_parser!(u32).range(1..=16))]
    input_channels: u32,

    /// Output file.  Default stdout.
    #[arg(short = 'o')]
    output: Option<PathBuf>,

    /// Output bin type.
    #[arg(short = 'O', default_value = DEFAULT_OUTPUT_BIN)]
    output_bin: String,

    /// Existing pipeline name as defined on the server.
    #[arg(short = 'p')]
    pipeline: String,

    /// Transformation rate: 1.0 = realtime, <= 0 as fast as possible.
    #[arg(short = 'r')]
    rate: Option<f64>,

    /// Request timeout in seconds (gRPC deadline).
    #[arg(short = 't', default_value_t = 5)]
    timeout_sec: u64,

    /// Write delay in ms; 0 none, negative for random delays up to |MS|.
    #[arg(short = 'w', default_value_t = 0)]
    write_delay: i64,

    /// The server endpoint, e.g. `http://127.0.0.1:5000`.
    endpoint: String,
}

fn build_transform_config(options: &Options) -> (proto::TransformConfig, Vec<String>) {
    let mut parameters = Vec::new();
    if let Some(mode) = options.flow_control {
        let policy = match mode {
            FlowControlMode::Block => proto::FlowControlEnforcementPolicy::Block,
            FlowControlMode::Error => proto::FlowControlEnforcementPolicy::Error,
            FlowControlMode::Notify | FlowControlMode::Clientside => {
                proto::FlowControlEnforcementPolicy::Notify
            }
        };
        parameters.push(proto::ServiceParameter {
            value: Some(
                proto::service_parameter::Value::FlowControlEnforcementPolicy(policy.into()),
            ),
        });
    }
    if let Some(rate) = options.rate {
        parameters.push(proto::ServiceParameter {
            value: Some(proto::service_parameter::Value::Rate(rate)),
        });
    }
    if let Some(limit) = options.length_limit {
        parameters.push(proto::ServiceParameter {
            value: Some(proto::service_parameter::Value::LengthLimitMilliseconds(
                limit,
            )),
        });
    }
    if let Some(buffer) = options.output_buffer {
        parameters.push(proto::ServiceParameter {
            value: Some(proto::service_parameter::Value::PipelineInputBuffer(buffer)),
        });
    }
    // RIFF outputs need finalized headers, so disable streaming output.
    if options.output_bin.contains("riff") {
        parameters.push(proto::ServiceParameter {
            value: Some(proto::service_parameter::Value::StreamOutput(false)),
        });
    }

    let extension = options
        .input
        .as_ref()
        .and_then(|path| path.extension())
        .and_then(|ext| ext.to_str())
        .map(|ext| format!(".{ext}"));

    let mut inputs = Vec::new();
    let mut input_channel_ids = Vec::new();
    for i in 0..options.input_channels {
        let id = format!("I{:02}", i + 1);
        input_channel_ids.push(id.clone());
        let mut metadata = std::collections::HashMap::new();
        if let Some(extension) = &extension {
            metadata.insert("extension".to_string(), extension.clone());
        }
        inputs.push(proto::ChannelSpecifier {
            channel_id: id,
            pipeline_bin_id: options.input_bin.clone(),
            pipeline_parameters: Default::default(),
            metadata,
        });
    }

    let outputs = vec![proto::ChannelSpecifier {
        channel_id: "O1".to_string(),
        pipeline_bin_id: options.output_bin.clone(),
        pipeline_parameters: Default::default(),
        metadata: Default::default(),
    }];

    (
        proto::TransformConfig {
            pipeline_name: options.pipeline.clone(),
            inputs,
            outputs,
            parameter_overrides: parameters,
        },
        input_channel_ids,
    )
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let options = Options::parse();

    // TRANSONIC_CLIENT_DEBUG selects a numeric log level; map it onto tracing
    // levels here.
    let level = match std::env::var("TRANSONIC_CLIENT_DEBUG").ok().as_deref() {
        Some("0") => tracing_subscriber::filter::LevelFilter::TRACE,
        Some("1") => tracing_subscriber::filter::LevelFilter::DEBUG,
        Some("3") => tracing_subscriber::filter::LevelFilter::WARN,
        Some("4") | Some("5") => tracing_subscriber::filter::LevelFilter::ERROR,
        Some("6") => tracing_subscriber::filter::LevelFilter::OFF,
        _ => tracing_subscriber::filter::LevelFilter::INFO,
    };
    tracing_subscriber::fmt()
        .with_max_level(level)
        .with_writer(std::io::stderr)
        .init();

    match transform(options).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(message) => {
            error!("{message}");
            std::process::ExitCode::FAILURE
        }
    }
}

async fn transform(options: Options) -> Result<(), String> {
    let request_id = format!("transonic-client-{}", uuid::Uuid::new_v4());
    info!("starting request ID {request_id}");

    let (config, input_channel_ids) = build_transform_config(&options);

    let endpoint = tonic::transport::Endpoint::from_shared(options.endpoint.clone())
        .map_err(|e| format!("invalid endpoint '{}': {e}", options.endpoint))?
        .keep_alive_while_idle(true)
        .http2_keep_alive_interval(Duration::from_secs(3))
        .keep_alive_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(options.timeout_sec));
    let channel = endpoint
        .connect()
        .await
        .map_err(|e| format!("failed to connect: {e}"))?;
    let mut client = proto::transform_service_client::TransformServiceClient::new(channel);

    // Request stream: config first, then payloads, then per-channel
    // completion messages.
    let (request_tx, request_rx) = mpsc::channel::<proto::TransformRequest>(16);
    let server_ready = Arc::new(AtomicBool::new(false));

    let mut request = tonic::Request::new(ReceiverStream::new(request_rx));
    request.metadata_mut().insert(
        "request_id",
        request_id
            .parse()
            .map_err(|_| "invalid request id metadata".to_string())?,
    );

    // Feed the config before the RPC starts consuming the stream.
    request_tx
        .send(proto::TransformRequest {
            channel_id: String::new(),
            request: Some(proto::transform_request::Request::Config(config)),
        })
        .await
        .map_err(|_| "request stream closed".to_string())?;

    let response = client
        .transform(request)
        .await
        .map_err(|e| format!("transform call failed: {e}"))?;
    let mut responses = response.into_inner();

    // Reader task: writes payloads to the output and tracks flow control.
    let output_path = options.output.clone();
    let ready_flag = Arc::clone(&server_ready);
    let reader = tokio::spawn(async move {
        let mut output: Box<dyn tokio::io::AsyncWrite + Unpin + Send> = match &output_path {
            Some(path) => Box::new(
                tokio::fs::File::create(path)
                    .await
                    .map_err(|e| format!("unable to open output file: {e}"))?,
            ),
            None => Box::new(tokio::io::stdout()),
        };

        let mut read_count = 0u64;
        let mut completed: Option<proto::TransformCompleted> = None;
        let mut status: Option<tonic::Status> = None;

        loop {
            match responses.message().await {
                Ok(Some(response)) => match response.response {
                    Some(proto::transform_response::Response::Payload(payload)) => {
                        for chunk in &payload.chunk_list {
                            trace!("got audio payload of size {}", chunk.len());
                            output
                                .write_all(chunk)
                                .await
                                .map_err(|e| format!("failed to write output: {e}"))?;
                        }
                        read_count += 1;
                    }
                    Some(proto::transform_response::Response::FlowControlNotification(
                        notification,
                    )) => match notification.notification_type() {
                        proto::FlowControlNotificationType::NeedData => {
                            trace!("got NEED_DATA flow control notification");
                            ready_flag.store(true, Ordering::SeqCst);
                        }
                        proto::FlowControlNotificationType::EnoughData => {
                            trace!("got ENOUGH_DATA flow control notification");
                            ready_flag.store(false, Ordering::SeqCst);
                        }
                    },
                    Some(proto::transform_response::Response::ServerLifecycleNotification(_)) => {
                        trace!("got SHUTTING_DOWN server lifecycle notification");
                    }
                    Some(proto::transform_response::Response::MediaProperties(properties)) => {
                        trace!("got media properties: {properties:?}");
                    }
                    Some(proto::transform_response::Response::TransformCompleted(done)) => {
                        completed = Some(done);
                    }
                    None => {}
                },
                Ok(None) => break,
                Err(s) => {
                    status = Some(s);
                    break;
                }
            }
        }
        output
            .flush()
            .await
            .map_err(|e| format!("failed to flush output: {e}"))?;
        Ok::<_, String>((read_count, completed, status))
    });

    // Writer loop: stream the input file through all input channels.
    let mut input: Box<dyn tokio::io::AsyncRead + Unpin + Send> = match &options.input {
        Some(path) => Box::new(
            tokio::fs::File::open(path)
                .await
                .map_err(|e| format!("unable to open input file: {e}"))?,
        ),
        None => Box::new(tokio::io::stdin()),
    };

    let manual_flow_control = options.flow_control == Some(FlowControlMode::Clientside);
    let mut total_written = 0u64;
    let mut buffer = vec![0u8; 4096];
    'writer: loop {
        if manual_flow_control && !server_ready.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(1)).await;
            continue;
        }
        let count = input
            .read(&mut buffer)
            .await
            .map_err(|e| format!("failed to read input: {e}"))?;
        if count == 0 {
            break;
        }
        let data = Bytes::copy_from_slice(&buffer[..count]);
        for channel_id in &input_channel_ids {
            let request = proto::TransformRequest {
                channel_id: channel_id.clone(),
                request: Some(proto::transform_request::Request::Payload(
                    proto::InputChannelPayload {
                        chunk_list: vec![data.clone()],
                    },
                )),
            };
            if request_tx.send(request).await.is_err() {
                break 'writer;
            }
            total_written += count as u64;
            trace!("written {count}, {total_written} so far");

            if options.write_delay != 0 {
                let delay = if options.write_delay < 0 {
                    use rand::Rng;
                    rand::thread_rng().gen_range(0..=options.write_delay.unsigned_abs())
                } else {
                    options.write_delay.unsigned_abs()
                };
                trace!("sleeping for {delay}ms");
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
        }
    }

    for channel_id in &input_channel_ids {
        let _ = request_tx
            .send(proto::TransformRequest {
                channel_id: channel_id.clone(),
                request: Some(proto::transform_request::Request::ChannelCompleted(
                    proto::ChannelCompleted {},
                )),
            })
            .await;
    }
    drop(request_tx);

    let (read_count, completed, status) = reader
        .await
        .map_err(|e| format!("reader task failed: {e}"))??;
    info!("finished reading, count {read_count}");

    let termination_reason = completed
        .as_ref()
        .map(|c| c.termination_reason())
        .unwrap_or(proto::TerminationReason::None);
    match status {
        None if termination_reason == proto::TerminationReason::EndOfStream => {
            info!("{completed:?}");
            Ok(())
        }
        status => Err(format!("status: {status:?}, completion: {completed:?}")),
    }
}
