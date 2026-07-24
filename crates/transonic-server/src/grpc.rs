//! The `transonic.v1.TransformService` gRPC implementation.
//!
//! The wire behavior mirrors the original service:
//!
//! * the first request message must be a `TransformConfig`;
//! * payload/completed messages are forwarded to the pipeline engine;
//! * output data, flow-control and media-properties messages stream back;
//! * the last message is always a `TransformCompleted` carrying the
//!   termination reason, followed by the (possibly non-OK) gRPC status;
//! * while the server is shutting down, a `SHUTTING_DOWN` lifecycle
//!   notification is sent before the next response message.

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, warn};

use transonic_core::engine::{ConnectionReader, ConnectionWriter, EngineError};
use transonic_core::error::{ProcessError, ProcessErrorCode};
use transonic_core::job::{Job, JobId, JobInput, JobOutput};
use transonic_core::messages::{
    ClientMessage, ClientMessageData, ServerMessage, ServerMessageData,
};
use transonic_core::template::{TemplateKey, TemplateValue};
use transonic_core::Processor;
use transonic_proto::v1 as proto;

use crate::health::HealthTracker;
use crate::metrics::Metrics;

///////////////////////////////////////////////////////////////////////////
// termination messages (verbatim from the original implementation)
///////////////////////////////////////////////////////////////////////////

const MSG_CONFIG_MISSING: &str =
    "operation marked 'finished' before `transonic.v1.TransformConfig` message sent to service";
const MSG_CONFIG_NOT_FIRST: &str =
    "first message sent to service must be of type `transonic.v1.TransformConfig`";
const MSG_DECODER_FAILURE: &str = "decoder failed to decode data in the stream";
const MSG_DECRYPTION_KEY_UNAVAILABLE: &str =
    "the stream can't be decrypted because no suitable key is available";
const MSG_DECRYPTION_UNSUPPORTED: &str =
    "the stream can't be decrypted because decryption is not supported by an element in \
     the pipeline";
const MSG_DEMUXER_FAILURE: &str = "demuxer failed to demux data in the stream";
const MSG_DUPLICATE_PARAM: &str =
    "`transonic.v1.TransformConfig` message has a service parameter key that's specified more \
     than once";
const MSG_ENCODER_FAILURE: &str = "encoder failed to encode data in the stream";
const MSG_INPUT_BIN_ID_INVALID: &str =
    "one or more input bin identifiers in `transonic.v1.TransformConfig` message does not map \
     to an actual input bin";
const MSG_INPUT_BUFFER_OVERFLOW: &str =
    "input bin buffer overflowed, possibly because client ignored \
     `transonic.v1.FlowControlNotification` messages, or because client sent a very large \
     `transonic.v1.InputChannelPayload` message";
const MSG_INPUT_STREAM_CLOSED: &str =
    "additional message received for stream after `transonic.v1.ChannelCompleted` message \
     received";
const MSG_INPUT_STREAM_FAILURE: &str = "failed to process input stream";
const MSG_INPUT_STREAM_ID_INVALID: &str =
    "client message contained a stream identifier that was not found in the \
     `transonic.v1.TransformConfig` message sent at the start of the `Transform` operation";
const MSG_INPUT_STREAM_TYPE_UNDETECTED: &str =
    "failed to detect the type of the data sent in `transonic.v1.InputChannelPayload` messages";
const MSG_INPUT_STREAM_TYPE_UNSUPPORTED: &str =
    "the type of the data sent in `transonic.v1.InputChannelPayload` messages is not supported \
     by this pipeline";
const MSG_INTERNAL_ERROR: &str = "an unexpected error has occurred";
const MSG_JOB_BUILD_ERROR: &str =
    "`transonic.v1.TransformConfig` message must contain a pipeline identifier, at least one \
     input, and at least one output";
const MSG_JOB_ID_ERROR: &str = "the job identifier specified in the operation metadata is invalid";
const MSG_JOB_PARAM_INVALID: &str =
    "one of more of the parameters specified in the `transonic.v1.TransformConfig` message is \
     invalid";
const MSG_MAX_MEDIA_DURATION_EXCEEDED: &str =
    "the duration of the data sent in `transonic.v1.InputChannelPayload` messages has exceeded \
     the configured maximum media duration";
const MSG_MUXER_FAILURE: &str = "muxer failed to mux data in the stream";
const MSG_OUTPUT_BIN_ID_INVALID: &str =
    "one or more output bin identifiers in `transonic.v1.TransformConfig` message does not map \
     to an actual output bin";
const MSG_PIPELINE_ID_INVALID: &str =
    "the pipeline identifier in `transonic.v1.TransformConfig` message does not map to an \
     actual pipeline";
const MSG_PIPELINE_PARSE_FAILURE: &str =
    "the pipeline string generated as a result of applying the parameters in the \
     `transonic.v1.TransformConfig` message was not successfully parsed";
const MSG_READ_TIMEOUT_EXCEEDED: &str = "the configured maximum read timeout was exceeded";
const MSG_SUCCESS: &str = "";
const MSG_UNEXPECTED_MESSAGE: &str =
    "`transonic.v1.TransformConfig` message cannot be sent to service more than once during a \
     `Transform` operation";

/// The lower-cased `transonic.v1.ClientMetadata.REQUEST_ID` metadata key.
const REQUEST_ID_METADATA_KEY: &str = "request_id";

///////////////////////////////////////////////////////////////////////////
// operation outcome
///////////////////////////////////////////////////////////////////////////

struct Outcome {
    grpc_code: tonic::Code,
    termination_reason: proto::TerminationReason,
    termination_message: &'static str,
}

impl Outcome {
    fn new(
        grpc_code: tonic::Code,
        termination_reason: proto::TerminationReason,
        termination_message: &'static str,
    ) -> Self {
        Outcome {
            grpc_code,
            termination_reason,
            termination_message,
        }
    }

    fn success() -> Self {
        Outcome::new(
            tonic::Code::Ok,
            proto::TerminationReason::EndOfStream,
            MSG_SUCCESS,
        )
    }

    fn internal() -> Self {
        Outcome::new(
            tonic::Code::Internal,
            proto::TerminationReason::InternalError,
            MSG_INTERNAL_ERROR,
        )
    }

    fn from_process_error(error: &ProcessError) -> Self {
        use proto::TerminationReason as Reason;
        use tonic::Code;
        use ProcessErrorCode as Errc;

        match error.code {
            Errc::DecoderFailure => {
                Outcome::new(Code::Unknown, Reason::DecoderFailure, MSG_DECODER_FAILURE)
            }
            Errc::DecryptionKeyUnavailable => Outcome::new(
                Code::InvalidArgument,
                Reason::DecryptionKeyUnavailable,
                MSG_DECRYPTION_KEY_UNAVAILABLE,
            ),
            Errc::DecryptionUnsupported => Outcome::new(
                Code::InvalidArgument,
                Reason::DecryptionUnsupported,
                MSG_DECRYPTION_UNSUPPORTED,
            ),
            Errc::DemuxerFailure => {
                Outcome::new(Code::Unknown, Reason::DemuxerFailure, MSG_DEMUXER_FAILURE)
            }
            Errc::EncoderFailure => {
                Outcome::new(Code::Unknown, Reason::EncoderFailure, MSG_ENCODER_FAILURE)
            }
            Errc::InputBinIdInvalid => Outcome::new(
                Code::NotFound,
                Reason::InvalidArgument,
                MSG_INPUT_BIN_ID_INVALID,
            ),
            Errc::InputBufferOverflow => Outcome::new(
                Code::Unknown,
                Reason::InputRateExceeded,
                MSG_INPUT_BUFFER_OVERFLOW,
            ),
            Errc::InputStreamClosed => Outcome::new(
                Code::InvalidArgument,
                Reason::InvalidArgument,
                MSG_INPUT_STREAM_CLOSED,
            ),
            Errc::InputStreamFailure => Outcome::new(
                Code::Unknown,
                Reason::InputStreamFailure,
                MSG_INPUT_STREAM_FAILURE,
            ),
            Errc::InputStreamIdInvalid => Outcome::new(
                Code::InvalidArgument,
                Reason::InvalidArgument,
                MSG_INPUT_STREAM_ID_INVALID,
            ),
            Errc::InputStreamTypeUndetected => Outcome::new(
                Code::InvalidArgument,
                Reason::InputFormatNotDetected,
                MSG_INPUT_STREAM_TYPE_UNDETECTED,
            ),
            Errc::InputStreamTypeUnsupported => Outcome::new(
                Code::InvalidArgument,
                Reason::InputStreamTypeUnsupported,
                MSG_INPUT_STREAM_TYPE_UNSUPPORTED,
            ),
            Errc::InternalFailure => Outcome::internal(),
            Errc::JobParamInvalid => Outcome::new(
                Code::InvalidArgument,
                Reason::InvalidArgument,
                MSG_JOB_PARAM_INVALID,
            ),
            Errc::MaxMediaDurationExceeded => Outcome::new(
                Code::InvalidArgument,
                Reason::AllowedDurationExceeded,
                MSG_MAX_MEDIA_DURATION_EXCEEDED,
            ),
            Errc::MuxerFailure => {
                Outcome::new(Code::Unknown, Reason::MuxerFailure, MSG_MUXER_FAILURE)
            }
            Errc::OutputBinIdInvalid => Outcome::new(
                Code::NotFound,
                Reason::InvalidArgument,
                MSG_OUTPUT_BIN_ID_INVALID,
            ),
            Errc::PipelineIdInvalid => Outcome::new(
                Code::NotFound,
                Reason::InvalidArgument,
                MSG_PIPELINE_ID_INVALID,
            ),
            Errc::PipelineParseFailure => Outcome::new(
                Code::Unknown,
                Reason::PipelineParseFailure,
                MSG_PIPELINE_PARSE_FAILURE,
            ),
            Errc::ReadTimeoutExceeded => Outcome::new(
                Code::Unknown,
                Reason::ReadTimeout,
                MSG_READ_TIMEOUT_EXCEEDED,
            ),
            Errc::UnexpectedConfigMessage => Outcome::new(
                Code::InvalidArgument,
                Reason::InvalidArgument,
                MSG_UNEXPECTED_MESSAGE,
            ),
        }
    }
}

///////////////////////////////////////////////////////////////////////////
// connection adapters
///////////////////////////////////////////////////////////////////////////

struct GrpcReader {
    stream: Streaming<proto::TransformRequest>,
    metrics: Arc<Metrics>,
}

#[async_trait::async_trait]
impl ConnectionReader for GrpcReader {
    async fn read(&mut self) -> Result<Option<ClientMessage>, EngineError> {
        let request = match self.stream.message().await {
            Ok(request) => request,
            // A transport error means the client went away.
            Err(_) => return Err(EngineError::Cancelled),
        };
        let Some(request) = request else {
            return Ok(None);
        };
        match request.request {
            Some(proto::transform_request::Request::Config(_)) => Err(ProcessError::new(
                ProcessErrorCode::UnexpectedConfigMessage,
                "`transonic.v1.TransformConfig` message received after previous \
                     `transonic.v1.TransformConfig` message",
            )
            .into()),
            Some(proto::transform_request::Request::Payload(payload)) => {
                let bytes: u64 = payload.chunk_list.iter().map(|c| c.len() as u64).sum();
                self.metrics.add_input_bytes(bytes);
                Ok(Some(ClientMessage {
                    stream_id: request.channel_id,
                    data: ClientMessageData::InputBuffers(payload.chunk_list),
                }))
            }
            Some(proto::transform_request::Request::ChannelCompleted(_)) => {
                Ok(Some(ClientMessage {
                    stream_id: request.channel_id,
                    data: ClientMessageData::Close,
                }))
            }
            None => Err(ProcessError::internal("request message has no payload").into()),
        }
    }
}

struct GrpcWriter {
    sender: mpsc::Sender<Result<proto::TransformResponse, Status>>,
    shutting_down: Arc<AtomicBool>,
    shutdown_message_sent: bool,
    metrics: Arc<Metrics>,
}

impl GrpcWriter {
    async fn send(&mut self, response: proto::TransformResponse) -> Result<(), EngineError> {
        self.sender
            .send(Ok(response))
            .await
            .map_err(|_| EngineError::Cancelled)
    }
}

#[async_trait::async_trait]
impl ConnectionWriter for GrpcWriter {
    async fn write(&mut self, message: ServerMessage) -> Result<(), EngineError> {
        if self.shutting_down.load(Ordering::SeqCst) && !self.shutdown_message_sent {
            self.shutdown_message_sent = true;
            self.send(proto::TransformResponse {
                channel_id: String::new(),
                response: Some(
                    proto::transform_response::Response::ServerLifecycleNotification(
                        proto::ServerLifecycleNotification {
                            notification_type: proto::ServerLifecycleNotificationType::ShuttingDown
                                .into(),
                        },
                    ),
                ),
            })
            .await?;
        }

        let response = match message.data {
            ServerMessageData::OutputBuffer(output) => {
                self.metrics.add_output_bytes(output.buffer.len() as u64);
                let mut metadata = Vec::new();
                if let Some(duration) = output.chunk_duration_ms {
                    metadata.push(proto::ChunkMetadata {
                        value: Some(proto::chunk_metadata::Value::ChunkDurationMilliseconds(
                            duration,
                        )),
                    });
                }
                if let Some(offset) = output.chunk_offset_ms {
                    metadata.push(proto::ChunkMetadata {
                        value: Some(proto::chunk_metadata::Value::ChunkOffsetMilliseconds(
                            offset,
                        )),
                    });
                }
                if let Some(offset) = output.input_stream_offset {
                    metadata.push(proto::ChunkMetadata {
                        value: Some(proto::chunk_metadata::Value::InputStreamOffset(offset)),
                    });
                }
                if let Some(silence) = output.silence {
                    metadata.push(proto::ChunkMetadata {
                        value: Some(proto::chunk_metadata::Value::HasOnlySilence(silence)),
                    });
                }
                proto::TransformResponse {
                    channel_id: message.stream_id,
                    response: Some(proto::transform_response::Response::Payload(
                        proto::OutputChannelPayload {
                            chunk_list: vec![output.buffer],
                            metadata_list: vec![proto::MetadataList { metadata }],
                        },
                    )),
                }
            }
            ServerMessageData::DataSaturation => {
                self.metrics.record_flow_control("enough_data");
                proto::TransformResponse {
                    channel_id: message.stream_id,
                    response: Some(
                        proto::transform_response::Response::FlowControlNotification(
                            proto::FlowControlNotification {
                                notification_type: proto::FlowControlNotificationType::EnoughData
                                    .into(),
                            },
                        ),
                    ),
                }
            }
            ServerMessageData::DataStarvation => {
                self.metrics.record_flow_control("need_data");
                proto::TransformResponse {
                    channel_id: message.stream_id,
                    response: Some(
                        proto::transform_response::Response::FlowControlNotification(
                            proto::FlowControlNotification {
                                notification_type: proto::FlowControlNotificationType::NeedData
                                    .into(),
                            },
                        ),
                    ),
                }
            }
            ServerMessageData::MediaProperties(properties) => proto::TransformResponse {
                channel_id: message.stream_id,
                response: Some(proto::transform_response::Response::MediaProperties(
                    proto::MediaProperties {
                        properties: Some(proto::media_properties::Properties::AudioProperties(
                            proto::AudioProperties {
                                encoding: properties.encoding.unwrap_or_default(),
                                sample_rate: properties.sample_rate.unwrap_or(0),
                                sample_type: properties.format.unwrap_or_default(),
                                channels: properties.channel_count.unwrap_or(0),
                            },
                        )),
                    },
                )),
            },
        };
        self.send(response).await
    }
}

///////////////////////////////////////////////////////////////////////////
// job deserialization
///////////////////////////////////////////////////////////////////////////

#[derive(Debug, thiserror::Error)]
enum DeserializeError {
    #[error("duplicate parameter")]
    DuplicateParam,
    #[error("job build error: {0}")]
    JobBuild(String),
    #[error("invalid parameter: {0}")]
    InvalidParam(String),
}

fn valid_extension(extension: &str) -> bool {
    !extension.is_empty()
        && extension
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '.')
}

fn deserialize_pad_params(
    params: &std::collections::HashMap<String, String>,
) -> Result<BTreeMap<TemplateKey, TemplateValue>, DeserializeError> {
    let mut result = BTreeMap::new();
    for (key, value) in params {
        let key = TemplateKey::new(key.clone())
            .map_err(|e| DeserializeError::InvalidParam(e.to_string()))?;
        let value = TemplateValue::new(value.clone())
            .map_err(|e| DeserializeError::InvalidParam(e.to_string()))?;
        result.insert(key, value);
    }
    Ok(result)
}

fn deserialize_job(
    config: &proto::TransformConfig,
    job_id: JobId,
) -> Result<Job, DeserializeError> {
    let mut inputs = Vec::with_capacity(config.inputs.len());
    for input in &config.inputs {
        let extension = input.metadata.get("extension").cloned();
        if let Some(extension) = &extension {
            if !valid_extension(extension) {
                return Err(DeserializeError::InvalidParam(format!(
                    "'{extension}' is not a valid file extension"
                )));
            }
        }
        inputs.push(JobInput {
            stream_id: input.channel_id.clone(),
            bin_id: input.pipeline_bin_id.clone(),
            extension,
            params: deserialize_pad_params(&input.pipeline_parameters)?,
        });
    }

    let mut outputs = Vec::with_capacity(config.outputs.len());
    for output in &config.outputs {
        outputs.push(JobOutput {
            stream_id: output.channel_id.clone(),
            bin_id: output.pipeline_bin_id.clone(),
            params: deserialize_pad_params(&output.pipeline_parameters)?,
        });
    }

    let mut job = Job {
        id: job_id,
        pipeline_id: config.pipeline_name.clone(),
        inputs,
        outputs,
        input_buffer_size: None,
        max_media_duration: None,
        output_buffer_duration: None,
        output_buffer_size: None,
        overflow_termination_enabled: None,
        read_timeout: None,
        realtime_rate: None,
        streaming_enabled: None,
    };

    macro_rules! set_once {
        ($field:expr, $value:expr) => {{
            if $field.is_some() {
                return Err(DeserializeError::DuplicateParam);
            }
            $field = Some($value);
        }};
    }

    for param in &config.parameter_overrides {
        use proto::service_parameter::Value;
        match &param.value {
            Some(Value::Rate(rate)) => set_once!(job.realtime_rate, *rate),
            Some(Value::FlowControlEnforcementPolicy(policy)) => {
                let policy = proto::FlowControlEnforcementPolicy::try_from(*policy)
                    .unwrap_or(proto::FlowControlEnforcementPolicy::Block);
                let value = !matches!(policy, proto::FlowControlEnforcementPolicy::Block);
                set_once!(job.overflow_termination_enabled, value);
            }
            Some(Value::LengthLimitMilliseconds(limit)) => {
                set_once!(job.max_media_duration, *limit)
            }
            // This parameter isn't used (same as the original).
            Some(Value::StartToleranceBytes(_)) => {}
            Some(Value::ReadTimeoutMilliseconds(timeout)) => set_once!(job.read_timeout, *timeout),
            Some(Value::PipelineInputBuffer(size)) => set_once!(job.input_buffer_size, *size),
            Some(Value::PipelineOutputBuffer(size)) => set_once!(job.output_buffer_size, *size),
            Some(Value::StreamOutput(enabled)) => set_once!(job.streaming_enabled, *enabled),
            Some(Value::PipelineOutputBufferMilliseconds(duration)) => {
                set_once!(job.output_buffer_duration, *duration)
            }
            None => {}
        }
    }

    job.validate()
        .map_err(|e| DeserializeError::JobBuild(e.to_string()))?;
    Ok(job)
}

///////////////////////////////////////////////////////////////////////////
// service implementation
///////////////////////////////////////////////////////////////////////////

/// The transform service.
pub struct TransformService {
    processor: Arc<Processor>,
    shutting_down: Arc<AtomicBool>,
    default_read_timeout: Duration,
    health: Arc<HealthTracker>,
    metrics: Arc<Metrics>,
}

impl TransformService {
    /// Creates the service.
    pub fn new(
        processor: Arc<Processor>,
        shutting_down: Arc<AtomicBool>,
        health: Arc<HealthTracker>,
        metrics: Arc<Metrics>,
    ) -> Self {
        let default_read_timeout =
            Duration::from_millis(u64::from(processor.config().read_timeout.default));
        TransformService {
            processor,
            shutting_down,
            default_read_timeout,
            health,
            metrics,
        }
    }
}

#[tonic::async_trait]
impl proto::transform_service_server::TransformService for TransformService {
    type TransformStream =
        Pin<Box<dyn Stream<Item = Result<proto::TransformResponse, Status>> + Send>>;

    async fn transform(
        &self,
        request: Request<Streaming<proto::TransformRequest>>,
    ) -> Result<Response<Self::TransformStream>, Status> {
        let peer = request
            .remote_addr()
            .map(|addr| addr.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        info!(peer = %peer, "media transform operation started");

        // The client may supply a job identifier through metadata.
        let job_id = match request.metadata().get(REQUEST_ID_METADATA_KEY) {
            Some(value) => value
                .to_str()
                .map_err(|e| e.to_string())
                .and_then(|s| JobId::new(s).map_err(|e| e.to_string())),
            None => Ok(JobId::generate()),
        };

        let stream = request.into_inner();
        let (tx, rx) = mpsc::channel::<Result<proto::TransformResponse, Status>>(64);

        let processor = Arc::clone(&self.processor);
        let shutting_down = Arc::clone(&self.shutting_down);
        let default_read_timeout = self.default_read_timeout;
        let health = Arc::clone(&self.health);
        let metrics = Arc::clone(&self.metrics);

        tokio::spawn(async move {
            metrics.job_started();
            let started = std::time::Instant::now();
            let outcome = run_operation(
                processor,
                shutting_down,
                default_read_timeout,
                stream,
                tx.clone(),
                &peer,
                job_id,
                Arc::clone(&metrics),
            )
            .await;
            metrics.job_finished();
            metrics.observe_duration(started.elapsed().as_secs_f64());

            match outcome {
                Some(outcome) => {
                    info!(
                        peer = %peer,
                        grpc_status = ?outcome.grpc_code,
                        result = ?outcome.termination_reason,
                        message = outcome.termination_message,
                        "media transform operation finished"
                    );
                    health.record_finished(outcome.grpc_code);
                    metrics.record_outcome(outcome.grpc_code);
                }
                None => {
                    warn!(peer = %peer, "media transform operation cancelled");
                    health.record_cancelled();
                    metrics.record_cancelled();
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }
}

/// Runs one transform operation; returns `None` when the operation was
/// cancelled by the client (no termination response could be delivered).
#[allow(clippy::too_many_arguments)]
async fn run_operation(
    processor: Arc<Processor>,
    shutting_down: Arc<AtomicBool>,
    default_read_timeout: Duration,
    stream: Streaming<proto::TransformRequest>,
    tx: mpsc::Sender<Result<proto::TransformResponse, Status>>,
    peer: &str,
    job_id: Result<JobId, String>,
    metrics: Arc<Metrics>,
) -> Option<Outcome> {
    let mut reader = GrpcReader {
        stream,
        metrics: Arc::clone(&metrics),
    };
    let mut writer = GrpcWriter {
        sender: tx.clone(),
        shutting_down,
        shutdown_message_sent: false,
        metrics,
    };

    let outcome = process_operation(
        &processor,
        default_read_timeout,
        &mut reader,
        &mut writer,
        peer,
        job_id,
    )
    .await;

    let outcome = match outcome {
        Some(outcome) => outcome,
        None => {
            // Cancelled: nothing more can be delivered.
            let _ = tx.send(Err(Status::cancelled("operation cancelled"))).await;
            return None;
        }
    };

    // Send the termination response, then the (possibly non-OK) status.
    let termination = proto::TransformResponse {
        channel_id: String::new(),
        response: Some(proto::transform_response::Response::TransformCompleted(
            proto::TransformCompleted {
                termination_reason: outcome.termination_reason.into(),
                termination_message: outcome.termination_message.to_string(),
            },
        )),
    };
    if tx.send(Ok(termination)).await.is_err() {
        return None;
    }
    if outcome.grpc_code != tonic::Code::Ok {
        let _ = tx
            .send(Err(Status::new(
                outcome.grpc_code,
                outcome.termination_message,
            )))
            .await;
    }
    Some(outcome)
}

async fn process_operation(
    processor: &Processor,
    default_read_timeout: Duration,
    reader: &mut GrpcReader,
    writer: &mut GrpcWriter,
    peer: &str,
    job_id: Result<JobId, String>,
) -> Option<Outcome> {
    let job_id = match job_id {
        Ok(job_id) => job_id,
        Err(_) => {
            return Some(Outcome::new(
                tonic::Code::InvalidArgument,
                proto::TerminationReason::InvalidArgument,
                MSG_JOB_ID_ERROR,
            ));
        }
    };
    info!(peer = %peer, job = %job_id, "job id generated");

    // Read the initial configuration message.
    let first = if default_read_timeout.is_zero() {
        reader.stream.message().await
    } else {
        match tokio::time::timeout(default_read_timeout, reader.stream.message()).await {
            Ok(result) => result,
            Err(_) => {
                return Some(Outcome::new(
                    tonic::Code::Unknown,
                    proto::TerminationReason::ReadTimeout,
                    MSG_READ_TIMEOUT_EXCEEDED,
                ));
            }
        }
    };
    let first = match first {
        Ok(first) => first,
        Err(_) => return None,
    };
    let Some(first) = first else {
        return Some(Outcome::new(
            tonic::Code::InvalidArgument,
            proto::TerminationReason::InvalidArgument,
            MSG_CONFIG_MISSING,
        ));
    };
    let Some(proto::transform_request::Request::Config(config)) = first.request else {
        return Some(Outcome::new(
            tonic::Code::InvalidArgument,
            proto::TerminationReason::InvalidArgument,
            MSG_CONFIG_NOT_FIRST,
        ));
    };
    info!(peer = %peer, job = %job_id, "config received");

    let job = match deserialize_job(&config, job_id) {
        Ok(job) => job,
        Err(DeserializeError::DuplicateParam) => {
            return Some(Outcome::new(
                tonic::Code::InvalidArgument,
                proto::TerminationReason::InvalidArgument,
                MSG_DUPLICATE_PARAM,
            ));
        }
        Err(DeserializeError::JobBuild(_)) => {
            return Some(Outcome::new(
                tonic::Code::InvalidArgument,
                proto::TerminationReason::InvalidArgument,
                MSG_JOB_BUILD_ERROR,
            ));
        }
        Err(DeserializeError::InvalidParam(_)) => {
            return Some(Outcome::new(
                tonic::Code::InvalidArgument,
                proto::TerminationReason::InvalidArgument,
                MSG_JOB_PARAM_INVALID,
            ));
        }
    };
    info!(peer = %peer, job = %job.id, pipeline = %job.pipeline_id, "job created");

    match processor.process(&job, reader, writer).await {
        Ok(()) => Some(Outcome::success()),
        Err(EngineError::Cancelled) => None,
        Err(EngineError::Process(error)) => {
            warn!(peer = %peer, job = %job.id, error = %error, "job failed");
            Some(Outcome::from_process_error(&error))
        }
    }
}
