//! Error types for the piped transform engine.

use thiserror::Error;

/// Expected error codes for piped transform operations.
///
/// This mirrors the original `piped_transform_process_errc` enumeration so
/// that the gRPC layer can perform the exact same mapping onto
/// `transonic.v1.TerminationReason` values and gRPC status codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ProcessErrorCode {
    /// A general decoder failure.
    DecoderFailure,
    /// Encrypted stream with no usable decryption key.
    DecryptionKeyUnavailable,
    /// Encrypted stream and no element supports decryption.
    DecryptionUnsupported,
    /// A general demuxer failure.
    DemuxerFailure,
    /// A general encoder failure.
    EncoderFailure,
    /// An input bin identifier does not map to an actual input bin.
    InputBinIdInvalid,
    /// The number of queued input bytes exceeded the hard buffer limit.
    InputBufferOverflow,
    /// An input message was received for an already-closed stream.
    InputStreamClosed,
    /// A general failure associated with the input stream data.
    InputStreamFailure,
    /// A client message referenced an unknown input stream identifier.
    InputStreamIdInvalid,
    /// The input stream type could not be inferred.
    InputStreamTypeUndetected,
    /// A container or codec type in the input stream is unsupported.
    InputStreamTypeUnsupported,
    /// An unexpected internal failure.
    InternalFailure,
    /// One or more job parameters is invalid.
    JobParamInvalid,
    /// The configured maximum media duration was exceeded.
    MaxMediaDurationExceeded,
    /// A general muxer failure.
    MuxerFailure,
    /// An output bin identifier does not map to an actual output bin.
    OutputBinIdInvalid,
    /// A pipeline identifier does not map to an actual pipeline.
    PipelineIdInvalid,
    /// The pipeline description failed to parse.
    PipelineParseFailure,
    /// The configured read timeout was exceeded.
    ReadTimeoutExceeded,
    /// A `TransformConfig` message was received after the initial one.
    UnexpectedConfigMessage,
}

/// An error raised while processing a piped transform job.
#[derive(Debug, Error, Clone)]
#[error("{message}")]
pub struct ProcessError {
    /// The error code used for termination-reason mapping.
    pub code: ProcessErrorCode,
    /// A human readable description.
    pub message: String,
}

impl ProcessError {
    /// Creates a new [`ProcessError`].
    pub fn new(code: ProcessErrorCode, message: impl Into<String>) -> Self {
        ProcessError {
            code,
            message: message.into(),
        }
    }

    /// Shorthand for an internal failure.
    pub fn internal(message: impl Into<String>) -> Self {
        ProcessError::new(ProcessErrorCode::InternalFailure, message)
    }

    /// Shorthand for an invalid job parameter.
    pub fn job_param(message: impl Into<String>) -> Self {
        ProcessError::new(ProcessErrorCode::JobParamInvalid, message)
    }
}

/// An error raised while loading or validating configuration.
#[derive(Debug, Error)]
#[error("{0}")]
pub struct ConfigError(pub String);

/// An error raised while parsing or applying pipeline templates.
#[derive(Debug, Error)]
#[error("{0}")]
pub struct TemplateError(pub String);
