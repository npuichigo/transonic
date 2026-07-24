//! Job description and finalized job parameters.

use std::collections::{BTreeMap, HashSet};
use std::fmt;

use crate::config::RootConfig;
use crate::error::{ProcessError, ProcessErrorCode};
use crate::template::{TemplateKey, TemplateValue};

/// A validated job identifier.
///
/// Valid identifiers contain at least one character, where each character is
/// a graphable ASCII character.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct JobId(String);

/// Error raised when a job identifier fails validation.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct JobIdError(pub String);

impl JobId {
    /// Validates and wraps a job identifier.
    pub fn new(s: impl Into<String>) -> Result<Self, JobIdError> {
        let s = s.into();
        if s.is_empty() {
            return Err(JobIdError(
                "the empty string is not a valid piped transform job id".into(),
            ));
        }
        if !s.bytes().all(|b| (0x21..0x7f).contains(&b)) {
            return Err(JobIdError(format!(
                "'{s}' is not a valid piped transform job id"
            )));
        }
        Ok(JobId(s))
    }

    /// Generates a random (UUID) job identifier.
    pub fn generate() -> Self {
        JobId(uuid::Uuid::new_v4().to_string())
    }

    /// Returns the identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Configuration for one input stream of a job.
#[derive(Debug, Clone)]
pub struct JobInput {
    /// The stream identifier referenced by input messages.
    pub stream_id: String,
    /// The input bin identifier to match in the configuration.
    pub bin_id: String,
    /// Optional file extension hint used by GStreamer typefinding.
    pub extension: Option<String>,
    /// Template parameters for the input bin.
    pub params: BTreeMap<TemplateKey, TemplateValue>,
}

/// Configuration for one output stream of a job.
#[derive(Debug, Clone)]
pub struct JobOutput {
    /// The stream identifier tagged onto output messages.
    pub stream_id: String,
    /// The output bin identifier to match in the configuration.
    pub bin_id: String,
    /// Template parameters for the output bin.
    pub params: BTreeMap<TemplateKey, TemplateValue>,
}

/// Error raised while building a job.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct JobBuildError(pub String);

/// Parameters used to submit a job to a pipeline.
#[derive(Debug, Clone)]
pub struct Job {
    /// The job identifier.
    pub id: JobId,
    /// The name of the predefined pipeline to run.
    pub pipeline_id: String,
    /// The input streams.
    pub inputs: Vec<JobInput>,
    /// The output streams.
    pub outputs: Vec<JobOutput>,
    /// Optional input buffer size override (bytes).
    pub input_buffer_size: Option<u32>,
    /// Optional maximum media duration override (milliseconds).
    pub max_media_duration: Option<u32>,
    /// Optional output buffer duration override (milliseconds).
    pub output_buffer_duration: Option<u32>,
    /// Optional output buffer size override (bytes).
    pub output_buffer_size: Option<u32>,
    /// Optional overflow termination override.
    pub overflow_termination_enabled: Option<bool>,
    /// Optional read timeout override (milliseconds).
    pub read_timeout: Option<u32>,
    /// Optional realtime rate override.
    pub realtime_rate: Option<f64>,
    /// Optional streaming enabled override.
    pub streaming_enabled: Option<bool>,
}

impl Job {
    /// Validates cross-field invariants: unique non-empty stream id sets and
    /// the single-input restriction when overflow termination is disabled.
    pub fn validate(&self) -> Result<(), JobBuildError> {
        if self.pipeline_id.is_empty() {
            return Err(JobBuildError(
                "piped transform job must reference a pipeline identifier".into(),
            ));
        }
        Self::verify_streams(
            self.inputs.iter().map(|i| i.stream_id.as_str()),
            "input",
            "piped transform job must specify at least one input",
        )?;
        Self::verify_streams(
            self.outputs.iter().map(|o| o.stream_id.as_str()),
            "output",
            "piped transform job must specify at least one output",
        )?;
        if self.overflow_termination_enabled == Some(false) && self.inputs.len() > 1 {
            return Err(JobBuildError(
                "piped transform job cannot reference more than one input stream when \
                 overflow termination is disabled"
                    .into(),
            ));
        }
        Ok(())
    }

    fn verify_streams<'a>(
        ids: impl Iterator<Item = &'a str>,
        kind: &str,
        empty_message: &str,
    ) -> Result<(), JobBuildError> {
        let mut seen = HashSet::new();
        let mut count = 0usize;
        for id in ids {
            count += 1;
            if !seen.insert(id) {
                return Err(JobBuildError(format!(
                    "piped transform job contains more than one {kind} that refers to \
                     the stream id '{id}'"
                )));
            }
        }
        if count == 0 {
            return Err(JobBuildError(empty_message.into()));
        }
        Ok(())
    }
}

/// Finalized job parameters, deduced from job overrides and configured
/// parameter limits.
#[derive(Debug, Clone, Copy)]
pub struct JobParams {
    /// Input buffer size (bytes).
    pub input_buffer_size: u32,
    /// Maximum media duration (milliseconds); 0 disables the limit.
    pub max_media_duration: u32,
    /// Output buffer duration (milliseconds); 0 disables duration flushing.
    pub output_buffer_duration: u32,
    /// Output buffer size (bytes).
    pub output_buffer_size: u32,
    /// Whether input overflow terminates the call (vs. blocking).
    pub overflow_termination_enabled: bool,
    /// Read timeout (milliseconds); 0 disables the timeout.
    pub read_timeout: u32,
    /// Realtime rate; 0 means "as fast as possible".
    pub realtime_rate: f64,
    /// Whether streaming output is enabled.
    pub streaming_enabled: bool,
}

impl JobParams {
    /// Deduces finalized parameters from the job and root configuration,
    /// validating the overrides against the configured ranges.
    pub fn deduce(config: &RootConfig, job: &Job) -> Result<Self, ProcessError> {
        let params = JobParams {
            input_buffer_size: config
                .input_buffer_size
                .deduce("pipelineInputBuffer", job.input_buffer_size)?,
            max_media_duration: config
                .media_duration
                .deduce("lengthLimitMilliseconds", job.max_media_duration)?,
            output_buffer_duration: config.output_buffer_duration.deduce(
                "pipelineOutputBufferMilliseconds",
                job.output_buffer_duration,
            )?,
            output_buffer_size: config
                .output_buffer_size
                .deduce("pipelineOutputBuffer", job.output_buffer_size)?,
            read_timeout: config
                .read_timeout
                .deduce("readTimeoutMilliseconds", job.read_timeout)?,
            realtime_rate: config.realtime_rate.deduce("rate", job.realtime_rate)?,
            streaming_enabled: job.streaming_enabled.unwrap_or(config.streaming_enabled),
            overflow_termination_enabled: job
                .overflow_termination_enabled
                .unwrap_or(config.overflow_termination_enabled),
        };
        if !params.overflow_termination_enabled && job.inputs.len() > 1 {
            return Err(ProcessError::new(
                ProcessErrorCode::JobParamInvalid,
                "piped transform job cannot reference more than one input stream when \
                 overflow termination is disabled",
            ));
        }
        Ok(params)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_job(inputs: &[&str], outputs: &[&str]) -> Job {
        Job {
            id: JobId::generate(),
            pipeline_id: "p".to_string(),
            inputs: inputs
                .iter()
                .map(|id| JobInput {
                    stream_id: id.to_string(),
                    bin_id: "in".to_string(),
                    extension: None,
                    params: Default::default(),
                })
                .collect(),
            outputs: outputs
                .iter()
                .map(|id| JobOutput {
                    stream_id: id.to_string(),
                    bin_id: "out".to_string(),
                    params: Default::default(),
                })
                .collect(),
            input_buffer_size: None,
            max_media_duration: None,
            output_buffer_duration: None,
            output_buffer_size: None,
            overflow_termination_enabled: None,
            read_timeout: None,
            realtime_rate: None,
            streaming_enabled: None,
        }
    }

    #[test]
    fn job_ids_must_be_graphic_ascii() {
        assert!(JobId::new("").is_err());
        assert!(JobId::new("has space").is_err());
        assert!(JobId::new("has\ttab").is_err());
        assert!(JobId::new("non-ascii-\u{00e9}").is_err());
        assert!(JobId::new("valid-id_123.!").is_ok());
        assert!(JobId::new(JobId::generate().as_str()).is_ok());
    }

    #[test]
    fn valid_job_passes_validation() {
        assert!(test_job(&["I1", "I2"], &["O1"]).validate().is_ok());
    }

    #[test]
    fn job_requires_pipeline_inputs_and_outputs() {
        let mut job = test_job(&["I1"], &["O1"]);
        job.pipeline_id = String::new();
        assert!(job.validate().is_err());

        assert!(test_job(&[], &["O1"]).validate().is_err());
        assert!(test_job(&["I1"], &[]).validate().is_err());
    }

    #[test]
    fn duplicate_stream_ids_are_rejected() {
        assert!(test_job(&["I1", "I1"], &["O1"]).validate().is_err());
        assert!(test_job(&["I1"], &["O1", "O1"]).validate().is_err());
    }

    #[test]
    fn blocking_policy_allows_only_one_input() {
        let mut job = test_job(&["I1", "I2"], &["O1"]);
        job.overflow_termination_enabled = Some(false);
        assert!(job.validate().is_err());

        job.overflow_termination_enabled = Some(true);
        assert!(job.validate().is_ok());
    }

    #[test]
    fn params_fall_back_to_configured_defaults() {
        let config = RootConfig::default();
        let params = JobParams::deduce(&config, &test_job(&["I1"], &["O1"])).unwrap();
        assert_eq!(params.input_buffer_size, config.input_buffer_size.default);
        assert_eq!(params.read_timeout, config.read_timeout.default);
        assert_eq!(params.realtime_rate, config.realtime_rate.default);
        assert_eq!(params.streaming_enabled, config.streaming_enabled);
        assert_eq!(
            params.overflow_termination_enabled,
            config.overflow_termination_enabled
        );
    }

    #[test]
    fn in_range_overrides_are_applied() {
        let config = RootConfig::default();
        let mut job = test_job(&["I1"], &["O1"]);
        job.realtime_rate = Some(2.0);
        job.read_timeout = Some(1234);
        job.streaming_enabled = Some(false);
        let params = JobParams::deduce(&config, &job).unwrap();
        assert_eq!(params.realtime_rate, 2.0);
        assert_eq!(params.read_timeout, 1234);
        assert!(!params.streaming_enabled);
    }

    #[test]
    fn out_of_range_overrides_are_rejected() {
        let config = RootConfig::default();
        let mut job = test_job(&["I1"], &["O1"]);
        job.realtime_rate = Some(1000.0);
        let error = JobParams::deduce(&config, &job).unwrap_err();
        assert_eq!(error.code, ProcessErrorCode::JobParamInvalid);
    }
}
