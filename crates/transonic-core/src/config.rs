//! Service configuration loading.
//!
//! The configuration file is a JSON document matching the
//! `transonic.v1.ServiceConfiguration` protobuf JSON schema (both camelCase and
//! snake_case field names are accepted, like protobuf JSON parsing does).
//!
//! It carries two things:
//!
//! * global service parameter limits (default / min / max), and
//! * the set of predefined pipelines, each consisting of named input and
//!   output bin templates.

use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;

use crate::error::ConfigError;
use crate::template::PipelineTemplate;

///////////////////////////////////////////////////////////////////////////
// ranged parameters
///////////////////////////////////////////////////////////////////////////

/// A ranged service parameter: a default value plus a valid [min, max]
/// range, itself constrained to a hard-coded absolute range.
#[derive(Debug, Clone, Copy)]
pub struct RangedParam<T> {
    /// The effective default value.
    pub default: T,
    /// The effective minimum value.
    pub min: T,
    /// The effective maximum value.
    pub max: T,
}

impl<T: PartialOrd + Copy + std::fmt::Display> RangedParam<T> {
    fn resolve(
        key: &str,
        standard: T,
        lowest: T,
        highest: T,
        default: Option<T>,
        min: Option<T>,
        max: Option<T>,
    ) -> Result<Self, ConfigError> {
        let in_range = |value: T| value >= lowest && value <= highest;

        let min_value = match min {
            Some(value) => {
                if !in_range(value) {
                    return Err(ConfigError(format!(
                        "{key}: proposed minimum value ({value}) is outside of the valid \
                         parameter range [{lowest}, {highest}]"
                    )));
                }
                value
            }
            None => lowest,
        };
        let max_value = match max {
            Some(value) => {
                if !in_range(value) {
                    return Err(ConfigError(format!(
                        "{key}: proposed maximum value ({value}) is outside of the valid \
                         parameter range [{lowest}, {highest}]"
                    )));
                }
                value
            }
            None => highest,
        };
        if min_value > max_value {
            return Err(ConfigError(format!(
                "{key}: proposed minimum value ({min_value}) is greater than proposed \
                 maximum value ({max_value})"
            )));
        }

        let default_value = match default {
            Some(value) => {
                if value < min_value || value > max_value {
                    return Err(ConfigError(format!(
                        "{key}: proposed default value ({value}) is outside of the \
                         proposed parameter range [{min_value}, {max_value}]"
                    )));
                }
                value
            }
            None => {
                // Clamp the standard value into the configured range.
                if standard < min_value {
                    min_value
                } else if standard > max_value {
                    max_value
                } else {
                    standard
                }
            }
        };

        Ok(RangedParam {
            default: default_value,
            min: min_value,
            max: max_value,
        })
    }

    /// Validates a job override against the configured range, falling back
    /// to the default when unset.
    pub fn deduce(&self, key: &str, value: Option<T>) -> Result<T, crate::error::ProcessError> {
        match value {
            None => Ok(self.default),
            Some(value) => {
                if value < self.min || value > self.max {
                    Err(crate::error::ProcessError::job_param(format!(
                        "'{key}' value of {value} is outside the configured range [{}, {}]",
                        self.min, self.max
                    )))
                } else {
                    Ok(value)
                }
            }
        }
    }
}

///////////////////////////////////////////////////////////////////////////
// JSON schema
///////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawServiceConfiguration {
    #[serde(default, alias = "serviceParameters")]
    service_parameters: Vec<RawServiceParameterLimits>,
    #[serde(default)]
    pipelines: HashMap<String, RawPipelineDefinition>,
    #[serde(default)]
    grpc: Option<RawGrpcConfiguration>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawServiceParameterLimits {
    #[serde(default)]
    #[allow(dead_code)]
    description: String,
    #[serde(default)]
    default: Option<RawServiceParameter>,
    #[serde(default)]
    min: Option<RawServiceParameter>,
    #[serde(default)]
    max: Option<RawServiceParameter>,
}

/// One member of the `transonic.v1.ServiceParameter` oneof.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields)]
enum RawServiceParameter {
    #[serde(rename = "rate")]
    Rate(f64),
    #[serde(
        rename = "flowControlEnforcementPolicy",
        alias = "flow_control_enforcement_policy"
    )]
    FlowControlEnforcementPolicy(RawFlowControlPolicy),
    #[serde(
        rename = "lengthLimitMilliseconds",
        alias = "length_limit_milliseconds"
    )]
    LengthLimitMilliseconds(u32),
    #[serde(rename = "startToleranceBytes", alias = "start_tolerance_bytes")]
    StartToleranceBytes(#[allow(dead_code)] u32),
    #[serde(
        rename = "readTimeoutMilliseconds",
        alias = "read_timeout_milliseconds"
    )]
    ReadTimeoutMilliseconds(u32),
    #[serde(rename = "pipelineInputBuffer", alias = "pipeline_input_buffer")]
    PipelineInputBuffer(u32),
    #[serde(rename = "pipelineOutputBuffer", alias = "pipeline_output_buffer")]
    PipelineOutputBuffer(u32),
    #[serde(rename = "streamOutput", alias = "stream_output")]
    StreamOutput(bool),
    #[serde(
        rename = "pipelineOutputBufferMilliseconds",
        alias = "pipeline_output_buffer_milliseconds"
    )]
    PipelineOutputBufferMilliseconds(u32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
enum RawFlowControlPolicy {
    #[serde(rename = "BLOCK")]
    Block,
    #[serde(rename = "ERROR")]
    Error,
    #[serde(rename = "NOTIFY")]
    Notify,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPipelineDefinition {
    #[serde(default)]
    input: HashMap<String, RawPipelineBin>,
    #[serde(default)]
    intermediate: HashMap<String, RawPipelineBin>,
    #[serde(default)]
    output: HashMap<String, RawPipelineBin>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawPipelineBin {
    #[serde(default, rename = "type", alias = "Type")]
    #[allow(dead_code)]
    bin_type: Option<String>,
    specs: String,
    #[serde(default, alias = "nonStreamable")]
    non_streamable: bool,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGrpcConfiguration {
    #[serde(default)]
    server: Option<RawGrpcServerConfiguration>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGrpcServerConfiguration {
    #[serde(default, alias = "healthService", alias = "health_service")]
    health_service: Option<RawGrpcHealthServiceConfiguration>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawGrpcHealthServiceConfiguration {
    #[serde(default, alias = "filterWindowSize", alias = "filter_window_size")]
    filter_window_size: Option<u32>,
    #[serde(default, alias = "unhealthyThreshold", alias = "unhealthy_threshold")]
    unhealthy_threshold: Option<f32>,
    #[serde(
        default,
        alias = "statusExpirationTime",
        alias = "status_expiration_time"
    )]
    #[allow(dead_code)]
    status_expiration_time: Option<u32>,
}

///////////////////////////////////////////////////////////////////////////
// resolved configuration
///////////////////////////////////////////////////////////////////////////

/// A parsed input bin: a pipeline template.
#[derive(Debug, Clone)]
pub struct InputBinConfig {
    /// The bin's pipeline template.
    pub spec: PipelineTemplate,
}

/// A parsed output bin: a pipeline template plus streamability.
#[derive(Debug, Clone)]
pub struct OutputBinConfig {
    /// The bin's pipeline template.
    pub spec: PipelineTemplate,
    /// Whether the bin output can be streamed incrementally.  Non-streamable
    /// bins (e.g. RIFF/WAV, which needs a finalized header) are routed
    /// through a file sink and drained after the pipeline stops.
    pub streamable: bool,
}

/// A predefined pipeline: named input and output bins.
#[derive(Debug, Clone, Default)]
pub struct PipelineConfig {
    /// The input bins, indexed by bin id.
    pub input_bins: HashMap<String, InputBinConfig>,
    /// The output bins, indexed by bin id.
    pub output_bins: HashMap<String, OutputBinConfig>,
}

/// gRPC health service configuration.
#[derive(Debug, Clone, Copy)]
pub struct GrpcHealthServiceConfig {
    /// Number of samples maintained by the moving average filter.
    pub filter_window_size: u32,
    /// The moving-average threshold above which the service is unhealthy.
    pub unhealthy_threshold: f32,
}

impl Default for GrpcHealthServiceConfig {
    fn default() -> Self {
        GrpcHealthServiceConfig {
            filter_window_size: 10_000,
            unhealthy_threshold: 0.3,
        }
    }
}

/// The root service configuration.
#[derive(Debug, Clone)]
pub struct RootConfig {
    /// Predefined pipelines, indexed by pipeline name.
    pub pipelines: HashMap<String, PipelineConfig>,
    /// Input buffer size limits (bytes).
    pub input_buffer_size: RangedParam<u32>,
    /// Maximum media duration limits (milliseconds).
    pub media_duration: RangedParam<u32>,
    /// Output buffer duration limits (milliseconds).
    pub output_buffer_duration: RangedParam<u32>,
    /// Output buffer size limits (bytes).
    pub output_buffer_size: RangedParam<u32>,
    /// Read timeout limits (milliseconds).
    pub read_timeout: RangedParam<u32>,
    /// Realtime rate limits.
    pub realtime_rate: RangedParam<f64>,
    /// Whether streaming output is enabled by default.
    pub streaming_enabled: bool,
    /// Whether input buffer overflow terminates the call (vs. blocking).
    pub overflow_termination_enabled: bool,
    /// Health service configuration.
    pub health: GrpcHealthServiceConfig,
}

impl Default for RootConfig {
    fn default() -> Self {
        // The built-in defaults for the ranged service parameters.
        RootConfig {
            pipelines: HashMap::new(),
            input_buffer_size: RangedParam {
                default: 256 * 1024,
                min: 1,
                max: 20 * 1024 * 1024,
            },
            media_duration: RangedParam {
                default: 60_000,
                min: 1,
                max: u32::MAX,
            },
            output_buffer_duration: RangedParam {
                default: 0,
                min: 0,
                max: u32::MAX,
            },
            output_buffer_size: RangedParam {
                default: 4096,
                min: 1,
                max: 5 * 1024 * 1024,
            },
            read_timeout: RangedParam {
                default: 15_000,
                min: 1,
                max: u32::MAX,
            },
            realtime_rate: RangedParam {
                default: 0.0,
                min: 0.0,
                max: 64.0,
            },
            streaming_enabled: true,
            overflow_termination_enabled: false,
            health: GrpcHealthServiceConfig::default(),
        }
    }
}

macro_rules! extract_ranged {
    ($key:literal, $variant:path, $default:expr, $min:expr, $max:expr) => {{
        let unwrap = |value: Option<RawServiceParameter>| -> Result<Option<_>, ConfigError> {
            match value {
                None => Ok(None),
                Some($variant(v)) => Ok(Some(v)),
                Some(_) => Err(ConfigError(format!(
                    "service parameter limits object contains two different field \
                     configuration keys (parameter '{}')",
                    $key
                ))),
            }
        };
        (unwrap($default)?, unwrap($min)?, unwrap($max)?)
    }};
}

impl RootConfig {
    /// Loads and validates the root configuration from the given JSON file.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| ConfigError(format!("{}: {e}", path.display())))?;
        let raw: RawServiceConfiguration = serde_json::from_str(&contents)
            .map_err(|e| ConfigError(format!("{}: {e}", path.display())))?;
        Self::from_raw(raw)
    }

    /// Loads and validates the root configuration from a JSON string.
    pub fn load_str(contents: &str) -> Result<Self, ConfigError> {
        let raw: RawServiceConfiguration =
            serde_json::from_str(contents).map_err(|e| ConfigError(e.to_string()))?;
        Self::from_raw(raw)
    }

    fn from_raw(raw: RawServiceConfiguration) -> Result<Self, ConfigError> {
        let mut config = RootConfig::default();

        // Pipelines.
        for (name, pipeline) in raw.pipelines {
            if !pipeline.intermediate.is_empty() {
                return Err(ConfigError(format!(
                    "pipeline '{name}': intermediate bins are not supported"
                )));
            }
            let mut parsed = PipelineConfig::default();
            for (bin_id, bin) in pipeline.input {
                let spec =
                    PipelineTemplate::parse(&bin.specs).map_err(|e| ConfigError(e.to_string()))?;
                parsed.input_bins.insert(bin_id, InputBinConfig { spec });
            }
            for (bin_id, bin) in pipeline.output {
                let spec =
                    PipelineTemplate::parse(&bin.specs).map_err(|e| ConfigError(e.to_string()))?;
                parsed.output_bins.insert(
                    bin_id,
                    OutputBinConfig {
                        spec,
                        streamable: !bin.non_streamable,
                    },
                );
            }
            config.pipelines.insert(name, parsed);
        }
        if config.pipelines.is_empty() {
            return Err(ConfigError(
                "pipelines configuration must contain at least one pipeline configuration".into(),
            ));
        }

        // Service parameters.  Each limits object configures exactly one
        // member of the ServiceParameter oneof, and each parameter may be
        // configured at most once.
        let mut seen = std::collections::HashSet::new();
        for limits in raw.service_parameters {
            let field = limits
                .default
                .or(limits.min)
                .or(limits.max)
                .ok_or_else(|| {
                    ConfigError(
                        "one or more parameter limit objects has neither a default value, \
                         a min value, nor a max value"
                            .into(),
                    )
                })?;
            let key = parameter_key(&field);
            if !seen.insert(key) {
                return Err(ConfigError(format!(
                    "service parameter '{key}' found more than once in configuration"
                )));
            }

            match field {
                RawServiceParameter::Rate(_) => {
                    let (default, min, max) = extract_ranged!(
                        "rate",
                        RawServiceParameter::Rate,
                        limits.default,
                        limits.min,
                        limits.max
                    );
                    config.realtime_rate =
                        RangedParam::resolve("rate", 0.0, 0.0, 64.0, default, min, max)?;
                }
                RawServiceParameter::FlowControlEnforcementPolicy(_) => {
                    if limits.min.is_some() || limits.max.is_some() {
                        return Err(ConfigError(format!(
                            "minimum and maximum parameters cannot be specified for \
                             parameter '{key}'"
                        )));
                    }
                    let Some(RawServiceParameter::FlowControlEnforcementPolicy(policy)) =
                        limits.default
                    else {
                        continue;
                    };
                    config.overflow_termination_enabled =
                        !matches!(policy, RawFlowControlPolicy::Block);
                }
                RawServiceParameter::LengthLimitMilliseconds(_) => {
                    let (default, min, max) = extract_ranged!(
                        "lengthLimitMilliseconds",
                        RawServiceParameter::LengthLimitMilliseconds,
                        limits.default,
                        limits.min,
                        limits.max
                    );
                    config.media_duration = RangedParam::resolve(
                        "lengthLimitMilliseconds",
                        60_000,
                        1,
                        u32::MAX,
                        default,
                        min,
                        max,
                    )?;
                }
                // This parameter isn't used (same as the original).
                RawServiceParameter::StartToleranceBytes(_) => {}
                RawServiceParameter::ReadTimeoutMilliseconds(_) => {
                    let (default, min, max) = extract_ranged!(
                        "readTimeoutMilliseconds",
                        RawServiceParameter::ReadTimeoutMilliseconds,
                        limits.default,
                        limits.min,
                        limits.max
                    );
                    // NOTE: the dev config ships a default of 0 (timeout
                    // disabled), so unlike the original hard range of
                    // [1, u32::MAX] we allow 0 here; 0 disables the timeout.
                    config.read_timeout = RangedParam::resolve(
                        "readTimeoutMilliseconds",
                        15_000,
                        0,
                        u32::MAX,
                        default,
                        min,
                        max,
                    )?;
                }
                RawServiceParameter::PipelineInputBuffer(_) => {
                    let (default, min, max) = extract_ranged!(
                        "pipelineInputBuffer",
                        RawServiceParameter::PipelineInputBuffer,
                        limits.default,
                        limits.min,
                        limits.max
                    );
                    config.input_buffer_size = RangedParam::resolve(
                        "pipelineInputBuffer",
                        256 * 1024,
                        1,
                        20 * 1024 * 1024,
                        default,
                        min,
                        max,
                    )?;
                }
                RawServiceParameter::PipelineOutputBuffer(_) => {
                    let (default, min, max) = extract_ranged!(
                        "pipelineOutputBuffer",
                        RawServiceParameter::PipelineOutputBuffer,
                        limits.default,
                        limits.min,
                        limits.max
                    );
                    config.output_buffer_size = RangedParam::resolve(
                        "pipelineOutputBuffer",
                        4096,
                        1,
                        5 * 1024 * 1024,
                        default,
                        min,
                        max,
                    )?;
                }
                RawServiceParameter::StreamOutput(_) => {
                    if limits.min.is_some() || limits.max.is_some() {
                        return Err(ConfigError(format!(
                            "minimum and maximum parameters cannot be specified for \
                             parameter '{key}'"
                        )));
                    }
                    if let Some(RawServiceParameter::StreamOutput(value)) = limits.default {
                        config.streaming_enabled = value;
                    }
                }
                RawServiceParameter::PipelineOutputBufferMilliseconds(_) => {
                    let (default, min, max) = extract_ranged!(
                        "pipelineOutputBufferMilliseconds",
                        RawServiceParameter::PipelineOutputBufferMilliseconds,
                        limits.default,
                        limits.min,
                        limits.max
                    );
                    config.output_buffer_duration = RangedParam::resolve(
                        "pipelineOutputBufferMilliseconds",
                        0,
                        0,
                        u32::MAX,
                        default,
                        min,
                        max,
                    )?;
                }
            }
        }

        // gRPC health service configuration.
        if let Some(health) = raw
            .grpc
            .and_then(|g| g.server)
            .and_then(|s| s.health_service)
        {
            let defaults = GrpcHealthServiceConfig::default();
            let filter_window_size = match health.filter_window_size {
                Some(0) => {
                    return Err(ConfigError("filter window size cannot be set to 0".into()));
                }
                Some(n) => n,
                None => defaults.filter_window_size,
            };
            let unhealthy_threshold = match health.unhealthy_threshold {
                Some(t) if t <= 0.0 || t > 1.0 => {
                    return Err(ConfigError(format!(
                        "proposed unhealthy threshold value ({t}) is outside of the valid \
                         parameter range (0.0, 1.0]"
                    )));
                }
                Some(t) => t,
                None => defaults.unhealthy_threshold,
            };
            config.health = GrpcHealthServiceConfig {
                filter_window_size,
                unhealthy_threshold,
            };
        }

        Ok(config)
    }
}

fn parameter_key(param: &RawServiceParameter) -> &'static str {
    match param {
        RawServiceParameter::Rate(_) => "rate",
        RawServiceParameter::FlowControlEnforcementPolicy(_) => "flowControlEnforcementPolicy",
        RawServiceParameter::LengthLimitMilliseconds(_) => "lengthLimitMilliseconds",
        RawServiceParameter::StartToleranceBytes(_) => "startToleranceBytes",
        RawServiceParameter::ReadTimeoutMilliseconds(_) => "readTimeoutMilliseconds",
        RawServiceParameter::PipelineInputBuffer(_) => "pipelineInputBuffer",
        RawServiceParameter::PipelineOutputBuffer(_) => "pipelineOutputBuffer",
        RawServiceParameter::StreamOutput(_) => "streamOutput",
        RawServiceParameter::PipelineOutputBufferMilliseconds(_) => {
            "pipelineOutputBufferMilliseconds"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"{
        "serviceParameters": [
            { "description": "rate", "default": { "rate": 1.0 } },
            {
                "default": { "pipelineInputBuffer": 2097152 },
                "min": { "pipelineInputBuffer": 262144 },
                "max": { "pipelineInputBuffer": 10485760 }
            },
            { "default": { "readTimeoutMilliseconds": 15000 } }
        ],
        "pipelines": {
            "p": {
                "input": { "in": { "specs": "decodebin ! queue ! c." } },
                "output": {
                    "out": { "specs": "concat name=c ! wavenc", "non_streamable": true }
                }
            }
        }
    }"#;

    #[test]
    fn sample_config_parses() {
        let config = RootConfig::load_str(SAMPLE).unwrap();
        assert_eq!(config.realtime_rate.default, 1.0);
        assert_eq!(config.input_buffer_size.default, 2_097_152);
        assert_eq!(config.input_buffer_size.min, 262_144);
        assert_eq!(config.read_timeout.default, 15_000);
        let pipeline = &config.pipelines["p"];
        assert!(pipeline.input_bins.contains_key("in"));
        assert!(!pipeline.output_bins["out"].streamable);
    }

    #[test]
    fn empty_pipelines_are_rejected() {
        assert!(RootConfig::load_str(r#"{"pipelines": {}}"#).is_err());
    }

    #[test]
    fn duplicate_parameters_are_rejected() {
        let doc = r#"{
            "serviceParameters": [
                { "default": { "rate": 1.0 } },
                { "default": { "rate": 2.0 } }
            ],
            "pipelines": { "p": { "input": {}, "output": {} } }
        }"#;
        assert!(RootConfig::load_str(doc).is_err());
    }

    #[test]
    fn out_of_range_default_is_rejected() {
        let doc = r#"{
            "serviceParameters": [
                {
                    "default": { "pipelineInputBuffer": 100 },
                    "min": { "pipelineInputBuffer": 1000 }
                }
            ],
            "pipelines": { "p": { "input": {}, "output": {} } }
        }"#;
        assert!(RootConfig::load_str(doc).is_err());
    }

    const PIPELINES_ONLY: &str = r#""pipelines": { "p": { "input": {}, "output": {} } }"#;

    fn with_params(params: &str) -> String {
        format!(r#"{{ "serviceParameters": [ {params} ], {PIPELINES_ONLY} }}"#)
    }

    #[test]
    fn min_greater_than_max_is_rejected() {
        let doc = with_params(
            r#"{ "min": { "pipelineInputBuffer": 2000 }, "max": { "pipelineInputBuffer": 1000 } }"#,
        );
        assert!(RootConfig::load_str(&doc).is_err());
    }

    #[test]
    fn limits_outside_the_hard_range_are_rejected() {
        // The hard maximum for the input buffer is 20 MiB.
        let doc = with_params(r#"{ "max": { "pipelineInputBuffer": 999999999 } }"#);
        assert!(RootConfig::load_str(&doc).is_err());
        // The hard maximum for the rate is 64.0.
        let doc = with_params(r#"{ "default": { "rate": 100.0 } }"#);
        assert!(RootConfig::load_str(&doc).is_err());
    }

    #[test]
    fn mismatched_limit_fields_are_rejected() {
        let doc = with_params(
            r#"{ "default": { "pipelineInputBuffer": 1000 }, "max": { "rate": 2.0 } }"#,
        );
        assert!(RootConfig::load_str(&doc).is_err());
    }

    #[test]
    fn empty_limits_object_is_rejected() {
        let doc = with_params(r#"{ "description": "no values" }"#);
        assert!(RootConfig::load_str(&doc).is_err());
    }

    #[test]
    fn flow_control_policy_controls_overflow_termination() {
        let doc = with_params(r#"{ "default": { "flowControlEnforcementPolicy": "BLOCK" } }"#);
        assert!(
            !RootConfig::load_str(&doc)
                .unwrap()
                .overflow_termination_enabled
        );
        let doc = with_params(r#"{ "default": { "flowControlEnforcementPolicy": "ERROR" } }"#);
        assert!(
            RootConfig::load_str(&doc)
                .unwrap()
                .overflow_termination_enabled
        );
        let doc = with_params(r#"{ "default": { "flowControlEnforcementPolicy": "NOTIFY" } }"#);
        assert!(
            RootConfig::load_str(&doc)
                .unwrap()
                .overflow_termination_enabled
        );
    }

    #[test]
    fn scalar_parameters_reject_min_and_max() {
        let doc = with_params(
            r#"{ "default": { "flowControlEnforcementPolicy": "BLOCK" },
                 "max": { "flowControlEnforcementPolicy": "ERROR" } }"#,
        );
        assert!(RootConfig::load_str(&doc).is_err());
        let doc = with_params(
            r#"{ "default": { "streamOutput": true }, "min": { "streamOutput": false } }"#,
        );
        assert!(RootConfig::load_str(&doc).is_err());
    }

    #[test]
    fn stream_output_default_is_applied() {
        let doc = with_params(r#"{ "default": { "streamOutput": false } }"#);
        assert!(!RootConfig::load_str(&doc).unwrap().streaming_enabled);
    }

    #[test]
    fn intermediate_bins_are_unsupported() {
        let doc = r#"{
            "pipelines": {
                "p": {
                    "input": {},
                    "intermediate": { "mid": { "specs": "queue" } },
                    "output": {}
                }
            }
        }"#;
        assert!(RootConfig::load_str(doc).is_err());
    }

    #[test]
    fn snake_case_aliases_are_accepted() {
        let doc = r#"{
            "service_parameters": [
                { "default": { "pipeline_input_buffer": 1048576 } },
                { "default": { "read_timeout_milliseconds": 100 } }
            ],
            "pipelines": {
                "p": {
                    "input": { "in": { "specs": "queue ! c." } },
                    "output": { "out": { "specs": "concat name=c", "nonStreamable": true } }
                }
            }
        }"#;
        let config = RootConfig::load_str(doc).unwrap();
        assert_eq!(config.input_buffer_size.default, 1_048_576);
        assert_eq!(config.read_timeout.default, 100);
        assert!(!config.pipelines["p"].output_bins["out"].streamable);
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let doc = r#"{ "bogus": 1, "pipelines": { "p": { "input": {}, "output": {} } } }"#;
        assert!(RootConfig::load_str(doc).is_err());
    }

    #[test]
    fn health_configuration_is_validated() {
        let base = r#""pipelines": { "p": { "input": {}, "output": {} } }"#;
        let doc = format!(
            r#"{{ "grpc": {{ "server": {{ "healthService": {{
                 "filterWindowSize": 100, "unhealthyThreshold": 0.5 }} }} }}, {base} }}"#
        );
        let config = RootConfig::load_str(&doc).unwrap();
        assert_eq!(config.health.filter_window_size, 100);
        assert_eq!(config.health.unhealthy_threshold, 0.5);

        let doc = format!(
            r#"{{ "grpc": {{ "server": {{ "healthService": {{ "filterWindowSize": 0 }} }} }}, {base} }}"#
        );
        assert!(RootConfig::load_str(&doc).is_err());

        let doc = format!(
            r#"{{ "grpc": {{ "server": {{ "healthService": {{ "unhealthyThreshold": 1.5 }} }} }}, {base} }}"#
        );
        assert!(RootConfig::load_str(&doc).is_err());
    }

    #[test]
    fn invalid_bin_template_is_rejected() {
        let doc = r#"{
            "pipelines": {
                "p": {
                    "input": { "in": { "specs": "queue ! $(bogus.key) ! c." } },
                    "output": {}
                }
            }
        }"#;
        assert!(RootConfig::load_str(doc).is_err());
    }
}
