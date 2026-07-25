//! Integration tests for the piped transform engine.
//!
//! These tests require a working GStreamer installation (core elements
//! only, unless noted otherwise).

use std::collections::VecDeque;
use std::sync::Once;

use async_trait::async_trait;
use bytes::Bytes;

use transonic_core::engine::{ConnectionReader, ConnectionWriter, EngineError};
use transonic_core::error::ProcessErrorCode;
use transonic_core::job::{Job, JobId, JobInput, JobOutput};
use transonic_core::messages::{
    ClientMessage, ClientMessageData, ServerMessage, ServerMessageData,
};
use transonic_core::{Processor, RootConfig};

///////////////////////////////////////////////////////////////////////////
// helpers
///////////////////////////////////////////////////////////////////////////

fn init() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        transonic_core::init().expect("gstreamer initialization");
    });
}

const CONFIG: &str = r#"{
    "serviceParameters": [
        { "default": { "readTimeoutMilliseconds": 5000 } }
    ],
    "pipelines": {
        "passthrough": {
            "input": {
                "in": { "specs": "queue ! c." }
            },
            "output": {
                "out": { "specs": "concat name=c" },
                "out-batch": { "specs": "concat name=c", "non_streamable": true }
            }
        }
    }
}"#;

fn processor() -> Processor {
    init();
    let config = RootConfig::load_str(CONFIG).expect("test config");
    Processor::new(config, std::env::temp_dir())
}

fn job(inputs: &[&str], outputs: &[&str]) -> Job {
    Job {
        id: JobId::generate(),
        pipeline_id: "passthrough".to_string(),
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

/// A scripted connection reader.
struct ScriptReader {
    messages: VecDeque<ClientMessage>,
    /// When set, the reader pends forever once the script is exhausted
    /// (used for read-timeout tests) instead of signalling end-of-stream.
    hang_at_end: bool,
    /// Optional delay before each read completes, giving the pipeline time
    /// to request data (used for flow-control tests).
    read_delay: Option<std::time::Duration>,
}

impl ScriptReader {
    fn new(messages: Vec<ClientMessage>) -> Self {
        ScriptReader {
            messages: messages.into(),
            hang_at_end: false,
            read_delay: None,
        }
    }
}

#[async_trait]
impl ConnectionReader for ScriptReader {
    async fn read(&mut self) -> Result<Option<ClientMessage>, EngineError> {
        if let Some(delay) = self.read_delay {
            tokio::time::sleep(delay).await;
        }
        match self.messages.pop_front() {
            Some(message) => Ok(Some(message)),
            None if self.hang_at_end => {
                futures::future::pending::<()>().await;
                unreachable!()
            }
            None => Ok(None),
        }
    }
}

/// A connection writer that collects everything it is given.
#[derive(Default)]
struct CollectWriter {
    messages: Vec<ServerMessage>,
}

#[async_trait]
impl ConnectionWriter for CollectWriter {
    async fn write(&mut self, message: ServerMessage) -> Result<(), EngineError> {
        self.messages.push(message);
        Ok(())
    }
}

fn payload(stream: &str, data: &[u8]) -> ClientMessage {
    ClientMessage {
        stream_id: stream.to_string(),
        data: ClientMessageData::InputBuffers(vec![Bytes::copy_from_slice(data)]),
    }
}

fn close(stream: &str) -> ClientMessage {
    ClientMessage {
        stream_id: stream.to_string(),
        data: ClientMessageData::Close,
    }
}

fn output_bytes(writer: &CollectWriter, stream: &str) -> Vec<u8> {
    writer
        .messages
        .iter()
        .filter(|m| m.stream_id == stream)
        .filter_map(|m| match &m.data {
            ServerMessageData::OutputBuffer(output) => Some(output.buffer.as_ref()),
            _ => None,
        })
        .flatten()
        .copied()
        .collect()
}

fn expect_error(result: Result<(), EngineError>, code: ProcessErrorCode) {
    match result {
        Err(EngineError::Process(error)) => assert_eq!(error.code, code, "{}", error.message),
        other => panic!("expected {code:?} error, got {other:?}"),
    }
}

///////////////////////////////////////////////////////////////////////////
// tests
///////////////////////////////////////////////////////////////////////////

#[tokio::test]
async fn passthrough_roundtrip() {
    let processor = processor();
    let data: Vec<u8> = (0..40_000u32).map(|i| (i % 251) as u8).collect();
    let mut reader = ScriptReader::new(vec![payload("I1", &data), close("I1")]);
    let mut writer = CollectWriter::default();

    processor
        .process(&job(&["I1"], &["O1"]), &mut reader, &mut writer)
        .await
        .expect("processing succeeds");

    assert_eq!(output_bytes(&writer, "O1"), data);
}

#[tokio::test]
async fn concat_preserves_input_order() {
    let processor = processor();
    let first = vec![1u8; 10_000];
    let second = vec![2u8; 10_000];
    let mut reader = ScriptReader::new(vec![
        payload("I1", &first),
        payload("I2", &second),
        close("I1"),
        close("I2"),
    ]);
    let mut writer = CollectWriter::default();

    // Multiple inputs require the ERROR/NOTIFY flow-control policy (the
    // blocking policy only supports a single input, as in the original).
    let mut job = job(&["I1", "I2"], &["O1"]);
    job.overflow_termination_enabled = Some(true);

    processor
        .process(&job, &mut reader, &mut writer)
        .await
        .expect("processing succeeds");

    let mut expected = first.clone();
    expected.extend_from_slice(&second);
    assert_eq!(output_bytes(&writer, "O1"), expected);
}

#[tokio::test]
async fn non_streamable_output_is_drained_from_file_sink() {
    let processor = processor();
    let data = vec![7u8; 20_000];
    let mut reader = ScriptReader::new(vec![payload("I1", &data), close("I1")]);
    let mut writer = CollectWriter::default();

    let mut job = job(&["I1"], &["O1"]);
    job.outputs[0].bin_id = "out-batch".to_string();

    processor
        .process(&job, &mut reader, &mut writer)
        .await
        .expect("processing succeeds");

    assert_eq!(output_bytes(&writer, "O1"), data);
}

#[tokio::test]
async fn streaming_disabled_uses_file_sink() {
    let processor = processor();
    let data = vec![3u8; 12_000];
    let mut reader = ScriptReader::new(vec![payload("I1", &data), close("I1")]);
    let mut writer = CollectWriter::default();

    let mut job = job(&["I1"], &["O1"]);
    job.streaming_enabled = Some(false);

    processor
        .process(&job, &mut reader, &mut writer)
        .await
        .expect("processing succeeds");

    assert_eq!(output_bytes(&writer, "O1"), data);
}

#[tokio::test]
async fn starvation_notifications_are_emitted() {
    let processor = processor();
    let mut reader = ScriptReader::new(vec![payload("I1", &[0u8; 1000]), close("I1")]);
    // Delay reads so the pipeline observes an empty input queue first and
    // emits a NEED_DATA notification.
    reader.read_delay = Some(std::time::Duration::from_millis(200));
    let mut writer = CollectWriter::default();

    processor
        .process(&job(&["I1"], &["O1"]), &mut reader, &mut writer)
        .await
        .expect("processing succeeds");

    assert!(writer
        .messages
        .iter()
        .any(|m| matches!(m.data, ServerMessageData::DataStarvation)));
}

#[tokio::test]
async fn invalid_pipeline_id_is_reported() {
    let processor = processor();
    let mut reader = ScriptReader::new(vec![]);
    let mut writer = CollectWriter::default();
    let mut job = job(&["I1"], &["O1"]);
    job.pipeline_id = "unknown".to_string();

    let result = processor.process(&job, &mut reader, &mut writer).await;
    expect_error(result, ProcessErrorCode::PipelineIdInvalid);
}

#[tokio::test]
async fn invalid_input_bin_id_is_reported() {
    let processor = processor();
    let mut reader = ScriptReader::new(vec![]);
    let mut writer = CollectWriter::default();
    let mut job = job(&["I1"], &["O1"]);
    job.inputs[0].bin_id = "unknown".to_string();

    let result = processor.process(&job, &mut reader, &mut writer).await;
    expect_error(result, ProcessErrorCode::InputBinIdInvalid);
}

#[tokio::test]
async fn invalid_output_bin_id_is_reported() {
    let processor = processor();
    let mut reader = ScriptReader::new(vec![]);
    let mut writer = CollectWriter::default();
    let mut job = job(&["I1"], &["O1"]);
    job.outputs[0].bin_id = "unknown".to_string();

    let result = processor.process(&job, &mut reader, &mut writer).await;
    expect_error(result, ProcessErrorCode::OutputBinIdInvalid);
}

#[tokio::test]
async fn invalid_stream_id_is_reported() {
    let processor = processor();
    let mut reader = ScriptReader::new(vec![payload("bogus", &[0u8; 16])]);
    let mut writer = CollectWriter::default();

    let result = processor
        .process(&job(&["I1"], &["O1"]), &mut reader, &mut writer)
        .await;
    expect_error(result, ProcessErrorCode::InputStreamIdInvalid);
}

#[tokio::test]
async fn double_close_is_reported() {
    let processor = processor();
    let mut reader = ScriptReader::new(vec![close("I1"), close("I1")]);
    let mut writer = CollectWriter::default();

    let result = processor
        .process(&job(&["I1"], &["O1"]), &mut reader, &mut writer)
        .await;
    expect_error(result, ProcessErrorCode::InputStreamClosed);
}

#[tokio::test]
async fn read_timeout_is_enforced() {
    let processor = processor();
    let mut reader = ScriptReader::new(vec![payload("I1", &[0u8; 16])]);
    reader.hang_at_end = true;
    let mut writer = CollectWriter::default();

    let mut job = job(&["I1"], &["O1"]);
    job.read_timeout = Some(100);

    let result = processor.process(&job, &mut reader, &mut writer).await;
    expect_error(result, ProcessErrorCode::ReadTimeoutExceeded);
}

#[tokio::test]
async fn out_of_range_job_param_is_reported() {
    let processor = processor();
    let mut reader = ScriptReader::new(vec![]);
    let mut writer = CollectWriter::default();

    let mut job = job(&["I1"], &["O1"]);
    job.realtime_rate = Some(1000.0);

    let result = processor.process(&job, &mut reader, &mut writer).await;
    expect_error(result, ProcessErrorCode::JobParamInvalid);
}

#[tokio::test]
async fn input_buffer_overflow_terminates_when_enabled() {
    let processor = processor();
    // The hard limit is 4x the soft limit; a single oversized payload
    // overflows it.
    let oversized = vec![0u8; 64 * 1024];
    let mut reader = ScriptReader::new(vec![payload("I1", &oversized), close("I1")]);
    let mut writer = CollectWriter::default();

    let mut job = job(&["I1"], &["O1"]);
    job.input_buffer_size = Some(1024);
    job.overflow_termination_enabled = Some(true);
    // Keep the pipeline from consuming data as fast as we push it.
    job.realtime_rate = Some(1.0);

    let result = processor.process(&job, &mut reader, &mut writer).await;
    expect_error(result, ProcessErrorCode::InputBufferOverflow);
}

#[tokio::test]
async fn multiple_inputs_require_overflow_termination() {
    let processor = processor();
    let mut reader = ScriptReader::new(vec![]);
    let mut writer = CollectWriter::default();

    let mut job = job(&["I1", "I2"], &["O1"]);
    job.overflow_termination_enabled = Some(false);

    let result = processor.process(&job, &mut reader, &mut writer).await;
    expect_error(result, ProcessErrorCode::JobParamInvalid);
}

///////////////////////////////////////////////////////////////////////////
// additional coverage (parity with the original piped_transform suite)
///////////////////////////////////////////////////////////////////////////

/// Returns true when the given GStreamer element factory is available.
fn have_element(name: &str) -> bool {
    init();
    gstreamer::ElementFactory::find(name).is_some()
}

const EXTENDED_CONFIG: &str = r#"{
    "serviceParameters": [
        { "default": { "readTimeoutMilliseconds": 5000 } }
    ],
    "pipelines": {
        "passthrough": {
            "input": { "in": { "specs": "queue ! c." } },
            "output": {
                "out": { "specs": "concat name=c" },
                "out-tee": { "specs": "concat name=c ! tee name=t ! queue" },
                "out-tap": { "specs": "t. ! queue" }
            }
        },
        "broken": {
            "input": { "in": { "specs": "nosuchelement ! c." } },
            "output": { "out": { "specs": "concat name=c" } }
        },
        "pcm": {
            "input": {
                "in": {
                    "specs": "rawaudioparse format=pcm pcm-format=s16le sample-rate=16000 num-channels=1 ! volume volume=$(pipeline_parameters.gain,1.0) ! c."
                }
            },
            "output": { "out": { "specs": "concat name=c" } }
        }
    }
}"#;

fn extended_processor() -> Processor {
    init();
    let config = RootConfig::load_str(EXTENDED_CONFIG).expect("extended test config");
    Processor::new(config, std::env::temp_dir())
}

#[tokio::test]
async fn pipeline_parse_failure_is_reported() {
    let processor = extended_processor();
    let mut reader = ScriptReader::new(vec![]);
    let mut writer = CollectWriter::default();
    let mut job = job(&["I1"], &["O1"]);
    job.pipeline_id = "broken".to_string();

    let result = processor.process(&job, &mut reader, &mut writer).await;
    expect_error(result, ProcessErrorCode::PipelineParseFailure);
}

#[tokio::test]
async fn write_after_close_is_reported() {
    let processor = processor();
    let mut reader = ScriptReader::new(vec![
        payload("I1", &[1u8; 64]),
        close("I1"),
        payload("I1", &[2u8; 64]),
    ]);
    let mut writer = CollectWriter::default();

    let result = processor
        .process(&job(&["I1"], &["O1"]), &mut reader, &mut writer)
        .await;
    expect_error(result, ProcessErrorCode::InputStreamClosed);
}

#[tokio::test]
async fn multiple_outputs_receive_the_stream() {
    let processor = extended_processor();
    let data = vec![9u8; 20_000];
    let mut reader = ScriptReader::new(vec![payload("I1", &data), close("I1")]);
    let mut writer = CollectWriter::default();

    let mut job = job(&["I1"], &["O1", "O2"]);
    job.outputs[0].bin_id = "out-tee".to_string();
    job.outputs[1].bin_id = "out-tap".to_string();

    processor
        .process(&job, &mut reader, &mut writer)
        .await
        .expect("processing succeeds");

    assert_eq!(output_bytes(&writer, "O1"), data);
    assert_eq!(output_bytes(&writer, "O2"), data);
}

#[tokio::test]
async fn input_extension_hint_is_accepted() {
    let processor = processor();
    let data = vec![5u8; 4_000];
    let mut reader = ScriptReader::new(vec![payload("I1", &data), close("I1")]);
    let mut writer = CollectWriter::default();

    let mut job = job(&["I1"], &["O1"]);
    job.inputs[0].extension = Some(".pcm".to_string());

    processor
        .process(&job, &mut reader, &mut writer)
        .await
        .expect("processing succeeds");
    assert_eq!(output_bytes(&writer, "O1"), data);
}

#[tokio::test]
async fn input_stream_template_parameters_are_applied() {
    if !(have_element("rawaudioparse") && have_element("volume")) {
        eprintln!("skipping: rawaudioparse/volume not available");
        return;
    }
    let processor = extended_processor();
    let loud: Vec<u8> = std::iter::repeat_n([0x10u8, 0x27u8], 8_000)
        .flatten()
        .collect();

    // gain=0.0 must silence the stream.
    let mut job = job(&["I1"], &["O1"]);
    job.pipeline_id = "pcm".to_string();
    job.inputs[0].params.insert(
        transonic_core::template::TemplateKey::new("gain").unwrap(),
        transonic_core::template::TemplateValue::new("0.0").unwrap(),
    );
    let mut reader = ScriptReader::new(vec![payload("I1", &loud), close("I1")]);
    let mut writer = CollectWriter::default();
    processor
        .process(&job, &mut reader, &mut writer)
        .await
        .expect("processing succeeds");
    let muted = output_bytes(&writer, "O1");
    assert_eq!(muted.len(), loud.len());
    assert!(muted.iter().all(|&b| b == 0), "gain=0 must mute the stream");

    // The default (gain=1.0) must pass the stream through unchanged.
    let mut job = job.clone();
    job.inputs[0].params.clear();
    let mut reader = ScriptReader::new(vec![payload("I1", &loud), close("I1")]);
    let mut writer = CollectWriter::default();
    processor
        .process(&job, &mut reader, &mut writer)
        .await
        .expect("processing succeeds");
    assert_eq!(output_bytes(&writer, "O1"), loud);
}

#[tokio::test]
async fn media_duration_limit_is_enforced() {
    if !have_element("rawaudioparse") {
        eprintln!("skipping: rawaudioparse not available");
        return;
    }
    let processor = extended_processor();
    // One second of 16 kHz mono S16LE audio with a 100 ms limit.
    let one_second = vec![0u8; 32_000];
    let mut job = job(&["I1"], &["O1"]);
    job.pipeline_id = "pcm".to_string();
    job.max_media_duration = Some(100);

    let mut reader = ScriptReader::new(vec![payload("I1", &one_second), close("I1")]);
    let mut writer = CollectWriter::default();
    let result = processor.process(&job, &mut reader, &mut writer).await;
    expect_error(result, ProcessErrorCode::MaxMediaDurationExceeded);
}

#[tokio::test]
async fn output_buffer_size_batches_output() {
    let processor = processor();
    let data = vec![1u8; 50_000];
    let mut job = job(&["I1"], &["O1"]);
    job.output_buffer_size = Some(10_000);

    let mut reader = ScriptReader::new(vec![payload("I1", &data), close("I1")]);
    let mut writer = CollectWriter::default();
    processor
        .process(&job, &mut reader, &mut writer)
        .await
        .expect("processing succeeds");

    let outputs: Vec<usize> = writer
        .messages
        .iter()
        .filter_map(|m| match &m.data {
            ServerMessageData::OutputBuffer(output) => Some(output.buffer.len()),
            _ => None,
        })
        .collect();
    assert_eq!(outputs.iter().sum::<usize>(), data.len());
    // Every flushed chunk except the trailing one must have reached the
    // configured aggregation threshold.
    for size in &outputs[..outputs.len().saturating_sub(1)] {
        assert!(*size >= 10_000, "chunk of {size} bytes below threshold");
    }
}

#[tokio::test]
async fn output_buffer_duration_carries_chunk_metadata() {
    if !have_element("rawaudioparse") {
        eprintln!("skipping: rawaudioparse not available");
        return;
    }
    let processor = extended_processor();
    let mut job = job(&["I1"], &["O1"]);
    job.pipeline_id = "pcm".to_string();
    job.output_buffer_duration = Some(50);
    job.output_buffer_size = Some(1_000_000);

    // Feed half a second of audio as 10 ms payloads so buffers reach the
    // sink individually and the duration threshold drives the flushing.
    let mut script: Vec<ClientMessage> = (0..50).map(|_| payload("I1", &[0u8; 320])).collect();
    script.push(close("I1"));
    let mut reader = ScriptReader::new(script);
    let mut writer = CollectWriter::default();
    processor
        .process(&job, &mut reader, &mut writer)
        .await
        .expect("processing succeeds");

    let chunks: Vec<_> = writer
        .messages
        .iter()
        .filter_map(|m| match &m.data {
            ServerMessageData::OutputBuffer(output) => Some(output),
            _ => None,
        })
        .collect();
    assert!(chunks.len() > 1, "duration flushing must produce chunks");
    assert!(
        chunks
            .iter()
            .any(|c| c.chunk_duration_ms.is_some_and(|d| d >= 50)),
        "chunks must carry duration metadata"
    );
}
