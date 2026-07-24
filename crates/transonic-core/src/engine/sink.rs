//! Output-side (appsink) processing.
//!
//! Each streamable output of a job is backed by an `appsink` element.  The
//! `new-sample` callback aggregates buffers (and their metadata) until the
//! configured output buffer size or duration threshold is reached, then
//! flushes an output message toward the client.  It also enforces the
//! maximum media duration limit.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;

use crate::error::{ProcessError, ProcessErrorCode};
use crate::messages::{OutputBuffer, ServerMessage, ServerMessageData};

use super::{EngineEvent, EventSender};

#[derive(Default)]
struct AggregateState {
    data: Vec<u8>,
    chunk_duration: Option<Duration>,
    chunk_offset: Option<Duration>,
    input_offset: Option<u64>,
    silence: Option<bool>,
}

/// Processor wrapping one `appsink` element.
pub(crate) struct AppSinkProcessor {
    appsink: gst_app::AppSink,
    name: String,
    stream_id: String,
    events: EventSender,
    max_media_duration: Duration,
    output_buffer_size: u32,
    output_buffer_duration: Duration,
    state: Mutex<AggregateState>,
    /// Lazily probed once: whether the sink operates on a TIME segment.
    time_segment: std::sync::OnceLock<bool>,
}

impl AppSinkProcessor {
    pub(crate) fn new(
        appsink: gst_app::AppSink,
        name: String,
        stream_id: String,
        max_media_duration_ms: u32,
        output_buffer_size: u32,
        output_buffer_duration_ms: u32,
        events: EventSender,
    ) -> Arc<Self> {
        // XXX: This should be configurable.  The old code hardcoded it into
        // the pipeline spec (carried over from the original implementation).
        appsink.set_max_buffers(100);

        let processor = Arc::new(AppSinkProcessor {
            appsink: appsink.clone(),
            name,
            stream_id,
            events,
            max_media_duration: Duration::from_millis(u64::from(max_media_duration_ms)),
            output_buffer_size,
            output_buffer_duration: Duration::from_millis(u64::from(output_buffer_duration_ms)),
            state: Mutex::new(AggregateState::default()),
            time_segment: std::sync::OnceLock::new(),
        });

        let sample_processor = Arc::clone(&processor);
        let eos_processor = Arc::clone(&processor);
        appsink.set_callbacks(
            gst_app::AppSinkCallbacks::builder()
                .new_sample(move |_| {
                    sample_processor.process_sample();
                    Ok(gst::FlowSuccess::Ok)
                })
                .eos(move |_| eos_processor.process_eos())
                .build(),
        );

        processor
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn stream_id(&self) -> &str {
        &self.stream_id
    }

    /// Detaches callbacks; called before dropping the pipeline.
    pub(crate) fn shutdown(&self) {
        self.appsink
            .set_callbacks(gst_app::AppSinkCallbacks::builder().build());
    }

    fn process_eos(&self) {
        let flushed = {
            let mut state = self.state.lock().unwrap();
            (!state.data.is_empty()).then(|| Self::take_output(&mut state))
        };
        if let Some(output) = flushed {
            self.send_output(output);
        }
        let _ = self.events.send(EngineEvent::SinkEos(self.name.clone()));
    }

    fn process_sample(&self) {
        if let Err(error) = self.process_sample_inner() {
            let _ = self.events.send(EngineEvent::Error(error));
        }
    }

    fn process_sample_inner(&self) -> Result<(), ProcessError> {
        let sample = self.appsink.pull_sample().map_err(|e| {
            ProcessError::internal(format!("appsink pull_sample(): failed to pull sample: {e}"))
        })?;
        let buffer = sample
            .buffer()
            .ok_or_else(|| ProcessError::internal("failed to get buffer from sample"))?;

        // Extract metadata from the sample buffer.
        let chunk_offset = buffer.pts().map(|pts| Duration::from_nanos(pts.nseconds()));
        let chunk_duration = buffer
            .duration()
            .map(|duration| Duration::from_nanos(duration.nseconds()));
        let meta = transonic_meta::get_custom_audio_meta(buffer);
        let input_stream_offset = meta
            .filter(|meta| meta.input_stream_offset >= 0)
            .map(|meta| meta.input_stream_offset as u64);
        let silence = meta.map(|meta| meta.silence);

        // Check if we've exceeded the maximum configured duration.  Use the
        // timestamp and duration from the sample buffer if available;
        // otherwise, execute a position query.
        if !self.max_media_duration.is_zero() {
            if let (Some(offset), Some(duration)) = (chunk_offset, chunk_duration) {
                if offset + duration > self.max_media_duration {
                    return Err(self.media_duration_error());
                }
            } else if self.time_segment_active() {
                if let Some(position) = self.appsink.query_position::<gst::ClockTime>() {
                    if Duration::from_nanos(position.nseconds()) > self.max_media_duration {
                        return Err(self.media_duration_error());
                    }
                }
            }
        }

        // Update the aggregate buffer data.
        let flushed = {
            let mut state = self.state.lock().unwrap();
            if state.data.is_empty() {
                state.chunk_duration = chunk_duration;
                state.chunk_offset = chunk_offset;
                state.input_offset = input_stream_offset;
                state.silence = silence;
            } else {
                // The aggregate silence flag stays `true` only while every
                // observed silence flag is `true`; a `false` flag pins it to
                // `false`, and a missing flag unsets it.
                match state.silence {
                    None => {
                        if silence == Some(false) {
                            state.silence = Some(false);
                        }
                    }
                    Some(true) => state.silence = silence,
                    Some(false) => {}
                }

                if state.chunk_duration.is_some() {
                    match chunk_duration {
                        Some(duration) => {
                            state.chunk_duration = state.chunk_duration.map(|d| d + duration);
                        }
                        None => state.chunk_duration = None,
                    }
                }
            }

            let map = buffer.map_readable().map_err(|_| {
                ProcessError::internal("failed to map output buffer memory for reading")
            })?;
            state.data.extend_from_slice(map.as_slice());

            let byte_threshold_reached = state.data.len() >= self.output_buffer_size as usize;
            let duration_threshold_reached = !self.output_buffer_duration.is_zero()
                && match state.chunk_duration {
                    None => true,
                    Some(duration) => duration >= self.output_buffer_duration,
                };
            (byte_threshold_reached || duration_threshold_reached)
                .then(|| Self::take_output(&mut state))
        };
        if let Some(output) = flushed {
            self.send_output(output);
        }

        Ok(())
    }

    fn take_output(state: &mut AggregateState) -> OutputBuffer {
        OutputBuffer {
            buffer: Bytes::from(std::mem::take(&mut state.data)),
            chunk_duration_ms: state.chunk_duration.take().map(|d| d.as_millis() as u64),
            chunk_offset_ms: state.chunk_offset.take().map(|d| d.as_millis() as u64),
            input_stream_offset: state.input_offset.take(),
            silence: state.silence.take(),
        }
    }

    fn send_output(&self, output: OutputBuffer) {
        let _ = self.events.send(EngineEvent::Server(ServerMessage {
            stream_id: self.stream_id.clone(),
            data: ServerMessageData::OutputBuffer(output),
        }));
    }

    /// Returns true when the sink operates on a TIME segment.  Position
    /// queries on other segment formats (e.g. BYTES from raw passthrough
    /// pipelines) can't answer in TIME and only produce warnings.
    fn time_segment_active(&self) -> bool {
        *self.time_segment.get_or_init(|| {
            self.appsink
                .static_pad("sink")
                .and_then(|pad| pad.sticky_event::<gst::event::Segment>(0))
                .map(|event| event.segment().format() == gst::Format::Time)
                .unwrap_or(false)
        })
    }

    fn media_duration_error(&self) -> ProcessError {
        ProcessError::new(
            ProcessErrorCode::MaxMediaDurationExceeded,
            format!(
                "maximum configured duration of {} milliseconds has been exceeded",
                self.max_media_duration.as_millis()
            ),
        )
    }
}
