//! Input-side (appsrc) processing.
//!
//! Each input stream of a job is backed by an `appsrc` element.  Client
//! payloads are appended to an internal byte queue; the `need-data` callback
//! drains the queue into the pipeline, tagging buffers with input-offset
//! metadata.  Flow control works exactly like the original:
//!
//! * `appsrc` `max-bytes` is set to the configured input buffer size (the
//!   "soft" limit).  Exceeding it emits an ENOUGH_DATA notification.
//! * The internal queue is bounded by a hard limit of four times the soft
//!   limit.  Depending on the enforcement policy, exceeding the hard limit
//!   either raises an input-buffer-overflow error or blocks the writer until
//!   the pipeline consumes data.

use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex};

use bytes::Bytes;
use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;

use transonic_meta::CustomAudioMeta;

use crate::error::{ProcessError, ProcessErrorCode};
use crate::messages::{ServerMessage, ServerMessageData};

use super::{EngineEvent, EventSender};

struct SourceState {
    queue: VecDeque<Bytes>,
    queued_bytes: u64,
    offset: u64,
    closed: bool,
    cancelled: bool,
}

/// Processor wrapping one `appsrc` element.
pub(crate) struct AppSource {
    appsrc: gst_app::AppSrc,
    stream_id: String,
    hard_limit: u64,
    overflow_termination_enabled: bool,
    events: EventSender,
    state: Mutex<SourceState>,
    /// Signalled when data is appended or the stream is closed/cancelled;
    /// waited on by the `need-data` callback.
    data_cond: Condvar,
    /// Signalled when the `need-data` callback drains data; waited on by
    /// blocking writers.
    space_cond: Condvar,
}

impl AppSource {
    pub(crate) fn new(
        appsrc: gst_app::AppSrc,
        stream_id: String,
        extension: Option<&str>,
        input_buffer_size: u32,
        overflow_termination_enabled: bool,
        events: EventSender,
    ) -> Result<Arc<Self>, ProcessError> {
        if let Some(extension) = extension {
            // Attach a fake URI carrying the file extension so that GStreamer
            // typefinding can use the extension as a hint.
            let path = if let Some(stripped) = extension.strip_prefix('.') {
                format!("/typefind.{stripped}")
            } else {
                format!("/typefind.{extension}")
            };
            let uri = format!("appsrc://{path}");
            appsrc
                .set_uri(&uri)
                .map_err(|e| ProcessError::internal(format!("failed to set appsrc URI: {e}")))?;
        }

        appsrc.set_max_bytes(u64::from(input_buffer_size));
        appsrc.set_stream_type(gst_app::AppStreamType::Stream);
        appsrc.set_block(false);

        let source = Arc::new(AppSource {
            appsrc: appsrc.clone(),
            stream_id,
            // XXX: Make the hard-limit multiplier configurable (carried over
            // from the original implementation).
            hard_limit: u64::from(input_buffer_size) * 4,
            overflow_termination_enabled,
            events,
            state: Mutex::new(SourceState {
                queue: VecDeque::new(),
                queued_bytes: 0,
                offset: 0,
                closed: false,
                cancelled: false,
            }),
            data_cond: Condvar::new(),
            space_cond: Condvar::new(),
        });

        let callback_source = Arc::clone(&source);
        appsrc.set_callbacks(
            gst_app::AppSrcCallbacks::builder()
                .need_data(move |_, _| callback_source.process_need_data())
                .build(),
        );

        Ok(source)
    }

    #[allow(dead_code)]
    pub(crate) fn stream_id(&self) -> &str {
        &self.stream_id
    }

    pub(crate) fn closed(&self) -> bool {
        self.state.lock().unwrap().closed
    }

    /// Appends client data to the queue.  Runs on a blocking thread because
    /// the BLOCK enforcement policy may need to wait for pipeline progress.
    pub(crate) fn write(&self, buffer: Bytes) -> Result<(), ProcessError> {
        if buffer.is_empty() {
            return Ok(());
        }
        if self.overflow_termination_enabled {
            self.write_non_blocking(buffer)
        } else {
            self.write_blocking(buffer)
        }
    }

    fn write_non_blocking(&self, buffer: Bytes) -> Result<(), ProcessError> {
        {
            let mut state = self.state.lock().unwrap();
            self.verify_writable(&state)?;

            let size = buffer.len() as u64;
            let updated_total = size + self.queued_byte_count(&state);
            if updated_total > self.hard_limit {
                return Err(ProcessError::new(
                    ProcessErrorCode::InputBufferOverflow,
                    format!(
                        "hard input buffer size limit of {} bytes exceeded",
                        self.hard_limit
                    ),
                ));
            }
            if updated_total > self.appsrc.max_bytes() {
                self.send_saturation();
            }
            state.queued_bytes += size;
            state.queue.push_back(buffer);
        }
        self.data_cond.notify_one();
        Ok(())
    }

    fn write_blocking(&self, mut buffer: Bytes) -> Result<(), ProcessError> {
        {
            let mut state = self.state.lock().unwrap();
            self.verify_writable(&state)?;
            self.check_cancelled(&state)?;

            let mut size = buffer.len() as u64;
            let queued = self.queued_byte_count(&state);
            if queued + size > self.appsrc.max_bytes() {
                self.send_saturation();
                loop {
                    let queued = self.queued_byte_count(&state);
                    let available = self.hard_limit.saturating_sub(queued);
                    if available >= size {
                        break;
                    }
                    if available > 0 {
                        let chunk = buffer.split_to(available as usize);
                        state.queued_bytes += chunk.len() as u64;
                        state.queue.push_back(chunk);
                        size -= available;
                        self.data_cond.notify_one();
                    }
                    state = self.space_cond.wait(state).unwrap();
                    self.check_cancelled(&state)?;
                }
            }
            state.queued_bytes += buffer.len() as u64;
            state.queue.push_back(buffer);
        }
        self.data_cond.notify_one();
        Ok(())
    }

    /// Marks the input stream as complete.  EOS is pushed into the pipeline
    /// once the queue drains.
    pub(crate) fn close(&self) -> Result<(), ProcessError> {
        {
            let mut state = self.state.lock().unwrap();
            if state.closed {
                return Err(ProcessError::new(
                    ProcessErrorCode::InputStreamClosed,
                    "can't close application source because application source is \
                     already closed",
                ));
            }
            state.closed = true;
        }
        self.data_cond.notify_one();
        Ok(())
    }

    /// Wakes up any waiter (blocking writes and the `need-data` callback) so
    /// teardown never hangs.
    pub(crate) fn cancel(&self) {
        {
            let mut state = self.state.lock().unwrap();
            state.cancelled = true;
        }
        self.data_cond.notify_all();
        self.space_cond.notify_all();
    }

    /// Detaches callbacks; called before dropping the pipeline.
    pub(crate) fn shutdown(&self) {
        self.appsrc
            .set_callbacks(gst_app::AppSrcCallbacks::builder().build());
    }

    fn verify_writable(&self, state: &SourceState) -> Result<(), ProcessError> {
        if state.closed {
            return Err(ProcessError::new(
                ProcessErrorCode::InputStreamClosed,
                "can't write to application source because application source is \
                 already closed",
            ));
        }
        Ok(())
    }

    fn check_cancelled(&self, state: &SourceState) -> Result<(), ProcessError> {
        if state.cancelled {
            return Err(ProcessError::internal("write operation cancelled"));
        }
        Ok(())
    }

    fn queued_byte_count(&self, state: &SourceState) -> u64 {
        state.queued_bytes + self.appsrc.current_level_bytes()
    }

    fn send_saturation(&self) {
        let _ = self.events.send(EngineEvent::Server(ServerMessage {
            stream_id: self.stream_id.clone(),
            data: ServerMessageData::DataSaturation,
        }));
    }

    /// The `need-data` callback.  Runs on a GStreamer thread and may block
    /// waiting for client data, mirroring the original implementation.
    fn process_need_data(&self) {
        let result = self.process_need_data_inner();
        if let Err(error) = result {
            let _ = self.events.send(EngineEvent::Error(error));
        }
        self.space_cond.notify_one();
    }

    fn process_need_data_inner(&self) -> Result<(), ProcessError> {
        let mut state = self.state.lock().unwrap();

        let send_eos = loop {
            if state.cancelled {
                return Ok(());
            }
            if state.queued_bytes > 0 {
                break false;
            }
            if state.closed {
                break true;
            }
            let _ = self.events.send(EngineEvent::Server(ServerMessage {
                stream_id: self.stream_id.clone(),
                data: ServerMessageData::DataStarvation,
            }));
            state = self.data_cond.wait(state).unwrap();
        };

        if !send_eos {
            let space = self
                .appsrc
                .max_bytes()
                .saturating_sub(self.appsrc.current_level_bytes());
            let available = state.queued_bytes;
            if space < available {
                if space > 0 {
                    self.push_bytes(&mut state, space)?;
                }
                return Ok(());
            }
            self.push_bytes(&mut state, available)?;
            if !state.closed {
                return Ok(());
            }
        }

        drop(state);
        self.appsrc.end_of_stream().map_err(|e| {
            ProcessError::internal(format!("appsrc end_of_stream(): failed to push EOS: {e}"))
        })?;
        Ok(())
    }

    fn push_bytes(&self, state: &mut SourceState, count: u64) -> Result<(), ProcessError> {
        // Assemble exactly `count` bytes from the queued chunks.
        let mut data = Vec::with_capacity(count as usize);
        let mut remaining = count as usize;
        while remaining > 0 {
            let Some(mut chunk) = state.queue.pop_front() else {
                return Err(ProcessError::internal(
                    "queued byte accounting is out of sync with the queue contents",
                ));
            };
            if chunk.len() <= remaining {
                remaining -= chunk.len();
                data.extend_from_slice(&chunk);
            } else {
                let taken = chunk.split_to(remaining);
                data.extend_from_slice(&taken);
                remaining = 0;
                state.queue.push_front(chunk);
            }
        }
        state.queued_bytes -= count;

        let offset = state.offset;
        let next_offset = offset + count;

        let mut buffer = gst::Buffer::from_mut_slice(data);
        {
            let buffer_ref = buffer.get_mut().ok_or_else(|| {
                ProcessError::internal("freshly created buffer is unexpectedly shared")
            })?;
            buffer_ref.set_offset(offset);
            buffer_ref.set_offset_end(next_offset);
            if !transonic_meta::add_custom_audio_meta(
                buffer_ref,
                CustomAudioMeta {
                    silence: false,
                    input_stream_offset: offset as i64,
                },
            ) {
                return Err(ProcessError::internal(
                    "failed to add custom audio metadata to input buffer",
                ));
            }
        }

        self.appsrc.push_buffer(buffer).map_err(|e| {
            ProcessError::internal(format!(
                "appsrc push_buffer(): failed to push buffer: {e:?}"
            ))
        })?;
        state.offset = next_offset;
        Ok(())
    }
}
