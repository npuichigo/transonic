//! The piped transform processor.
//!
//! Transforms media via GStreamer pipelines, with data pushed in and pulled
//! out through connection halves supplied by the transport layer (gRPC).
//!
//! For each job the processor:
//!
//! 1. renders the pipeline description from the configured bin templates and
//!    the job's channel parameters,
//! 2. parses and configures the pipeline (app sources, app sinks, file
//!    sinks, buffered elements),
//! 3. starts the pipeline and applies the requested processing rate,
//! 4. concurrently pumps client messages into the sources, pipeline events
//!    from the bus, and output/flow-control messages back to the client,
//! 5. stops the pipeline and, for non-streamable outputs, drains the file
//!    sinks back to the client.

mod sink;
mod source;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;
use tokio::sync::mpsc;
use tracing::{debug, info};

use crate::config::RootConfig;
use crate::error::{ProcessError, ProcessErrorCode};
use crate::job::{Job, JobParams};
use crate::messages::{
    ClientMessage, ClientMessageData, MediaProperties, OutputBuffer, ServerMessage,
    ServerMessageData,
};

use sink::AppSinkProcessor;
use source::AppSource;

///////////////////////////////////////////////////////////////////////////
// engine events
///////////////////////////////////////////////////////////////////////////

/// Internal events produced by pipeline callbacks and the bus monitor.
pub(crate) enum EngineEvent {
    /// A message that should be forwarded to the client.
    Server(ServerMessage),
    /// The pipeline reached end-of-stream.
    PipelineEos,
    /// One application sink reached end-of-stream.
    SinkEos(String),
    /// An error occurred inside a pipeline callback.
    Error(ProcessError),
}

pub(crate) type EventSender = mpsc::UnboundedSender<EngineEvent>;

///////////////////////////////////////////////////////////////////////////
// errors and connection traits
///////////////////////////////////////////////////////////////////////////

/// Error returned by [`Processor::process`] and the connection halves.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// A processing error with a termination-reason mapping.
    #[error(transparent)]
    Process(#[from] ProcessError),
    /// The connection was cancelled by the peer.
    #[error("operation cancelled by peer")]
    Cancelled,
}

/// The read half of a transform connection.
#[async_trait]
pub trait ConnectionReader: Send {
    /// Reads the next client message.  Returns `None` once the client has
    /// finished sending messages.
    async fn read(&mut self) -> Result<Option<ClientMessage>, EngineError>;
}

/// The write half of a transform connection.
#[async_trait]
pub trait ConnectionWriter: Send {
    /// Writes a message to the client.
    async fn write(&mut self, message: ServerMessage) -> Result<(), EngineError>;
}

///////////////////////////////////////////////////////////////////////////
// processor
///////////////////////////////////////////////////////////////////////////

/// Transforms media via GStreamer pipelines.
pub struct Processor {
    config: RootConfig,
    temp_dir: PathBuf,
}

struct FileSinkDrain {
    stream_id: String,
    // Held so the temporary file is removed when the job completes.
    file: tempfile::NamedTempFile,
}

struct RunContext {
    sources: HashMap<String, Arc<AppSource>>,
    sinks: Vec<Arc<AppSinkProcessor>>,
    file_sinks: Vec<FileSinkDrain>,
}

impl Processor {
    /// Creates a processor from the given configuration.  `temp_dir` is used
    /// for file-sink temporary storage.
    pub fn new(config: RootConfig, temp_dir: PathBuf) -> Self {
        Processor { config, temp_dir }
    }

    /// Returns the processor configuration.
    pub fn config(&self) -> &RootConfig {
        &self.config
    }

    /// Processes a job, reading input from `reader` and sending output to
    /// `writer`.
    pub async fn process<R, W>(
        &self,
        job: &Job,
        reader: &mut R,
        writer: &mut W,
    ) -> Result<(), EngineError>
    where
        R: ConnectionReader,
        W: ConnectionWriter,
    {
        let params = JobParams::deduce(&self.config, job)?;

        let pipeline_config = self.config.pipelines.get(&job.pipeline_id).ok_or_else(|| {
            ProcessError::new(
                ProcessErrorCode::PipelineIdInvalid,
                format!(
                    "identifier '{}' is not associated with a pipeline",
                    job.pipeline_id
                ),
            )
        })?;

        // Render the pipeline description.  Sinks first, then sources,
        // matching the original description layout.
        let mut description = String::new();
        let mut sink_init: HashMap<String, String> = HashMap::new(); // name -> stream id
        let mut file_sink_init: HashMap<String, String> = HashMap::new(); // name -> stream id
        for (i, output) in job.outputs.iter().enumerate() {
            let bin = pipeline_config
                .output_bins
                .get(&output.bin_id)
                .ok_or_else(|| {
                    ProcessError::new(
                        ProcessErrorCode::OutputBinIdInvalid,
                        format!(
                            "pipeline doesn't contain an output bin with id '{}'",
                            output.bin_id
                        ),
                    )
                })?;
            let fragment = bin
                .spec
                .apply(&output.params)
                .map_err(|e| ProcessError::job_param(e.to_string()))?;
            let sink_id = format!("sink_{i}");
            if params.streaming_enabled && bin.streamable {
                description.push_str(&format!(" {fragment} ! appsink name={sink_id}"));
                sink_init.insert(sink_id, output.stream_id.clone());
            } else {
                description.push_str(&format!(" {fragment} ! filesink name={sink_id}"));
                file_sink_init.insert(sink_id, output.stream_id.clone());
            }
        }

        let mut source_init: HashMap<String, (String, Option<String>)> = HashMap::new();
        for (i, input) in job.inputs.iter().enumerate() {
            let bin = pipeline_config
                .input_bins
                .get(&input.bin_id)
                .ok_or_else(|| {
                    ProcessError::new(
                        ProcessErrorCode::InputBinIdInvalid,
                        format!(
                            "pipeline doesn't contain an input bin with id '{}'",
                            input.bin_id
                        ),
                    )
                })?;
            let fragment = bin
                .spec
                .apply(&input.params)
                .map_err(|e| ProcessError::job_param(e.to_string()))?;
            let source_id = format!("src_{i}");
            description.push_str(&format!(" appsrc name={source_id} ! {fragment}"));
            source_init.insert(
                source_id,
                (input.stream_id.clone(), input.extension.clone()),
            );
        }

        info!(
            job = %job.id,
            spec = %description,
            input_buffer_size = params.input_buffer_size,
            max_media_duration = params.max_media_duration,
            output_buffer_duration = params.output_buffer_duration,
            output_buffer_size = params.output_buffer_size,
            overflow_termination_enabled = params.overflow_termination_enabled,
            read_timeout = params.read_timeout,
            realtime_rate = params.realtime_rate,
            streaming_enabled = params.streaming_enabled,
            "pipeline spec generated"
        );

        // Parse the pipeline on a blocking thread (pipeline construction can
        // be CPU intensive).
        let parse_description = description.clone();
        let pipeline_element =
            tokio::task::spawn_blocking(move || gst::parse::launch(&parse_description))
                .await
                .map_err(|e| {
                    ProcessError::internal(format!("pipeline construction panicked: {e}"))
                })?
                .map_err(|e| {
                    ProcessError::new(
                        ProcessErrorCode::PipelineParseFailure,
                        format!("gst::parse::launch(): {e}"),
                    )
                })?;
        let pipeline = pipeline_element
            .downcast::<gst::Pipeline>()
            .map_err(|_| ProcessError::internal("parsed pipeline is not a GstPipeline"))?;

        debug!(job = %job.id, "pipeline created");

        // Emit plugin-selection observability, replacing the original
        // decodebin `autoplug-select` statistics hook.
        pipeline.connect_deep_element_added(|_, _, element| {
            if let Some(factory) = element.factory() {
                debug!(
                    element = %element.name(),
                    factory = %factory.name(),
                    plugin = factory
                        .plugin()
                        .map(|p| p.plugin_name().to_string())
                        .unwrap_or_default(),
                    "pipeline element added"
                );
            }
        });

        // Configure the elements contained in the pipeline.
        let (events_tx, mut events_rx) = mpsc::unbounded_channel::<EngineEvent>();
        let mut run_context = self.configure_pipeline_elements(
            &pipeline,
            &params,
            &events_tx,
            &mut sink_init,
            &mut file_sink_init,
            &mut source_init,
        )?;

        debug!(job = %job.id, "pipeline configured");

        // Start the pipeline.
        let start_pipeline = pipeline.clone();
        tokio::task::spawn_blocking(move || start_pipeline.set_state(gst::State::Playing))
            .await
            .map_err(|e| ProcessError::internal(format!("pipeline start panicked: {e}")))?
            .map_err(|e| {
                ProcessError::internal(format!("failed to change pipeline state to PLAYING: {e}"))
            })?;

        debug!(job = %job.id, "pipeline started");

        let run_result = self
            .run_pipeline(
                job,
                &params,
                &pipeline,
                &mut run_context,
                reader,
                writer,
                events_tx.clone(),
                &mut events_rx,
            )
            .await;

        // In the case of an error, it's possible application sources are
        // still open.  Make sure they're closed (and any blocked writers are
        // released) before stopping the pipeline, since the stop operation
        // would hang otherwise.
        for source in run_context.sources.values() {
            source.cancel();
            if !source.closed() {
                let _ = source.close();
            }
        }

        // Stop the pipeline.
        let stop_pipeline = pipeline.clone();
        let stop_result =
            tokio::task::spawn_blocking(move || match stop_pipeline.set_state(gst::State::Null) {
                Ok(gst::StateChangeSuccess::Async) => {
                    let (result, _, _) = stop_pipeline.state(gst::ClockTime::NONE);
                    result.map(|_| ()).map_err(|e| {
                        ProcessError::internal(format!("failed to sync pipeline state: {e}"))
                    })
                }
                Ok(_) => Ok(()),
                Err(e) => Err(ProcessError::internal(format!(
                    "failed to change pipeline state to NULL: {e}"
                ))),
            })
            .await
            .map_err(|e| ProcessError::internal(format!("pipeline stop panicked: {e}")))?;

        // Detach callbacks so no stale events fire while tearing down.
        for source in run_context.sources.values() {
            source.shutdown();
        }
        for sink in &run_context.sinks {
            sink.shutdown();
        }

        debug!(job = %job.id, "pipeline stopped");

        run_result?;
        stop_result?;

        // Drain the file sinks.
        //
        // XXX: The media duration limit doesn't apply to file sinks because
        // there's no view into how much data is written to the file (carried
        // over from the original implementation).
        for drain in &run_context.file_sinks {
            debug!(job = %job.id, stream = %drain.stream_id, "file sink opened");
            let mut file = tokio::fs::File::open(drain.file.path())
                .await
                .map_err(|e| {
                    ProcessError::internal(format!("failed to open file sink output: {e}"))
                })?;
            let mut buffer = vec![0u8; params.output_buffer_size as usize];
            loop {
                use tokio::io::AsyncReadExt;
                let count = file.read(&mut buffer).await.map_err(|e| {
                    ProcessError::internal(format!("failed to read file sink output: {e}"))
                })?;
                if count == 0 {
                    break;
                }
                writer
                    .write(ServerMessage {
                        stream_id: drain.stream_id.clone(),
                        data: ServerMessageData::OutputBuffer(OutputBuffer {
                            buffer: Bytes::copy_from_slice(&buffer[..count]),
                            chunk_duration_ms: None,
                            chunk_offset_ms: None,
                            input_stream_offset: None,
                            silence: None,
                        }),
                    })
                    .await?;
            }
            debug!(job = %job.id, stream = %drain.stream_id, "file sink closed");
        }

        Ok(())
    }

    fn configure_pipeline_elements(
        &self,
        pipeline: &gst::Pipeline,
        params: &JobParams,
        events_tx: &EventSender,
        sink_init: &mut HashMap<String, String>,
        file_sink_init: &mut HashMap<String, String>,
        source_init: &mut HashMap<String, (String, Option<String>)>,
    ) -> Result<RunContext, ProcessError> {
        let mut context = RunContext {
            sources: HashMap::new(),
            sinks: Vec::new(),
            file_sinks: Vec::new(),
        };

        for element in pipeline.iterate_recurse() {
            let element = match element {
                Ok(element) => element,
                Err(gst::IteratorError::Resync) => {
                    return Err(ProcessError::internal(
                        "pipeline element iterator resynced unexpectedly",
                    ));
                }
                Err(gst::IteratorError::Error) => {
                    return Err(ProcessError::internal("pipeline element iterator error"));
                }
            };
            let name = element.name().to_string();

            if let Ok(appsrc) = element.clone().dynamic_cast::<gst_app::AppSrc>() {
                let (stream_id, extension) = source_init.remove(&name).ok_or_else(|| {
                    ProcessError::internal(format!(
                        "pipeline contains unexpected application source with name '{name}'"
                    ))
                })?;
                let source = AppSource::new(
                    appsrc,
                    stream_id.clone(),
                    extension.as_deref(),
                    params.input_buffer_size,
                    params.overflow_termination_enabled,
                    events_tx.clone(),
                )?;
                context.sources.insert(stream_id, source);
            } else if let Ok(appsink) = element.clone().dynamic_cast::<gst_app::AppSink>() {
                let stream_id = sink_init.remove(&name).ok_or_else(|| {
                    ProcessError::internal(format!(
                        "pipeline contains unexpected application sink with name '{name}'"
                    ))
                })?;
                let sink = AppSinkProcessor::new(
                    appsink,
                    name,
                    stream_id,
                    params.max_media_duration,
                    params.output_buffer_size,
                    params.output_buffer_duration,
                    events_tx.clone(),
                );
                context.sinks.push(sink);
            } else if let Some(factory) = element.factory() {
                match factory.name().as_str() {
                    "filesink" => {
                        let stream_id = file_sink_init.remove(&name).ok_or_else(|| {
                            ProcessError::internal(format!(
                                "pipeline contains unexpected file sink with name '{name}'"
                            ))
                        })?;
                        let file =
                            tempfile::NamedTempFile::new_in(&self.temp_dir).map_err(|e| {
                                ProcessError::internal(format!(
                                    "failed to create temporary file for file sink: {e}"
                                ))
                            })?;
                        element.set_property(
                            "location",
                            file.path().to_str().ok_or_else(|| {
                                ProcessError::internal("temporary file path is not valid UTF-8")
                            })?,
                        );
                        context.file_sinks.push(FileSinkDrain { stream_id, file });
                    }
                    "queue" => {
                        element.set_property("max-size-bytes", params.input_buffer_size);
                    }
                    "decodebin" => {
                        element.set_property("max-size-bytes", params.input_buffer_size);
                    }
                    _ => {}
                }
            }
        }

        if !(sink_init.is_empty() && file_sink_init.is_empty() && source_init.is_empty()) {
            return Err(ProcessError::internal(
                "one or more configured sources/sinks were not found in the pipeline",
            ));
        }
        Ok(context)
    }

    #[allow(clippy::too_many_arguments)]
    async fn run_pipeline<R, W>(
        &self,
        job: &Job,
        params: &JobParams,
        pipeline: &gst::Pipeline,
        run_context: &mut RunContext,
        reader: &mut R,
        writer: &mut W,
        events_tx: EventSender,
        events_rx: &mut mpsc::UnboundedReceiver<EngineEvent>,
    ) -> Result<(), EngineError>
    where
        R: ConnectionReader,
        W: ConnectionWriter,
    {
        // Set the processing rate of the pipeline.  This needs to be done
        // after the pipeline is started so that our step event isn't
        // overridden by the initial step event sent by the pipeline.
        let realtime_rate = params.realtime_rate;
        if realtime_rate == 0.0 {
            pipeline.use_clock(Option::<&gst::Clock>::None);
        } else {
            let event = gst::event::Step::new(
                gst::format::Percent::from_percent(100),
                realtime_rate,
                false,
                false,
            );
            if !pipeline.send_event(event) {
                return Err(ProcessError::internal("pipeline failed to handle step event").into());
            }
        }

        let sources_closed = AtomicBool::new(false);

        let events_fut = Self::process_events(pipeline, &run_context.sinks, events_tx);
        let reads_fut = Self::process_reads(params, &run_context.sources, reader, &sources_closed);
        let writes_fut = Self::process_writes(job, &run_context.sinks, writer, events_rx);

        // The three loops run concurrently; the first failure cancels the
        // rest (mirroring the original `&&` awaitable composition).
        //
        // NOTE: `events_fut` produces bus-driven events that are consumed by
        // `writes_fut` through the shared events channel; the borrow is
        // shaped so only `writes_fut` receives from the channel.
        let result = tokio::try_join!(events_fut, reads_fut, writes_fut);
        result.map(|_| ())
    }

    /// Monitors the pipeline bus: raises pipeline errors, reports media
    /// properties, and signals end-of-stream.
    async fn process_events(
        pipeline: &gst::Pipeline,
        sinks: &[Arc<AppSinkProcessor>],
        events_tx: EventSender,
    ) -> Result<(), EngineError> {
        let bus = pipeline
            .bus()
            .ok_or_else(|| ProcessError::internal("pipeline has no bus"))?;
        let mut stream = bus.stream_filtered(&[
            gst::MessageType::Eos,
            gst::MessageType::Error,
            gst::MessageType::StateChanged,
        ]);

        while let Some(message) = stream.next().await {
            match message.view() {
                gst::MessageView::Eos(_) => {
                    let _ = events_tx.send(EngineEvent::PipelineEos);
                    return Ok(());
                }
                gst::MessageView::Error(err) => {
                    return Err(map_gst_error(&err.error(), err.debug()).into());
                }
                gst::MessageView::StateChanged(state) => {
                    let Some(source) = state.src() else { continue };
                    let Some(element) = source.downcast_ref::<gst::Element>() else {
                        continue;
                    };
                    let name = element.name();
                    let Some(sink) = sinks.iter().find(|s| s.name() == name.as_str()) else {
                        continue;
                    };
                    let Some(pad) = element.static_pad("sink") else {
                        continue;
                    };
                    let caps = pad.current_caps().or_else(|| Some(pad.query_caps(None)));
                    let Some(caps) = caps else { continue };
                    if caps.is_any() || caps.is_empty() || caps.size() == 0 {
                        continue;
                    }
                    let structure = caps.structure(0).expect("checked caps size above");
                    let properties = MediaProperties {
                        format: structure.get::<&str>("format").ok().map(str::to_owned),
                        encoding: Some(structure.name().to_string()),
                        sample_rate: structure.get::<i32>("rate").ok().map(|v| v as u32),
                        channel_count: structure.get::<i32>("channels").ok().map(|v| v as u32),
                    };
                    let _ = events_tx.send(EngineEvent::Server(ServerMessage {
                        stream_id: sink.stream_id().to_owned(),
                        data: ServerMessageData::MediaProperties(properties),
                    }));
                }
                _ => {}
            }
        }
        Ok(())
    }

    /// Reads client messages and feeds the application sources.
    async fn process_reads<R>(
        params: &JobParams,
        sources: &HashMap<String, Arc<AppSource>>,
        reader: &mut R,
        sources_closed: &AtomicBool,
    ) -> Result<(), EngineError>
    where
        R: ConnectionReader,
    {
        let read_timeout = Duration::from_millis(u64::from(params.read_timeout));
        loop {
            let message = if read_timeout.is_zero() {
                reader.read().await?
            } else {
                match tokio::time::timeout(read_timeout, reader.read()).await {
                    Ok(result) => result?,
                    Err(_) => {
                        return Err(ProcessError::new(
                            ProcessErrorCode::ReadTimeoutExceeded,
                            format!(
                                "connection read operation time exceeded read timeout \
                                 threshold of {} milliseconds",
                                read_timeout.as_millis()
                            ),
                        )
                        .into());
                    }
                }
            };
            let Some(message) = message else { break };

            let source = sources.get(&message.stream_id).ok_or_else(|| {
                ProcessError::new(
                    ProcessErrorCode::InputStreamIdInvalid,
                    format!("'{}' is not a valid stream identifier", message.stream_id),
                )
            })?;
            match message.data {
                ClientMessageData::InputBuffers(buffers) => {
                    for buffer in buffers {
                        let source = Arc::clone(source);
                        tokio::task::spawn_blocking(move || source.write(buffer))
                            .await
                            .map_err(|e| {
                                ProcessError::internal(format!("source write panicked: {e}"))
                            })??;
                    }
                }
                ClientMessageData::Close => {
                    source.close()?;
                }
            }
        }

        // The client is done sending; close any remaining sources so the
        // pipeline can reach EOS.
        for source in sources.values() {
            if !source.closed() {
                source.close()?;
            }
        }
        sources_closed.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Forwards engine events to the client and tracks completion.
    async fn process_writes<W>(
        job: &Job,
        sinks: &[Arc<AppSinkProcessor>],
        writer: &mut W,
        events_rx: &mut mpsc::UnboundedReceiver<EngineEvent>,
    ) -> Result<(), EngineError>
    where
        W: ConnectionWriter,
    {
        let expected_sinks = sinks.len();
        let mut finished_sinks: HashSet<String> = HashSet::new();
        let mut pipeline_eos = false;

        while let Some(event) = events_rx.recv().await {
            match event {
                EngineEvent::Server(message) => {
                    writer.write(message).await?;
                }
                EngineEvent::Error(error) => return Err(error.into()),
                EngineEvent::PipelineEos => {
                    pipeline_eos = true;
                    if finished_sinks.len() == expected_sinks {
                        break;
                    }
                }
                EngineEvent::SinkEos(name) => {
                    if !finished_sinks.insert(name.clone()) {
                        return Err(ProcessError::internal(format!(
                            "application sink '{name}' marked EOS more than once"
                        ))
                        .into());
                    }
                    if pipeline_eos && finished_sinks.len() == expected_sinks {
                        break;
                    }
                }
            }
        }

        debug!(job = %job.id, "output streaming complete");
        Ok(())
    }
}

///////////////////////////////////////////////////////////////////////////
// error mapping
///////////////////////////////////////////////////////////////////////////

/// Maps a GStreamer bus error to a [`ProcessError`] using the same domain
/// and code mapping as the original implementation.
fn map_gst_error(error: &gst::glib::Error, debug: Option<gst::glib::GString>) -> ProcessError {
    use ProcessErrorCode as Code;

    let detail = match debug {
        Some(debug) => format!("{error} ({debug})"),
        None => error.to_string(),
    };

    if let Some(stream_error) = error.kind::<gst::StreamError>() {
        let code = match stream_error {
            gst::StreamError::CodecNotFound
            | gst::StreamError::Format
            | gst::StreamError::WrongType => Code::InputStreamTypeUnsupported,
            gst::StreamError::Decode => Code::DecoderFailure,
            gst::StreamError::Decrypt => Code::DecryptionUnsupported,
            gst::StreamError::DecryptNokey => Code::DecryptionKeyUnavailable,
            gst::StreamError::Demux => Code::DemuxerFailure,
            gst::StreamError::Encode => Code::EncoderFailure,
            gst::StreamError::Failed => Code::InputStreamFailure,
            gst::StreamError::Mux => Code::MuxerFailure,
            gst::StreamError::TypeNotFound => Code::InputStreamTypeUndetected,
            _ => Code::InternalFailure,
        };
        return ProcessError::new(code, detail);
    }

    if let Some(core_error) = error.kind::<gst::CoreError>() {
        if core_error == gst::CoreError::MissingPlugin {
            return ProcessError::new(Code::InputStreamTypeUnsupported, detail);
        }
    }

    ProcessError::internal(detail)
}
