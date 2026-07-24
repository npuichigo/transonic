//! Transonic core: the piped transform engine.
//!
//! It contains:
//!
//! * [`template`] — the `$(pipeline_parameters.key,default)` pipeline
//!   template language.
//! * [`config`] — service configuration loading (JSON documents matching the
//!   `transonic.v1.ServiceConfiguration` schema).
//! * [`job`] — validated job descriptions built from `TransformConfig`
//!   messages.
//! * [`messages`] — the transport-agnostic client/server message model.
//! * [`engine`] — the processor that builds, runs and supervises GStreamer
//!   pipelines fed and drained through connection halves.

pub mod config;
pub mod engine;
pub mod error;
pub mod job;
pub mod messages;
pub mod template;

pub use config::{PipelineConfig, RootConfig};
pub use engine::{ConnectionReader, ConnectionWriter, EngineError, Processor};
pub use error::{ProcessError, ProcessErrorCode};
pub use job::{Job, JobId, JobInput, JobOutput, JobParams};
pub use messages::{ClientMessage, ServerMessage};

/// Initializes GStreamer and registers the custom audio meta.  Must be
/// called once before using the engine.
pub fn init() -> Result<(), gstreamer::glib::Error> {
    gstreamer::init()?;
    transonic_meta::register();
    Ok(())
}
