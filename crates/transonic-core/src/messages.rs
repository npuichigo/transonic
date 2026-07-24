//! The transport-agnostic message model exchanged between the engine and a
//! connection (typically the gRPC layer).

use bytes::Bytes;

// Re-export bytes so downstream crates use a consistent version.
pub use bytes;

/// A message received from the client over a connection.
#[derive(Debug, Clone)]
pub struct ClientMessage {
    /// The input stream identifier the message refers to.
    pub stream_id: String,
    /// The message payload.
    pub data: ClientMessageData,
}

/// Payload variants of a [`ClientMessage`].
#[derive(Debug, Clone)]
pub enum ClientMessageData {
    /// Input stream data buffers.
    InputBuffers(Vec<Bytes>),
    /// All data has been sent for the input stream.
    Close,
}

/// Media properties inferred for an output stream.
#[derive(Debug, Clone, Default)]
pub struct MediaProperties {
    /// The sample format (e.g. `S16LE`), when known.
    pub format: Option<String>,
    /// The encoding (the caps structure name, e.g. `audio/x-raw`).
    pub encoding: Option<String>,
    /// The sample rate, when known.
    pub sample_rate: Option<u32>,
    /// The channel count, when known.
    pub channel_count: Option<u32>,
}

/// An output buffer with optional chunk metadata.
#[derive(Debug, Clone)]
pub struct OutputBuffer {
    /// The output data.
    pub buffer: Bytes,
    /// Duration covered by the buffer, in milliseconds.
    pub chunk_duration_ms: Option<u64>,
    /// Presentation offset of the buffer, in milliseconds.
    pub chunk_offset_ms: Option<u64>,
    /// The input stream byte offset corresponding to this buffer.
    pub input_stream_offset: Option<u64>,
    /// Whether the buffer contains only silence.
    pub silence: Option<bool>,
}

/// A message sent to the client over a connection.
#[derive(Debug, Clone)]
pub struct ServerMessage {
    /// The output stream identifier the message refers to.
    pub stream_id: String,
    /// The message payload.
    pub data: ServerMessageData,
}

/// Payload variants of a [`ServerMessage`].
#[derive(Debug, Clone)]
pub enum ServerMessageData {
    /// Output stream data.
    OutputBuffer(OutputBuffer),
    /// The input buffer soft limit has been exceeded (ENOUGH_DATA).
    DataSaturation,
    /// Significant space is available in the input buffer (NEED_DATA).
    DataStarvation,
    /// Media properties inferred for the output stream.
    MediaProperties(MediaProperties),
}
