//! Custom per-buffer audio metadata.
//!
//! This is the Rust equivalent of the original `customaudiometa.c` GStreamer
//! meta.  It carries two pieces of information alongside each buffer:
//!
//! * `silence` — whether the buffer is known to contain only silence
//!   (populated by the `smartvadstartsilencetrimmer` element).
//! * `input-stream-offset` — the byte offset in the *input* stream that this
//!   buffer corresponds to (populated by the app source when data is pushed
//!   into the pipeline).
//!
//! The metadata is implemented on top of `GstCustomMeta` (available since
//! GStreamer 1.20), which stores its payload in a `GstStructure` and is
//! automatically copied across buffer transforms.

use gstreamer as gst;

/// The registered name of the custom audio meta.
pub const CUSTOM_AUDIO_META_NAME: &str = "TransonicCustomAudioMeta";

const FIELD_SILENCE: &str = "silence";
const FIELD_INPUT_STREAM_OFFSET: &str = "input-stream-offset";

/// A plain-data view of the custom audio meta attached to a buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CustomAudioMeta {
    /// Whether the buffer contains only silence.
    pub silence: bool,

    /// The input stream byte offset associated with the buffer, or -1 when
    /// unknown (mirrors the original C implementation's default).
    pub input_stream_offset: i64,
}

impl Default for CustomAudioMeta {
    fn default() -> Self {
        CustomAudioMeta {
            silence: false,
            input_stream_offset: -1,
        }
    }
}

/// Registers the custom audio meta type.  Must be called once after
/// `gst::init()` and before any buffer carrying the meta is created.
/// Registration is idempotent.
pub fn register() {
    if !gst::meta::CustomMeta::is_registered(CUSTOM_AUDIO_META_NAME) {
        gst::meta::CustomMeta::register(CUSTOM_AUDIO_META_NAME, &["audio"]);
    }
}

/// Attaches (or updates) the custom audio meta on the given buffer.
///
/// Returns `false` when the meta could not be attached (e.g. the buffer is
/// not writable), mirroring the defensive behavior of the C original.
pub fn add_custom_audio_meta(buffer: &mut gst::BufferRef, meta: CustomAudioMeta) -> bool {
    // Update in place if the meta is already present (the original
    // `add_or_update_custom_audio_metadata` helper does the same).
    if let Ok(mut existing) = gst::meta::CustomMeta::from_mut_buffer(buffer, CUSTOM_AUDIO_META_NAME)
    {
        let structure = existing.mut_structure();
        structure.set(FIELD_SILENCE, meta.silence);
        structure.set(FIELD_INPUT_STREAM_OFFSET, meta.input_stream_offset);
        return true;
    }

    match gst::meta::CustomMeta::add(buffer, CUSTOM_AUDIO_META_NAME) {
        Ok(mut custom) => {
            let structure = custom.mut_structure();
            structure.set(FIELD_SILENCE, meta.silence);
            structure.set(FIELD_INPUT_STREAM_OFFSET, meta.input_stream_offset);
            true
        }
        Err(_) => false,
    }
}

/// Reads the custom audio meta from the given buffer, if present.
pub fn get_custom_audio_meta(buffer: &gst::BufferRef) -> Option<CustomAudioMeta> {
    let meta = gst::meta::CustomMeta::from_buffer(buffer, CUSTOM_AUDIO_META_NAME).ok()?;
    let structure = meta.structure();
    Some(CustomAudioMeta {
        silence: structure.get(FIELD_SILENCE).unwrap_or(false),
        input_stream_offset: structure.get(FIELD_INPUT_STREAM_OFFSET).unwrap_or(-1),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init() {
        static INIT: std::sync::Once = std::sync::Once::new();
        INIT.call_once(|| {
            gst::init().expect("gstreamer initialization");
            register();
        });
    }

    #[test]
    fn missing_meta_reads_as_none() {
        init();
        let buffer = gst::Buffer::with_size(16).unwrap();
        assert_eq!(get_custom_audio_meta(&buffer), None);
    }

    #[test]
    fn add_and_read_roundtrip() {
        init();
        let mut buffer = gst::Buffer::with_size(16).unwrap();
        let meta = CustomAudioMeta {
            silence: true,
            input_stream_offset: 4242,
        };
        assert!(add_custom_audio_meta(buffer.get_mut().unwrap(), meta));
        assert_eq!(get_custom_audio_meta(&buffer), Some(meta));
    }

    #[test]
    fn add_updates_existing_meta_in_place() {
        init();
        let mut buffer = gst::Buffer::with_size(16).unwrap();
        let buffer_ref = buffer.get_mut().unwrap();
        assert!(add_custom_audio_meta(
            buffer_ref,
            CustomAudioMeta {
                silence: true,
                input_stream_offset: 1,
            }
        ));
        assert!(add_custom_audio_meta(
            buffer_ref,
            CustomAudioMeta {
                silence: false,
                input_stream_offset: 2,
            }
        ));
        assert_eq!(
            get_custom_audio_meta(buffer_ref),
            Some(CustomAudioMeta {
                silence: false,
                input_stream_offset: 2,
            })
        );
    }

    #[test]
    fn meta_survives_buffer_copies() {
        init();
        let mut buffer = gst::Buffer::with_size(16).unwrap();
        let meta = CustomAudioMeta {
            silence: true,
            input_stream_offset: 7,
        };
        assert!(add_custom_audio_meta(buffer.get_mut().unwrap(), meta));
        let copy = buffer.copy_deep().unwrap();
        assert_eq!(get_custom_audio_meta(&copy), Some(meta));
    }
}
