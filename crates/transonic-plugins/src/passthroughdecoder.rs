//! `passthroughdecoder` element.
//!
//! !!!WARNING!!! (carried over from the original)
//! This element is only meant for use in transitional periods; it is not
//! meant as a permanent solution.  It passes audio data through as-is,
//! presenting it as `audio/x-raw, format=S16LE, rate=16000, channels=1`.
//!
//! A typefinder is also registered that suggests `audio/passthrough` with
//! LIKELY probability for any content, so `decodebin` can route unknown
//! payloads through this element.

use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer::subclass::prelude::*;
use gstreamer_audio as gst_audio;
use gstreamer_audio::prelude::*;
use gstreamer_audio::subclass::prelude::*;

use std::sync::LazyLock;

const DEFAULT_SAMPLE_RATE: u32 = 16_000;

mod imp {
    use super::*;

    #[derive(Default)]
    pub struct PassthroughDecoder;

    #[glib::object_subclass]
    impl ObjectSubclass for PassthroughDecoder {
        const NAME: &'static str = "TransonicPassthroughDecoder";
        type Type = super::PassthroughDecoder;
        type ParentType = gst_audio::AudioDecoder;
    }

    impl ObjectImpl for PassthroughDecoder {
        fn constructed(&self) {
            self.parent_constructed();
            let obj = self.obj();
            obj.set_needs_format(true);
            obj.set_use_default_pad_acceptcaps(true);
            // Accept anything matching the pad template (the original set
            // GST_PAD_SET_ACCEPT_TEMPLATE on the sink pad).
            obj.sink_pad().set_pad_flags(gst::PadFlags::ACCEPT_TEMPLATE);
        }
    }

    impl GstObjectImpl for PassthroughDecoder {}

    impl ElementImpl for PassthroughDecoder {
        fn metadata() -> Option<&'static gst::subclass::ElementMetadata> {
            static METADATA: LazyLock<gst::subclass::ElementMetadata> = LazyLock::new(|| {
                gst::subclass::ElementMetadata::new(
                    "Passthrough Decoder",
                    "Decoder/Audio",
                    "Passes through data as is.",
                    "Transonic",
                )
            });
            Some(&METADATA)
        }

        fn pad_templates() -> &'static [gst::PadTemplate] {
            static TEMPLATES: LazyLock<Vec<gst::PadTemplate>> = LazyLock::new(|| {
                let sink_caps = gst::Caps::builder("audio/passthrough").build();
                let src_caps = gst_audio::AudioCapsBuilder::new_interleaved()
                    .format(gst_audio::AudioFormat::S16le)
                    .rate(DEFAULT_SAMPLE_RATE as i32)
                    .channels(1)
                    .build();
                vec![
                    gst::PadTemplate::new(
                        "sink",
                        gst::PadDirection::Sink,
                        gst::PadPresence::Always,
                        &sink_caps,
                    )
                    .unwrap(),
                    gst::PadTemplate::new(
                        "src",
                        gst::PadDirection::Src,
                        gst::PadPresence::Always,
                        &src_caps,
                    )
                    .unwrap(),
                ]
            });
            TEMPLATES.as_ref()
        }
    }

    impl AudioDecoderImpl for PassthroughDecoder {
        fn start(&self) -> Result<(), gst::ErrorMessage> {
            // Allow the base class to perform byte-to-time estimated
            // conversion.
            self.obj().set_estimate_rate(true);
            Ok(())
        }

        fn stop(&self) -> Result<(), gst::ErrorMessage> {
            Ok(())
        }

        fn set_format(&self, _caps: &gst::Caps) -> Result<(), gst::LoggableError> {
            let info = gst_audio::AudioInfo::builder(
                gst_audio::AudioFormat::S16le,
                DEFAULT_SAMPLE_RATE,
                1,
            )
            .build()
            .map_err(|_| gst::loggable_error!(CAT, "failed to build audio info"))?;
            self.obj()
                .set_output_format(&info)
                .map_err(|_| gst::loggable_error!(CAT, "failed to set output format"))?;
            Ok(())
        }

        fn handle_frame(
            &self,
            buffer: Option<&gst::Buffer>,
        ) -> Result<gst::FlowSuccess, gst::FlowError> {
            let Some(buffer) = buffer else {
                return Ok(gst::FlowSuccess::Ok);
            };

            let map = buffer.map_readable().map_err(|_| gst::FlowError::Error)?;
            let mut out = gst::Buffer::with_size(map.len()).map_err(|_| gst::FlowError::Error)?;
            {
                let out_ref = out.get_mut().ok_or(gst::FlowError::Error)?;
                let mut out_map = out_ref.map_writable().map_err(|_| gst::FlowError::Error)?;
                out_map.copy_from_slice(map.as_slice());
            }
            drop(map);

            self.obj().finish_frame(Some(out), 1)
        }
    }

    pub static CAT: LazyLock<gst::DebugCategory> = LazyLock::new(|| {
        gst::DebugCategory::new(
            "passthroughdecoder",
            gst::DebugColorFlags::empty(),
            Some("Passthrough decoder"),
        )
    });
}

glib::wrapper! {
    pub struct PassthroughDecoder(ObjectSubclass<imp::PassthroughDecoder>)
        @extends gst_audio::AudioDecoder, gst::Element, gst::Object;
}

pub fn register(plugin: &gst::Plugin) -> Result<(), glib::BoolError> {
    let caps = gst::Caps::builder("audio/passthrough").build();
    // NOTE: probability is LIKELY (80%) to sit below CERTAIN/MAXIMUM
    // typefinders but above plugins that report 50% (some video plugins do).
    gst::TypeFind::register(
        Some(plugin),
        "audio/passthrough",
        gst::Rank::PRIMARY,
        None,
        Some(&caps.clone()),
        move |typefind| {
            typefind.suggest(gst::TypeFindProbability::Likely, &caps);
        },
    )?;

    gst::Element::register(
        Some(plugin),
        "passthroughdecoder",
        gst::Rank::PRIMARY,
        PassthroughDecoder::static_type(),
    )
}
