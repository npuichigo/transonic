//! `smartvadstartsilencetrimmer` element.
//!
//! VAD-based variant of `startsilencecutter`.  In addition to detecting and
//! optionally trimming the leading silence, every buffer that leaves the
//! element is tagged with the custom audio meta's `silence` flag so that
//! downstream consumers (and the service's chunk metadata) know whether a
//! chunk contains only silence.
//!
//! Voice activity is detected with the energy-based detector from
//! [`crate::vad`]; swap that module out for a model-based VAD if higher
//! accuracy is required.

use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer::subclass::prelude::*;
use gstreamer_audio as gst_audio;

use std::sync::{LazyLock, Mutex};

use transonic_meta::CustomAudioMeta;

use crate::prebuffer::PreBuffer;
use crate::vad::EnergyVad;

const DEFAULT_THRESHOLD_LEVEL: f64 = 0.1;
const DEFAULT_PRE_LENGTH_NS: u64 = 200 * 1_000_000;

fn mark_silence(buffer: &mut gst::Buffer, has_only_silence: bool) {
    // Only writable buffers can carry updated metadata, mirroring the
    // original's defensive `gst_buffer_is_writable` check.
    if let Some(buffer_ref) = buffer.get_mut() {
        let offset = transonic_meta::get_custom_audio_meta(buffer_ref)
            .map(|meta| meta.input_stream_offset)
            .unwrap_or(-1);
        transonic_meta::add_custom_audio_meta(
            buffer_ref,
            CustomAudioMeta {
                silence: has_only_silence,
                input_stream_offset: offset,
            },
        );
    }
}

mod imp {
    use super::*;

    pub struct Settings {
        pub threshold: f64,
        pub pre_length_ns: u64,
        pub trim: bool,
    }

    impl Default for Settings {
        fn default() -> Self {
            Settings {
                threshold: DEFAULT_THRESHOLD_LEVEL,
                pre_length_ns: DEFAULT_PRE_LENGTH_NS,
                trim: false,
            }
        }
    }

    #[derive(Default)]
    pub struct State {
        pub info: Option<gst_audio::AudioInfo>,
        pub silent: bool,
        pub start_silence_processed: bool,
        pub pre_buffer: PreBuffer,
        pub vad: Option<EnergyVad>,
        pub last_sample_timestamp: Option<gst::ClockTime>,
    }

    pub struct SmartVadStartSilenceTrimmer {
        pub(super) sinkpad: gst::Pad,
        pub(super) srcpad: gst::Pad,
        pub(super) settings: Mutex<Settings>,
        pub(super) state: Mutex<State>,
    }

    #[glib::object_subclass]
    impl ObjectSubclass for SmartVadStartSilenceTrimmer {
        const NAME: &'static str = "TransonicSmartVadStartSilenceTrimmer";
        type Type = super::SmartVadStartSilenceTrimmer;
        type ParentType = gst::Element;

        fn with_class(klass: &Self::Class) -> Self {
            let sinkpad = gst::Pad::builder_from_template(&klass.pad_template("sink").unwrap())
                .chain_function(|pad, parent, buffer| {
                    SmartVadStartSilenceTrimmer::catch_panic_pad_function(
                        parent,
                        || Err(gst::FlowError::Error),
                        |this| this.sink_chain(pad, buffer),
                    )
                })
                .event_function(|pad, parent, event| {
                    SmartVadStartSilenceTrimmer::catch_panic_pad_function(
                        parent,
                        || false,
                        |this| this.sink_event(pad, event),
                    )
                })
                .build();
            let srcpad =
                gst::Pad::builder_from_template(&klass.pad_template("src").unwrap()).build();

            SmartVadStartSilenceTrimmer {
                sinkpad,
                srcpad,
                settings: Mutex::new(Settings::default()),
                state: Mutex::new(State {
                    silent: true,
                    ..State::default()
                }),
            }
        }
    }

    impl ObjectImpl for SmartVadStartSilenceTrimmer {
        fn properties() -> &'static [glib::ParamSpec] {
            static PROPERTIES: LazyLock<Vec<glib::ParamSpec>> = LazyLock::new(|| {
                vec![
                    glib::ParamSpecDouble::builder("threshold")
                        .nick("Threshold")
                        .blurb("VAD threshold before trigger")
                        .default_value(0.0)
                        .build(),
                    glib::ParamSpecUInt64::builder("pre-length")
                        .nick("Pre-recording buffer length")
                        .blurb("Length of pre-recording buffer (in nanoseconds)")
                        .default_value(0)
                        .build(),
                    glib::ParamSpecBoolean::builder("trim")
                        .nick("Trim the initial silence")
                        .blurb("Do we want to trim the initial silence?")
                        .default_value(false)
                        .build(),
                ]
            });
            PROPERTIES.as_ref()
        }

        fn set_property(&self, _id: usize, value: &glib::Value, pspec: &glib::ParamSpec) {
            let mut settings = self.settings.lock().unwrap();
            match pspec.name() {
                "threshold" => settings.threshold = value.get().unwrap(),
                "pre-length" => settings.pre_length_ns = value.get().unwrap(),
                "trim" => settings.trim = value.get().unwrap(),
                _ => unimplemented!(),
            }
        }

        fn property(&self, _id: usize, pspec: &glib::ParamSpec) -> glib::Value {
            let settings = self.settings.lock().unwrap();
            match pspec.name() {
                "threshold" => settings.threshold.to_value(),
                "pre-length" => settings.pre_length_ns.to_value(),
                "trim" => settings.trim.to_value(),
                _ => unimplemented!(),
            }
        }

        fn constructed(&self) {
            self.parent_constructed();
            let obj = self.obj();
            obj.add_pad(&self.sinkpad).unwrap();
            obj.add_pad(&self.srcpad).unwrap();
        }
    }

    impl GstObjectImpl for SmartVadStartSilenceTrimmer {}

    impl ElementImpl for SmartVadStartSilenceTrimmer {
        fn metadata() -> Option<&'static gst::subclass::ElementMetadata> {
            static METADATA: LazyLock<gst::subclass::ElementMetadata> = LazyLock::new(|| {
                gst::subclass::ElementMetadata::new(
                    "Audio start silence trimmer",
                    "Filter/Editor/Audio",
                    "Audio start silence trimmer to get the end location of initial silence bits",
                    "Transonic",
                )
            });
            Some(&METADATA)
        }

        fn pad_templates() -> &'static [gst::PadTemplate] {
            static TEMPLATES: LazyLock<Vec<gst::PadTemplate>> = LazyLock::new(|| {
                let caps = gst::Caps::builder("audio/x-raw")
                    .field("format", "S16LE")
                    .field("rate", gst::List::new([8_000i32, 16_000i32]))
                    .field("channels", 1i32)
                    .field("layout", "interleaved")
                    .build();
                vec![
                    gst::PadTemplate::new(
                        "src",
                        gst::PadDirection::Src,
                        gst::PadPresence::Always,
                        &caps,
                    )
                    .unwrap(),
                    gst::PadTemplate::new(
                        "sink",
                        gst::PadDirection::Sink,
                        gst::PadPresence::Always,
                        &caps,
                    )
                    .unwrap(),
                ]
            });
            TEMPLATES.as_ref()
        }

        fn change_state(
            &self,
            transition: gst::StateChange,
        ) -> Result<gst::StateChangeSuccess, gst::StateChangeError> {
            let result = self.parent_change_state(transition)?;
            if transition == gst::StateChange::PausedToReady {
                self.state.lock().unwrap().pre_buffer.clear();
            }
            Ok(result)
        }
    }

    impl SmartVadStartSilenceTrimmer {
        fn post_detection_message(
            &self,
            timestamp: Option<gst::ClockTime>,
            pre_length_ns: u64,
            only_silence_detected: bool,
        ) {
            let structure = gst::Structure::builder("startsilenceenddetected")
                .field(
                    "timestamp",
                    timestamp.map(|t| t.nseconds()).unwrap_or(u64::MAX),
                )
                .field("presilencebufferlength", pre_length_ns)
                .field("onlysilencedetected", only_silence_detected)
                .build();
            let message = gst::message::Element::builder(structure)
                .src(&*self.obj())
                .build();
            let _ = self.obj().post_message(message);
        }

        fn sink_event(&self, pad: &gst::Pad, event: gst::Event) -> bool {
            match event.view() {
                gst::EventView::Caps(caps_event) => {
                    let caps = caps_event.caps_owned();
                    let Ok(info) = gst_audio::AudioInfo::from_caps(&caps) else {
                        return false;
                    };
                    self.state.lock().unwrap().info = Some(info);
                    self.srcpad.push_event(gst::event::Caps::new(&caps))
                }
                gst::EventView::Eos(_) => {
                    let flushed = {
                        let mut state = self.state.lock().unwrap();
                        if state.silent {
                            self.post_detection_message(
                                state.last_sample_timestamp,
                                state.pre_buffer.run_length_ns(),
                                true,
                            );
                            Some(state.pre_buffer.take_all())
                        } else {
                            None
                        }
                    };
                    if let Some(buffers) = flushed {
                        for mut buffer in buffers {
                            mark_silence(&mut buffer, true);
                            let _ = self.srcpad.push(buffer);
                        }
                    }
                    gst::Pad::event_default(pad, Some(&*self.obj()), event)
                }
                _ => gst::Pad::event_default(pad, Some(&*self.obj()), event),
            }
        }

        fn sink_chain(
            &self,
            _pad: &gst::Pad,
            mut buffer: gst::Buffer,
        ) -> Result<gst::FlowSuccess, gst::FlowError> {
            let (bpf, rate) = {
                let state = self.state.lock().unwrap();
                let Some(info) = state.info.as_ref() else {
                    return Err(gst::FlowError::NotNegotiated);
                };
                (info.bpf() as usize, info.rate() as u64)
            };

            {
                let mut state = self.state.lock().unwrap();
                state.last_sample_timestamp = buffer.pts();
                // If the start silence has already been processed, pass the
                // data through tagged as non-silent.
                if state.start_silence_processed {
                    drop(state);
                    mark_silence(&mut buffer, false);
                    return self.srcpad.push(buffer);
                }
            }

            let threshold = self.settings.lock().unwrap().threshold;
            let (pre_length_ns, trim) = {
                let settings = self.settings.lock().unwrap();
                (settings.pre_length_ns, settings.trim)
            };

            // Run the VAD over the buffer contents.
            let duration_ns;
            let speech_detected;
            {
                let map = buffer.map_readable().map_err(|_| gst::FlowError::Error)?;
                let data = map.as_slice();
                let frames = (data.len() / bpf.max(1)) as u64;
                duration_ns = frames * gst::ClockTime::SECOND.nseconds() / rate.max(1);

                let mut state = self.state.lock().unwrap();
                let vad = state
                    .vad
                    .get_or_insert_with(|| EnergyVad::new(threshold, rate as u32));
                speech_detected = vad.process(data).into_iter().any(|is_speech| is_speech);
            }

            let mut state = self.state.lock().unwrap();
            if speech_detected {
                state.silent = false;
            }

            if !state.silent {
                // Speech detected: signal, then flush the pre-record window
                // marked as non-silent.
                state.start_silence_processed = true;
                self.post_detection_message(buffer.pts(), state.pre_buffer.run_length_ns(), false);
                let flushed = state.pre_buffer.take_all();
                drop(state);
                for mut prebuf in flushed {
                    mark_silence(&mut prebuf, false);
                    let _ = self.srcpad.push(prebuf);
                }
                mark_silence(&mut buffer, false);
                return self.srcpad.push(buffer);
            }

            // Still silent: hold the buffer and evict data beyond the
            // pre-record window, marked as silence.
            state.pre_buffer.push(buffer, duration_ns);
            let evicted = state.pre_buffer.drain_excess(pre_length_ns, bpf, rate);
            drop(state);
            let mut result = Ok(gst::FlowSuccess::Ok);
            for mut prebuf in evicted {
                if trim {
                    drop(prebuf);
                } else {
                    mark_silence(&mut prebuf, true);
                    result = self.srcpad.push(prebuf);
                }
            }
            result
        }
    }
}

glib::wrapper! {
    pub struct SmartVadStartSilenceTrimmer(ObjectSubclass<imp::SmartVadStartSilenceTrimmer>)
        @extends gst::Element, gst::Object;
}

pub fn register(plugin: &gst::Plugin) -> Result<(), glib::BoolError> {
    gst::Element::register(
        Some(plugin),
        "smartvadstartsilencetrimmer",
        gst::Rank::NONE,
        SmartVadStartSilenceTrimmer::static_type(),
    )
}
