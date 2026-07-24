//! `startsilencecutter` element.
//!
//! Analyses the audio signal for the initial silence period.  The end of the
//! initial silence is signalled by a bus element message named
//! `startsilenceenddetected` carrying:
//!
//! * `timestamp` — the timestamp of the end of the initial silence, and
//! * `presilencebufferlength` — the length of the retained pre-record
//!   window.
//!
//! When `trim=true` the leading silence (beyond the `pre-length` window) is
//! dropped instead of being forwarded downstream.
//!
//! Borrowed heavily from gst-plugins-good's `cutter` element, like the
//! original C implementation.

use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer::subclass::prelude::*;
use gstreamer_audio as gst_audio;

use std::sync::{LazyLock, Mutex};

use crate::prebuffer::PreBuffer;

const DEFAULT_THRESHOLD_LEVEL: f64 = 0.1;
const DEFAULT_PRE_LENGTH_NS: u64 = 200 * 1_000_000;

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
    }

    pub struct StartSilenceCutter {
        pub(super) sinkpad: gst::Pad,
        pub(super) srcpad: gst::Pad,
        pub(super) settings: Mutex<Settings>,
        pub(super) state: Mutex<State>,
    }

    #[glib::object_subclass]
    impl ObjectSubclass for StartSilenceCutter {
        const NAME: &'static str = "TransonicStartSilenceCutter";
        type Type = super::StartSilenceCutter;
        type ParentType = gst::Element;

        fn with_class(klass: &Self::Class) -> Self {
            let sinkpad = gst::Pad::builder_from_template(&klass.pad_template("sink").unwrap())
                .chain_function(|pad, parent, buffer| {
                    StartSilenceCutter::catch_panic_pad_function(
                        parent,
                        || Err(gst::FlowError::Error),
                        |this| this.sink_chain(pad, buffer),
                    )
                })
                .event_function(|pad, parent, event| {
                    StartSilenceCutter::catch_panic_pad_function(
                        parent,
                        || false,
                        |this| this.sink_event(pad, event),
                    )
                })
                .build();
            let srcpad =
                gst::Pad::builder_from_template(&klass.pad_template("src").unwrap()).build();

            StartSilenceCutter {
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

    impl ObjectImpl for StartSilenceCutter {
        fn properties() -> &'static [glib::ParamSpec] {
            static PROPERTIES: LazyLock<Vec<glib::ParamSpec>> = LazyLock::new(|| {
                vec![
                    glib::ParamSpecDouble::builder("threshold")
                        .nick("Threshold")
                        .blurb("Volume threshold before trigger")
                        .default_value(0.0)
                        .build(),
                    glib::ParamSpecDouble::builder("threshold-dB")
                        .nick("Threshold (dB)")
                        .blurb("Volume threshold before trigger (in dB)")
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
                // value in dB = 20 * log(value); values < 0 dB map to
                // thresholds between 0 and 1.
                "threshold-dB" => {
                    settings.threshold = 10f64.powf(value.get::<f64>().unwrap() / 20.0)
                }
                "pre-length" => settings.pre_length_ns = value.get().unwrap(),
                "trim" => settings.trim = value.get().unwrap(),
                _ => unimplemented!(),
            }
        }

        fn property(&self, _id: usize, pspec: &glib::ParamSpec) -> glib::Value {
            let settings = self.settings.lock().unwrap();
            match pspec.name() {
                "threshold" => settings.threshold.to_value(),
                "threshold-dB" => (20.0 * settings.threshold.log10()).to_value(),
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

    impl GstObjectImpl for StartSilenceCutter {}

    impl ElementImpl for StartSilenceCutter {
        fn metadata() -> Option<&'static gst::subclass::ElementMetadata> {
            static METADATA: LazyLock<gst::subclass::ElementMetadata> = LazyLock::new(|| {
                gst::subclass::ElementMetadata::new(
                    "Audio start silence cutter",
                    "Filter/Editor/Audio",
                    "Audio start silence cutter to get the end location of initial silence bits",
                    "Transonic",
                )
            });
            Some(&METADATA)
        }

        fn pad_templates() -> &'static [gst::PadTemplate] {
            static TEMPLATES: LazyLock<Vec<gst::PadTemplate>> = LazyLock::new(|| {
                let caps = gst::Caps::builder("audio/x-raw")
                    .field("format", gst::List::new(["S8", "S16LE"]))
                    .field("rate", gst::IntRange::new(1i32, i32::MAX))
                    .field("channels", gst::IntRange::new(1i32, i32::MAX))
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

    impl StartSilenceCutter {
        fn post_detection_message(&self, timestamp: Option<gst::ClockTime>, pre_length_ns: u64) {
            let structure = gst::Structure::builder("startsilenceenddetected")
                .field(
                    "timestamp",
                    timestamp.map(|t| t.nseconds()).unwrap_or(u64::MAX),
                )
                .field("presilencebufferlength", pre_length_ns)
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
                            self.post_detection_message(None, state.pre_buffer.run_length_ns());
                            Some(state.pre_buffer.take_all())
                        } else {
                            None
                        }
                    };
                    if let Some(buffers) = flushed {
                        // NOTE: the EOS flush always forwards the retained
                        // pre-record window, even when trimming (carried over
                        // from the original implementation).
                        for buffer in buffers {
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
            buffer: gst::Buffer,
        ) -> Result<gst::FlowSuccess, gst::FlowError> {
            let (bpf, rate, format) = {
                let state = self.state.lock().unwrap();
                let Some(info) = state.info.as_ref() else {
                    return Err(gst::FlowError::NotNegotiated);
                };
                (info.bpf() as usize, info.rate() as u64, info.format())
            };

            // If we have already finished processing the start silence, pass
            // the data through.
            if self.state.lock().unwrap().start_silence_processed {
                return self.srcpad.push(buffer);
            }

            let (rms, duration_ns) = {
                let map = buffer.map_readable().map_err(|_| gst::FlowError::Error)?;
                let data = map.as_slice();
                let normalized_mean_square = match format {
                    gst_audio::AudioFormat::S16le => {
                        let sample_count = data.len() / 2;
                        let mut square_sum = 0.0f64;
                        for chunk in data.chunks_exact(2) {
                            let sample = i16::from_le_bytes([chunk[0], chunk[1]]) as f64;
                            square_sum += sample * sample;
                        }
                        square_sum / f64::from(1u32 << 30) / sample_count.max(1) as f64
                    }
                    gst_audio::AudioFormat::S8 => {
                        let sample_count = data.len();
                        let mut square_sum = 0.0f64;
                        for byte in data {
                            let sample = (*byte as i8) as f64;
                            square_sum += sample * sample;
                        }
                        square_sum / f64::from(1u32 << 14) / sample_count.max(1) as f64
                    }
                    _ => 0.0,
                };
                let frames = (data.len() / bpf.max(1)) as u64;
                let duration_ns = frames * gst::ClockTime::SECOND.nseconds() / rate.max(1);
                (normalized_mean_square.sqrt(), duration_ns)
            };

            let threshold = self.settings.lock().unwrap().threshold;
            let (pre_length_ns, trim) = {
                let settings = self.settings.lock().unwrap();
                (settings.pre_length_ns, settings.trim)
            };

            let mut state = self.state.lock().unwrap();
            state.silent = rms < threshold;

            if !state.silent {
                // Speech detected: signal, then flush the pre-record window.
                state.start_silence_processed = true;
                self.post_detection_message(buffer.pts(), state.pre_buffer.run_length_ns());
                let flushed = state.pre_buffer.take_all();
                drop(state);
                for prebuf in flushed {
                    let _ = self.srcpad.push(prebuf);
                }
                return self.srcpad.push(buffer);
            }

            // Still silent: hold the buffer and evict data beyond the
            // pre-record window.
            state.pre_buffer.push(buffer, duration_ns);
            let evicted = state.pre_buffer.drain_excess(pre_length_ns, bpf, rate);
            drop(state);
            let mut result = Ok(gst::FlowSuccess::Ok);
            for prebuf in evicted {
                if trim {
                    drop(prebuf);
                } else {
                    result = self.srcpad.push(prebuf);
                }
            }
            result
        }
    }
}

glib::wrapper! {
    pub struct StartSilenceCutter(ObjectSubclass<imp::StartSilenceCutter>)
        @extends gst::Element, gst::Object;
}

pub fn register(plugin: &gst::Plugin) -> Result<(), glib::BoolError> {
    gst::Element::register(
        Some(plugin),
        "startsilencecutter",
        gst::Rank::NONE,
        StartSilenceCutter::static_type(),
    )
}
