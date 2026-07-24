//! `backgroundaudiomixer` element.
//!
//! Mixes a looping background audio track into a primary audio stream.  The
//! element exposes three always pads:
//!
//! * `sink-audio` — the primary audio stream (S16LE mono),
//! * `sink-backgroundaudio` — the background audio track (S16LE mono),
//! * `src-mixed` — the mixed output.
//!
//! The whole background track is buffered first (the audio chain blocks
//! until the background pad reaches EOS), and is then mixed underneath the
//! primary audio, looping when it is shorter than the primary stream.
//! `volume` scales the background track, `fadein-duration` ramps it in at
//! the start and `fadeout-duration` appends a faded-out tail after the
//! primary stream ends.

use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer::subclass::prelude::*;

use std::sync::{Condvar, LazyLock, Mutex};

const VOLUME_DEFAULT: f64 = 1.0;
const VOLUME_MIN: f64 = 0.0;
const VOLUME_MAX: f64 = 100.0;

const FADE_DURATION_DEFAULT: i32 = 0;
const FADE_DURATION_MIN: i32 = 0;
const FADE_DURATION_MAX: i32 = 10_000;

// The original element assumes a 16 kHz sample rate for duration-to-sample
// conversions even though 24 kHz caps are accepted (carried over).
const SAMPLES_PER_MILLISECOND: i64 = 16;

/// Fade-in/out step duration, based on a 12.5 ms frame.
const FADE_STEP_MS: f64 = 12.5;

fn clamp_sample(sample: i64) -> i16 {
    sample.clamp(i64::from(i16::MIN), i64::from(i16::MAX)) as i16
}

fn duration_to_sample_count(duration_ms: i32) -> i64 {
    i64::from(duration_ms) * SAMPLES_PER_MILLISECOND
}

mod imp {
    use super::*;

    #[derive(Clone, Copy)]
    pub struct Settings {
        volume: f64,
        fadein_ms: i32,
        fadeout_ms: i32,
    }

    impl Default for Settings {
        fn default() -> Self {
            Settings {
                volume: VOLUME_DEFAULT,
                fadein_ms: FADE_DURATION_DEFAULT,
                fadeout_ms: FADE_DURATION_DEFAULT,
            }
        }
    }

    #[derive(Default)]
    pub struct MixState {
        /// The fully buffered background track.
        full_background: Vec<i16>,
        /// The loop position inside the background track.
        background_offset: usize,
        /// Scratch buffer holding the background samples for the current mix.
        mix_buffer: Vec<i16>,
        /// Total number of primary audio samples processed so far.
        processed_samples: i64,
    }

    #[derive(Default)]
    struct BackgroundReceiveState {
        complete: bool,
    }

    pub struct BackgroundAudioMixer {
        pub(super) audio_sinkpad: gst::Pad,
        pub(super) background_sinkpad: gst::Pad,
        pub(super) src_mixed_pad: gst::Pad,
        settings: Mutex<Settings>,
        state: Mutex<MixState>,
        background_receive: Mutex<BackgroundReceiveState>,
        background_receive_cond: Condvar,
    }

    #[glib::object_subclass]
    impl ObjectSubclass for BackgroundAudioMixer {
        const NAME: &'static str = "TransonicBackgroundAudioMixer";
        type Type = super::BackgroundAudioMixer;
        type ParentType = gst::Element;

        fn with_class(klass: &Self::Class) -> Self {
            let audio_sinkpad =
                gst::Pad::builder_from_template(&klass.pad_template("sink-audio").unwrap())
                    .chain_function(|pad, parent, buffer| {
                        BackgroundAudioMixer::catch_panic_pad_function(
                            parent,
                            || Err(gst::FlowError::Error),
                            |this| this.audio_chain(pad, buffer),
                        )
                    })
                    .event_function(|pad, parent, event| {
                        BackgroundAudioMixer::catch_panic_pad_function(
                            parent,
                            || false,
                            |this| this.audio_event(pad, event),
                        )
                    })
                    .build();

            let background_sinkpad = gst::Pad::builder_from_template(
                &klass.pad_template("sink-backgroundaudio").unwrap(),
            )
            .chain_function(|pad, parent, buffer| {
                BackgroundAudioMixer::catch_panic_pad_function(
                    parent,
                    || Err(gst::FlowError::Error),
                    |this| this.background_chain(pad, buffer),
                )
            })
            .event_function(|pad, parent, event| {
                BackgroundAudioMixer::catch_panic_pad_function(
                    parent,
                    || false,
                    |this| this.background_event(pad, event),
                )
            })
            .build();

            let src_mixed_pad =
                gst::Pad::builder_from_template(&klass.pad_template("src-mixed").unwrap()).build();

            BackgroundAudioMixer {
                audio_sinkpad,
                background_sinkpad,
                src_mixed_pad,
                settings: Mutex::new(Settings::default()),
                state: Mutex::new(MixState::default()),
                background_receive: Mutex::new(BackgroundReceiveState::default()),
                background_receive_cond: Condvar::new(),
            }
        }
    }

    impl ObjectImpl for BackgroundAudioMixer {
        fn properties() -> &'static [glib::ParamSpec] {
            static PROPERTIES: LazyLock<Vec<glib::ParamSpec>> = LazyLock::new(|| {
                vec![
                    glib::ParamSpecDouble::builder("volume")
                        .nick("The background audio volume.")
                        .blurb("Specify the background audio volume.")
                        .minimum(VOLUME_MIN)
                        .maximum(VOLUME_MAX)
                        .default_value(VOLUME_DEFAULT)
                        .mutable_playing()
                        .build(),
                    glib::ParamSpecInt::builder("fadein-duration")
                        .nick("The background audio start fadein duration in milliseconds.")
                        .blurb(
                            "Specify the background audio start fadein duration in \
                             milliseconds, only support 0(default) or positive.",
                        )
                        .minimum(FADE_DURATION_MIN)
                        .maximum(FADE_DURATION_MAX)
                        .default_value(FADE_DURATION_DEFAULT)
                        .mutable_playing()
                        .build(),
                    glib::ParamSpecInt::builder("fadeout-duration")
                        .nick("The background audio stop fadeout duration in milliseconds.")
                        .blurb(
                            "Specify the background audio stop fadeout duration in \
                             milliseconds, only support 0(default) or positive.",
                        )
                        .minimum(FADE_DURATION_MIN)
                        .maximum(FADE_DURATION_MAX)
                        .default_value(FADE_DURATION_DEFAULT)
                        .mutable_playing()
                        .build(),
                ]
            });
            PROPERTIES.as_ref()
        }

        fn set_property(&self, _id: usize, value: &glib::Value, pspec: &glib::ParamSpec) {
            let mut settings = self.settings.lock().unwrap();
            match pspec.name() {
                "volume" => settings.volume = value.get().unwrap(),
                "fadein-duration" => settings.fadein_ms = value.get().unwrap(),
                "fadeout-duration" => settings.fadeout_ms = value.get().unwrap(),
                _ => unimplemented!(),
            }
        }

        fn property(&self, _id: usize, pspec: &glib::ParamSpec) -> glib::Value {
            let settings = self.settings.lock().unwrap();
            match pspec.name() {
                "volume" => settings.volume.to_value(),
                "fadein-duration" => settings.fadein_ms.to_value(),
                "fadeout-duration" => settings.fadeout_ms.to_value(),
                _ => unimplemented!(),
            }
        }

        fn constructed(&self) {
            self.parent_constructed();
            let obj = self.obj();
            obj.add_pad(&self.src_mixed_pad).unwrap();
            obj.add_pad(&self.audio_sinkpad).unwrap();
            obj.add_pad(&self.background_sinkpad).unwrap();
        }
    }

    impl GstObjectImpl for BackgroundAudioMixer {}

    impl ElementImpl for BackgroundAudioMixer {
        fn metadata() -> Option<&'static gst::subclass::ElementMetadata> {
            static METADATA: LazyLock<gst::subclass::ElementMetadata> = LazyLock::new(|| {
                gst::subclass::ElementMetadata::new(
                    "Background Audio Mixer",
                    "Generic",
                    "Mix background audio.",
                    "Transonic",
                )
            });
            Some(&METADATA)
        }

        fn pad_templates() -> &'static [gst::PadTemplate] {
            static TEMPLATES: LazyLock<Vec<gst::PadTemplate>> = LazyLock::new(|| {
                let caps = gst::Caps::builder("audio/x-raw")
                    .field("format", "S16LE")
                    .field("rate", gst::List::new([16_000i32, 24_000i32]))
                    .field("channels", 1i32)
                    .field("layout", "interleaved")
                    .build();
                vec![
                    gst::PadTemplate::new(
                        "src-mixed",
                        gst::PadDirection::Src,
                        gst::PadPresence::Always,
                        &caps,
                    )
                    .unwrap(),
                    gst::PadTemplate::new(
                        "sink-audio",
                        gst::PadDirection::Sink,
                        gst::PadPresence::Always,
                        &caps,
                    )
                    .unwrap(),
                    gst::PadTemplate::new(
                        "sink-backgroundaudio",
                        gst::PadDirection::Sink,
                        gst::PadPresence::Always,
                        &caps,
                    )
                    .unwrap(),
                ]
            });
            TEMPLATES.as_ref()
        }
    }

    impl BackgroundAudioMixer {
        fn audio_chain(
            &self,
            _pad: &gst::Pad,
            mut buffer: gst::Buffer,
        ) -> Result<gst::FlowSuccess, gst::FlowError> {
            // Wait until the full background track has been received.
            {
                let mut receive = self.background_receive.lock().unwrap();
                while !receive.complete {
                    receive = self.background_receive_cond.wait(receive).unwrap();
                }
            }

            let settings = *self.settings.lock().unwrap();
            {
                let buffer_ref = buffer.make_mut();
                let mut map = buffer_ref
                    .map_writable()
                    .map_err(|_| gst::FlowError::Error)?;
                let bytes = map.as_mut_slice();
                let sample_count = bytes.len() / 2;

                let mut state = self.state.lock().unwrap();
                state.mix_buffer.clear();
                Self::fill_background_segments(&mut state, sample_count as i64);
                Self::apply_volume(&mut state.mix_buffer, settings.volume);
                let processed_samples = state.processed_samples;
                Self::apply_fadein(&mut state.mix_buffer, processed_samples, settings.fadein_ms);

                let mix_len = state.mix_buffer.len().min(sample_count);
                for i in 0..mix_len {
                    let offset = i * 2;
                    let sample = i16::from_le_bytes([bytes[offset], bytes[offset + 1]]) as i64;
                    let mixed = clamp_sample(sample + i64::from(state.mix_buffer[i]));
                    bytes[offset..offset + 2].copy_from_slice(&mixed.to_le_bytes());
                }
                state.processed_samples += sample_count as i64;
            }

            self.src_mixed_pad.push(buffer)
        }

        fn audio_event(&self, pad: &gst::Pad, event: gst::Event) -> bool {
            if event.type_() == gst::EventType::Eos {
                let _ = self.push_fadeout();
            }
            gst::Pad::event_default(pad, Some(&*self.obj()), event)
        }

        fn background_chain(
            &self,
            _pad: &gst::Pad,
            buffer: gst::Buffer,
        ) -> Result<gst::FlowSuccess, gst::FlowError> {
            let map = buffer.map_readable().map_err(|_| gst::FlowError::Error)?;
            let mut state = self.state.lock().unwrap();
            for chunk in map.as_slice().chunks_exact(2) {
                state
                    .full_background
                    .push(i16::from_le_bytes([chunk[0], chunk[1]]));
            }
            Ok(gst::FlowSuccess::Ok)
        }

        fn background_event(&self, pad: &gst::Pad, event: gst::Event) -> bool {
            match event.type_() {
                // Eat stream management events on the background pad so they
                // don't race the primary stream's events downstream.
                gst::EventType::StreamStart | gst::EventType::Segment => true,
                gst::EventType::Eos => {
                    {
                        let mut receive = self.background_receive.lock().unwrap();
                        receive.complete = true;
                    }
                    self.background_receive_cond.notify_all();
                    true
                }
                _ => gst::Pad::event_default(pad, Some(&*self.obj()), event),
            }
        }

        /// Emits a faded-out background tail after the primary stream ends.
        fn push_fadeout(&self) -> Result<gst::FlowSuccess, gst::FlowError> {
            let settings = *self.settings.lock().unwrap();
            let buffer = {
                let mut state = self.state.lock().unwrap();
                if settings.fadeout_ms <= 0 || state.full_background.is_empty() {
                    return Ok(gst::FlowSuccess::Ok);
                }
                state.mix_buffer.clear();
                Self::fill_background_segments(
                    &mut state,
                    duration_to_sample_count(settings.fadeout_ms),
                );
                if state.mix_buffer.is_empty() {
                    return Ok(gst::FlowSuccess::Ok);
                }
                Self::apply_fadeout(&mut state.mix_buffer, settings.volume, settings.fadeout_ms);

                let mut data = Vec::with_capacity(state.mix_buffer.len() * 2);
                for sample in &state.mix_buffer {
                    data.extend_from_slice(&sample.to_le_bytes());
                }
                gst::Buffer::from_mut_slice(data)
            };
            self.src_mixed_pad.push(buffer)
        }

        /// Fills the mix buffer with `requested` samples from the looping
        /// background track.
        fn fill_background_segments(state: &mut MixState, requested: i64) {
            if state.full_background.is_empty() {
                return;
            }
            let mut fetched: i64 = 0;
            while fetched < requested {
                let available = state.full_background.len() - state.background_offset;
                let take = (requested - fetched).min(available as i64) as usize;
                if take == 0 {
                    break;
                }
                let start = state.background_offset;
                let segment = state.full_background[start..start + take].to_vec();
                state.mix_buffer.extend_from_slice(&segment);
                state.background_offset += take;
                if state.background_offset >= state.full_background.len() {
                    state.background_offset = 0;
                }
                fetched += take as i64;
            }
        }

        fn apply_volume(mix_buffer: &mut [i16], volume: f64) {
            for sample in mix_buffer.iter_mut() {
                *sample = clamp_sample((f64::from(*sample) * volume) as i64);
            }
        }

        /// Ramps the mix buffer in over the configured fade-in window; the
        /// volume has already been applied so the ramp scales toward 1.0.
        fn apply_fadein(mix_buffer: &mut [i16], processed_samples: i64, fadein_ms: i32) {
            let total_fadein_samples = duration_to_sample_count(fadein_ms);
            if processed_samples >= total_fadein_samples {
                return;
            }

            let left = total_fadein_samples - processed_samples;
            let to_process = left.min(mix_buffer.len() as i64) as usize;

            let total_steps = (f64::from(fadein_ms) / FADE_STEP_MS) as i64 + 1;
            let factor_per_step = 1.0 / (total_steps + 1) as f64;
            let samples_per_step = total_fadein_samples / total_steps + 1;

            for (i, sample) in mix_buffer.iter_mut().take(to_process).enumerate() {
                let sample_offset = processed_samples + i as i64;
                let current_step = sample_offset / samples_per_step;
                let factor = (current_step + 1) as f64 * factor_per_step;
                *sample = clamp_sample((f64::from(*sample) * factor) as i64);
            }
        }

        /// Fades the mix buffer out (from `volume` down to zero) toward its
        /// end; the fade-out incorporates the volume scaling itself.
        fn apply_fadeout(mix_buffer: &mut [i16], volume: f64, fadeout_ms: i32) {
            if fadeout_ms <= 0 {
                return;
            }
            let step_count = (f64::from(fadeout_ms) / FADE_STEP_MS) as i64 + 1;
            let factor_per_step = volume / (step_count + 1) as f64;
            let samples_per_step = mix_buffer.len() as i64 / step_count + 1;

            let mut factor = factor_per_step;
            let mut step_samples = 0i64;
            for sample in mix_buffer.iter_mut().rev() {
                *sample = clamp_sample((f64::from(*sample) * factor) as i64);
                step_samples += 1;
                if step_samples > samples_per_step {
                    step_samples = 0;
                    factor += factor_per_step;
                }
            }
        }
    }
}

glib::wrapper! {
    pub struct BackgroundAudioMixer(ObjectSubclass<imp::BackgroundAudioMixer>)
        @extends gst::Element, gst::Object;
}

pub fn register(plugin: &gst::Plugin) -> Result<(), glib::BoolError> {
    gst::Element::register(
        Some(plugin),
        "backgroundaudiomixer",
        gst::Rank::NONE,
        BackgroundAudioMixer::static_type(),
    )
}
