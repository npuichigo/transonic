//! `demuxtomultichannel` element.
//!
//! Splits an interleaved multichannel audio stream into many mono streams,
//! one per sometimes pad `src_%u`.  The `channels` property sets the minimum
//! number of output pads; when the input carries fewer channels, the extra
//! pads receive a single empty buffer on the first input buffer so that
//! downstream queues can preroll.

use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer::subclass::prelude::*;
use gstreamer_audio as gst_audio;

use std::sync::{LazyLock, Mutex};

mod imp {
    use super::*;

    #[derive(Default)]
    pub struct State {
        pub info: Option<gst_audio::AudioInfo>,
        pub sink_caps: Option<gst::Caps>,
        pub srcpads: Vec<gst::Pad>,
        pub pending_events: Vec<gst::Event>,
        pub buffer_sent: bool,
        pub n_channels: i32,
    }

    pub struct DemuxToMultichannel {
        pub(super) sinkpad: gst::Pad,
        pub(super) state: Mutex<State>,
    }

    #[glib::object_subclass]
    impl ObjectSubclass for DemuxToMultichannel {
        const NAME: &'static str = "TransonicDemuxToMultichannel";
        type Type = super::DemuxToMultichannel;
        type ParentType = gst::Element;

        fn with_class(klass: &Self::Class) -> Self {
            let sinkpad = gst::Pad::builder_from_template(&klass.pad_template("sink").unwrap())
                .chain_function(|pad, parent, buffer| {
                    DemuxToMultichannel::catch_panic_pad_function(
                        parent,
                        || Err(gst::FlowError::Error),
                        |this| this.sink_chain(pad, buffer),
                    )
                })
                .event_function(|pad, parent, event| {
                    DemuxToMultichannel::catch_panic_pad_function(
                        parent,
                        || false,
                        |this| this.sink_event(pad, event),
                    )
                })
                .build();

            DemuxToMultichannel {
                sinkpad,
                state: Mutex::new(State::default()),
            }
        }
    }

    impl ObjectImpl for DemuxToMultichannel {
        fn properties() -> &'static [glib::ParamSpec] {
            static PROPERTIES: LazyLock<Vec<glib::ParamSpec>> = LazyLock::new(|| {
                vec![glib::ParamSpecInt::builder("channels")
                    .nick("(Minimum) Number of input channels")
                    .blurb(
                        "Set the desired number of output channels. The actual value \
                         will be at least the number of channels in the audio.",
                    )
                    .minimum(0)
                    .maximum(i32::MAX)
                    .default_value(0)
                    .build()]
            });
            PROPERTIES.as_ref()
        }

        fn set_property(&self, _id: usize, value: &glib::Value, pspec: &glib::ParamSpec) {
            match pspec.name() {
                "channels" => self.state.lock().unwrap().n_channels = value.get().unwrap(),
                _ => unimplemented!(),
            }
        }

        fn property(&self, _id: usize, pspec: &glib::ParamSpec) -> glib::Value {
            match pspec.name() {
                "channels" => self.state.lock().unwrap().n_channels.to_value(),
                _ => unimplemented!(),
            }
        }

        fn constructed(&self) {
            self.parent_constructed();
            self.obj().add_pad(&self.sinkpad).unwrap();
        }
    }

    impl GstObjectImpl for DemuxToMultichannel {}

    impl ElementImpl for DemuxToMultichannel {
        fn metadata() -> Option<&'static gst::subclass::ElementMetadata> {
            static METADATA: LazyLock<gst::subclass::ElementMetadata> = LazyLock::new(|| {
                gst::subclass::ElementMetadata::new(
                    "demuxtomultichannel",
                    "Filter/Converter/Audio",
                    "Splits a multichannel audio stream into many mono audio streams",
                    "Transonic",
                )
            });
            Some(&METADATA)
        }

        fn pad_templates() -> &'static [gst::PadTemplate] {
            static TEMPLATES: LazyLock<Vec<gst::PadTemplate>> = LazyLock::new(|| {
                let sink_caps = gst::Caps::builder("audio/x-raw")
                    .field("layout", "interleaved")
                    .build();
                let src_caps = gst::Caps::builder("audio/x-raw")
                    .field("channels", 1i32)
                    .field("layout", gst::List::new(["non-interleaved", "interleaved"]))
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
                        "src_%u",
                        gst::PadDirection::Src,
                        gst::PadPresence::Sometimes,
                        &src_caps,
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
            if matches!(
                transition,
                gst::StateChange::ReadyToPaused | gst::StateChange::PausedToReady
            ) {
                self.remove_pads();
            }
            self.parent_change_state(transition)
        }
    }

    impl DemuxToMultichannel {
        fn remove_pads(&self) {
            let (pads, events) = {
                let mut state = self.state.lock().unwrap();
                state.sink_caps = None;
                state.info = None;
                state.buffer_sent = false;
                (
                    std::mem::take(&mut state.srcpads),
                    std::mem::take(&mut state.pending_events),
                )
            };
            drop(events);
            for pad in pads {
                let _ = self.obj().remove_pad(&pad);
            }
        }

        fn push_pending_events(&self) {
            let (events, pads) = {
                let mut state = self.state.lock().unwrap();
                (
                    std::mem::take(&mut state.pending_events),
                    state.srcpads.clone(),
                )
            };
            for event in events {
                for pad in &pads {
                    if pad.is_active() {
                        let _ = pad.push_event(event.clone());
                    }
                }
            }
        }

        fn src_query(&self, pad: &gst::Pad, query: &mut gst::QueryRef) -> bool {
            let result = gst::Pad::query_default(pad, Some(&*self.obj()), query);
            if !result {
                return result;
            }
            let channels = {
                let state = self.state.lock().unwrap();
                state
                    .info
                    .as_ref()
                    .map(|info| info.channels() as i64)
                    .unwrap_or(1)
                    .max(1)
            };
            match query.view_mut() {
                gst::QueryViewMut::Duration(duration) => {
                    if let (gst::GenericFormattedValue::Bytes(Some(bytes)), true) =
                        (duration.result(), channels > 1)
                    {
                        duration.set(gst::format::Bytes::from_u64(*bytes / channels as u64));
                    }
                }
                gst::QueryViewMut::Position(position) => {
                    if let (gst::GenericFormattedValue::Bytes(Some(bytes)), true) =
                        (position.result(), channels > 1)
                    {
                        position.set(gst::format::Bytes::from_u64(*bytes / channels as u64));
                    }
                }
                _ => {}
            }
            result
        }

        fn check_caps_change(
            old_info: &gst_audio::AudioInfo,
            new_info: &gst_audio::AudioInfo,
        ) -> bool {
            // Caps changes are allowed as long as the channel count and the
            // channel positions stay the same.
            let new_channels = new_info.channels();
            let old_channels = old_info.channels();
            if new_channels != old_channels {
                return false;
            }
            let is_unpositioned = |info: &gst_audio::AudioInfo| {
                info.flags().contains(gst_audio::AudioFlags::UNPOSITIONED) || info.channels() == 1
            };
            if is_unpositioned(new_info) != is_unpositioned(old_info) {
                return false;
            }
            if !is_unpositioned(new_info) {
                match (old_info.positions(), new_info.positions()) {
                    (Some(old), Some(new)) => {
                        if old != new {
                            return false;
                        }
                    }
                    (None, None) => {}
                    _ => return false,
                }
            }
            true
        }

        fn add_new_pads(&self, new_info: &gst_audio::AudioInfo) {
            let n_channels = self.state.lock().unwrap().n_channels;
            let mut sticky_events: Vec<gst::Event> = Vec::new();
            self.sinkpad.sticky_events_foreach(|event| {
                sticky_events.push(event.clone());
                std::ops::ControlFlow::Continue(gst::EventForeachAction::Keep)
            });
            let audio_channels = new_info.channels() as i32;
            let actual_channels = n_channels.max(audio_channels);

            let mono_info = gst_audio::AudioInfo::builder(new_info.format(), new_info.rate(), 1)
                .build()
                .expect("mono audio info");
            let mono_caps = mono_info.to_caps().expect("mono caps");

            let templ = self.obj().pad_template("src_%u").unwrap();
            let mut pads = Vec::with_capacity(actual_channels as usize);
            for i in 0..actual_channels {
                let name = format!("src_{i}");
                let pad = gst::Pad::builder_from_template(&templ)
                    .name(name.as_str())
                    .query_function(|pad, parent, query| {
                        DemuxToMultichannel::catch_panic_pad_function(
                            parent,
                            || false,
                            |this| this.src_query(pad, query),
                        )
                    })
                    .build();
                pad.set_active(true).ok();

                // Copy previously received sticky events to the pad, with the
                // caps event replaced by the mono caps.
                let mut caps_sent = false;
                for event in &sticky_events {
                    if event.type_() >= gst::EventType::Caps && !caps_sent {
                        let _ = pad.push_event(gst::event::Caps::new(&mono_caps));
                        caps_sent = true;
                    }
                    if event.type_() != gst::EventType::Caps {
                        let _ = pad.push_event(event.clone());
                    }
                }
                if !caps_sent {
                    let _ = pad.push_event(gst::event::Caps::new(&mono_caps));
                }

                self.obj().add_pad(&pad).unwrap();
                pads.push(pad);
            }

            self.state.lock().unwrap().srcpads = pads;
            self.obj().no_more_pads();
        }

        fn sink_setcaps(&self, caps: &gst::Caps) -> bool {
            let Ok(new_info) = gst_audio::AudioInfo::from_caps(caps) else {
                gst::error!(CAT, imp = self, "invalid caps");
                return false;
            };

            let needs_pads = {
                let state = self.state.lock().unwrap();
                if let (Some(sink_caps), Some(old_info)) = (&state.sink_caps, &state.info) {
                    if sink_caps != caps && !Self::check_caps_change(old_info, &new_info) {
                        gst::warning!(
                            CAT,
                            imp = self,
                            "caps change not supported: channel number or channel \
                             positions change"
                        );
                        return false;
                    }
                }
                state.srcpads.is_empty()
            };

            if needs_pads {
                self.add_new_pads(&new_info);
            } else {
                // Update existing pads to the (mono) equivalent of the new
                // caps.
                let mono_info =
                    gst_audio::AudioInfo::builder(new_info.format(), new_info.rate(), 1)
                        .build()
                        .expect("mono audio info");
                let mono_caps = mono_info.to_caps().expect("mono caps");
                let pads = self.state.lock().unwrap().srcpads.clone();
                for pad in pads {
                    let _ = pad.push_event(gst::event::Caps::new(&mono_caps));
                }
            }

            let mut state = self.state.lock().unwrap();
            state.sink_caps = Some(caps.clone());
            state.info = Some(new_info);
            true
        }

        fn sink_event(&self, pad: &gst::Pad, event: gst::Event) -> bool {
            match event.view() {
                gst::EventView::Caps(caps_event) => {
                    let caps = caps_event.caps_owned();
                    let result = self.sink_setcaps(&caps);
                    if result {
                        self.push_pending_events();
                    }
                    result
                }
                // Send EOS and flush events downstream immediately.
                gst::EventView::Eos(_)
                | gst::EventView::FlushStart(_)
                | gst::EventView::FlushStop(_) => {
                    gst::Pad::event_default(pad, Some(&*self.obj()), event)
                }
                _ => {
                    let has_pads = !self.state.lock().unwrap().srcpads.is_empty();
                    if has_pads || event.is_sticky() {
                        gst::Pad::event_default(pad, Some(&*self.obj()), event)
                    } else {
                        // No source pads yet, so events can't be propagated;
                        // cache the event until pads exist.
                        self.state.lock().unwrap().pending_events.push(event);
                        true
                    }
                }
            }
        }

        fn sink_chain(
            &self,
            _pad: &gst::Pad,
            buffer: gst::Buffer,
        ) -> Result<gst::FlowSuccess, gst::FlowError> {
            let (info, pads, buffer_sent) = {
                let state = self.state.lock().unwrap();
                let Some(info) = state.info.clone() else {
                    return Err(gst::FlowError::NotNegotiated);
                };
                (info, state.srcpads.clone(), state.buffer_sent)
            };

            let n_audio_channels = info.channels() as usize;
            let sample_width = (info.width() / 8) as usize;
            if n_audio_channels == 0 || sample_width == 0 {
                return Err(gst::FlowError::NotNegotiated);
            }

            self.push_pending_events();

            let map = buffer.map_readable().map_err(|_| gst::FlowError::Error)?;
            let input = map.as_slice();
            let n_frames = input.len() / n_audio_channels / sample_width;
            let out_size = n_frames * sample_width;

            let mut pads_pushed = 0usize;
            let mut result = Ok(gst::FlowSuccess::Ok);

            for (i, pad) in pads.iter().enumerate() {
                if i < n_audio_channels {
                    // Deinterleave channel `i` into its own buffer.
                    let mut out =
                        gst::Buffer::with_size(out_size).map_err(|_| gst::FlowError::Error)?;
                    {
                        let out_ref = out.get_mut().ok_or(gst::FlowError::Error)?;
                        buffer
                            .copy_into(out_ref, gst::BufferCopyFlags::META, 0..)
                            .map_err(|_| gst::FlowError::Error)?;
                        let mut out_map =
                            out_ref.map_writable().map_err(|_| gst::FlowError::Error)?;
                        let out_data = out_map.as_mut_slice();
                        for frame in 0..n_frames {
                            let src = (frame * n_audio_channels + i) * sample_width;
                            let dst = frame * sample_width;
                            out_data[dst..dst + sample_width]
                                .copy_from_slice(&input[src..src + sample_width]);
                        }
                    }
                    match pad.push(out) {
                        Ok(_) => pads_pushed += 1,
                        Err(gst::FlowError::NotLinked) => {}
                        Err(error) => {
                            result = Err(error);
                            break;
                        }
                    }
                } else if !buffer_sent {
                    // On the first incoming buffer, send an empty buffer to
                    // each unused src pad to preroll the connected queues.
                    let out = gst::Buffer::new();
                    match pad.push(out) {
                        Ok(_) => pads_pushed += 1,
                        Err(gst::FlowError::NotLinked) => {}
                        Err(error) => {
                            result = Err(error);
                            break;
                        }
                    }
                }
            }
            drop(map);

            result?;
            if pads_pushed == 0 {
                return Err(gst::FlowError::NotLinked);
            }
            self.state.lock().unwrap().buffer_sent = true;
            Ok(gst::FlowSuccess::Ok)
        }
    }

    pub static CAT: LazyLock<gst::DebugCategory> = LazyLock::new(|| {
        gst::DebugCategory::new(
            "demuxtomultichannel",
            gst::DebugColorFlags::empty(),
            Some("demuxtomultichannel"),
        )
    });
}

glib::wrapper! {
    pub struct DemuxToMultichannel(ObjectSubclass<imp::DemuxToMultichannel>)
        @extends gst::Element, gst::Object;
}

pub fn register(plugin: &gst::Plugin) -> Result<(), glib::BoolError> {
    gst::Element::register(
        Some(plugin),
        "demuxtomultichannel",
        gst::Rank::NONE,
        DemuxToMultichannel::static_type(),
    )
}
